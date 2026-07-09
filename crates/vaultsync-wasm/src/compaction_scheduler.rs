use crate::runtime::Runtime;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use vaultsync_core::storage::compaction::{CompactionDecision, CompactionEngine, CompactionReason};
use vaultsync_core::storage::manager::StorageManager;
use wasm_bindgen::prelude::JsValue;

/// Phase 6: CompactionScheduler — Runtime-owned compaction with auto-trigger conditions.
///
/// Trigger conditions:
///   1. Idle — no pending uploads and no recent mutations
///   2. Tombstone ratio > threshold (delegates to CompactionEngine.should_compact())
///   3. Page count > threshold
///
/// Replaces manual compactNamespace() JS calls and background tokio tasks.
pub struct CompactionScheduler {
    runtime: Arc<Runtime>,
    storage_manager: Arc<dyn StorageManager>,
    namespace: String,
    compaction_engine: CompactionEngine,
    max_page_count: u64,
    idle_time_ms: u64,
    last_compaction_ms: AtomicU64,
}

impl CompactionScheduler {
    pub fn new(
        runtime: Arc<Runtime>,
        storage_manager: Arc<dyn StorageManager>,
        namespace: &str,
    ) -> Self {
        let engine_ref = storage_manager.compaction_engine();
        let policy = engine_ref.policy().clone();
        Self {
            runtime,
            storage_manager,
            namespace: namespace.to_string(),
            compaction_engine: vaultsync_core::storage::compaction::CompactionEngine::new(policy),
            max_page_count: 5000,
            idle_time_ms: 2000,
            last_compaction_ms: AtomicU64::new(0),
        }
    }

    pub fn set_max_page_count(&mut self, count: u64) {
        self.max_page_count = count;
    }

    pub fn set_idle_time_ms(&mut self, ms: u64) {
        self.idle_time_ms = ms;
    }

    /// Check if compaction should run based on trigger conditions.
    pub fn should_compact(&self) -> CompactionDecision {
        // Condition 1: Must be idle (no pending uploads)
        if self.runtime.upload_scheduler.pending_count() > 0 {
            return CompactionDecision {
                should_run: false,
                reason: vaultsync_core::storage::compaction::CompactionReason::NotNeeded,
            };
        }

        // Condition 2: Check idle time since last mutation
        let now = js_sys::Date::now() as u64;
        let last_schedule = self.runtime.upload_scheduler.last_schedule_time();
        if now.saturating_sub(last_schedule) < self.idle_time_ms {
            return CompactionDecision {
                should_run: false,
                reason: vaultsync_core::storage::compaction::CompactionReason::NotNeeded,
            };
        }

        // Condition 3: Check page count and tombstone ratio via StoreManifest
        if let Some(ref pages) = *self.runtime.pages.lock().unwrap() {
            let m = pages.doc_data.manifest.lock().unwrap();
            let (live_count, tombstone_count) = match m.as_ref() {
                Some(manifest) => (manifest.live_pages as u64, manifest.tombstoned_pages as u64),
                None => (0, 0),
            };
            drop(m);

            if live_count > self.max_page_count {
                return CompactionDecision {
                    should_run: true,
                    reason: CompactionReason::SegmentFull {
                        size: live_count,
                        max: self.max_page_count,
                    },
                };
            }

            // Condition 4: Tombstone ratio (delegate to CompactionEngine)
            let decision = self.compaction_engine.should_compact(live_count, tombstone_count);
            if decision.should_run {
                return decision;
            }
        }

        CompactionDecision {
            should_run: false,
            reason: CompactionReason::NotNeeded,
        }
    }

    /// Run compaction via StorageManager and record metrics.
    pub async fn run_compaction(&self) -> Result<String, JsValue> {
        let t0 = js_sys::Date::now();
        let stats = self
            .storage_manager
            .compact_namespace(&self.namespace)
            .await
            .map_err(|e| JsValue::from_str(&format!("Compaction failed: {:?}", e)))?;
        let elapsed = (js_sys::Date::now() - t0) as u64;

        self.last_compaction_ms.store(elapsed, Ordering::Relaxed);
        self.runtime.metrics.page_writes.fetch_add(
            stats.documents_compacted + stats.tombstones_removed,
            Ordering::Relaxed,
        );

        engine_info!(
            "[compaction] done docs={} tombstones={} saved={} elapsed={}ms",
            stats.documents_compacted,
            stats.tombstones_removed,
            stats.bytes_saved,
            elapsed,
        );

        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Called periodically (e.g., from RuntimeScheduler's Compaction task).
    /// Checks conditions and runs compaction if needed.
    pub async fn try_compact(&self) -> Option<String> {
        let decision = self.should_compact();
        if !decision.should_run {
            return None;
        }
        let result = self.run_compaction().await;
        match result {
            Ok(json) => Some(json),
            Err(e) => {
                engine_warn!("[compaction] trigger failed: {:?}", e);
                None
            }
        }
    }

    pub fn last_compaction_ms(&self) -> u64 {
        self.last_compaction_ms.load(Ordering::Relaxed)
    }
}
