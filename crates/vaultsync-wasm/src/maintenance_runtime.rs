use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use async_trait::async_trait;

use crate::metadata_runtime::MetadataRuntime;
use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime;
use crate::storage_runtime::StorageRuntime;
use vaultsync_core::runtime_state::RuntimeLifecycle;
use vaultsync_core::storage::manager::StorageManager;

/// MaintenanceRuntime — background loop for GC, compaction, checkpoint, and health.
///
/// Phase-agnostic tick() that JS SDK calls periodically via setInterval.
/// Never blocks startup. Driven externally.
pub struct MaintenanceRuntime {
    storage_runtime: Arc<StorageRuntime>,
    _storage_manager: Arc<dyn StorageManager>,
    _runtime: Arc<Runtime>,
    _metrics: Arc<RuntimeMetrics>,
    metadata_runtime: Option<Arc<MetadataRuntime>>,
    running: AtomicBool,
    gc_interval_ms: AtomicU64,
    compact_interval_ms: AtomicU64,
    checkpoint_interval_ms: AtomicU64,
    health_interval_ms: AtomicU64,
    last_gc_ms: AtomicU64,
    last_compact_ms: AtomicU64,
    last_checkpoint_ms: AtomicU64,
    last_health_ms: AtomicU64,
}

impl fmt::Debug for MaintenanceRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenanceRuntime")
            .field("running", &self.running)
            .field("gc_interval_ms", &self.gc_interval_ms)
            .field("compact_interval_ms", &self.compact_interval_ms)
            .field("checkpoint_interval_ms", &self.checkpoint_interval_ms)
            .field("health_interval_ms", &self.health_interval_ms)
            .finish()
    }
}

impl MaintenanceRuntime {
    pub fn new(
        storage_runtime: Arc<StorageRuntime>,
        storage_manager: Arc<dyn StorageManager>,
        runtime: Arc<Runtime>,
        metrics: Arc<RuntimeMetrics>,
        metadata_runtime: Option<Arc<MetadataRuntime>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage_runtime,
            _storage_manager: storage_manager,
            _runtime: runtime,
            _metrics: metrics,
            metadata_runtime,
            running: AtomicBool::new(true),
            gc_interval_ms: AtomicU64::new(30_000),
            compact_interval_ms: AtomicU64::new(120_000),
            checkpoint_interval_ms: AtomicU64::new(60_000),
            health_interval_ms: AtomicU64::new(15_000),
            last_gc_ms: AtomicU64::new(0),
            last_compact_ms: AtomicU64::new(0),
            last_checkpoint_ms: AtomicU64::new(0),
            last_health_ms: AtomicU64::new(0),
        })
    }

    pub fn set_gc_interval_ms(&self, ms: u64) {
        self.gc_interval_ms.store(ms, Ordering::Release);
    }
    pub fn set_compact_interval_ms(&self, ms: u64) {
        self.compact_interval_ms.store(ms, Ordering::Release);
    }
    pub fn set_checkpoint_interval_ms(&self, ms: u64) {
        self.checkpoint_interval_ms.store(ms, Ordering::Release);
    }
    pub fn set_health_interval_ms(&self, ms: u64) {
        self.health_interval_ms.store(ms, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Run a single maintenance tick. Tracks which phases are due.
    /// Actual async work is executed by the idle() lifecycle method.
    /// Returns the number of phases due (0 if none).
    pub fn tick(&self) -> u32 {
        if !self.running.load(Ordering::Acquire) {
            return 0;
        }
        let now = js_sys::Date::now() as u64;
        let mut phases = 0u32;

        // Phase 1: GC — schedule cleanup if tombstone ratio is high
        let last_gc = self.last_gc_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_gc) >= self.gc_interval_ms.load(Ordering::Acquire) {
            let (live_count, tombstone_count, _) = self.storage_runtime.page_count_metrics();
            let ratio = if live_count > 0 {
                tombstone_count as f64 / live_count as f64
            } else {
                0.0
            };
            if ratio >= 0.10 {
                engine_debug!("[maintenance] GC due: live={} tombstones={} ratio={:.2}", live_count, tombstone_count, ratio);
            }
            self.last_gc_ms.store(now, Ordering::Release);
            phases += 1;
        }

        // Phase 2: Compaction — schedule when due
        let last_compact = self.last_compact_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_compact) >= self.compact_interval_ms.load(Ordering::Acquire) {
            let (live_count, tombstone_count, _) = self.storage_runtime.page_count_metrics();
            let ratio = if live_count > 0 {
                tombstone_count as f64 / live_count as f64
            } else {
                0.0
            };
            if ratio >= 0.25 {
                engine_debug!("[maintenance] compaction due: live={} tombstones={} ratio={:.2}", live_count, tombstone_count, ratio);
            }
            self.last_compact_ms.store(now, Ordering::Release);
            phases += 1;
        }

        // Phase 3: Checkpoint — persist MetadataRuntime dirty values
        let last_checkpoint = self.last_checkpoint_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_checkpoint) >= self.checkpoint_interval_ms.load(Ordering::Acquire) {
            if let Some(ref mr) = self.metadata_runtime {
                if mr.is_dirty() {
                    engine_trace!("[maintenance] checkpoint due (dirty)");
                }
            }
            self.last_checkpoint_ms.store(now, Ordering::Release);
            phases += 1;
        }

        // Phase 4: Health — log current health status
        let last_health = self.last_health_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_health) >= self.health_interval_ms.load(Ordering::Acquire) {
            let health = self.storage_runtime.health();
            engine_trace!("[maintenance] health tick: {:?}", health);
            self.last_health_ms.store(now, Ordering::Release);
            phases += 1;
        }

        phases
    }

    /// Run deferred async work (GC cleanup, checkpoint, compaction).
    /// Called by the idle() lifecycle phase.
    pub async fn run_deferred(&self) {
        let now = js_sys::Date::now() as u64;

        // GC: cleanup tombstoned pages from disk
        let last_gc = self.last_gc_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_gc) < self.gc_interval_ms.load(Ordering::Acquire) {
            if let Err(e) = self.storage_runtime.cleanup_tombstoned_pages().await {
                engine_warn!("[maintenance] GC cleanup failed: {:?}", e);
            }
        }

        // Checkpoint: persist MetadataRuntime dirty values through MetadataStore
        let last_checkpoint = self.last_checkpoint_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_checkpoint) < self.checkpoint_interval_ms.load(Ordering::Acquire) {
            if let Some(ref mr) = self.metadata_runtime {
                if mr.is_dirty() {
                    if let Err(e) = mr.checkpoint().await {
                        engine_warn!("[maintenance] checkpoint failed: {:?}", e);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl RuntimeLifecycle for MaintenanceRuntime {
    async fn boot(&self) -> Result<(), String> { Ok(()) }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> {
        // Sprint B: Deferred cleanup — physically remove tombstoned page files.
        // Never blocks startup; runs once during warm phase.
        match self.storage_runtime.cleanup_tombstoned_pages().await {
            Ok(n) => {
                if n > 0 {
                    engine_info!("[maintenance] warm cleanup: removed {} tombstoned pages", n);
                }
            }
            Err(e) => engine_warn!("[maintenance] warm cleanup failed: {:?}", e),
        }
        Ok(())
    }
    async fn idle(&self) -> Result<(), String> {
        self.tick();
        self.run_deferred().await;
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), String> {
        // Flush metadata on shutdown
        if let Some(ref mr) = self.metadata_runtime {
            if mr.is_dirty() {
                let _ = mr.checkpoint().await;
            }
        }
        self.stop();
        Ok(())
    }
}
