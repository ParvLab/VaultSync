use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::document_runtime::DocumentRuntime;
use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime as LegacyRuntime;
use crate::storage_runtime::StorageRuntime;
use crate::storage_scheduler::StorageScheduler;
use crate::sync_runtime::SyncRuntime;
use vaultsync_core::runtime_bus::RuntimeBus;

/// Phase 5: VaultRuntime — single top-level runtime host.
///
/// Wraps the legacy Runtime and adds new services alongside.
/// New code uses the VaultRuntime services; existing code continues
/// via self.runtime (legacy Runtime).
pub struct VaultRuntime {
    // ── Legacy Runtime (keep until full migration) ──
    pub runtime: Arc<LegacyRuntime>,

    // ── New services ──

    /// Storage service: PageManager, ContentIndex v2, free list, GC, tombstones
    pub storage: Arc<StorageRuntime>,
    /// Priority-based storage scheduler with transactions
    pub scheduler: Arc<StorageScheduler>,

    /// Sync state: cursor, generation, PendingIndex, recovery state, mirror
    pub sync: Option<Arc<SyncRuntime>>,

    /// Document cache: SegmentedLruCache-backed, JS subscriptions, hot-doc tracking
    pub document: Option<Arc<DocumentRuntime>>,

    /// Sync dispatch event bus
    pub bus: Option<Arc<RuntimeBus>>,

    /// Runtime metrics
    pub metrics: Arc<RuntimeMetrics>,

    // ── Lifecycle ──

    pub start_time: js_sys::Date,
    pub is_ready: AtomicBool,
}

impl VaultRuntime {
    pub fn new(
        runtime: Arc<LegacyRuntime>,
        storage: Arc<StorageRuntime>,
        scheduler: Arc<StorageScheduler>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            storage,
            scheduler,
            sync: None,
            document: None,
            bus: None,
            metrics,
            start_time: js_sys::Date::new_0(),
            is_ready: AtomicBool::new(false),
        })
    }

    pub fn with_document(self: Arc<Self>, document: Arc<DocumentRuntime>) -> Arc<Self> {
        unsafe { &mut *(Arc::as_ptr(&self) as *mut Self) }.document = Some(document);
        self
    }

    pub fn with_sync(self: Arc<Self>, sync: Arc<SyncRuntime>) -> Arc<Self> {
        unsafe { &mut *(Arc::as_ptr(&self) as *mut Self) }.sync = Some(sync);
        self
    }

    pub fn with_bus(self: Arc<Self>, bus: Arc<RuntimeBus>) -> Arc<Self> {
        unsafe { &mut *(Arc::as_ptr(&self) as *mut Self) }.bus = Some(bus);
        self
    }

    pub fn mark_ready(&self) {
        self.is_ready.store(true, Ordering::Release);
    }

    pub fn ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    pub fn uptime_ms(&self) -> u64 {
        (js_sys::Date::now() - self.start_time.get_time()) as u64
    }

    /// Shutdown — clear scheduler, flush metrics.
    pub fn shutdown(&self) {
        self.scheduler.clear();
        if let Some(ref doc) = self.document {
            doc.clear();
        }
        engine_info!("[VaultRuntime] shutdown after {}ms uptime", self.uptime_ms());
    }
}
