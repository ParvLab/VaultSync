use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cache_runtime::CacheRuntime;
use crate::compaction_scheduler::CompactionScheduler;
use crate::document_runtime::DocumentRuntime;
use crate::engine_context::EngineContext;
use crate::maintenance_runtime::MaintenanceRuntime;
use crate::metrics::RuntimeMetrics;
use crate::namespace_runtime::WorkspaceRuntime;
use crate::runtime::Runtime as LegacyRuntime;
use crate::storage_runtime::StorageRuntime;
use crate::storage_scheduler::StorageScheduler;
use crate::sync_runtime::SyncRuntime;
use vaultsync_core::runtime_bus::{RuntimeBus, RuntimeEvent};

/// VaultRuntime — single top-level runtime host.
///
/// Owns an EngineContext containing all shared runtime references.
/// Provides builder methods for optional services and lifecycle management.
pub struct VaultRuntime {
    /// Shared container for all runtime references
    pub ctx: Arc<EngineContext>,

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
        let ctx = Arc::new(EngineContext::new(runtime, storage, scheduler, metrics));
        Arc::new(Self {
            ctx,
            start_time: js_sys::Date::new_0(),
            is_ready: AtomicBool::new(false),
        })
    }

    fn replace_ctx(&self, ctx: EngineContext) -> Arc<Self> {
        // Single-threaded WASM: safe to create a new Arc with cloned fields + new ctx.
        Arc::new(VaultRuntime {
            ctx: Arc::new(ctx),
            start_time: js_sys::Date::new_0(),
            is_ready: AtomicBool::new(false),
        })
    }

    pub fn with_sync(self: Arc<Self>, sync: Arc<SyncRuntime>) -> Arc<Self> {
        self.replace_ctx(self.ctx.as_ref().clone().with_sync(sync))
    }

    pub fn with_document(self: Arc<Self>, document: Arc<DocumentRuntime>) -> Arc<Self> {
        self.replace_ctx(self.ctx.as_ref().clone().with_document(document))
    }

    pub fn with_maintenance(self: Arc<Self>, maintenance: Arc<MaintenanceRuntime>) -> Arc<Self> {
        self.replace_ctx(self.ctx.as_ref().clone().with_maintenance(maintenance))
    }

    pub fn with_compaction(self: Arc<Self>, compaction: Arc<CompactionScheduler>) -> Arc<Self> {
        self.replace_ctx(self.ctx.as_ref().clone().with_compaction(compaction))
    }

    pub fn with_bus(self: Arc<Self>, bus: Arc<RuntimeBus<RuntimeEvent>>) -> Arc<Self> {
        self.replace_ctx(self.ctx.as_ref().clone().with_bus(bus))
    }

    /// Convenience accessors
    pub fn runtime(&self) -> &Arc<LegacyRuntime> { &self.ctx.runtime }
    pub fn storage(&self) -> &Arc<StorageRuntime> { &self.ctx.storage }
    pub fn scheduler(&self) -> &Arc<StorageScheduler> { &self.ctx.scheduler }
    pub fn metrics(&self) -> &Arc<RuntimeMetrics> { &self.ctx.metrics }
    pub fn cache(&self) -> &Arc<CacheRuntime> { &self.ctx.cache }
    pub fn workspace(&self) -> &Arc<WorkspaceRuntime> { &self.ctx.workspace }
    pub fn sync(&self) -> Option<&Arc<SyncRuntime>> { self.ctx.sync.as_ref() }
    pub fn document(&self) -> Option<&Arc<DocumentRuntime>> { self.ctx.document.as_ref() }
    pub fn maintenance(&self) -> Option<&Arc<MaintenanceRuntime>> { self.ctx.maintenance.as_ref() }
    pub fn compaction(&self) -> Option<&Arc<CompactionScheduler>> { self.ctx.compaction.as_ref() }
    pub fn bus(&self) -> Option<&Arc<RuntimeBus<RuntimeEvent>>> { self.ctx.bus.as_ref() }

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
        self.ctx.shutdown();
        engine_info!("[VaultRuntime] shutdown after {}ms uptime", self.uptime_ms());
    }
}
