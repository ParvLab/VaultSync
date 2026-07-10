use std::sync::Arc;

use crate::cache_runtime::CacheRuntime;
use crate::compaction_scheduler::CompactionScheduler;
use crate::document_runtime::DocumentRuntime;
use crate::maintenance_runtime::MaintenanceRuntime;
use crate::metrics::RuntimeMetrics;
use crate::namespace_runtime::WorkspaceRuntime;
use crate::runtime::Runtime as LegacyRuntime;
use crate::storage_runtime::StorageRuntime;
use crate::storage_scheduler::StorageScheduler;
use crate::sync_runtime::SyncRuntime;
use vaultsync_core::runtime_bus::RuntimeBus;

/// EngineContext — single container for all shared runtime references.
///
/// Passed as `Arc<EngineContext>` instead of 6+ separate Arcs.
/// Each subsystem extracts what it needs via field access.
#[derive(Clone)]
pub struct EngineContext {
    // ── Core ──
    pub runtime: Arc<LegacyRuntime>,
    pub storage: Arc<StorageRuntime>,
    pub scheduler: Arc<StorageScheduler>,
    pub metrics: Arc<RuntimeMetrics>,
    pub cache: Arc<CacheRuntime>,

    // ── Optional services (set via builder) ──
    pub sync: Option<Arc<SyncRuntime>>,
    pub document: Option<Arc<DocumentRuntime>>,
    pub maintenance: Option<Arc<MaintenanceRuntime>>,
    pub compaction: Option<Arc<CompactionScheduler>>,
    pub bus: Option<Arc<RuntimeBus>>,

    // ── Namespace management ──
    pub workspace: Arc<WorkspaceRuntime>,
}

impl EngineContext {
    pub fn new(
        runtime: Arc<LegacyRuntime>,
        storage: Arc<StorageRuntime>,
        scheduler: Arc<StorageScheduler>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let cache = Arc::new(CacheRuntime::new(metrics.clone()));
        let workspace = WorkspaceRuntime::new(metrics.clone());
        Self {
            runtime,
            storage,
            scheduler,
            metrics,
            cache,
            workspace,
            sync: None,
            document: None,
            maintenance: None,
            compaction: None,
            bus: None,
        }
    }

    pub fn with_sync(mut self, sync: Arc<SyncRuntime>) -> Self {
        self.sync = Some(sync);
        self
    }

    pub fn with_document(mut self, document: Arc<DocumentRuntime>) -> Self {
        self.document = Some(document);
        self
    }

    pub fn with_maintenance(mut self, maintenance: Arc<MaintenanceRuntime>) -> Self {
        self.maintenance = Some(maintenance);
        self
    }

    pub fn with_compaction(mut self, compaction: Arc<CompactionScheduler>) -> Self {
        self.compaction = Some(compaction);
        self
    }

    pub fn with_bus(mut self, bus: Arc<RuntimeBus>) -> Self {
        self.bus = Some(bus);
        self
    }

    /// Shutdown: clear scheduler, caches, and all runtimes.
    pub fn shutdown(&self) {
        self.scheduler.clear();
        self.cache.clear();
        self.workspace.clear();
        if let Some(ref doc) = self.document {
            doc.clear();
        }
        if let Some(ref m) = self.maintenance {
            m.stop();
        }
        engine_info!("[EngineContext] shutdown");
    }
}
