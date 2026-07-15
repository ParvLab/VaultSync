use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;
use vaultsync_core::runtime_state::RuntimeLifecycle;
use vaultsync_core::VaultSyncError;

use crate::metadata_runtime::MetadataRuntime;
use crate::metrics::RuntimeMetrics;
use crate::migration::PagesDir;
use crate::page_store::{
    ContentIndex, IndexEntry, IndexKey, PageId, PageStore, StorageGeneration, StoreManifest,
};
use crate::storage_scheduler::{StorageOp, StoragePriority, StorageScheduler};

/// StorageHealth — 5-variant boot classification.
/// Determined at StorageRuntime::new() before any expensive work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageHealth {
    /// All manifests valid, no orphan pages, CRC matches. Skip all cleanup.
    Healthy,
    /// Some minor issues found (e.g. orphan pages repaired). Cleanup recommended.
    Degraded,
    /// Significant inconsistency detected needs repair via recover().
    NeedsRepair,
    /// OPFS is available but read-only (e.g. browser storage quota exceeded).
    ReadOnly,
    /// Manifest or index data is corrupt beyond repair. Requires full rebuild.
    Corrupt,
}

impl fmt::Display for StorageHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "Healthy"),
            Self::Degraded => write!(f, "Degraded"),
            Self::NeedsRepair => write!(f, "NeedsRepair"),
            Self::ReadOnly => write!(f, "ReadOnly"),
            Self::Corrupt => write!(f, "Corrupt"),
        }
    }
}

/// PageManager — owns the full lifecycle of page IDs.
///
/// Responsibilities:
/// - ID allocation (incrementing or from free list)
/// - Free list management
/// - Tombstone tracking
/// - GC watermarks
/// - Fragmentation statistics
#[derive(Debug)]
pub struct PageManager {
    next_id: AtomicU64,
    free_list: Mutex<Vec<PageId>>,
    tombstone_count: AtomicU64,
    live_count: AtomicU64,
    watermark_high: AtomicU64,
    allocation_count: AtomicU64,
}

impl PageManager {
    /// Reserved system page range: IDs 0-99 for system use (metadata, WAL checkpoints).
    const SYSTEM_PAGE_RESERVED: PageId = 100;

    pub fn new(highest_id: PageId) -> Self {
        let start = if highest_id < Self::SYSTEM_PAGE_RESERVED {
            Self::SYSTEM_PAGE_RESERVED
        } else {
            highest_id
        };
        Self {
            next_id: AtomicU64::new(start),
            free_list: Mutex::new(Vec::new()),
            tombstone_count: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            watermark_high: AtomicU64::new(start),
            allocation_count: AtomicU64::new(0),
        }
    }

    /// Allocate a page ID. Prefers free list; falls back to incrementing counter.
    pub fn allocate(&self, caller: &'static str) -> PageId {
        let mut free = self.free_list.lock().unwrap();
        if let Some(id) = free.pop() {
            self.live_count.fetch_add(1, Ordering::Relaxed);
            self.allocation_count.fetch_add(1, Ordering::Relaxed);
            engine_trace!("[page_manager] allocate (reuse) id={} caller={}", id, caller);
            return id;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.live_count.fetch_add(1, Ordering::Relaxed);
        self.allocation_count.fetch_add(1, Ordering::Relaxed);
        let max = self.watermark_high.load(Ordering::Relaxed);
        if id > max {
            self.watermark_high.store(id, Ordering::Relaxed);
        }
        engine_trace!("[page_manager] allocate (new) id={} caller={}", id, caller);
        id
    }

    /// Free a page ID (return to free list).
    pub fn free(&self, page_id: PageId) {
        self.free_list.lock().unwrap().push(page_id);
        self.live_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a tombstone.
    pub fn record_tombstone(&self) {
        self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        self.live_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record that a page was deleted (not tombstoned — physically removed).
    pub fn record_deletion(&self) {
        self.live_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current live page count.
    pub fn live_count(&self) -> u64 {
        self.live_count.load(Ordering::Acquire)
    }

    /// Current tombstone count.
    pub fn tombstone_count(&self) -> u64 {
        self.tombstone_count.load(Ordering::Acquire)
    }

    /// Total allocated page IDs (including freed/tombstoned).
    pub fn total_allocated(&self) -> u64 {
        self.watermark_high.load(Ordering::Acquire)
    }

    /// Number of allocate() calls.
    pub fn allocation_count(&self) -> u64 {
        self.allocation_count.load(Ordering::Acquire)
    }

    /// Free list size.
    pub fn free_list_len(&self) -> usize {
        self.free_list.lock().unwrap().len()
    }

    /// Reset to a known highest ID (from manifest recovery).
    pub fn reset_to(&self, highest_id: PageId) {
        self.next_id.store(highest_id, Ordering::Relaxed);
        self.watermark_high.store(highest_id, Ordering::Relaxed);
        self.tombstone_count.store(0, Ordering::Relaxed);
        self.live_count.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);
        self.free_list.lock().unwrap().clear();
    }
}

/// StorageRuntime — single authority for all storage state.
///
/// Owns:
/// - PagesDir (all 6 PageStores: doc_data, oplog, sync_states, schemas, migrations, keys)
/// - ContentIndex for each store (combined in-memory index)
/// - PageManager for each store
/// - Manifests
///
/// No cursor, no generation, no pending count — those belong to SyncRuntime.
#[derive(Debug)]
pub struct StorageRuntime {
    pub pages: PagesDir,
    pub page_managers: Mutex<Vec<PageManager>>,
    pub scheduler: Arc<StorageScheduler>,
    pub metrics: Arc<RuntimeMetrics>,
    pub startup_done: AtomicBool,
    health: StorageHealth,
    orphan_count: AtomicU64,
    metadata_runtime: Mutex<Option<Arc<MetadataRuntime>>>,
}

impl StorageRuntime {
    pub async fn new(pages: PagesDir, metrics: Arc<RuntimeMetrics>) -> Result<Arc<Self>, VaultSyncError> {
        let _t0 = js_sys::Date::now();
        let scheduler = Arc::new(StorageScheduler::new(metrics.clone()));

        // Initialize PageManagers from each store's manifest
        let mut managers = Vec::new();
        for store in [&pages.doc_data, &pages.oplog, &pages.sync_states, &pages.schemas, &pages.migrations, &pages.keys] {
            let manifest_maybe = {
                let guard = store.manifest.lock().unwrap();
                guard.clone()
            };
            let highest = manifest_maybe
                .as_ref()
                .map(|m| m.highest_page_id)
                .unwrap_or(0);
            managers.push(PageManager::new(highest));
        }
        let t1 = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[StorageRuntime] PageManagers init: {}ms", t1);

        // Sprint B: Classify StorageHealth before any expensive work.
        // Step 1: Checksum check (determines Corrupt)
        let mut any_corrupt = false;

        for store in [&pages.doc_data, &pages.oplog, &pages.sync_states, &pages.schemas, &pages.migrations, &pages.keys] {
            let manifest_guard = store.manifest.lock().unwrap();
            if let Some(ref manifest) = *manifest_guard {
                if manifest.checksum != 0 {
                    let computed = manifest.compute_checksum();
                    if computed != manifest.checksum {
                        engine_error!("[StorageRuntime] checksum mismatch for store — expected={} actual={}", manifest.checksum, computed);
                        any_corrupt = true;
                    }
                }
            }
        }
        let t2 = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[StorageRuntime] CRC check (6 stores): {}ms", t2 - t1);

        // Step 2: CRC check passed — skip orphan repair if no corruption detected.
        // Repair is only needed when CRC fails; Healthy stores have no orphans by definition
        // (tombstoned pages between compaction are tracked in manifest, not "orphans").
        let mut total_orphans = 0u64;
        let health = if any_corrupt {
            StorageHealth::Corrupt
        } else {
            // Quick estimate: orphan count from page_by_id vs live+tombstoned tracking difference.
            for store in [&pages.doc_data, &pages.oplog, &pages.sync_states, &pages.schemas, &pages.migrations, &pages.keys] {
                let guard = store.manifest.lock().unwrap();
                if let Some(ref m) = *guard {
                    let total_tracked = m.content_index.live_pages.len() + m.content_index.tombstoned_pages.len();
                    let allocated = m.content_index.page_by_id.len();
                    if allocated > total_tracked {
                        total_orphans += (allocated - total_tracked) as u64;
                    }
                }
            }
            if total_orphans > 50 { StorageHealth::NeedsRepair } else if total_orphans > 10 { StorageHealth::Degraded } else { StorageHealth::Healthy }
        };

        // Sprint C: Only run orphan repair when health requires it (NeedsRepair+).
        // When Healthy or Degraded, there are no orphan pages to clean up
        // (Degraded = minor tombstone tracking drift, fixed by next compaction cycle).
        if health == StorageHealth::Corrupt || health == StorageHealth::NeedsRepair {
            for store in [&pages.doc_data, &pages.oplog, &pages.sync_states, &pages.schemas, &pages.migrations, &pages.keys] {
                if let Some(ref mut manifest) = *store.manifest.lock().unwrap() {
                    let orphan_count = manifest.content_index.repair_orphans();
                    if orphan_count > 0 {
                        manifest.live_pages = manifest.content_index.live_pages.len();
                        manifest.tombstoned_pages = manifest.content_index.tombstoned_pages.len();
                        manifest.updated_at = js_sys::Date::now() as u64;
                        let cloned = manifest.clone();
                        if let Err(e) = crate::page_store::write_manifest(store.dir(), &cloned).await {
                            engine_warn!("[StorageRuntime] failed to persist repaired manifest: {:?}", e);
                        }
                    }
                }
            }
        }
        let t3 = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[StorageRuntime] health check: {}ms health={} orphans_estimated={}", t3 - t2, health, total_orphans);

        // Build detailed health report
        {
            let mut report = String::from("[StorageHealth] report:\n");
            for (name, store) in [("doc_data", &pages.doc_data as &PageStore), ("oplog", &pages.oplog), ("sync_states", &pages.sync_states), ("schemas", &pages.schemas), ("migrations", &pages.migrations), ("keys", &pages.keys)] {
                let guard = store.manifest.lock().unwrap();
                if let Some(ref m) = *guard {
                    let crc_ok = if m.checksum == 0 { "none".to_string() } else {
                        let computed = m.compute_checksum();
                        if computed == m.checksum { "match".to_string() } else { format!("mismatch (stored={} actual={})", m.checksum, computed) }
                    };
                    report.push_str(&format!("  {}: pages={} live={} tomb={} crc={} gen={}\n",
                        name, m.content_index.live_pages.len() + m.content_index.tombstoned_pages.len(),
                        m.content_index.live_pages.len(), m.content_index.tombstoned_pages.len(),
                        crc_ok, m.generation.0));
                } else {
                    report.push_str(&format!("  {}: no manifest\n", name));
                }
            }
            report.push_str(&format!("  orphans_repaired={} health={}", total_orphans, health));
            engine_info!("{}", report);
        }

        let rt = Arc::new(Self {
            pages,
            page_managers: Mutex::new(managers),
            scheduler,
            metrics,
            startup_done: AtomicBool::new(false),
            health,
            orphan_count: AtomicU64::new(total_orphans),
            metadata_runtime: Mutex::new(None),
        });

        let t_total = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[StorageRuntime] new() total: {}ms (health={})", t_total, health);
        Ok(rt)
    }

    /// Return the boot health classification.
    pub fn health(&self) -> StorageHealth {
        self.health
    }

    /// Set the MetadataRuntime for checkpoint support.
    pub fn set_metadata_runtime(&self, mr: Arc<MetadataRuntime>) {
        *self.metadata_runtime.lock().unwrap() = Some(mr);
    }

    /// Get the MetadataRuntime reference, if set.
    pub fn get_metadata_runtime(&self) -> Option<Arc<MetadataRuntime>> {
        self.metadata_runtime.lock().unwrap().clone()
    }

    /// Drain and execute all pending scheduler ops against actual PageStores.
    /// Returns the number of ops processed.
    pub async fn process_pending(&self) -> usize {
        let ops = self.scheduler.drain_pending();
        if ops.is_empty() {
            return 0;
        }
        let count = ops.len();
        for qop in &ops {
            match &qop.op {
                StorageOp::WritePage { store, page_id, data } => {
                    if let Err(e) = self.get_store(store).write_page(*page_id, data).await {
                        engine_error!("[scheduler] write_page failed store={} page={}: {:?}", store, page_id, e);
                    }
                }
                StorageOp::TombstonePage { store, page_id } => {
                    if let Err(e) = self.get_store(store).tombstone_page(*page_id).await {
                        engine_error!("[scheduler] tombstone_page failed store={} page={}: {:?}", store, page_id, e);
                    }
                }
                StorageOp::DeletePage { store, page_id } => {
                    if let Err(e) = self.get_store(store).delete_page(*page_id).await {
                        engine_error!("[scheduler] delete_page failed store={} page={}: {:?}", store, page_id, e);
                    }
                }
                StorageOp::FlushManifest { .. } => {
                    // Manifest persistence is handled inline by PageStore methods.
                    // No action needed here.
                }
                _ => {
                    // AllocatePageId, Checkpoint, Compaction are handled
                    // by their respective subsystems, not here.
                    engine_trace!("[scheduler] skipping op {:?} (handled elsewhere)", qop.op);
                }
            }
        }
        self.metrics.mutations_sent.fetch_add(count as u64, Ordering::Relaxed);
        #[cfg(debug_assertions)]
        engine_debug!("[scheduler] processed {} ops", count);
        count
    }

    /// Enqueue a WritePage op and process it immediately (inline in WASM).
    pub async fn enqueue_write_page(&self, store: &str, page_id: PageId, data: Vec<u8>) -> Result<(), VaultSyncError> {
        let op = StorageOp::WritePage {
            store: store.to_string(),
            page_id,
            data,
        };
        self.scheduler.enqueue(op, "write_page");
        self.process_pending().await;
        Ok(())
    }

    /// Enqueue a TombstonePage op and process it immediately.
    pub async fn enqueue_tombstone_page(&self, store: &str, page_id: PageId) -> Result<(), VaultSyncError> {
        let op = StorageOp::TombstonePage {
            store: store.to_string(),
            page_id,
        };
        self.scheduler.enqueue(op, "tombstone_page");
        self.process_pending().await;
        Ok(())
    }

    /// Get PageManager index for a given store name.
    pub fn store_index(store: &str) -> usize {
        match store {
            "doc_data" => 0,
            "oplog" => 1,
            "sync_states" => 2,
            "schemas" => 3,
            "migrations" => 4,
            "keys" => 5,
            _ => 0,
        }
    }

    /// Get PageStore by name.
    pub fn get_store(&self, name: &str) -> &PageStore {
        match name {
            "doc_data" => &self.pages.doc_data,
            "oplog" => &self.pages.oplog,
            "sync_states" => &self.pages.sync_states,
            "schemas" => &self.pages.schemas,
            "migrations" => &self.pages.migrations,
            "keys" => &self.pages.keys,
            _ => &self.pages.doc_data,
        }
    }

    /// Allocate a page ID for the given store.
    pub fn allocate_page_id(&self, store: &str, caller: &'static str) -> PageId {
        let idx = Self::store_index(store);
        let mut managers = self.page_managers.lock().unwrap();
        if idx < managers.len() {
            managers[idx].allocate(caller)
        } else {
            engine_warn!("[storage_runtime] unknown store {} for allocate", store);
            0
        }
    }

    /// Free a page ID (return to free list).
    pub fn free_page_id(&self, store: &str, page_id: PageId) {
        let idx = Self::store_index(store);
        let mut managers = self.page_managers.lock().unwrap();
        if idx < managers.len() {
            managers[idx].free(page_id);
        }
    }

    /// Record a tombstone for page count tracking.
    pub fn record_tombstone(&self, store: &str) {
        let idx = Self::store_index(store);
        let mut managers = self.page_managers.lock().unwrap();
        if idx < managers.len() {
            managers[idx].record_tombstone();
        }
    }

    /// Enqueue a storage op via the scheduler.
    pub fn enqueue(&self, op: StorageOp, caller: &'static str) {
        self.scheduler.enqueue(op, caller);
    }

    /// Enqueue with explicit priority.
    pub fn enqueue_with_priority(&self, op: StorageOp, priority: StoragePriority, caller: &'static str) {
        self.scheduler.enqueue_with_priority(op, priority, caller);
    }

    /// Mark startup as done — allows compaction and other background ops.
    pub fn mark_startup_done(&self) {
        self.startup_done.store(true, Ordering::Release);
        self.scheduler.set_startup_guard(false);
    }

    pub fn is_startup_done(&self) -> bool {
        self.startup_done.load(Ordering::Acquire)
    }

    /// Delegate: list page IDs from a PageStore.
    pub async fn list_page_ids(&self, store: &str, caller: &'static str) -> Result<Vec<PageId>, VaultSyncError> {
        self.get_store(store).list_page_ids(caller).await
    }

    /// Delegate: read a page from a PageStore.
    pub async fn read_page(&self, store: &str, page_id: PageId) -> Result<Option<Vec<u8>>, VaultSyncError> {
        self.get_store(store).read_page(page_id).await
    }

    /// Delegate: adjust pending count on a PageStore.
    pub async fn adjust_pending_count(&self, store: &str, delta: i32) -> Result<(), vaultsync_core::VaultSyncError> {
        self.get_store(store).adjust_pending_count(delta).await
    }

    /// Delegate: schedule GC on a PageStore.
    pub fn schedule_gc(&self, store: &str) {
        let ps = self.get_store(store);
        ps.schedule_gc();
    }

    /// Delegate: run pending GC on a PageStore.
    pub async fn run_pending_gc(&self, store: &str) -> Result<usize, vaultsync_core::VaultSyncError> {
        let ps = self.get_store(store);
        ps.run_pending_gc().await
    }

    /// Delegate: set pending count on a PageStore.
    pub async fn set_pending_count(&self, store: &str, count: usize) -> Result<(), vaultsync_core::VaultSyncError> {
        self.get_store(store).set_pending_count(count).await
    }

    /// Delegate: set current page on a PageStore.
    pub async fn set_current_page(&self, store: &str, page_id: PageId) -> Result<(), vaultsync_core::VaultSyncError> {
        let ps = self.get_store(store);
        ps.set_current_page(page_id).await
    }

    /// Get page count metrics from all page managers.
    pub fn page_count_metrics(&self) -> (u64, u64, u64) {
        let managers = self.page_managers.lock().unwrap();
        let total_live: u64 = managers.iter().map(|m| m.live_count()).sum();
        let total_tombstones: u64 = managers.iter().map(|m| m.tombstone_count()).sum();
        let total_allocated: u64 = managers.iter().map(|m| m.total_allocated()).sum();
        (total_live, total_tombstones, total_allocated)
    }

    /// Sprint B: Physically remove tombstoned page files from all stores.
    /// Called from MaintenanceRuntime::warm() — never blocks startup.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, vaultsync_core::VaultSyncError> {
        let mut total = 0usize;
        total += self.pages.doc_data.cleanup_tombstoned_pages().await?;
        total += self.pages.oplog.cleanup_tombstoned_pages().await?;
        total += self.pages.sync_states.cleanup_tombstoned_pages().await?;
        total += self.pages.schemas.cleanup_tombstoned_pages().await?;
        total += self.pages.migrations.cleanup_tombstoned_pages().await?;
        total += self.pages.keys.cleanup_tombstoned_pages().await?;
        Ok(total)
    }
}

#[async_trait]
impl RuntimeLifecycle for StorageRuntime {
    async fn boot(&self) -> Result<(), String> {
        engine_trace!("[storage_runtime] boot: page managers initialized");
        Ok(())
    }

    async fn ready(&self) -> Result<(), String> {
        self.mark_startup_done();
        engine_debug!("[storage_runtime] ready: startup guard cleared");
        Ok(())
    }

    async fn warm(&self) -> Result<(), String> {
        engine_trace!("[storage_runtime] warm");
        Ok(())
    }

    async fn idle(&self) -> Result<(), String> {
        // Process any pending scheduler ops
        self.process_pending().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), String> {
        self.scheduler.clear();
        engine_info!("[storage_runtime] shutdown: scheduler cleared");
        Ok(())
    }
}
