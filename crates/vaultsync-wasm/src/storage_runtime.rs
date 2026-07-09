use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use vaultsync_core::VaultSyncError;

use crate::metrics::RuntimeMetrics;
use crate::migration::PagesDir;
use crate::page_store::{
    ContentIndex, IndexEntry, IndexKey, PageId, PageStore, StorageGeneration, StoreManifest,
};
use crate::storage_scheduler::{StorageOp, StoragePriority, StorageScheduler};

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
    pub fn new(highest_id: PageId) -> Self {
        Self {
            next_id: AtomicU64::new(highest_id),
            free_list: Mutex::new(Vec::new()),
            tombstone_count: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            watermark_high: AtomicU64::new(highest_id),
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
}

impl StorageRuntime {
    pub async fn new(pages: PagesDir, metrics: Arc<RuntimeMetrics>) -> Result<Arc<Self>, VaultSyncError> {
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

        let rt = Arc::new(Self {
            pages,
            page_managers: Mutex::new(managers),
            scheduler,
            metrics,
            startup_done: AtomicBool::new(false),
        });

        Ok(rt)
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

    /// Get page count metrics from all page managers.
    pub fn page_count_metrics(&self) -> (u64, u64, u64) {
        let managers = self.page_managers.lock().unwrap();
        let total_live: u64 = managers.iter().map(|m| m.live_count()).sum();
        let total_tombstones: u64 = managers.iter().map(|m| m.tombstone_count()).sum();
        let total_allocated: u64 = managers.iter().map(|m| m.total_allocated()).sum();
        (total_live, total_tombstones, total_allocated)
    }
}
