use js_sys::Uint8Array;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(debug_assertions)]
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::collections::VecDeque;
use vaultsync_core::time_utils::SendJsFuture;
use vaultsync_core::VaultSyncError;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::*;

use crate::migration::DocEntry;

pub type PageId = u64;

/// Guard that emits diagnostics if a manifest rebuild finds >1 live page per key.
/// In debug builds, this panics with a clear message. In release, it logs a warning.
/// This enforces the invariant: "A document can never have >1 live page after a
/// committed transaction."
#[cfg(debug_assertions)]
pub fn check_manifest_rebuild_duplicates(index: &ContentIndex, store: &str) {
    let mut doc_page_count: HashMap<(&str, &str), Vec<PageId>> = HashMap::new();
    for (key, entry) in &index.entries {
        if let IndexKey::Document { doc_id, record_id } = key {
            doc_page_count
                .entry((doc_id.as_str(), record_id.as_str()))
                .or_default()
                .push(entry.page_id);
        }
    }
    for ((doc_id, record_id), pages) in &doc_page_count {
        if pages.len() > 1 {
            panic!(
                "[ManifestRebuildGuard] store={} doc={}/{} has {} live pages (max 1): {:?}",
                store, doc_id, record_id, pages.len(), pages
            );
        }
    }
}

#[cfg(not(debug_assertions))]
pub fn check_manifest_rebuild_duplicates(index: &ContentIndex, store: &str) {
    let mut doc_page_count: HashMap<(&str, &str), Vec<PageId>> = HashMap::new();
    for (key, entry) in &index.entries {
        if let IndexKey::Document { doc_id, record_id } = key {
            doc_page_count
                .entry((doc_id.as_str(), record_id.as_str()))
                .or_default()
                .push(entry.page_id);
        }
    }
    for ((doc_id, record_id), pages) in &doc_page_count {
        if pages.len() > 1 {
            engine_warn!(
                "[ManifestRebuildGuard] store={} doc={}/{} has {} live pages (max 1): {:?}",
                store, doc_id, record_id, pages.len(), pages
            );
        }
    }
}

/// V3 format: [0x56, 0x53] + [postcard PageHeader] + [data]
const PAGE_MAGIC: [u8; 2] = [0x56, 0x53];

/// Phase 1: Runtime generations for the storage layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGeneration(pub u64);

impl Default for StorageGeneration {
    fn default() -> Self { Self(0) }
}

/// Phase 1: Index key types for ContentIndex lookups.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IndexKey {
    Document { doc_id: String, record_id: String },
    Sequence(u64),
    Namespace(String),
    Schema(String),
    Migration(String),
}

/// Phase 7: Index entry with per-document metadata. O(1) for all lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub page_id: PageId,
    pub storage_gen: StorageGeneration,
    pub checksum: u32,
    pub size: u32,
    pub last_modified: u64,
    /// Phase 7: Document version — incremented on each mutation
    pub version: u64,
    /// Phase 7: Whether document has un-uploaded changes
    pub dirty: bool,
}

/// Phase 2: Content index v2 — multi-index lookups eliminating all OPFS scans.
/// All map fields use BTreeMap (not HashMap) to guarantee deterministic
/// serialization ordering for manifest CRC integrity checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentIndex {
    pub generation: StorageGeneration,
    /// Legacy flat key→entry map (backward compat)
    pub entries: std::collections::BTreeMap<IndexKey, IndexEntry>,
    /// All live page IDs
    pub live_pages: BTreeSet<PageId>,
    /// doc_id → record_id → entry (O(1) document lookup)
    #[serde(default)]
    pub page_by_document: std::collections::BTreeMap<String, std::collections::BTreeMap<String, IndexEntry>>,
    /// sequence → entry (O(1) sequence-based lookup)
    #[serde(default)]
    pub page_by_sequence: std::collections::BTreeMap<u64, IndexEntry>,
    /// Pages with un-uploaded changes
    #[serde(default)]
    pub dirty_pages: BTreeSet<PageId>,
    /// Reverse index: page_id → IndexKey (O(1) for GC, repair, compaction)
    #[serde(default)]
    pub page_by_id: std::collections::BTreeMap<PageId, IndexKey>,
    /// Tombstoned page IDs (no longer live, kept for recovery)
    #[serde(default)]
    pub tombstoned_pages: BTreeSet<PageId>,
    /// CRC checksum over all index entries (integrity check)
    #[serde(default)]
    pub checksum: u32,
}

impl ContentIndex {
    pub fn new() -> Self {
        Self {
            generation: StorageGeneration(0),
            entries: std::collections::BTreeMap::new(),
            live_pages: BTreeSet::new(),
            page_by_document: std::collections::BTreeMap::new(),
            page_by_sequence: std::collections::BTreeMap::new(),
            dirty_pages: BTreeSet::new(),
            page_by_id: std::collections::BTreeMap::new(),
            tombstoned_pages: BTreeSet::new(),
            checksum: 0,
        }
    }

    pub fn lookup(&self, key: &IndexKey) -> Option<&IndexEntry> {
        self.entries.get(key)
    }

    pub fn lookup_page_id(&self, key: &IndexKey) -> Option<PageId> {
        self.entries.get(key).map(|e| e.page_id)
    }

    /// O(1) document lookup by doc_id + record_id.
    pub fn lookup_document(&self, doc_id: &str, record_id: &str) -> Option<&IndexEntry> {
        self.page_by_document.get(doc_id)?.get(record_id)
    }

    /// O(log n) lookup by sequence.
    pub fn lookup_sequence(&self, seq: u64) -> Option<&IndexEntry> {
        self.page_by_sequence.get(&seq)
    }

    /// Check if a page is dirty (has un-uploaded changes).
    pub fn is_dirty(&self, page_id: PageId) -> bool {
        self.dirty_pages.contains(&page_id)
    }

    /// Number of dirty pages.
    pub fn dirty_count(&self) -> usize {
        self.dirty_pages.len()
    }

    pub fn insert(&mut self, key: IndexKey, entry: IndexEntry) {
        // If page was tombstoned, remove from tombstoned set
        self.tombstoned_pages.remove(&entry.page_id);
        self.live_pages.insert(entry.page_id);
        self.page_by_id.insert(entry.page_id, key.clone());
        self.entries.insert(key.clone(), entry.clone());
        // Populate v2 indexes
        match &key {
            IndexKey::Document { doc_id, record_id } => {
                self.page_by_document
                    .entry(doc_id.clone())
                    .or_default()
                    .insert(record_id.clone(), entry);
            }
            IndexKey::Sequence(seq) => {
                self.page_by_sequence.insert(*seq, entry);
            }
            _ => {}
        }
    }

    /// Insert with explicit dirty tracking.
    pub fn insert_dirty(&mut self, key: IndexKey, entry: IndexEntry) {
        self.dirty_pages.insert(entry.page_id);
        self.insert(key, entry);
    }

    pub fn remove(&mut self, key: &IndexKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.live_pages.remove(&entry.page_id);
            self.dirty_pages.remove(&entry.page_id);
            self.page_by_id.remove(&entry.page_id);
            // Track tombstoned for GC
            self.tombstoned_pages.insert(entry.page_id);
            // Clean v2 indexes
            match key {
                IndexKey::Document { doc_id, record_id } => {
                    if let Some(docs) = self.page_by_document.get_mut(doc_id) {
                        docs.remove(record_id);
                        if docs.is_empty() {
                            self.page_by_document.remove(doc_id);
                        }
                    }
                }
                IndexKey::Sequence(seq) => {
                    self.page_by_sequence.remove(seq);
                }
                _ => {}
            }
        }
    }

    /// Remove an entry by page_id (used during GC/repair when only page_id is known).
    pub fn remove_by_page_id(&mut self, page_id: PageId) -> Option<IndexKey> {
        if let Some(key) = self.page_by_id.remove(&page_id) {
            self.remove(&key);
            // Ensure the page is removed from live_pages even for stub entries
            // (stubs exist in page_by_id but not in entries, so remove() is a no-op).
            self.live_pages.remove(&page_id);
            self.tombstoned_pages.insert(page_id);
            Some(key)
        } else {
            self.live_pages.remove(&page_id);
            self.tombstoned_pages.insert(page_id);
            None
        }
    }

    /// Reverse lookup: get the IndexKey for a page_id.
    pub fn lookup_key_by_page_id(&self, page_id: PageId) -> Option<&IndexKey> {
        self.page_by_id.get(&page_id)
    }

    /// Sprint A: Repair orphan pages — pages in live_pages that have no corresponding
    /// entry in `page_by_id` (and thus no way to be looked up). These are created by
    /// earlier tombstone failures (NotReadableError) where the page file was left on
    /// disk but the index entry was removed.
    /// Returns the number of orphans removed.
    pub fn repair_orphans(&mut self) -> usize {
        // Skip when page_by_id is empty — the manifest was just rebuilt from an
        // OPFS directory scan which populates live_pages but not page_by_id.
        // Without page_by_id, every live page would falsely appear orphaned.
        if self.page_by_id.is_empty() {
            return 0;
        }
        let orphan_ids: Vec<PageId> = self
            .live_pages
            .iter()
            .filter(|pid| !self.page_by_id.contains_key(pid))
            .copied()
            .collect();
        let count = orphan_ids.len();
        for pid in &orphan_ids {
            self.live_pages.remove(pid);
            self.tombstoned_pages.insert(*pid);
        }
        if count > 0 {
            engine_info!(
                "[ContentIndex] repaired {} orphan pages (live but untracked)",
                count
            );
        }
        count
    }

    /// Mark a page as dirty (un-uploaded changes).
    pub fn mark_dirty(&mut self, page_id: PageId) {
        self.dirty_pages.insert(page_id);
    }

    /// Mark a page as clean (upload confirmed).
    pub fn mark_clean(&mut self, page_id: PageId) {
        self.dirty_pages.remove(&page_id);
    }

    pub async fn async_rebuild_from_scan(dir: &FileSystemDirectoryHandle) -> Result<Self, VaultSyncError> {
        let mut index = ContentIndex::new();
        let iter = dir.entries();
        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };
            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };
            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };
            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if done { break; }
            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                }) {
                Some(n) => n,
                None => continue,
            };
            if let Some(id) = parse_page_id(&name) {
                index.live_pages.insert(id);
            }
        }
        Ok(index)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub version: u8,
    pub checksum: u32,
    pub flags: u8,
    pub data_len: u32,
}

/// Phase 1: Merged StoreManifest with embedded ContentIndex.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreManifest {
    pub version: u8,
    pub generation: StorageGeneration,
    pub highest_page_id: PageId,
    pub current_page: PageId,
    pub last_sequence: u64,
    pub pending_count: usize,
    pub live_pages: usize,
    pub tombstoned_pages: usize,
    pub created_at: u64,
    pub updated_at: u64,
    pub content_index: ContentIndex,
    pub checksum: u32,
    /// Tracks the ContentIndex serialization format. Distinguishes format migration
    /// (serialization_version < CURRENT) from genuine corruption (version >= CURRENT but CRC fails).
    #[serde(default)]
    pub serialization_version: u16,
}

/// Serialization version tracking. Increment when ContentIndex structure changes.
/// Version 1: complete ContentIndex v2 (page_by_document, page_by_id, tombstoned_pages).
pub(crate) const CURRENT_SERIALIZATION_VERSION: u16 = 1;

impl StoreManifest {
    fn new() -> Self {
        let now = js_sys::Date::now() as u64;
        let mut m = Self {
            version: 2,
            generation: StorageGeneration(0),
            highest_page_id: 0,
            current_page: 0,
            last_sequence: 0,
            pending_count: 0,
            live_pages: 0,
            tombstoned_pages: 0,
            created_at: now,
            updated_at: now,
            content_index: ContentIndex::new(),
            checksum: 0,
            serialization_version: CURRENT_SERIALIZATION_VERSION,
        };
        m.checksum = m.compute_checksum();
        m
    }

    pub fn compute_checksum(&self) -> u32 {
        let mut copy = self.clone();
        copy.checksum = 0;
        let buf = match postcard::to_allocvec(&copy) {
            Ok(buf) => buf,
            Err(e) => {
                engine_warn!("[checksum] serialization failed: {:?}", e);
                return 0;
            }
        };
        crc32fast::hash(&buf)
    }

    pub fn verify(&self) -> bool {
        let expected = self.checksum;
        let mut copy = self.clone();
        copy.checksum = 0;
        copy.compute_checksum() == expected
    }

    pub fn increment_generation(&mut self) {
        self.generation = StorageGeneration(self.generation.0 + 1);
    }
}

/// Debug-only metrics for storage layer performance analysis.
/// Tracks page-level operations to identify bottlenecks before/after caching.
#[cfg(debug_assertions)]
#[derive(Debug, Default)]
pub struct StorageMetrics {
    /// Number of full OPFS directory enumerations (list_page_ids calls)
    pub page_enumerations: AtomicU64,
    /// Number of individual page file reads (read_page calls, excluding headers)
    pub page_reads: AtomicU64,
    /// Number of page header reads (within list_page_ids for tombstone filtering)
    pub header_reads: AtomicU64,
    /// Number of page writes (write_page + write_page_raw)
    pub page_writes: AtomicU64,
    /// Number of page tombsone operations
    pub page_tombstones: AtomicU64,
    /// Cache hits (after Phase 3 — broadcast-invalidated page cache)
    pub cache_hits: AtomicU64,
    /// Cache misses (after Phase 3)
    pub cache_misses: AtomicU64,
}

#[cfg(debug_assertions)]
impl StorageMetrics {
    pub fn snapshot(&self) -> String {
        format!(
            "enumerations={} reads={} headers={} writes={} tombstones={} cache_hits={} cache_misses={}",
            self.page_enumerations.load(Ordering::Relaxed),
            self.page_reads.load(Ordering::Relaxed),
            self.header_reads.load(Ordering::Relaxed),
            self.page_writes.load(Ordering::Relaxed),
            self.page_tombstones.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }
}

/// Phase 3: PageCache — LRU cache inside PageStore for frequently-accessed pages.
#[derive(Debug)]
pub struct PageCache {
    max_entries: usize,
    max_bytes: usize,
    entries: HashMap<PageId, CachedPage>,
    order: VecDeque<PageId>,
    current_bytes: usize,
}

/// Phase 2: Transaction operation types for page writes.
/// Carries index key info so commit_tx() can atomically update ContentIndex.
#[derive(Debug, Clone)]
pub enum PageTxOp {
    Write { page_id: PageId, key: Option<IndexKey> },
    Tombstone { page_id: PageId },
    Delete { page_id: PageId },
}

/// Phase 2: In-flight page write transaction. Defers manifest/index
/// persistence until commit_tx().
#[derive(Debug)]
pub struct PageTransaction {
    pub ops: Vec<PageTxOp>,
    pub started_at: u64,
}

#[derive(Debug, Clone)]
struct CachedPage {
    data: Arc<Vec<u8>>,
    last_access: u64,
    size: usize,
}

impl PageCache {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
            entries: HashMap::new(),
            order: VecDeque::new(),
            current_bytes: 0,
        }
    }

    pub fn get(&mut self, page_id: PageId) -> Option<Arc<Vec<u8>>> {
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.last_access = js_sys::Date::now() as u64;
            // Move to back (most recently used)
            if let Some(pos) = self.order.iter().position(|id| *id == page_id) {
                self.order.remove(pos);
                self.order.push_back(page_id);
            }
            Some(Arc::clone(&entry.data))
        } else {
            None
        }
    }

    pub fn insert(&mut self, page_id: PageId, data: Vec<u8>) {
        let size = data.len();
        // If already present, update
        if let Some(existing) = self.entries.get_mut(&page_id) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.size);
            existing.data = Arc::new(data);
            existing.size = size;
            existing.last_access = js_sys::Date::now() as u64;
            self.current_bytes += size;
            return;
        }
        // Evict if needed
        while self.entries.len() >= self.max_entries || self.current_bytes + size > self.max_bytes {
            if !self.evict_one() {
                break;
            }
        }
        self.entries.insert(page_id, CachedPage {
            data: Arc::new(data),
            last_access: js_sys::Date::now() as u64,
            size,
        });
        self.order.push_back(page_id);
        self.current_bytes += size;
    }

    pub fn remove(&mut self, page_id: PageId) {
        if let Some(entry) = self.entries.remove(&page_id) {
            self.current_bytes = self.current_bytes.saturating_sub(entry.size);
            if let Some(pos) = self.order.iter().position(|id| *id == page_id) {
                self.order.remove(pos);
            }
        }
    }

    fn evict_one(&mut self) -> bool {
        if let Some(page_id) = self.order.pop_front() {
            if let Some(entry) = self.entries.remove(&page_id) {
                self.current_bytes = self.current_bytes.saturating_sub(entry.size);
                #[cfg(debug_assertions)]
                tracing::trace!("[page_cache] evicted page {} ({} bytes)", page_id, entry.size);
                return true;
            }
        }
        false
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.current_bytes = 0;
    }
}

#[derive(Debug)]
pub struct PageStore {
    dir: Arc<FileSystemDirectoryHandle>,
    store_name: Arc<String>,
    gc_needed: Arc<AtomicBool>,
    split_pending: Arc<AtomicBool>,
    pub(crate) manifest: Arc<StdMutex<Option<StoreManifest>>>,
    page_cache: Arc<StdMutex<PageCache>>,
    cached_pending_count: Arc<AtomicUsize>,
    /// Serializes manifest writes through async mutex.
    /// Prevents TOCTOU races: only one persist_manifest() runs at a time.
    manifest_write_lock: Arc<futures::lock::Mutex<()>>,
    #[cfg(debug_assertions)]
    metrics: Arc<StorageMetrics>,
}

impl Clone for PageStore {
    fn clone(&self) -> Self {
        Self {
            dir: Arc::clone(&self.dir),
            store_name: Arc::clone(&self.store_name),
            gc_needed: Arc::clone(&self.gc_needed),
            split_pending: Arc::clone(&self.split_pending),
            manifest: Arc::clone(&self.manifest),
            page_cache: Arc::clone(&self.page_cache),
            cached_pending_count: Arc::clone(&self.cached_pending_count),
            manifest_write_lock: Arc::clone(&self.manifest_write_lock),
            #[cfg(debug_assertions)]
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl PageStore {
    pub async fn open(
        root: &FileSystemDirectoryHandle,
        store_name: &str,
    ) -> Result<Self, VaultSyncError> {
        let _t0 = js_sys::Date::now();
        let dir = ensure_dir(root, store_name).await?;
        let t_ensure = (js_sys::Date::now() - _t0) as u64;
        let (manifest, did_rebuild) = match read_manifest_with_diagnostics(&dir, store_name).await {
            Some(m) if m.verify() => {
                let t_read = (js_sys::Date::now() - _t0) as u64;
                engine_info!(
                    "[PageStore::open] {}: read_manifest {}ms (valid) current_page={} gen={} live={} tombstoned={}",
                    store_name, t_read, m.current_page, m.generation.0,
                    m.content_index.live_pages.len(), m.content_index.tombstoned_pages.len(),
                );
                (m, false)
            }
            Some(m) => {
                // Manifest loaded but verify() failed
                let t_read = (js_sys::Date::now() - _t0) as u64;
                engine_warn!(
                    "[PageStore::open] {}: manifest VERIFY FAILED (t={}ms) current_page={} gen={} checksum=0x{:08X} live={} tombstoned={} — rebuilding",
                    store_name, t_read, m.current_page, m.generation.0, m.checksum,
                    m.content_index.live_pages.len(), m.content_index.tombstoned_pages.len(),
                );
                let rebuilt = rebuild_manifest_with_index(&dir, store_name).await?;
                write_manifest(&dir, store_name, "open_verify_failed", &rebuilt).await?;
                let t_rebuild = (js_sys::Date::now() - _t0) as u64;
                engine_info!(
                    "[PageStore::open] {}: rebuild_manifest {}ms (after verify fail) current_page=0 gen={}",
                    store_name, t_rebuild, rebuilt.generation.0,
                );
                (rebuilt, true)
            }
            None => {
                // No manifest at all (first boot or missing file)
                let t_read = (js_sys::Date::now() - _t0) as u64;
                engine_info!("[PageStore::open] {}: no manifest found (t={}ms) — rebuilding", store_name, t_read);
                let rebuilt = rebuild_manifest_with_index(&dir, store_name).await?;
                write_manifest(&dir, store_name, "open_fresh", &rebuilt).await?;
                let t_rebuild = (js_sys::Date::now() - _t0) as u64;
                engine_info!(
                    "[PageStore::open] {}: rebuild_manifest {}ms (fresh) current_page=0 gen={}",
                    store_name, t_rebuild, rebuilt.generation.0,
                );
                (rebuilt, true)
            }
        };
        let total_t = (js_sys::Date::now() - _t0) as u64;
        if total_t > 50 {
            engine_info!("[PageStore::open] {}: total {}ms (ensure_dir={}ms, rebuild={})", store_name, total_t, t_ensure, did_rebuild);
        }
        let cache = PageCache::new(500, 20 * 1024 * 1024);
        let initial_pending = manifest.pending_count;
        Ok(Self {
            dir: Arc::new(dir),
            store_name: Arc::new(store_name.to_string()),
            gc_needed: Arc::new(AtomicBool::new(false)),
            split_pending: Arc::new(AtomicBool::new(false)),
            manifest: Arc::new(StdMutex::new(Some(manifest))),
            page_cache: Arc::new(StdMutex::new(cache)),
            cached_pending_count: Arc::new(AtomicUsize::new(initial_pending)),
            manifest_write_lock: Arc::new(futures::lock::Mutex::new(())),
            #[cfg(debug_assertions)]
            metrics: Arc::new(StorageMetrics::default()),
        })
    }

    /// Expose directory handle for recovery rebuild
    pub fn dir(&self) -> &FileSystemDirectoryHandle {
        &self.dir
    }

    /// Return the store name (e.g. "sync_states", "schemas", "oplog").
    pub fn store_name(&self) -> &str {
        &self.store_name
    }

    /// Return page cache usage stats: (current_entries, current_bytes)
    pub fn page_cache_stats(&self) -> (usize, usize) {
        let cache = self.page_cache.lock().unwrap();
        (cache.entries.len(), cache.current_bytes)
    }

    /// Phase 1: list_page_ids returns live_pages from ContentIndex — no OPFS enumeration.
    pub async fn list_page_ids(&self, caller: &str) -> Result<Vec<PageId>, VaultSyncError> {
        #[cfg(debug_assertions)]
        self.metrics.page_enumerations.fetch_add(1, Ordering::Relaxed);

        let guard = self.manifest.lock().unwrap();
        if let Some(ref manifest) = *guard {
            let ids: Vec<PageId> = manifest.content_index.live_pages.iter().copied().collect();
            engine_debug!("[list_page_ids] caller={} pages={} (from index)", caller, ids.len());
            Ok(ids)
        } else {
            Ok(Vec::new())
        }
    }

    /// Phase 1: content_index_lookup — single O(1) lookup instead of OPFS scan.
    /// Checks entries HashMap first, then falls back to page_by_document for Document keys.
    pub fn content_index_lookup(&self, key: &IndexKey) -> Option<IndexEntry> {
        let guard = self.manifest.lock().unwrap();
        let ci = guard.as_ref()?;
        if let Some(entry) = ci.content_index.lookup(key) {
            return Some(entry.clone());
        }
        match key {
            IndexKey::Document { doc_id, record_id } => {
                ci.content_index.lookup_document(doc_id, record_id).cloned()
            }
            _ => None,
        }
    }

    /// Phase 1: update_content_index after write.
    pub(crate) async fn update_content_index(&self, key: IndexKey, page_id: PageId) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut manifest) = *guard {
                manifest.increment_generation();
                let entry = IndexEntry {
                    page_id,
                    storage_gen: manifest.generation,
                    checksum: 0,
                    size: 0,
                    last_modified: js_sys::Date::now() as u64,
                    version: 1,
                    dirty: true,
                };
                manifest.content_index.insert(key, entry);
                manifest.live_pages = manifest.content_index.live_pages.len();
                manifest.updated_at = js_sys::Date::now() as u64;
                Some(manifest.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "mark_document_written", m).await?;
        }
        Ok(())
    }

    /// Phase 7: Mark an entry as dirty (has un-uploaded changes).
    pub(crate) async fn mark_index_dirty(&self, key: &IndexKey) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut manifest) = *guard {
                if let Some(entry) = manifest.content_index.entries.get_mut(key) {
                    entry.dirty = true;
                    entry.version += 1;
                    entry.last_modified = js_sys::Date::now() as u64;
                }
                manifest.increment_generation();
                manifest.updated_at = js_sys::Date::now() as u64;
                Some(manifest.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "mark_index_dirty", m).await?;
        }
        Ok(())
    }

    /// Phase 7: Mark an entry as clean (uploaded successfully).
    pub(crate) async fn mark_index_clean(&self, key: &IndexKey) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut manifest) = *guard {
                if let Some(entry) = manifest.content_index.entries.get_mut(key) {
                    entry.dirty = false;
                }
                manifest.increment_generation();
                manifest.updated_at = js_sys::Date::now() as u64;
                Some(manifest.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "mark_index_clean", m).await?;
        }
        Ok(())
    }

    /// Phase 7: Return number of dirty (un-uploaded) entries.
    pub fn dirty_count(&self) -> usize {
        let guard = self.manifest.lock().unwrap();
        guard.as_ref()
            .map(|m| m.content_index.entries.values().filter(|e| e.dirty).count())
            .unwrap_or(0)
    }

    /// Phase 1: remove from content_index after tombstone.
    pub(crate) async fn remove_from_index(&self, key: &IndexKey) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut manifest) = *guard {
                manifest.increment_generation();
                manifest.content_index.remove(key);
                manifest.tombstoned_pages += 1;
                manifest.live_pages = manifest.content_index.live_pages.len();
                manifest.updated_at = js_sys::Date::now() as u64;
                Some(manifest.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "remove_from_index", m).await?;
        }
        Ok(())
    }

    /// Allocate a page ID with a structured reason for auditing.
    /// Logs store name, page ID, and reason for every allocation.
    /// Commit a pre-allocated page ID to ContentIndex (used when StorageRuntime
    /// owns allocation). Updates ContentIndex + manifest without incrementing
    /// highest_page_id.
    pub async fn commit_allocated_page_id(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let manifest_copy = {
            let mut guard = self.manifest.lock().unwrap();
            let manifest = guard.as_mut().unwrap();
            manifest.content_index.live_pages.insert(page_id);
            if !manifest.content_index.page_by_id.contains_key(&page_id) {
                manifest.content_index.page_by_id.insert(page_id, IndexKey::Sequence(page_id));
            }
            manifest.live_pages += 1;
            manifest.updated_at = js_sys::Date::now() as u64;
            engine_debug!(
                "[PageAlloc] store={} reason=commit_allocated page={}",
                self.store_name, page_id
            );
            manifest.clone()
        };
        write_manifest(&self.dir, &self.store_name, "commit_allocated_page_id", &manifest_copy).await?;
        Ok(())
    }

    pub async fn allocate_page_id(&self, reason: &str) -> Result<PageId, VaultSyncError> {
        let manifest_copy = {
            let mut guard = self.manifest.lock().unwrap();
            let manifest = guard.as_mut().unwrap();
            let page_id = manifest.highest_page_id;
            if page_id < 100 {
                // Reserved system range 0-99: skip to 100 on first allocation
                manifest.highest_page_id = 101;
                manifest.content_index.live_pages.insert(100);
                if !manifest.content_index.page_by_id.contains_key(&100) {
                    manifest.content_index.page_by_id.insert(100, IndexKey::Sequence(100));
                }
                manifest.updated_at = js_sys::Date::now() as u64;
                engine_debug!(
                    "[PageAlloc] store={} reason={} page={} (skipped reserved range)",
                    self.store_name, reason, 100
                );
                (100u64, manifest.clone())
            } else {
                manifest.highest_page_id = page_id + 1;
                manifest.content_index.live_pages.insert(page_id);
                if !manifest.content_index.page_by_id.contains_key(&page_id) {
                    manifest.content_index.page_by_id.insert(page_id, IndexKey::Sequence(page_id));
                }
                manifest.updated_at = js_sys::Date::now() as u64;
                engine_debug!(
                    "[PageAlloc] store={} reason={} page={}",
                    self.store_name, reason, page_id
                );
                (page_id, manifest.clone())
            }
        };
        write_manifest(&self.dir, &self.store_name, "allocate_page_id", &manifest_copy.1).await?;
        Ok(manifest_copy.0)
    }

    /// Write raw page data without any split checks. Used by split_page internally.
    /// Does NOT update ContentIndex — the caller (write_page, commit_tx) is responsible
    /// for updating `live_pages`, `page_by_id`, and all other ContentIndex fields.
    async fn write_page_raw(&self, page_id: PageId, data: &[u8]) -> Result<(), VaultSyncError> {
        #[cfg(debug_assertions)]
        self.metrics.page_writes.fetch_add(1, Ordering::Relaxed);

        let buf = encode_v3_page(data, 0)?;
        write_file(&self.dir, &page_filename(page_id), &buf).await?;

        // Phase 3: cache the written data
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.insert(page_id, data.to_vec());
        }

        Ok(())
    }

    /// Write a page and atomically update ContentIndex via an internal transaction.
    /// This is the canonical write path — ensures `live_pages`, `page_by_id`, and
    /// all ContentIndex indexes are updated together.
    pub async fn write_page(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        // Phase 2: Bounded Pages — check multi-metric thresholds.
        let entry_count = Self::estimate_entry_count(data);

        // Hard threshold: synchronous emergency split before write.
        if data.len() > Self::HARD_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE * 2 {
            engine_warn!("[page_store] emergency split page {}: {} bytes, ~{} entries", page_id, data.len(), entry_count);
            // Write to a temporary page, split it, and return instead of writing the oversized page.
            let tmp_id = self.allocate_page_id("emergency_split").await?;
            self.write_page_raw(tmp_id, data).await?;
            return self.split_page(tmp_id).await;
        }

        // Soft threshold: schedule background split.
        if data.len() > Self::SOFT_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE {
            engine_debug!("[page_store] scheduling background split for page {}: {} bytes, ~{} entries", page_id, data.len(), entry_count);
            self.split_pending.store(true, Ordering::Release);
        }

        // Write to OPFS and atomically update ContentIndex
        self.write_page_raw(page_id, data).await?;
        self.commit_page_write(page_id, None).await
    }

    pub async fn write_page_verify(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        let tmp_id = page_id.wrapping_add(0x8000_0000_0000_0000);
        let buf = encode_v3_page(data, 0)?;

        write_file(&self.dir, &page_filename(tmp_id), &buf).await?;

        let read_back = read_page_file(&self.dir, &page_filename(tmp_id)).await?;
        match read_back {
            Some((_, read_data)) if read_data == data => {}
            Some(_) => {
                let _ = delete_file(&self.dir, &page_filename(tmp_id)).await;
                return Err(VaultSyncError::Storage("verify mismatch on write".into()));
            }
            None => {
                return Err(VaultSyncError::Storage("verify read failed".into()));
            }
        }

        rename_file(&self.dir, &page_filename(tmp_id), &page_filename(page_id)).await?;
        self.commit_page_write(page_id, None).await
    }

    pub async fn read_page(&self, page_id: PageId) -> Result<Option<Vec<u8>>, VaultSyncError> {
        // Phase 3: Check page cache first
        {
            let mut cache = self.page_cache.lock().unwrap();
            if let Some(cached) = cache.get(page_id) {
                #[cfg(debug_assertions)]
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some((*cached).clone()));
            }
        }
        #[cfg(debug_assertions)]
        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        #[cfg(debug_assertions)]
        self.metrics.page_reads.fetch_add(1, Ordering::Relaxed);

        let (header, data) = match read_page_file(&self.dir, &page_filename(page_id)).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let actual_checksum = crc32fast::hash(&data);
        if header.version >= 1 && actual_checksum != header.checksum {
            return Err(VaultSyncError::Storage(format!(
                "checksum mismatch page {}: expected {} got {}",
                page_id, header.checksum, actual_checksum
            )));
        }
        if header.flags & 0x02 != 0 {
            return Ok(None);
        }
        // Phase 3: cache the read data
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.insert(page_id, data.clone());
        }
        Ok(Some(data))
    }

    pub async fn delete_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        delete_file(&self.dir, &page_filename(page_id)).await?;
        // Phase 3: remove from cache
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.remove(page_id);
        }
        // Phase 1: use cached manifest
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut m) = *guard {
                m.increment_generation();
                if m.live_pages > 0 {
                    m.live_pages = m.live_pages.saturating_sub(1);
                } else if m.tombstoned_pages > 0 {
                    m.tombstoned_pages = m.tombstoned_pages.saturating_sub(1);
                }
                m.updated_at = js_sys::Date::now() as u64;
                Some(m.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "delete_page", m).await?;
        }
        Ok(())
    }

    pub async fn tombstone_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        #[cfg(debug_assertions)]
        self.metrics.page_tombstones.fetch_add(1, Ordering::Relaxed);

        let name = page_filename(page_id);
        let existing = match read_all_bytes(&self.dir, &name).await {
            Ok(b) => b,
            Err(e) if is_not_found(&e) || is_not_readable(&e) => {
                engine_debug!("[page_store] tombstone_page {}: page already removed or unreadable", page_id);
                // Clean up ContentIndex even if OPFS file is gone/unreadable
                self.remove_from_manifest_index(page_id).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let data = extract_page_data(&existing)?;
        let buf = encode_v3_page(&data, 0x02)?;
        write_file(&self.dir, &name, &buf).await?;

        // Phase 3: remove from cache
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.remove(page_id);
        }

        // Phase 1: update cached manifest — clean ALL ContentIndex entries
        self.remove_from_manifest_index(page_id).await?;
        Ok(())
    }

    /// Remove a page from all ContentIndex indexes and persist manifest.
    async fn remove_from_manifest_index(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut m) = *guard {
                m.increment_generation();
                m.content_index.remove_by_page_id(page_id);
                m.live_pages = m.content_index.live_pages.len();
                m.tombstoned_pages = m.content_index.tombstoned_pages.len();
                m.updated_at = js_sys::Date::now() as u64;
                Some(m.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "remove_from_manifest_index", m).await?;
        }
        Ok(())
    }

    /// Atomically update ContentIndex after writing a page.
    /// Adds to both `live_pages` and `page_by_id` so the page is tracked
    /// in all indexes and not flagged as an orphan by `repair_orphans()`.
    /// When `key` is provided, also adds to `entries` and document/sequence indexes.
    async fn commit_page_write(&self, page_id: PageId, key: Option<IndexKey>) -> Result<(), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut m) = *guard {
                m.increment_generation();
                m.content_index.live_pages.insert(page_id);
                // Add to page_by_id with a stub key if no real key is available.
                // Without this, repair_orphans() would tombstone the page on next startup.
                if !m.content_index.page_by_id.contains_key(&page_id) {
                    let stub_key = key.clone().unwrap_or_else(|| IndexKey::Sequence(page_id));
                    m.content_index.page_by_id.insert(page_id, stub_key);
                }
                if let Some(index_key) = key {
                    let entry = IndexEntry {
                        page_id,
                        storage_gen: m.generation,
                        checksum: 0,
                        size: 0,
                        last_modified: js_sys::Date::now() as u64,
                        version: 1,
                        dirty: true,
                    };
                    m.content_index.insert(index_key, entry);
                }
                m.live_pages = m.content_index.live_pages.len();
                m.updated_at = js_sys::Date::now() as u64;
                Some(m.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "commit_page_write", m).await?;
        }
        Ok(())
    }

    /// Recover the store by rebuilding the manifest from OPFS scan.
    /// This is the only public entry point for manifest repair.
    pub async fn recover(&self) -> Result<(), VaultSyncError> {
        engine_warn!("[PageStore] recovering manifest from OPFS scan");
        let rebuilt = rebuild_manifest_with_index(&self.dir, &self.store_name).await?;
        write_manifest(&self.dir, &self.store_name, "recover", &rebuilt).await
    }

    /// Remove pages that have the tombstone flag (0x02) set.
    /// Returns the number of pages cleaned up.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, VaultSyncError> {
        let mut cleaned = 0usize;
        let old_current = {
            let guard = self.manifest.lock().unwrap();
            guard.as_ref().map(|m| m.current_page).unwrap_or(0)
        };
        let old_live = {
            let guard = self.manifest.lock().unwrap();
            guard.as_ref().map(|m| m.live_pages).unwrap_or(0)
        };
        engine_info!(
            "[cleanup] store={} old_current_page={} old_live_pages={}",
            self.store_name, old_current, old_live,
        );
        let iter = self.dir.entries();

        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };

            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };

            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };

            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if done {
                break;
            }

            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                })
            {
                Some(n) => n,
                None => continue,
            };

            if let Some(id) = parse_page_id(&name) {
                if id == 0 {
                    // Phase 6: Check for stale 0.page (old WAL checkpoint format, JSON-based).
                    // Read first byte to detect JSON vs binary format.
                    let bytes = read_header_bytes(&self.dir, &name).await.unwrap_or_default();
                    if bytes.first() == Some(&b'{') {
                        engine_info!("[cleanup] removing stale JSON checkpoint 0.page");
                        delete_file(&self.dir, &name).await?;
                        cleaned += 1;
                        continue;
                    }
                }
                match read_page_header(&self.dir, &name).await {
                    Ok(Some(header)) if header.flags & 0x02 != 0 => {
                        delete_file(&self.dir, &name).await?;
                        cleaned += 1;
                    }
                    Ok(_) => {}
                    Err(ref e) if e.to_string().contains("empty page") => {
                        engine_info!("[cleanup] removing empty/corrupt page {}", name);
                        delete_file(&self.dir, &name).await?;
                        cleaned += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        if cleaned > 0 {
            // After cleanup, rebuild manifest with ContentIndex to populate live_pages.
            // Must use rebuild_manifest_with_index — rebuild_manifest creates an empty
            // ContentIndex, causing list_page_ids() to return stale data (Bug C).
            let old_current_for_rebuild = old_current;
            let rebuilt = rebuild_manifest_with_index(&self.dir, &self.store_name).await?;
            engine_info!(
                "[cleanup] store={} cleaned={} old_current_page={} rebuilt_current_page={} rebuilt_live={} rebuilt_highest_page={}",
                self.store_name, cleaned, old_current_for_rebuild,
                rebuilt.current_page, rebuilt.live_pages, rebuilt.highest_page_id,
            );
            self.persist_manifest("cleanup_tombstoned_pages", &rebuilt).await?;
        } else {
            engine_info!("[cleanup] store={} cleaned=0 (no action)", self.store_name);
        }

        Ok(cleaned)
    }

    /// Set the current full-state page ID in the manifest.
    /// Signals that this page represents the complete store state.
    pub async fn set_current_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let old = {
            let guard = self.manifest.lock().unwrap();
            guard.as_ref().map(|m| m.current_page).unwrap_or(0)
        };
        let mut m = {
            let guard = self.manifest.lock().unwrap();
            guard.clone().unwrap_or_else(StoreManifest::new)
        };
        m.current_page = page_id;
        m.updated_at = js_sys::Date::now() as u64;
        let result = self.persist_manifest("set_current_page", &m).await;
        engine_debug!(
            "[set_current_page] store={} old={} new={} gen={}",
            self.store_name,
            old,
            page_id,
            m.generation.0,
        );
        result
    }

    /// Allocate a new page, set current_page to it, and tombstone the old current_page
    /// in ContentIndex — all under a single manifest lock + single OPFS write.
    /// Returns (new_page_id, old_page_id).
    /// The caller must still write page data to OPFS via write_page_raw_data and
    /// tombstone the old page file via tombstone_page_file_only.
    pub async fn allocate_and_rotate(&self, reason: &str) -> Result<(PageId, PageId), VaultSyncError> {
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            let manifest = guard.as_mut().unwrap();
            let old_page = manifest.current_page;

            // Allocate new page ID
            let new_page = if manifest.highest_page_id < 100 {
                manifest.highest_page_id = 101;
                100u64
            } else {
                let pid = manifest.highest_page_id;
                manifest.highest_page_id = pid + 1;
                pid
            };

            manifest.content_index.live_pages.insert(new_page);
            if !manifest.content_index.page_by_id.contains_key(&new_page) {
                manifest.content_index.page_by_id.insert(new_page, IndexKey::Sequence(new_page));
            }
            manifest.current_page = new_page;

            // Tombstone old page in ContentIndex (not OPFS — caller does that)
            if old_page > 0 && old_page != new_page {
                manifest.content_index.remove_by_page_id(old_page);
            }

            manifest.live_pages = manifest.content_index.live_pages.len();
            manifest.tombstoned_pages = manifest.content_index.tombstoned_pages.len();
            manifest.updated_at = js_sys::Date::now() as u64;

            engine_info!(
                "[PageAlloc] allocate_and_rotate store={} this=0x{:x} manifest_arc=0x{:x} reason={} old_current={} new_current={} live={} tombstoned={} gen={}",
                self.store_name,
                self as *const _ as u64,
                Arc::as_ptr(&self.manifest) as u64,
                reason, old_page, new_page,
                manifest.live_pages, manifest.tombstoned_pages,
                manifest.generation.0,
            );

            (new_page, old_page, manifest.clone())
        };
        self.persist_manifest("allocate_and_rotate", &cloned.2).await?;
        Ok((cloned.0, cloned.1))
    }

    /// Write page data to OPFS without any manifest/ContentIndex updates.
    /// Caller must have already set up ContentIndex entries (e.g., via allocate_and_rotate).
    pub(crate) async fn write_page_raw_data(&self, page_id: PageId, data: &[u8]) -> Result<(), VaultSyncError> {
        self.write_page_raw(page_id, data).await
    }

    /// Tombstone a page file in OPFS without updating ContentIndex.
    /// Caller must have already updated ContentIndex (e.g., via allocate_and_rotate).
    pub(crate) async fn tombstone_page_file_only(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let name = page_filename(page_id);
        // Remove from cache
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.remove(page_id);
        }
        let existing = match read_all_bytes(&self.dir, &name).await {
            Ok(b) => b,
            Err(e) if is_not_found(&e) || is_not_readable(&e) => {
                engine_debug!("[page_store] tombstone_page_file_only {}: already removed", page_id);
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let data = extract_page_data(&existing)?;
        let buf = encode_v3_page(&data, 0x02)?;
        write_file(&self.dir, &name, &buf).await
    }

    /// Get the current full-state page ID from the manifest.
    /// Returns 0 if no current page is set (legacy store).
    pub async fn current_page(&self) -> PageId {
        let disk_val = read_manifest(&self.dir)
            .await
            .map(|m| m.current_page)
            .unwrap_or(0);
        let mem_val = {
            let guard = self.manifest.lock().unwrap();
            guard.as_ref().map(|m| m.current_page).unwrap_or(0)
        };
        engine_trace!(
            "[PageStore::current_page] store={} mem={} disk={}",
            self.store_name,
            mem_val,
            disk_val,
        );
        disk_val
    }

    /// Track last sequence number in the manifest. pending_count is no longer
    /// persisted — it's maintained in cached_pending_count (AtomicUsize) and
    /// injected into the manifest struct at persist time.
    pub async fn set_manifest_meta(
        &self,
        last_sequence: u64,
        _pending_count: usize,
    ) -> Result<(), VaultSyncError> {
        let mut m = {
            let guard = self.manifest.lock().unwrap();
            guard.clone().unwrap_or_else(StoreManifest::new)
        };
        m.last_sequence = last_sequence;
        m.updated_at = js_sys::Date::now() as u64;
        self.persist_manifest("set_manifest_meta", &m).await
    }

    /// Mark that GC is needed (called after a full-state rewrite).
    pub fn schedule_gc(&self) {
        self.gc_needed.store(true, Ordering::Release);
    }

    /// Get a snapshot of debug storage metrics. Returns empty string in release builds.
    pub fn metrics_snapshot(&self) -> String {
        #[cfg(debug_assertions)]
        { self.metrics.snapshot() }
        #[cfg(not(debug_assertions))]
        { String::new() }
    }

    // ── Page splitting (Phase 2: Bounded Pages) ──────────────────────

    /// Thresholds for page splitting.
    const TARGET_PAGE_BYTES: usize = 4_000_000;
    const SOFT_SPLIT_BYTES: usize = 8_000_000;
    const HARD_SPLIT_BYTES: usize = 16_000_000;
    const MAX_ENTRIES_PER_PAGE: usize = 4096;

    /// Check whether a page should be split based on multiple metrics.
    pub fn should_split(&self, data: &[u8], entry_count_hint: usize) -> bool {
        data.len() > Self::HARD_SPLIT_BYTES
            || entry_count_hint > Self::MAX_ENTRIES_PER_PAGE
            || data.len() > Self::SOFT_SPLIT_BYTES
    }

    /// Estimate the number of entries in a postcard-encoded page.
    /// Tries to decode as Vec<T> first; if that fails, estimates from byte length.
    fn estimate_entry_count(data: &[u8]) -> usize {
        // Try to decode as Vec<OplogEntry> (the most common case)
        if let Ok(vec) = postcard::from_bytes::<Vec<vaultsync_core::oplog::entry::OplogEntry>>(data) {
            return vec.len();
        }
        // Fallback: assume average entry size of ~256 bytes
        if data.is_empty() {
            return 0;
        }
        std::cmp::max(1, data.len() / 256)
    }

    /// Read a page, split its entries into two balanced pages, write both, and GC the original.
    pub async fn split_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let data = match self.read_page(page_id).await? {
            Some(d) => d,
            None => return Ok(()),
        };

        // Try to split entries evenly
        let entries: Vec<Vec<u8>> = if let Ok(vec) = postcard::from_bytes::<Vec<vaultsync_core::oplog::entry::OplogEntry>>(&data) {
            let mid = vec.len() / 2;
            let left: Vec<_> = vec[..mid].to_vec();
            let right: Vec<_> = vec[mid..].to_vec();
            vec![
                postcard::to_allocvec(&left).map_err(|e| VaultSyncError::Storage(format!("split encode left: {:?}", e)))?,
                postcard::to_allocvec(&right).map_err(|e| VaultSyncError::Storage(format!("split encode right: {:?}", e)))?,
            ]
        } else {
            // Cannot decode — skip splitting this page
            engine_warn!("[page_store] cannot split page {}: not a Vec<OplogEntry>", page_id);
            return Ok(());
        };

        if entries.len() != 2 {
            return Ok(());
        }

        let mut tx = self.begin_tx();
        let left_id = self.allocate_page_id("split_page_left").await?;
        let right_id = self.allocate_page_id("split_page_right").await?;

        self.write_page_tx(&mut tx, left_id, None, &entries[0]).await?;
        self.write_page_tx(&mut tx, right_id, None, &entries[1]).await?;
        self.tombstone_page_tx(&mut tx, page_id).await?;
        self.commit_tx(tx).await?;

        engine_debug!("[page_store] split page {} -> {} and {}", page_id, left_id, right_id);
        Ok(())
    }

    /// Run pending page splits (triggered by write_page for oversized data).
    pub async fn run_pending_split(&self) -> Result<usize, VaultSyncError> {
        if !self.is_split_pending() {
            return Ok(0);
        }

        let page_ids = self.list_page_ids("run_pending_split").await?;
        let mut split_count = 0usize;

        for id in &page_ids {
            if let Some(data) = self.read_page(*id).await? {
                let entry_count = Self::estimate_entry_count(&data);
                if data.len() > Self::SOFT_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE {
                    if let Err(e) = self.split_page(*id).await {
                        engine_warn!("[page_store] split page {} failed: {:?}", id, e);
                    } else {
                        split_count += 1;
                    }
                }
            }
        }

        Ok(split_count)
    }

    /// Schedule a background page split for an oversized page.
    pub async fn schedule_split(&self, page_id: &PageId) {
        self.split_pending.store(true, Ordering::Release);
    }

    /// Check and clear the split-pending flag.
    pub fn is_split_pending(&self) -> bool {
        self.split_pending.swap(false, Ordering::Acquire)
    }

    /// Get pending count from in-memory cache (no OPFS read).
    pub fn pending_count(&self) -> usize {
        self.cached_pending_count.load(Ordering::Acquire)
    }

    /// Set pending count in in-memory cache only (no OPFS write).
    /// The persisted manifest's pending_count is best-effort; it's set on
    /// next manifest persist but never read on startup (we recompute from OPFS).
    pub fn set_pending_count(&self, count: usize) {
        self.cached_pending_count.store(count, Ordering::Release);
    }

    /// Adjust pending count by a delta (positive or negative) in-memory only.
    pub fn adjust_pending_count(&self, delta: i32) {
        let before = self.cached_pending_count.load(Ordering::Acquire);
        let after = if delta >= 0 {
            before.saturating_add(delta as usize)
        } else {
            before.saturating_sub(delta.unsigned_abs() as usize)
        };
        self.cached_pending_count.store(after, Ordering::Release);
        engine_trace!("[pending] cache_update before={} delta={} after={}", before, delta, after);
    }

    // ── Transactional writes (Phase 2) ──

    /// Start a write transaction. Returns a transaction token.
    /// After begin_tx, all writes update in-memory state; persists only on commit.
    pub fn begin_tx(&self) -> PageTransaction {
        PageTransaction {
            ops: Vec::new(),
            started_at: js_sys::Date::now() as u64,
        }
    }

    /// Write a page within a transaction. Updates OPFS immediately but
    /// defers manifest/index persistence until commit_tx.
    /// Pass an IndexKey to have commit_tx() atomically update ContentIndex.
    pub async fn write_page_tx(&self, tx: &mut PageTransaction, page_id: PageId, key: Option<IndexKey>, data: &[u8]) -> Result<(), VaultSyncError> {
        // Write to OPFS immediately (can't defer — OPFS is the durability layer)
        self.write_page_raw(page_id, data).await?;
        tx.ops.push(PageTxOp::Write { page_id, key });
        Ok(())
    }

    /// Tombstone a page within a transaction.
    pub async fn tombstone_page_tx(&self, tx: &mut PageTransaction, page_id: PageId) -> Result<(), VaultSyncError> {
        // Phase 3: mark in cache
        {
            let mut cache = self.page_cache.lock().unwrap();
            cache.remove(page_id);
        }
        let name = page_filename(page_id);
        let existing = match read_all_bytes(&self.dir, &name).await {
            Ok(b) => b,
            Err(e) if is_not_found(&e) => {
                engine_debug!("[page_store] tombstone_page_tx {}: page already removed", page_id);
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let data = extract_page_data(&existing)?;
        let buf = encode_v3_page(&data, 0x02)?;
        write_file(&self.dir, &name, &buf).await?;
        tx.ops.push(PageTxOp::Tombstone { page_id });
        Ok(())
    }

    /// Commit a transaction — persists manifest/index atomically.
    /// Applies all ContentIndex changes (inserts from Write ops, removes from
    /// Tombstone/Delete ops) in a single manifest write.
    /// If commit fails, the OPFS writes are still there (safe) but the
    /// in-memory manifest will be out of sync. Recovery rebuilds from OPFS.
    pub async fn commit_tx(&self, tx: PageTransaction) -> Result<(), VaultSyncError> {
        if tx.ops.is_empty() {
            return Ok(());
        }
        // Apply all ops to the manifest in a single lock + write
        let cloned = {
            let mut guard = self.manifest.lock().unwrap();
            if let Some(ref mut m) = *guard {
                m.increment_generation();
                for op in &tx.ops {
                    match op {
                        PageTxOp::Write { page_id, key } => {
                            if let Some(index_key) = key {
                                // Remove any existing entry for old page_id first
                                m.content_index.remove_by_page_id(*page_id);
                                let entry = IndexEntry {
                                    page_id: *page_id,
                                    storage_gen: m.generation,
                                    checksum: 0,
                                    size: 0,
                                    last_modified: js_sys::Date::now() as u64,
                                    version: 1,
                                    dirty: true,
                                };
                                m.content_index.insert(index_key.clone(), entry);
                            } else {
                                // No key — ensure page is tracked in live_pages + page_by_id
                                m.content_index.live_pages.insert(*page_id);
                                if !m.content_index.page_by_id.contains_key(page_id) {
                                    m.content_index.page_by_id.insert(*page_id, IndexKey::Sequence(*page_id));
                                }
                            }
                        }
                        PageTxOp::Tombstone { page_id } => {
                            m.content_index.remove_by_page_id(*page_id);
                        }
                        PageTxOp::Delete { page_id } => {
                            m.content_index.remove_by_page_id(*page_id);
                        }
                    }
                }
                m.live_pages = m.content_index.live_pages.len();
                m.tombstoned_pages = m.content_index.tombstoned_pages.len();
                m.updated_at = js_sys::Date::now() as u64;
                Some(m.clone())
            } else {
                None
            }
        };
        if let Some(ref m) = cloned {
            write_manifest(&self.dir, &self.store_name, "commit_tx", m).await?;
        }
        let elapsed = (js_sys::Date::now() as u64).saturating_sub(tx.started_at);
        engine_trace!("[page_store] commit_tx: {} ops in {}ms", tx.ops.len(), elapsed);
        Ok(())
    }

    /// Run pending GC if any — deletes pages older than the current full-state page.
    /// Returns the number of pages deleted.
    pub async fn run_pending_gc(&self) -> Result<usize, VaultSyncError> {
        if !self.gc_needed.load(Ordering::Acquire) {
            return Ok(0);
        }

        let manifest = match read_manifest(&self.dir).await {
            Some(m) if m.verify() && m.current_page > 0 => m,
            _ => return Ok(0),
        };

        let current = manifest.current_page;
        let mut deleted = 0usize;

        let iter = self.dir.entries();
        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };
            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };
            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };
            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if done { break; }
            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                }) {
                Some(n) => n,
                None => continue,
            };
            if let Some(id) = parse_page_id(&name) {
                if id < current {
                    if let Err(e) = delete_file(&self.dir, &name).await {
                        if !is_not_found(&e) {
                            return Err(e);
                        }
                    }
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            // Rebuild manifest with full ContentIndex to reflect deleted pages.
            // Must use rebuild_manifest_with_index to populate content_index.live_pages
            // (the BTreeSet that list_page_ids reads from).
            let rebuilt = rebuild_manifest_with_index(&self.dir, &self.store_name).await?;
            let mut final_manifest = rebuilt;
            final_manifest.current_page = current;
            final_manifest.updated_at = js_sys::Date::now() as u64;
            self.persist_manifest("run_pending_gc", &final_manifest).await?;
        }

        self.gc_needed.store(false, Ordering::Release);
        Ok(deleted)
    }

    /// Write manifest to OPFS AND update in-memory cache atomically.
    /// This is the ONLY function that should persist manifests — prevents
    /// cache/disk divergence (the root cause of stale page references in
    /// VersionChain and repeated repair_if_diverged cycles).
    async fn persist_manifest(&self, caller: &'static str, m: &StoreManifest) -> Result<(), VaultSyncError> {
        // Phase 5: Serialize manifest writes through an async mutex.
        // This prevents concurrent writers from racing on the OPFS manifest file.
        // Only one persist_manifest() runs at a time per PageStore.
        let _guard = self.manifest_write_lock.lock().await;

        let mut m = m.clone();
        // Inject in-memory cached pending count into the persisted manifest.
        // pending_count is no longer authoritative from disk — it's best-effort
        // for backward compat with other tabs that may read this manifest.
        m.pending_count = self.cached_pending_count.load(Ordering::Relaxed);
        write_manifest(&self.dir, &self.store_name, caller, &m).await?;
        let mut guard = self.manifest.lock().unwrap();
        *guard = Some(m);
        Ok(())
    }

    /// Persist the current in-memory manifest to disk.
    /// Used by repair mechanisms to make corrections durable.
    pub(crate) async fn persist_current_manifest(&self, caller: &'static str) -> Result<(), VaultSyncError> {
        let m = {
            let guard = self.manifest.lock().unwrap();
            guard.clone()
        };
        if let Some(m) = m {
            self.persist_manifest(caller, &m).await
        } else {
            Ok(())
        }
    }

    /// Assert that the in-memory manifest cache matches the disk copy.
    /// Catches cache/disk divergence that would cause stale page references.
    /// Only active in debug builds — call after cleanup, GC, compaction, or in tests.
    #[cfg(debug_assertions)]
    pub async fn validate_manifest_cache(&self) {
        let disk = read_manifest(&self.dir).await;
        let cache = {
            let guard = self.manifest.lock().unwrap();
            guard.clone()
        };
        if let (Some(ref disk_m), Some(ref cache_m)) = (disk, cache) {
            if disk_m.content_index.live_pages != cache_m.content_index.live_pages {
                engine_warn!(
                    "[validate_manifest_cache] CACHE/DISK DIVERGENCE store={}: cache live_pages={} disk live_pages={}",
                    self.store_name,
                    cache_m.content_index.live_pages.len(),
                    disk_m.content_index.live_pages.len(),
                );
                engine_warn!(
                    "[validate_manifest_cache] cache={:?} disk={:?}",
                    cache_m.content_index.live_pages.iter().copied().collect::<Vec<_>>(),
                    disk_m.content_index.live_pages.iter().copied().collect::<Vec<_>>(),
                );
            }
            if disk_m.content_index.page_by_id != cache_m.content_index.page_by_id {
                engine_warn!(
                    "[validate_manifest_cache] CACHE/DISK PAGE_BY_ID DIVERGENCE store={}",
                    self.store_name,
                );
            }
            if disk_m.current_page != cache_m.current_page {
                engine_warn!(
                    "[validate_manifest_cache] CACHE/DISK CURRENT_PAGE DIVERGENCE store={}: cache={} disk={}",
                    self.store_name, cache_m.current_page, disk_m.current_page,
                );
            }
        } else if disk.is_some() != cache.is_some() {
            engine_warn!(
                "[validate_manifest_cache] CACHE/DISK EXISTENCE DIVERGENCE store={}: cache_exists={} disk_exists={}",
                self.store_name, cache.is_some(), disk.is_some(),
            );
        }
    }
}

fn page_filename(page_id: PageId) -> String {
    format!("{}.page", page_id)
}

fn parse_page_id(name: &str) -> Option<PageId> {
    if let Some(stripped) = name.strip_suffix(".page") {
        stripped.parse::<PageId>().ok()
    } else {
        None
    }
}

fn encode_v3_page(data: &[u8], flags: u8) -> Result<Vec<u8>, VaultSyncError> {
    let checksum = crc32fast::hash(data);
    let header = PageHeader {
        version: 1,
        checksum,
        flags,
        data_len: data.len() as u32,
    };
    let header_bytes = postcard::to_allocvec(&header)
        .map_err(|e| VaultSyncError::Storage(format!("header encode: {:?}", e)))?;

    let mut buf = Vec::with_capacity(PAGE_MAGIC.len() + header_bytes.len() + data.len());
    buf.extend_from_slice(&PAGE_MAGIC);
    buf.extend_from_slice(&header_bytes);
    buf.extend_from_slice(data);
    Ok(buf)
}

fn encoded_header_len(header: &PageHeader) -> usize {
    postcard::to_allocvec(header)
        .expect("PageHeader serialization cannot fail")
        .len()
}

fn is_not_found(err: &VaultSyncError) -> bool {
    let s = format!("{:?}", err);
    s.contains("NotFoundError") || s.contains("NotFound")
}

fn is_not_readable(err: &VaultSyncError) -> bool {
    let s = format!("{:?}", err);
    s.contains("NotReadableError") || s.contains("NotReadable")
}

/// Maximum bytes to read when only the header is needed.
/// Covers V1 (14 bytes), V2/V3 (~12 bytes), and future expansion.
const MAX_HEADER_SIZE: u32 = 128;

/// Read only the first `MAX_HEADER_SIZE` bytes of a page file using Blob.slice().
/// Avoids reading the entire file payload from OPFS when only metadata is needed.
async fn read_header_bytes(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Vec<u8>, VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    let file_val = SendJsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
    let file_handle: FileSystemFileHandle = file_val.into();
    let file_val = SendJsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
    let file: File = file_val.into();
    let blob: &Blob = file.as_ref();

    // Slice only the header region instead of reading the entire file.
    // Browser clamps end to file size, so this is safe for small files too.
    let sliced = blob
        .slice_with_i32_and_i32(0, MAX_HEADER_SIZE as i32)
        .map_err(|e| VaultSyncError::Storage(format!("blob.slice: {:?}", e)))?;

    let buf_val = SendJsFuture::from(sliced.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    Ok(bytes)
}

/// Read a page header (flags, checksum, etc.) without extracting the data payload.
/// Uses Blob.slice() to read only MAX_HEADER_SIZE bytes instead of the full file.
/// Returns None if the page doesn't exist.
async fn read_page_header(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<PageHeader>, VaultSyncError> {
    let bytes = match read_header_bytes(dir, name).await {
        Ok(b) => b,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };

    if bytes.is_empty() {
        return Err(VaultSyncError::Storage(format!("empty page {}", name)));
    }

    let header = decode_page_header(&bytes)?;
    Ok(Some(header))
}

/// Decode a page header from raw file bytes, detecting V3/V2/V1 format.
fn decode_page_header(bytes: &[u8]) -> Result<PageHeader, VaultSyncError> {
    // V3: magic + postcard header
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage(format!(
                "unknown V3 version {}", header.version
            )));
        }
        return Ok(header);
    }

    // V2: postcard from byte 0 (brief period after initial fix)
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            return Ok(header);
        }
    }

    // V1: 14-byte fixed header (bug-era legacy)
    if bytes.len() >= 14 {
        let flags = u16::from_le_bytes(
            bytes[8..10].try_into().unwrap_or([0, 0]),
        ) as u8;
        let data_len = u32::from_le_bytes(
            bytes[10..14].try_into().unwrap_or([0, 0, 0, 0]),
        );
        return Ok(PageHeader {
            version: 0,
            checksum: 0,
            flags,
            data_len,
        });
    }

    Err(VaultSyncError::Storage(format!(
        "unrecognized page format ({} bytes)",
        bytes.len()
    )))
}

/// Extract the data payload from raw page bytes (any format).
/// Used by tombstone_page to re-encode pages in V3 format.
fn extract_page_data(bytes: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
    if bytes.is_empty() {
        return Err(VaultSyncError::Storage("empty page".into()));
    }

    // V3: magic + postcard header + data
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage("unknown V3 version".into()));
        }
        let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
        if header_end > bytes.len() {
            return Err(VaultSyncError::Storage("truncated V3 page".into()));
        }
        let data_end = header_end + (header.data_len as usize).min(bytes.len() - header_end);
        return Ok(bytes[header_end..data_end].to_vec());
    }

    // V2: postcard from byte 0
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            let header_end = encoded_header_len(&header);
            if header_end > bytes.len() {
                return Err(VaultSyncError::Storage("truncated V2 page".into()));
            }
            let data_end = header_end + (header.data_len as usize).min(bytes.len() - header_end);
            return Ok(bytes[header_end..data_end].to_vec());
        }
    }

    // V1: 14-byte fixed header
    if bytes.len() >= 14 {
        let data_len = u32::from_le_bytes(
            bytes[10..14].try_into().unwrap_or([0, 0, 0, 0]),
        ) as usize;
        let start = 14usize;
        let end = start + data_len.min(bytes.len().saturating_sub(start));
        return Ok(bytes[start..end].to_vec());
    }

    Err(VaultSyncError::Storage("unrecognized page format".into()))
}

/// Read a page file and return (header, data).
/// Supports V3, V2, and V1 formats.
async fn read_page_file_raw(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    let bytes = match read_all_bytes(dir, name).await {
        Ok(b) => b,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };

    if bytes.is_empty() {
        return Err(VaultSyncError::Storage(format!("empty page {}", name)));
    }

    // V3: magic + postcard header + data
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage(format!(
                "unknown V3 version {}", header.version
            )));
        }
        let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
        if header_end > bytes.len() {
            return Err(VaultSyncError::Storage(format!(
                "truncated V3 page {}", name
            )));
        }
        let data = bytes[header_end..].to_vec();
        return Ok(Some((header, data)));
    }

    // V2: postcard from byte 0
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            let header_end = encoded_header_len(&header);
            if header_end > bytes.len() {
                return Err(VaultSyncError::Storage(format!(
                    "truncated V2 page {}", name
                )));
            }
            let data = bytes[header_end..].to_vec();
            return Ok(Some((header, data)));
        }
    }

    // V1: 14-byte fixed header
    if bytes.len() >= 14 {
        let flags = u16::from_le_bytes(bytes[8..10].try_into().unwrap_or([0, 0])) as u8;
        let data_len = u32::from_le_bytes(bytes[10..14].try_into().unwrap_or([0, 0, 0, 0])) as usize;
        let start = 14usize;
        let end = start + data_len.min(bytes.len().saturating_sub(start));
        let data = bytes[start..end].to_vec();
        return Ok(Some((
            PageHeader {
                version: 0,
                checksum: 0,
                flags,
                data_len: data.len() as u32,
            },
            data,
        )));
    }

    Err(VaultSyncError::Storage(format!(
        "corrupt page {}: unrecognized format",
        name
    )))
}

/// Read a page file and validate data_len matches extracted data.
async fn read_page_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    match read_page_file_raw(dir, name).await? {
        Some((header, data)) => {
            if header.version >= 1 && data.len() as u32 != header.data_len {
                return Err(VaultSyncError::Storage(format!(
                    "page data len mismatch: header {} actual {}",
                    header.data_len,
                    data.len()
                )));
            }
            Ok(Some((header, data)))
        }
        None => Ok(None),
    }
}

pub(crate) async fn ensure_dir(
    root: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(true);
    let promise = root.get_directory_handle_with_options(name, &opts);
    let val = SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("ensure_dir failed: {:?}", e)))?;
    Ok(val.into())
}

pub async fn read_manifest(dir: &FileSystemDirectoryHandle) -> Option<StoreManifest> {
    read_all_bytes(dir, "_manifest").await.ok().and_then(|bytes| {
        let m: StoreManifest = postcard::from_bytes(&bytes).ok()?;
        if m.verify() { Some(m) } else { None }
    })
}

/// Read manifest with detailed diagnostics for debugging persistence issues.
/// Logs a structured [ManifestReport] line for every store on every boot.
pub async fn read_manifest_with_diagnostics(
    dir: &FileSystemDirectoryHandle,
    store_name: &str,
) -> Option<StoreManifest> {
    let bytes = match read_all_bytes(dir, "_manifest").await {
        Ok(bytes) => bytes,
        Err(e) => {
            engine_warn!(
                "[ManifestReport] store={} loaded=false reason=FileMissing error=\"{:?}\"",
                store_name, e
            );
            return None;
        }
    };

    let manifest = match postcard::from_bytes::<StoreManifest>(&bytes) {
        Ok(m) => m,
        Err(e) => {
            engine_warn!(
                "[ManifestReport] store={} loaded=false reason=DeserializeError bytes={} error=\"{:?}\"",
                store_name, bytes.len(), e
            );
            return None;
        }
    };

    if manifest.verify() {
        let doc_count = manifest.content_index.page_by_document.len();
        let seq_count = manifest.content_index.page_by_sequence.len();
        let page_by_id_count = manifest.content_index.page_by_id.len();
        engine_info!(
            "[ManifestReport] store={} loaded=true version={} serialization_version={} live_pages={} tombstoned={} page_by_doc={} page_by_seq={} page_by_id={} gen={} current_page={} bytes={}",
            store_name,
            manifest.version,
            manifest.serialization_version,
            manifest.content_index.live_pages.len(),
            manifest.content_index.tombstoned_pages.len(),
            doc_count,
            seq_count,
            page_by_id_count,
            manifest.generation.0,
            manifest.current_page,
            bytes.len(),
        );
        Some(manifest)
    } else {
        let computed = manifest.compute_checksum();
        let serial_v = manifest.serialization_version;
        let current_v = CURRENT_SERIALIZATION_VERSION;
        if serial_v < current_v {
            engine_info!(
                "[ManifestReport] store={} crc_mismatch reason=Migration serialization_version={}->{} live_pages={} page_by_id={} — rebuilding index",
                store_name, serial_v, current_v,
                manifest.content_index.live_pages.len(),
                manifest.content_index.page_by_id.len(),
            );
            // Return the manifest anyway — caller will see None-from-verify and rebuild
            // from OPFS scan. Return None so caller knows to rebuild.
            return None;
        }
        engine_warn!(
            "[ManifestReport] store={} loaded=false reason=CRCMismatch stored_crc=0x{:08X} computed_crc=0x{:08X} serialization_version={} bytes={} live_pages={} page_by_id={} current_page={} gen={}",
            store_name,
            manifest.checksum,
            computed,
            serial_v,
            bytes.len(),
            manifest.content_index.live_pages.len(),
            manifest.content_index.page_by_id.len(),
            manifest.current_page,
            manifest.generation.0,
        );
        None
    }
}

pub async fn write_manifest(dir: &FileSystemDirectoryHandle, store: &str, caller: &str, m: &StoreManifest) -> Result<(), VaultSyncError> {
    let current_page = m.current_page;
    let gen = m.generation.0;
    let live = m.live_pages;
    let tombstoned = m.tombstoned_pages;
    let pending = m.pending_count;
    let mut copy = m.clone();
    copy.checksum = 0;
    let checksum = copy.compute_checksum();
    copy.checksum = checksum;
    let bytes = postcard::to_allocvec(&copy)
        .map_err(|e| VaultSyncError::Storage(format!("manifest encode: {:?}", e)))?;
    let result = write_file(dir, "_manifest", &bytes).await;
    engine_info!(
        "[write_manifest] store={} caller={} current_page={} gen={} live={} tombstoned={} pending={} checksum=0x{:08X} bytes={} ok={}",
        store, caller, current_page, gen, live, tombstoned, pending, checksum, bytes.len(),
        result.is_ok(),
    );
    result
}

/// Guard token that must be passed to rebuild_manifest* functions.
///
/// In debug builds, the guarded functions check that the guard was created
/// by an allowed caller. In release builds, a warning is emitted.
/// This prevents accidental OPFS directory walks during normal execution.
#[derive(Debug)]
pub(crate) struct ManifestRebuildGuard {
    _private: (),
}

impl ManifestRebuildGuard {
    /// Create a new guard for an allowed rebuild context.
    /// Only call this from PageStore::open(), PageStore::recover(),
    /// or cleanup operations. Never from normal read/write paths.
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

/// Phase 1: Rebuild manifest with ContentIndex from OPFS scan.
/// Used on first startup after upgrade or after corruption.
async fn rebuild_manifest_with_index(dir: &FileSystemDirectoryHandle, store: &str) -> Result<StoreManifest, VaultSyncError> {
    let now = js_sys::Date::now() as u64;
    let mut highest_page_id: PageId = 0;
    let mut index = ContentIndex::new();

    let iter = dir.entries();
    loop {
        let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
            .ok()
            .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
        {
            Some(f) => f,
            None => break,
        };
        let result = match next_fn.call0(&iter) {
            Ok(r) => r,
            Err(_) => break,
        };
        let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
            match SendJsFuture::from(promise.clone()).await {
                Ok(v) => v,
                Err(_) => continue,
            }
        } else {
            result
        };
        let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if done { break; }
        let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
            .ok()
            .and_then(|v| {
                v.as_string().or_else(|| {
                    js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                        .ok()
                        .and_then(|n| n.as_string())
                })
            }) {
            Some(n) => n,
            None => continue,
        };
        if let Some(id) = parse_page_id(&name) {
            if id > highest_page_id {
                highest_page_id = id;
            }
            match read_page_file(dir, &name).await {
                Ok(Some((header, data))) => {
                    if header.flags & 0x02 != 0 {
                        index.tombstoned_pages.insert(id);
                    } else {
                        index.live_pages.insert(id);
                        let index_entry = IndexEntry {
                            page_id: id,
                            storage_gen: StorageGeneration(0),
                            checksum: header.checksum,
                            size: data.len() as u32,
                            last_modified: now,
                            version: 1,
                            dirty: false,
                        };
                        if let Ok(doc_entry) = postcard::from_bytes::<DocEntry>(&data) {
                            let key = IndexKey::Document {
                                doc_id: doc_entry.doc_id,
                                record_id: doc_entry.record_id,
                            };
                            index.insert(key, index_entry);
                        } else {
                            // Non-DocEntry page (oplog, migration, etc.) — use page-based key
                            let key = IndexKey::Sequence(id);
                            index.entries.insert(key.clone(), index_entry);
                            index.page_by_id.insert(id, key);
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    engine_warn!("[rebuild] skipping corrupt page {}: {:?}", name, e);
                }
            }
        }
    }

    check_manifest_rebuild_duplicates(&index, store);

    engine_info!(
        "[rebuild_manifest_with_index] store={} highest_page_id={} live={} tombstoned={} current_page=0 gen=0 — rebuild from OPFS scan",
        store, highest_page_id,
        index.live_pages.len(),
        index.tombstoned_pages.len(),
    );

    let mut m = StoreManifest {
        version: 2,
        generation: StorageGeneration(0),
        highest_page_id,
        current_page: 0,
        last_sequence: 0,
        pending_count: 0,
        live_pages: index.live_pages.len(),
        tombstoned_pages: index.tombstoned_pages.len(),
        created_at: now,
        updated_at: now,
        content_index: index,
        checksum: 0,
        serialization_version: CURRENT_SERIALIZATION_VERSION,
    };
    m.checksum = m.compute_checksum();
    Ok(m)
}

/// Legacy rebuild for backward compatibility during migration.
/// Same as rebuild_manifest_with_index but without the Phase 1 ContentIndex.
/// Used by cleanup_tombstoned_pages which only needs page counts.
async fn rebuild_manifest(dir: &FileSystemDirectoryHandle) -> Result<StoreManifest, VaultSyncError> {
    let now = js_sys::Date::now() as u64;
    let mut highest_page_id: PageId = 0;
    let mut live_pages = 0usize;
    let mut tombstoned_pages = 0usize;

    let iter = dir.entries();
    loop {
        let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
            .ok()
            .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
        {
            Some(f) => f,
            None => break,
        };
        let result = match next_fn.call0(&iter) {
            Ok(r) => r,
            Err(_) => break,
        };
        let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
            match SendJsFuture::from(promise.clone()).await {
                Ok(v) => v,
                Err(_) => continue,
            }
        } else {
            result
        };
        let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if done { break; }
        let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
            .ok()
            .and_then(|v| {
                v.as_string().or_else(|| {
                    js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                        .ok()
                        .and_then(|n| n.as_string())
                })
            }) {
            Some(n) => n,
            None => continue,
        };
        if let Some(id) = parse_page_id(&name) {
            if id > highest_page_id {
                highest_page_id = id;
            }
            // Phase 6: Classified corruption recovery — skip corrupt pages with warning
            match read_page_header(dir, &name).await {
                Ok(Some(header)) => {
                    if header.flags & 0x02 != 0 {
                        tombstoned_pages += 1;
                    } else {
                        live_pages += 1;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    engine_warn!("[rebuild] skipping corrupt page {}: {:?}", name, e);
                }
            }
        }
    }

    engine_info!(
        "[rebuild_manifest] highest_page_id={} live={} tombstoned={} current_page=0 gen=0 — legacy rebuild from OPFS scan",
        highest_page_id, live_pages, tombstoned_pages,
    );

    let mut m = StoreManifest {
        version: 1,
        generation: StorageGeneration(0),
        highest_page_id,
        current_page: 0,
        last_sequence: 0,
        pending_count: 0,
        live_pages,
        tombstoned_pages,
        created_at: now,
        updated_at: now,
        content_index: ContentIndex::new(),
        checksum: 0,
        serialization_version: CURRENT_SERIALIZATION_VERSION,
    };
    m.checksum = m.compute_checksum();
    Ok(m)
}

async fn read_all_bytes(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Vec<u8>, VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    let file_val = SendJsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
    let file_handle: FileSystemFileHandle = file_val.into();
    let file_val = SendJsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
    let file: File = file_val.into();
    let blob: &Blob = file.as_ref();
    let buf_val = SendJsFuture::from(blob.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    Ok(bytes)
}

async fn write_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
    data: &[u8],
) -> Result<(), VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let file_val = SendJsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
    let file_handle: FileSystemFileHandle = file_val.into();
    let writable_val = SendJsFuture::from(file_handle.create_writable())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("create_writable: {:?}", e)))?;
    let writable: FileSystemWritableFileStream = writable_val.into();
    let js_array = Uint8Array::new_with_length(data.len() as u32);
    js_array.copy_from(data);
    let write_promise = writable
        .write_with_buffer_source(&js_array)
        .map_err(|e| VaultSyncError::Storage(format!("write: {:?}", e)))?;
    SendJsFuture::from(write_promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("write done: {:?}", e)))?;
    let close: WritableStream = writable.into();
    SendJsFuture::from(close.close())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("close: {:?}", e)))?;
    Ok(())
}

async fn rename_file(
    dir: &FileSystemDirectoryHandle,
    from: &str,
    to: &str,
) -> Result<(), VaultSyncError> {
    if from == to {
        return Ok(());
    }
    let data = read_all_bytes(dir, from).await?;
    let create_opts = FileSystemGetFileOptions::new();
    create_opts.set_create(true);
    let dst_handle: FileSystemFileHandle = SendJsFuture::from(
        dir.get_file_handle_with_options(to, &create_opts),
    )
    .await
    .map_err(|e| VaultSyncError::Storage(format!("get_file_handle dst: {:?}", e)))?
    .into();
    let writable_val = SendJsFuture::from(dst_handle.create_writable())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("create_writable dst: {:?}", e)))?;
    let writable: FileSystemWritableFileStream = writable_val.into();
    let js_array = Uint8Array::new_with_length(data.len() as u32);
    js_array.copy_from(&data);
    let write_promise = writable
        .write_with_buffer_source(&js_array)
        .map_err(|e| VaultSyncError::Storage(format!("write dst: {:?}", e)))?;
    SendJsFuture::from(write_promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("write dst done: {:?}", e)))?;
    let close: WritableStream = writable.into();
    SendJsFuture::from(close.close())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("close dst: {:?}", e)))?;
    delete_file(dir, from).await?;
    Ok(())
}

async fn delete_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<(), VaultSyncError> {
    let promise = dir.remove_entry(name);
    SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("remove_entry: {:?}", e)))?;
    Ok(())
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    fn roundtrip_v3(payload: &[u8]) {
        let encoded = encode_v3_page(payload, 0).unwrap();
        let decoded_header = decode_page_header(&encoded).unwrap();
        assert_eq!(decoded_header.version, 1);
        assert_eq!(decoded_header.flags, 0);
        assert_eq!(decoded_header.data_len, payload.len() as u32);

        let decoded_data = extract_page_data(&encoded).unwrap();
        assert_eq!(decoded_data, payload);

        let (_, raw_data) = decode_v3_raw(&encoded).unwrap();
        assert_eq!(raw_data, payload);
    }

    /// Test helper: inline version of read_page_file_raw for pre-encoded bytes.
    /// Test helper: decode V3 page from raw bytes.
    fn decode_v3_raw(bytes: &[u8]) -> Result<(PageHeader, Vec<u8>), VaultSyncError> {
        if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
            let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
                .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
            let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
            Ok((header, bytes[header_end..].to_vec()))
        } else {
            Err(VaultSyncError::Storage("not V3".into()))
        }
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_empty() {
        roundtrip_v3(&[]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_small() {
        roundtrip_v3(b"hello world");
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_45() {
        roundtrip_v3(&vec![0xAB; 45]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_128() {
        roundtrip_v3(&vec![0xCD; 128]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_16384() {
        roundtrip_v3(&vec![0xEF; 16384]);
    }

    #[wasm_bindgen_test]
    fn v3_checksum_validation() {
        let payload = b"test data";
        let mut encoded = encode_v3_page(payload, 0).unwrap();
        // Corrupt payload
        let data_start = encoded.len() - payload.len();
        encoded[data_start] ^= 0xFF;
        // header still decodes (postcard from start)
        let header = decode_page_header(&encoded).unwrap();
        assert_eq!(header.version, 1);
        let extracted = extract_page_data(&encoded).unwrap();
        // corrupt payload returns corrupt data
        assert_ne!(extracted, payload);
        // checksum mismatch: crc32 of corrupt data != stored checksum
        let actual = crc32fast::hash(&extracted);
        assert_ne!(actual, header.checksum);
    }

    #[wasm_bindgen_test]
    fn v3_tombstone_flag() {
        let payload = b"test data";
        let encoded = encode_v3_page(payload, 0x02).unwrap();
        let header = decode_page_header(&encoded).unwrap();
        assert!(header.flags & 0x02 != 0);
    }

    #[wasm_bindgen_test]
    fn encode_v3_consistency() {
        for len in [0, 1, 10, 100, 1000] {
            let data: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
            let encoded = encode_v3_page(&data, 0).unwrap();
            let decoded = extract_page_data(&encoded).unwrap();
            assert_eq!(decoded, data, "encode/decode mismatch at len={}", len);
        }
    }
}
