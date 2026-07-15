use async_trait::async_trait;
use js_sys::Uint8Array;
use std::sync::{Arc, Mutex};
use vaultsync_core::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::time_utils::SendJsFuture;
use vaultsync_core::VaultSyncError;
use web_sys::*;

use crate::metadata_runtime::MetadataRuntime;
use crate::metadata_store::MetadataStore;
use crate::migration::{DocEntry, PagesDir};
use crate::page_store::{IndexKey, PageId, PageStore};
use crate::storage_runtime::StorageRuntime;
use crate::version_chain::VersionChain;

#[derive(Debug, Clone)]
pub struct OpfsStorage {
    root: Arc<Mutex<FileSystemDirectoryHandle>>,
    pages: PagesDir,
    /// StorageRuntime for allocation authority and scheduling
    storage_runtime: Arc<Mutex<Option<Arc<StorageRuntime>>>>,
    /// MetadataRuntime for in-memory metadata cache (zero OPFS reads during operation)
    metadata_runtime: Arc<Mutex<Option<Arc<MetadataRuntime>>>>,
    /// MetadataStore for SyncState (singleton, not append-only)
    sync_state_store: MetadataStore<SyncState>,
    /// MetadataStore for Schema metadata (all schemas as a single page)
    schema_store: MetadataStore<Vec<(String, SchemaMeta)>>,
    /// MetadataStore for Migration records (all records as a single page)
    migration_store: MetadataStore<Vec<MigrationRecord>>,
    /// MetadataStore for Key records (all keys as a single page)
    key_store: MetadataStore<Vec<KeyRecord>>,
}

impl OpfsStorage {
    /// Expose the PagesDir so all subsystems share the same PageStore instances
    pub fn pages(&self) -> &PagesDir {
        &self.pages
    }

    /// Set the StorageRuntime (called after construction from bootstrap).
    /// Enables allocation routing through PageManager + StorageScheduler.
    pub fn set_storage_runtime(&self, sr: Arc<StorageRuntime>) {
        let mut guard = self.storage_runtime.lock().unwrap();
        *guard = Some(sr);
    }

    /// Set the MetadataRuntime (called after construction from bootstrap).
    /// Routes SyncState, Schema, Key, Migration reads/writes through in-memory cache.
    pub fn set_metadata_runtime(&self, mr: Arc<MetadataRuntime>) {
        let mut guard = self.metadata_runtime.lock().unwrap();
        *guard = Some(mr);
    }

    /// Load MetadataRuntime from the MetadataStore backends.
    /// Returns None if MetadataRuntime construction fails (non-fatal — falls back to direct MetadataStore).
    pub async fn try_load_metadata_runtime(&self) -> Option<Arc<MetadataRuntime>> {
        match MetadataRuntime::load(
            self.sync_state_store.clone(),
            self.schema_store.clone(),
            self.migration_store.clone(),
            self.key_store.clone(),
        ).await {
            Ok(mr) => Some(mr),
            Err(e) => {
                engine_debug!("[MetadataRuntime] load deferred: {:?}", e);
                None
            }
        }
    }

    /// Allocate a doc_data page ID, routing through StorageRuntime when available.
    async fn alloc_doc_data(&self, reason: &'static str) -> Result<PageId, VaultSyncError> {
        self.allocate_store_page_id("doc_data", &self.pages.doc_data, reason).await
    }

    /// Allocate an oplog page ID, routing through StorageRuntime when available.
    async fn alloc_oplog(&self, reason: &'static str) -> Result<PageId, VaultSyncError> {
        self.allocate_store_page_id("oplog", &self.pages.oplog, reason).await
    }

    /// Write a page through StorageRuntime scheduler (when available) or directly.
    async fn write_store_page(&self, store_name: &'static str, store: &PageStore, page_id: PageId, data: Vec<u8>) -> Result<(), VaultSyncError> {
        let sr = {
            let guard = self.storage_runtime.lock().unwrap();
            guard.clone()
        };
        if let Some(sr) = sr {
            sr.enqueue_write_page(store_name, page_id, data).await
        } else {
            store.write_page(page_id, &data).await
        }
    }

    /// Tombstone a page through StorageRuntime scheduler (when available) or directly.
    async fn tombstone_store_page(&self, store_name: &'static str, store: &PageStore, page_id: PageId) -> Result<(), VaultSyncError> {
        let sr = {
            let guard = self.storage_runtime.lock().unwrap();
            guard.clone()
        };
        if let Some(sr) = sr {
            sr.enqueue_tombstone_page(store_name, page_id).await
        } else {
            store.tombstone_page(page_id).await
        }
    }

    /// Write a doc_data page through the scheduler.
    async fn write_doc_page(&self, page_id: PageId, data: Vec<u8>) -> Result<(), VaultSyncError> {
        self.write_store_page("doc_data", &self.pages.doc_data, page_id, data).await
    }

    /// Write an oplog page through the scheduler.
    async fn write_oplog_page(&self, page_id: PageId, data: Vec<u8>) -> Result<(), VaultSyncError> {
        self.write_store_page("oplog", &self.pages.oplog, page_id, data).await
    }

    /// Tombstone a doc_data page through the scheduler.
    async fn tombstone_doc_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        self.tombstone_store_page("doc_data", &self.pages.doc_data, page_id).await
    }

    /// Tombstone an oplog page through the scheduler.
    async fn tombstone_oplog_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        self.tombstone_store_page("oplog", &self.pages.oplog, page_id).await
    }

    /// Allocate a page ID through StorageRuntime (when set) or PageStore (fallback).
    /// StorageRuntime handles PageManager tracking; PageStore handles ContentIndex update.
    async fn allocate_store_page_id(
        &self,
        store_name: &'static str,
        store: &PageStore,
        reason: &'static str,
    ) -> Result<PageId, VaultSyncError> {
        // Check if StorageRuntime is available (quick lock, no await in scope)
        let page_id_opt = {
            let guard = self.storage_runtime.lock().unwrap();
            guard.as_ref().map(|sr| sr.allocate_page_id(store_name, reason))
        };
        if let Some(page_id) = page_id_opt {
            // Commit the pre-allocated page to ContentIndex (no lock held)
            store.commit_allocated_page_id(page_id).await?;
            Ok(page_id)
        } else {
            store.allocate_page_id(reason).await
        }
    }

    /// Get a clone of the root directory handle (for PageStore creation)
    pub async fn db_dir(&self) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
        Ok(self.root.lock().unwrap().clone())
    }

    /// Write a file to _pages/_system/ for metadata (checkpoint, manifest backups, etc.)
    pub(crate) async fn write_system_file(&self, name: &str, data: &[u8]) -> Result<(), VaultSyncError> {
        let db_dir = self.root.lock().unwrap().clone();
        let pages_root = ensure_dir(&db_dir, "_pages").await?;
        let sys_dir = ensure_dir(&pages_root, "_system").await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);
        let file_val = SendJsFuture::from(sys_dir.get_file_handle_with_options(name, &opts))
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
        let ws: web_sys::WritableStream = writable.into();
        SendJsFuture::from(ws.close())
            .await
            .map_err(|e| VaultSyncError::Storage(format!("close: {:?}", e)))?;
        Ok(())
    }

    /// Read a file from _pages/_system/.
    pub(crate) async fn read_system_file(&self, name: &str) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let db_dir = self.root.lock().unwrap().clone();
        let pages_root = ensure_dir(&db_dir, "_pages").await?;
        let sys_dir = ensure_dir(&pages_root, "_system").await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);
        let file_val = SendJsFuture::from(sys_dir.get_file_handle_with_options(name, &opts))
            .await
            .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)));
        let file_handle: FileSystemFileHandle = match file_val {
            Ok(v) => v.into(),
            Err(_) => return Ok(None),
        };
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
        Ok(Some(bytes))
    }

    pub async fn new(db_name: &str) -> Result<Self, VaultSyncError> {
        let _t0 = js_sys::Date::now();
        let window =
            web_sys::window().ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
        let storage_mgr = window.navigator().storage();

        let root_val = vaultsync_core::time_utils::SendJsFuture::from(storage_mgr.get_directory())
            .await
            .map_err(|e| VaultSyncError::Storage(format!("get_directory failed: {:?}", e)))?;
        let t_root = (js_sys::Date::now() - _t0) as u64;
        let root_handle: FileSystemDirectoryHandle = root_val.clone().into();
        let db_dir = ensure_dir(&root_handle, db_name).await?;
        let t_dbdir = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[OpfsStorage] get_directory: {}ms | ensure_dir: {}ms | total so far: {}ms", t_root, t_dbdir - t_root, t_dbdir);

        let migration_done = crate::migration::try_migrate_from_v1(db_name).await.unwrap_or(false);
        let t_mig = (js_sys::Date::now() - _t0) as u64;
        if migration_done {
            engine_info!("[opfs] v1→v2 migration complete (at {}ms)", t_mig);
        }

        let pages = PagesDir::open(&db_dir).await?;
        let t_pages = (js_sys::Date::now() - _t0) as u64;
        let sync_state_store = MetadataStore::new(pages.sync_states.clone());
        let schema_store = MetadataStore::new(pages.schemas.clone());
        let migration_store = MetadataStore::new(pages.migrations.clone());
        let key_store = MetadataStore::new(pages.keys.clone());
        let t_total = (js_sys::Date::now() - _t0) as u64;
        engine_info!("[OpfsStorage] pages_dir: {}ms | MetadataStores: {}ms | total: {}ms", t_pages - t_mig, t_total - t_pages, t_total);

        Ok(Self {
            root: Arc::new(Mutex::new(db_dir)),
            pages,
            storage_runtime: Arc::new(Mutex::new(None)),
            metadata_runtime: Arc::new(Mutex::new(None)),
            sync_state_store,
            schema_store,
            migration_store,
            key_store,
        })
    }

    fn lock_name(&self, store: &str) -> String {
        format!("vaultsync-opfs-{}", store)
    }

    /// Physically remove tombstoned page files from disk.
    /// Returns total number of pages cleaned across all stores.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, VaultSyncError> {
        self.cleanup_tombstoned_pages_routed().await
    }

    async fn tombstone_existing_doc_pages(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<usize, VaultSyncError> {
        // Use ContentIndex for O(1) lookup — eliminates all OPFS scans.
        // Orphan pages are repaired at startup in StorageRuntime::new(), so the
        // ContentIndex is always authoritative during normal writes.
        let store = &self.pages.doc_data;
        let key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        let existing = store.content_index_lookup(&key);
        if let Some(entry) = existing {
            self.tombstone_doc_page(entry.page_id).await?;
            store.remove_from_index(&key).await?;
            Ok(1)
        } else {
            // No entry in ContentIndex — trust the index (orphan repair happens at startup).
            // Return 0 instead of falling through to O(n) OPFS scan.
            Ok(0)
        }
    }
}

// ── StorageRuntime routing helpers ──────────────────────────────────────
// These check for an active StorageRuntime and route through it when
// available, falling back to direct PageStore access otherwise.
impl OpfsStorage {
    /// Get StorageRuntime handle (cloned) if available.
    fn with_sr(&self) -> Option<Arc<StorageRuntime>> {
        let guard = self.storage_runtime.lock().unwrap();
        guard.clone()
    }

    /// Route cleanup_tombstoned_pages through StorageRuntime or directly.
    async fn cleanup_tombstoned_pages_routed(&self) -> Result<usize, VaultSyncError> {
        // No per-store routing needed — iterates all stores
        // StorageRuntime processes via scheduler, but cleanup is a bulk
        // maintenance operation that reads disk directly.
        let mut total = 0usize;
        total += self.pages.doc_data.cleanup_tombstoned_pages().await?;
        total += self.pages.oplog.cleanup_tombstoned_pages().await?;
        total += self.pages.sync_states.cleanup_tombstoned_pages().await?;
        total += self.pages.schemas.cleanup_tombstoned_pages().await?;
        total += self.pages.migrations.cleanup_tombstoned_pages().await?;
        total += self.pages.keys.cleanup_tombstoned_pages().await?;
        Ok(total)
    }

    /// Route list_page_ids for doc_data through StorageRuntime or directly.
    async fn list_doc_page_ids(&self, caller: &'static str) -> Result<Vec<PageId>, VaultSyncError> {
        if let Some(sr) = self.with_sr() {
            sr.list_page_ids("doc_data", caller).await
        } else {
            self.pages.doc_data.list_page_ids(caller).await
        }
    }

    /// Route read_page for doc_data through StorageRuntime or directly.
    async fn read_doc_page(&self, page_id: PageId) -> Result<Option<Vec<u8>>, VaultSyncError> {
        if let Some(sr) = self.with_sr() {
            sr.read_page("doc_data", page_id).await
        } else {
            self.pages.doc_data.read_page(page_id).await
        }
    }

    /// Route adjust_pending_count on oplog through StorageRuntime or directly.
    async fn adjust_oplog_pending_count(&self, delta: i32) -> Result<(), VaultSyncError> {
        if let Some(sr) = self.with_sr() {
            sr.adjust_pending_count("oplog", delta).await
        } else {
            self.pages.oplog.adjust_pending_count(delta).await
        }
    }

    /// Route schedule_gc on oplog through StorageRuntime or directly.
    fn schedule_oplog_gc(&self) {
        if let Some(sr) = self.with_sr() {
            sr.schedule_gc("oplog");
        } else {
            self.pages.oplog.schedule_gc();
        }
    }

    /// Route run_pending_gc on oplog through StorageRuntime or directly.
    async fn run_oplog_gc(&self) -> Result<usize, VaultSyncError> {
        if let Some(sr) = self.with_sr() {
            sr.run_pending_gc("oplog").await
        } else {
            self.pages.oplog.run_pending_gc().await
        }
    }

    /// Route set_current_page on oplog through StorageRuntime or directly.
    async fn set_oplog_current_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        if let Some(sr) = self.with_sr() {
            sr.set_current_page("oplog", page_id).await
        } else {
            self.pages.oplog.set_current_page(page_id).await
        }
    }
}

fn get_lock_manager() -> Result<LockManager, VaultSyncError> {
    let window = web_sys::window()
        .ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
    Ok(window.navigator().locks())
}

#[async_trait]
impl Storage for OpfsStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        // Tombstone existing pages (uses ContentIndex O(1) when populated)
        let tombstoned = self.tombstone_existing_doc_pages(doc_id, record_id).await?;
        if tombstoned > 1 {
            tracing::warn!(
                "[OpfsStorage] insert_document: tombstoned {} live pages for {}/{} (expected 1)",
                tombstoned, doc_id, record_id
            );
        }
        let entry = DocEntry {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            bytes: bytes.to_vec(),
        };
        let page_id = self.alloc_doc_data("insert_document").await?;
        let encoded = postcard::to_allocvec(&entry)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        self.write_doc_page(page_id, encoded).await?;
        // Populate ContentIndex forward + reverse indexes at write time
        let key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        self.pages.doc_data.update_content_index(key, page_id).await
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        // Use ContentIndex for O(1) lookup
        let key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        if let Some(entry) = self.pages.doc_data.content_index_lookup(&key) {
            return match self.read_doc_page(entry.page_id).await? {
                Some(data) => {
                    if let Ok(doc) = postcard::from_bytes::<DocEntry>(&data) {
                        Ok(Some(doc.bytes))
                    } else {
                        Ok(Some(data))
                    }
                }
                None => Ok(None),
            };
        }
        // ContentIndex is authoritative — no O(n) OPFS scan fallback.
        // Orphan pages are repaired at startup in StorageRuntime::new().
        Ok(None)
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        // Use ContentIndex for O(1) tombstone lookup + index cleanup
        if let Some(entry) = self.pages.doc_data.content_index_lookup(&key) {
            self.tombstone_doc_page(entry.page_id).await?;
            self.pages.doc_data.remove_from_index(&key).await?;
        } else {
            // Fallback: O(n) scan for pre-migration data
            let count = self.tombstone_existing_doc_pages(doc_id, record_id).await?;
            if count > 1 {
                tracing::warn!(
                    "[OpfsStorage] delete_document: tombstoned {} live pages for {}/{}",
                    count, doc_id, record_id
                );
            }
        }
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let page_ids = self.list_doc_page_ids("list_documents").await?;
        let mut latest: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
        for id in page_ids {
            if let Some(data) = self.read_doc_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id {
                        latest.insert(entry.record_id, entry.bytes);
                    }
                }
            }
        }
        Ok(latest.into_iter().collect())
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        // Tombstone existing pages (uses ContentIndex O(1) when populated)
        let tombstoned = self.tombstone_existing_doc_pages(doc_id, record_id).await?;
        if tombstoned > 1 {
            tracing::warn!(
                "[OpfsStorage] write_document_and_oplog: tombstoned {} live pages for {}/{} (expected 1)",
                tombstoned, doc_id, record_id
            );
        }
        // Write new document page
        let doc_entry = DocEntry {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            bytes: bytes.to_vec(),
        };
        let encoded_doc = postcard::to_allocvec(&doc_entry)
            .map_err(|e| VaultSyncError::Storage(format!("encode doc: {:?}", e)))?;
        let doc_page_id = self.alloc_doc_data("write_document").await?;
        self.write_doc_page(doc_page_id, encoded_doc).await?;
        // Populate ContentIndex forward + reverse indexes at write time
        let doc_key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        self.pages.doc_data.update_content_index(doc_key, doc_page_id).await?;

        // Write oplog entry
        let encoded_entry = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode oplog: {:?}", e)))?;
        let oplog_page_id = self.alloc_oplog("write_oplog").await?;
        self.write_oplog_page(oplog_page_id, encoded_entry).await?;
        self.adjust_oplog_pending_count(1).await
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        self.delete_document(doc_id, record_id).await?;
        let encoded = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("delete_oplog").await?;
        self.write_oplog_page(page_id, encoded).await?;
        self.adjust_oplog_pending_count(1).await
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("append_oplog").await?;
        self.write_oplog_page(page_id, encoded).await?;
        self.adjust_oplog_pending_count(1).await
    }

    async fn pending_count(&self, _namespace: &str) -> Result<usize, VaultSyncError> {
        Ok(self.pages.oplog.pending_count().await)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn vaultsync_core::storage::transaction::StorageTransaction>, VaultSyncError> {
        let tx = crate::transaction::OpfsTransaction::new(&self.pages.oplog).await?;
        let guard = self.storage_runtime.lock().unwrap();
        if let Some(ref sr) = *guard {
            Ok(Box::new(tx.with_runtime(sr.clone())))
        } else {
            Ok(Box::new(tx))
        }
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let filtered: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.namespace == namespace && e.sync_status.is_uploadable())
            .take(limit)
            .collect();
        Ok(filtered)
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        // Delta write: read only the entry to modify, write a small delta page.
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let mut updated: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.id == id)
            .collect();
        if updated.is_empty() {
            return Ok(());
        }
        for e in &mut updated {
            e.sync_status = SyncStatus::Synced;
            e.sequence = Some(sequence);
        }
        let encoded = postcard::to_allocvec(&updated)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("mark_synced").await?;
        self.write_oplog_page(page_id, encoded).await?;

        // Delta page — do NOT update current_page. The base page stays canonical.
        self.adjust_oplog_pending_count(-1).await?;
        self.schedule_oplog_gc();

        Ok(())
    }

    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), VaultSyncError> {
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let mut updated: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.id == id)
            .collect();
        if updated.is_empty() {
            tracing::warn!("[mark_failed] id={}.. not found in storage, skipping", if id.len() >= 8 { &id[..8] } else { id });
            return Ok(());
        }
        for e in &mut updated {
            e.sync_status = SyncStatus::Failed;
        }
        let encoded = postcard::to_allocvec(&updated)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("mark_failed").await?;
        self.write_oplog_page(page_id, encoded).await?;

        self.adjust_oplog_pending_count(-1).await?;
        self.schedule_oplog_gc();

        Ok(())
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let filtered: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .collect();
        Ok(filtered)
    }

    async fn read_sync_state(&self, _namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        // Route through MetadataRuntime (in-memory) when available — zero OPFS reads
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            return Ok(mr.read_sync_state());
        }
        self.sync_state_store.read().await
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        // Route through MetadataRuntime (in-memory + mark dirty) when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            mr.write_sync_state(state.clone());
            return Ok(());
        }
        self.sync_state_store.write(state).await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            return Ok(mr.read_schema(doc_id));
        }
        let all = self.schema_store.read().await?.unwrap_or_default();
        Ok(all.into_iter().find(|(d, _)| d == doc_id).map(|(_, s)| s))
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            mr.write_schema(meta.clone());
            return Ok(());
        }
        let mut all = self.schema_store.read().await?.unwrap_or_default();
        if let Some(pos) = all.iter().position(|(d, _)| d == &meta.doc_id) {
            all[pos] = (meta.doc_id.clone(), meta.clone());
        } else {
            all.push((meta.doc_id.clone(), meta.clone()));
        }
        self.schema_store.write(&all).await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            return Ok(mr.read_migrations());
        }
        Ok(self.migration_store.read().await?.unwrap_or_default())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            mr.write_migration(record.clone());
            return Ok(());
        }
        let mut all = self.migration_store.read().await?.unwrap_or_default();
        all.push(record.clone());
        self.migration_store.write(&all).await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            return Ok(mr.read_keys(namespace));
        }
        let all = self.key_store.read().await?.unwrap_or_default();
        Ok(all.into_iter().filter(|k| k.namespace == namespace).collect())
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        // Route through MetadataRuntime when available
        if let Some(mr) = self.metadata_runtime.lock().unwrap().as_ref() {
            mr.write_key(key.clone());
            return Ok(());
        }
        let mut all = self.key_store.read().await?.unwrap_or_default();
        all.push(key.clone());
        self.key_store.write(&all).await
    }

    // ADR-001: Failed Entry Lifecycle (WASM)
    //
    // Status: UNDEFINED PRODUCT BEHAVIOR — NOT A BUG
    //
    // The engine is behaving consistently. The product simply does not define
    // what should happen to Failed entries after they reach that state.
    //
    // Current behavior:
    //   Pending → Optimistic → Synced     ✓ (happy path)
    //   Pending → Optimistic → Failed     ⚠️ (terminal — no transition defined)
    //
    // Consequences:
    //   - Failed entries are NOT uploadable (is_uploadable() returns false)
    //   - Failed entries survive compaction (delete_synced_before only removes Synced)
    //   - No code path converts Failed→Pending on WASM (reset_stale_pending is a no-op)
    //   - Failed entries accumulate indefinitely in OPFS storage
    //   - IndexedDB backend HAS a real reset_stale_pending (converts Failed→Pending)
    //
    // Options (choose one):
    //   A) Implement reset_stale_pending for WASM (convert Failed→Pending on reconnect,
    //      matching IndexedDB behavior). Treats all failures as retryable transport errors.
    //   B) Add RetryableFailed/PermanentFailed distinction. Transport failures retry
    //      automatically; logical failures (schema, auth) surface to user. TTL-based
    //      cleanup for RetryableFailed.
    //   C) Do nothing — Failed accumulates forever. Acceptable if a storage wipe or
    //      manual intervention path exists.
    //
    // Recommendation: Do NOT change failure semantics until the engine has been
    // frozen and soak-tested. Changing this path risks reopening the upload pipeline.
    // ============================================================================
    async fn reset_stale_pending(
        &self,
        _namespace: &str,
        _older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        Ok(0)
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        self.run_oplog_gc().await?;
        let cutoff = (js_sys::Date::now() as u64).saturating_sub(older_than_secs * 1000);
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let before = entries.len();
        let remaining: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| {
                !(e.namespace == namespace
                    && e.sync_status == SyncStatus::Synced
                    && e.created_at < cutoff)
            })
            .collect();
        let removed = before - remaining.len();
        let encoded = postcard::to_allocvec(&remaining)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("delete_synced_oplog_older_than").await?;
        self.write_oplog_page(page_id, encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.set_oplog_current_page(page_id).await?;
        self.schedule_oplog_gc();

        Ok(removed)
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let now_ms = js_sys::Date::now() as u64;
        let threshold = now_ms.saturating_sub(older_than_secs * 1000);
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let results: Vec<(String, String)> = entries
            .into_iter()
            .filter(|e| {
                e.namespace == namespace
                    && e.mutation_type == MutationType::CrdtDelete
                    && e.sync_status == SyncStatus::Synced
                    && e.created_at < threshold
            })
            .map(|e| (e.doc_id, e.record_id))
            .collect();
        Ok(results)
    }

    async fn update_oplog_encrypted_blob(
        &self,
        _id: &str,
        _new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let mut seen = std::collections::HashSet::new();
        for entry in &entries {
            if entry.namespace == namespace {
                seen.insert((entry.doc_id.clone(), entry.record_id.clone()));
            }
        }
        Ok(seen.into_iter().collect())
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let results: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| {
                e.namespace == namespace
                    && e.doc_id == doc_id
                    && e.record_id == record_id
                    && e.sync_status == SyncStatus::Synced
            })
            .collect();
        Ok(results)
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        self.run_oplog_gc().await?;
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let before = entries.len();
        let remaining: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| {
                !(e.namespace == namespace
                    && e.doc_id == doc_id
                    && e.record_id == record_id
                    && e.sync_status == SyncStatus::Synced
                    && e.created_at < timestamp)
            })
            .collect();
        let removed = before - remaining.len();
        let encoded = postcard::to_allocvec(&remaining)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("delete_synced_oplog_before_timestamp").await?;
        self.write_oplog_page(page_id, encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.set_oplog_current_page(page_id).await?;
        self.schedule_oplog_gc();

        Ok(removed)
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        self.run_oplog_gc().await?;
        let entries = read_all_oplog_entries(&self.pages.oplog).await?;
        let before = entries.len();
        let remaining: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| {
                !(e.namespace == namespace
                    && e.sync_status == SyncStatus::Synced
                    && e.created_at < cutoff_ms)
            })
            .collect();
        let removed = before - remaining.len();
        let encoded = postcard::to_allocvec(&remaining)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.alloc_oplog("delete_synced_before").await?;
        self.write_oplog_page(page_id, encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.set_oplog_current_page(page_id).await?;
        self.schedule_oplog_gc();

        Ok(removed)
    }
}

async fn read_all_entries<T: serde::de::DeserializeOwned + serde::Serialize>(
    store: &PageStore,
    caller: &str,
) -> Result<Vec<T>, VaultSyncError> {
    engine_debug!("[storage] read_all_entries: reading current_page");
    let current = store.current_page().await;
    engine_debug!("[storage] read_all_entries: current_page={}", current);

    engine_debug!("[storage] read_all_entries: listing page_ids");
    let page_ids = match store.list_page_ids(caller).await {
        Ok(ids) => ids,
        Err(e) => {
            engine_error!("[storage] list_page_ids failed: {:?}", e);
            return Ok(Vec::new());
        }
    };
    engine_debug!("[storage] read_all_entries: page_ids.len={}", page_ids.len());

    // If manifest knows the current full-state page, only read that page
    // plus any pages written after it (delta pages).
    let ids_to_read: Vec<PageId> = if current > 0 {
        page_ids.into_iter().filter(|id| *id >= current).collect()
    } else {
        page_ids
    };

    let total = ids_to_read.len();
    engine_debug!("[storage] read_all_entries: ids_to_read={}", total);

    let mut all = Vec::new();
    let mut next_log_pct = 10u8;
    let mut read_errors = 0usize;
    for (i, id) in ids_to_read.iter().enumerate() {
        // Progress log at INFO every 10% for large scans (no current_page = legacy store)
        if current == 0 && total > 100 {
            let pct = ((i + 1) * 100 / total) as u8;
            if pct >= next_log_pct {
                next_log_pct = pct + 10;
                engine_info!("[storage] scanning page {}/{} ({}%)", i + 1, total, pct);
            }
        }
        // Gracefully handle individual page read/parse errors
        match store.read_page(*id).await {
            Ok(Some(data)) => {
                // Size guard: pages > 16MB trigger emergency split; attempt read regardless.
                // This guard is a circuit breaker, not a data filter — we never silently skip.
                if data.len() > 16_000_000 {
                    engine_warn!("[storage] page {} oversized ({} bytes), attempting read", id, data.len());
                } else if data.len() > 8_000_000 {
                    engine_debug!("[storage] page {} large ({} bytes), scheduling background split", id, data.len());
                    store.schedule_split(id).await;
                }
                if let Ok(chunk) = postcard::from_bytes::<Vec<T>>(&data) {
                    all.extend(chunk);
                } else if let Ok(single) = postcard::from_bytes::<T>(&data) {
                    all.push(single);
                }
            }
            Ok(None) => {} // tombstoned or deleted
            Err(e) => {
                read_errors += 1;
                if read_errors <= 3 {
                    engine_warn!("[storage] read_page {} error: {:?}", id, e);
                }
                if read_errors == 3 {
                    engine_warn!("[storage] suppressing further read_page errors");
                }
            }
        }
    }

    if read_errors > 0 {
        engine_warn!("[storage] read_all_entries: {} page read errors", read_errors);
    }

    Ok(all)
}

/// Read all oplog entries, resolving base + delta pages via VersionChain.
/// Always deduplicates by entry ID — later pages override earlier ones.
pub async fn read_all_oplog_entries(
    store: &PageStore,
) -> Result<Vec<OplogEntry>, VaultSyncError> {
    let current = store.current_page().await;
    let mut page_ids = store.list_page_ids("read_all_oplog_entries").await.unwrap_or_default();
    page_ids.sort_unstable();

    if page_ids.is_empty() {
        return Ok(Vec::new());
    }

    // When current_page is 0 (no full-state page yet), treat ALL pages as
    // deltas with base_id=0. VersionChain deduplicates by entry ID — later
    // pages (deltas written by mark_synced) override earlier ones, preventing
    // stale Pending entries from causing upload loops (Bug C).
    let (base_id, deltas): (PageId, Vec<PageId>) = if current > 0 {
        (current, page_ids.into_iter().filter(|id| *id > current).collect())
    } else {
        (0, page_ids)
    };

    let mut chain = VersionChain::new(base_id);
    for d in &deltas {
        chain.add_delta(*d);
    }
    chain.resolve(store).await
}

use crate::indexeddb::IndexedDbStorage;

#[derive(Debug)]
pub enum BrowserStorage {
    Opfs(OpfsStorage),
    Idb(IndexedDbStorage),
}

impl BrowserStorage {
    /// Physically remove tombstoned page files from disk.
    /// No-op for IndexedDB backend.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(opfs) => opfs.cleanup_tombstoned_pages().await,
            Self::Idb(_) => Ok(0),
        }
    }

    pub async fn new(db_name: &str, backend: Option<&str>) -> Result<Self, VaultSyncError> {
        engine_debug!("[Rust] BrowserStorage::new: db_name={}, requested_backend={:?}", db_name, backend);

        match backend {
            Some("opfs") => {
                let opfs = OpfsStorage::new(db_name).await?;
                Ok(Self::Opfs(opfs))
            }
            Some("indexeddb") => {
                let idb = IndexedDbStorage::new(db_name).await?;
                Ok(Self::Idb(idb))
            }
            _ => {
                match OpfsStorage::new(db_name).await {
                    Ok(opfs) => Ok(Self::Opfs(opfs)),
                    Err(e) => {
                        engine_info!("[Rust] OPFS unavailable, falling back to IndexedDB: {:?}", e);
                        let idb = IndexedDbStorage::new(db_name).await?;
                        Ok(Self::Idb(idb))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Storage for BrowserStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.insert_document(doc_id, record_id, bytes).await,
            Self::Idb(s) => s.insert_document(doc_id, record_id, bytes).await,
        }
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.get_document(doc_id, record_id).await,
            Self::Idb(s) => s.get_document(doc_id, record_id).await,
        }
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_document(doc_id, record_id).await,
            Self::Idb(s) => s.delete_document(doc_id, record_id).await,
        }
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.list_documents(doc_id).await,
            Self::Idb(s) => s.list_documents(doc_id).await,
        }
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.write_document_and_oplog(doc_id, record_id, bytes, entry)
                    .await
            }
            Self::Idb(s) => {
                s.write_document_and_oplog(doc_id, record_id, bytes, entry)
                    .await
            }
        }
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_document_and_oplog(doc_id, record_id, entry).await,
            Self::Idb(s) => s.delete_document_and_oplog(doc_id, record_id, entry).await,
        }
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.append_oplog(entry).await,
            Self::Idb(s) => s.append_oplog(entry).await,
        }
    }

    async fn pending_count(&self, namespace: &str) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.pending_count(namespace).await,
            Self::Idb(s) => s.pending_count(namespace).await,
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn vaultsync_core::storage::transaction::StorageTransaction>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.begin_transaction().await,
            Self::Idb(s) => s.begin_transaction().await,
        }
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_pending_oplog(namespace, limit).await,
            Self::Idb(s) => s.read_pending_oplog(namespace, limit).await,
        }
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.mark_synced(id, sequence).await,
            Self::Idb(s) => s.mark_synced(id, sequence).await,
        }
    }

    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.mark_failed(id, error).await,
            Self::Idb(s) => s.mark_failed(id, error).await,
        }
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_oplog_after_sequence(namespace, seq).await,
            Self::Idb(s) => s.read_oplog_after_sequence(namespace, seq).await,
        }
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_sync_state(namespace).await,
            Self::Idb(s) => s.read_sync_state(namespace).await,
        }
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_sync_state(state).await,
            Self::Idb(s) => s.write_sync_state(state).await,
        }
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_schema(doc_id).await,
            Self::Idb(s) => s.read_schema(doc_id).await,
        }
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_schema(meta).await,
            Self::Idb(s) => s.write_schema(meta).await,
        }
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_migrations().await,
            Self::Idb(s) => s.read_migrations().await,
        }
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_migration(record).await,
            Self::Idb(s) => s.write_migration(record).await,
        }
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_keys(namespace).await,
            Self::Idb(s) => s.read_keys(namespace).await,
        }
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_key(key).await,
            Self::Idb(s) => s.write_key(key).await,
        }
    }

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.reset_stale_pending(namespace, older_than_ms).await,
            Self::Idb(s) => s.reset_stale_pending(namespace, older_than_ms).await,
        }
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.delete_synced_oplog_older_than(namespace, older_than_secs)
                    .await
            }
            Self::Idb(s) => {
                s.delete_synced_oplog_older_than(namespace, older_than_secs)
                    .await
            }
        }
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.list_tombstoned_documents(namespace, older_than_secs)
                    .await
            }
            Self::Idb(s) => {
                s.list_tombstoned_documents(namespace, older_than_secs)
                    .await
            }
        }
    }

    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.update_oplog_encrypted_blob(id, new_blob).await,
            Self::Idb(s) => s.update_oplog_encrypted_blob(id, new_blob).await,
        }
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.list_active_documents(namespace).await,
            Self::Idb(s) => s.list_active_documents(namespace).await,
        }
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.read_synced_oplog_for_document(namespace, doc_id, record_id)
                    .await
            }
            Self::Idb(s) => {
                s.read_synced_oplog_for_document(namespace, doc_id, record_id)
                    .await
            }
        }
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.delete_synced_oplog_before_timestamp(namespace, doc_id, record_id, timestamp)
                    .await
            }
            Self::Idb(s) => {
                s.delete_synced_oplog_before_timestamp(namespace, doc_id, record_id, timestamp)
                    .await
            }
        }
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_synced_before(namespace, cutoff_ms).await,
            Self::Idb(s) => s.delete_synced_before(namespace, cutoff_ms).await,
        }
    }
}

async fn ensure_dir(
    root: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(true);
    let promise = root.get_directory_handle_with_options(name, &opts);
    let val = vaultsync_core::time_utils::SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("ensure_dir failed: {:?}", e)))?;
    Ok(val.into())
}
