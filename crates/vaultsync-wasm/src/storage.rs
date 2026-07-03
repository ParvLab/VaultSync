use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use vaultsync_core::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;
use web_sys::*;

use crate::migration::{DocEntry, PagesDir};
use crate::page_store::PageStore;

#[derive(Debug, Clone)]
pub struct OpfsStorage {
    root: Arc<Mutex<FileSystemDirectoryHandle>>,
    pages: PagesDir,
}

impl OpfsStorage {
    pub async fn new(db_name: &str) -> Result<Self, VaultSyncError> {
        let window =
            web_sys::window().ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
        let storage_mgr = window.navigator().storage();

        let root_val = vaultsync_core::time_utils::SendJsFuture::from(storage_mgr.get_directory())
            .await
            .map_err(|e| VaultSyncError::Storage(format!("get_directory failed: {:?}", e)))?;
        let root_handle: FileSystemDirectoryHandle = root_val.clone().into();
        let db_dir = ensure_dir(&root_handle, db_name).await?;

        let migration_done = crate::migration::try_migrate_from_v1(db_name).await.unwrap_or(false);
        if migration_done {
            web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] v1→v2 migration complete"));
        }

        let pages = PagesDir::open(&db_dir).await?;

        Ok(Self {
            root: Arc::new(Mutex::new(db_dir)),
            pages,
        })
    }

    fn lock_name(&self, store: &str) -> String {
        format!("vaultsync-opfs-{}", store)
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
        let entry = DocEntry {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            bytes: bytes.to_vec(),
        };
        let page_id = self.pages.doc_data.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(&entry)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        self.pages.doc_data.write_page(page_id, &encoded).await
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let page_ids = self.pages.doc_data.list_page_ids().await?;
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id && entry.record_id == record_id {
                        return Ok(Some(entry.bytes));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let page_ids = self.pages.doc_data.list_page_ids().await?;
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id && entry.record_id == record_id {
                        self.pages.doc_data.tombstone_page(id).await?;
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let page_ids = self.pages.doc_data.list_page_ids().await?;
        let mut results = Vec::new();
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id {
                        results.push((entry.record_id, entry.bytes));
                    }
                }
            }
        }
        Ok(results)
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let doc_entry = DocEntry {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            bytes: bytes.to_vec(),
        };
        let encoded_doc = postcard::to_allocvec(&doc_entry)
            .map_err(|e| VaultSyncError::Storage(format!("encode doc: {:?}", e)))?;
        let doc_page_id = self.pages.doc_data.allocate_page_id().await?;
        self.pages.doc_data.write_page(doc_page_id, &encoded_doc).await?;

        let encoded_entry = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode oplog: {:?}", e)))?;
        let oplog_page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(oplog_page_id, &encoded_entry).await
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
        let filtered: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.namespace == namespace && e.sync_status.is_uploadable())
            .take(limit)
            .collect();
        Ok(filtered)
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
        let updated: Vec<OplogEntry> = entries
            .into_iter()
            .map(|mut e| {
                if e.id == id {
                    e.sync_status = SyncStatus::Synced;
                    e.sequence = Some(sequence);
                }
                e
            })
            .collect();
        let encoded = postcard::to_allocvec(&updated)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), VaultSyncError> {
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
        let updated: Vec<OplogEntry> = entries
            .into_iter()
            .map(|mut e| {
                if e.id == id {
                    e.sync_status = SyncStatus::Failed;
                }
                e
            })
            .collect();
        let encoded = postcard::to_allocvec(&updated)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
        let filtered: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .collect();
        Ok(filtered)
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        let all = read_all_entries::<(String, SyncState)>(&self.pages.sync_states).await?;
        Ok(all.into_iter().find(|(ns, _)| ns == namespace).map(|(_, s)| s))
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[(state.namespace.clone(), state.clone())])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.sync_states.allocate_page_id().await?;
        self.pages.sync_states.write_page(page_id, &encoded).await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        let all = read_all_entries::<(String, SchemaMeta)>(&self.pages.schemas).await?;
        Ok(all.into_iter().find(|(d, _)| d == doc_id).map(|(_, s)| s))
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[(meta.doc_id.clone(), meta.clone())])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.schemas.allocate_page_id().await?;
        self.pages.schemas.write_page(page_id, &encoded).await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        read_all_entries::<MigrationRecord>(&self.pages.migrations).await
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(record)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.migrations.allocate_page_id().await?;
        self.pages.migrations.write_page(page_id, &encoded).await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        let all = read_all_entries::<KeyRecord>(&self.pages.keys).await?;
        Ok(all.into_iter().filter(|k| k.namespace == namespace).collect())
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(key)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.keys.allocate_page_id().await?;
        self.pages.keys.write_page(page_id, &encoded).await
    }

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
        let cutoff = (js_sys::Date::now() as u64).saturating_sub(older_than_secs * 1000);
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;
        Ok(removed)
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let now_ms = js_sys::Date::now() as u64;
        let threshold = now_ms.saturating_sub(older_than_secs * 1000);
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;
        Ok(removed)
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let entries = read_all_entries::<OplogEntry>(&self.pages.oplog).await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;
        Ok(removed)
    }
}

async fn read_all_entries<T: serde::de::DeserializeOwned>(
    store: &PageStore,
) -> Result<Vec<T>, VaultSyncError> {
    let page_ids = store.list_page_ids().await?;
    let mut all = Vec::new();
    for id in page_ids {
        if let Some(data) = store.read_page(id).await? {
            if let Ok(chunk) = postcard::from_bytes::<Vec<T>>(&data) {
                all.extend(chunk);
            } else if let Ok(single) = postcard::from_bytes::<T>(&data) {
                all.push(single);
            }
        }
    }
    Ok(all)
}

use crate::indexeddb::IndexedDbStorage;

#[derive(Debug)]
pub enum BrowserStorage {
    Opfs(OpfsStorage),
    Idb(IndexedDbStorage),
}

impl BrowserStorage {
    pub async fn new(db_name: &str, backend: Option<&str>) -> Result<Self, VaultSyncError> {
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
            "[Rust] BrowserStorage::new: db_name={}, requested_backend={:?}",
            db_name, backend
        )));

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
                        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
                            "[Rust] OPFS unavailable, falling back to IndexedDB: {:?}",
                            e
                        )));
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
