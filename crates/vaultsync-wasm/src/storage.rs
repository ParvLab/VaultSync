use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use vaultsync_core::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;
use web_sys::*;

use crate::migration::{DocEntry, PagesDir};
use crate::page_store::{PageId, PageStore};
use crate::version_chain::VersionChain;

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
            engine_info!("[opfs] v1→v2 migration complete");
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

    /// Physically remove tombstoned page files from disk.
    /// Returns total number of pages cleaned across all stores.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, VaultSyncError> {
        let mut total = 0usize;
        total += self.pages.doc_data.cleanup_tombstoned_pages().await?;
        total += self.pages.oplog.cleanup_tombstoned_pages().await?;
        total += self.pages.sync_states.cleanup_tombstoned_pages().await?;
        total += self.pages.schemas.cleanup_tombstoned_pages().await?;
        total += self.pages.migrations.cleanup_tombstoned_pages().await?;
        total += self.pages.keys.cleanup_tombstoned_pages().await?;
        Ok(total)
    }

    async fn tombstone_existing_doc_pages(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<usize, VaultSyncError> {
        let page_ids = self.pages.doc_data.list_page_ids("tombstone_existing_doc_pages").await?;
        let mut count = 0;
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id && entry.record_id == record_id {
                        self.pages.doc_data.tombstone_page(id).await?;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
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
        let page_ids = self.pages.doc_data.list_page_ids("get_document").await?;
        let mut found: Vec<(u64, Vec<u8>)> = Vec::new();
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
                if let Ok(entry) = postcard::from_bytes::<DocEntry>(&data) {
                    if entry.doc_id == doc_id && entry.record_id == record_id {
                        found.push((id, entry.bytes));
                    }
                }
            }
        }
        match found.len() {
            0 => Ok(None),
            1 => Ok(Some(found.into_iter().next().unwrap().1)),
            n => {
                tracing::warn!(
                    "[OpfsStorage] get_document: found {} live pages for {}/{}",
                    n, doc_id, record_id
                );
                found.sort_by_key(|(id, _)| *id);
                Ok(Some(found.into_iter().last().unwrap().1))
            }
        }
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let count = self.tombstone_existing_doc_pages(doc_id, record_id).await?;
        if count > 1 {
            tracing::warn!(
                "[OpfsStorage] delete_document: tombstoned {} live pages for {}/{}",
                count, doc_id, record_id
            );
        }
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let page_ids = self.pages.doc_data.list_page_ids("list_documents").await?;
        let mut latest: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
        for id in page_ids {
            if let Some(data) = self.pages.doc_data.read_page(id).await? {
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
        tracing::trace!("[OpfsStorage] write_document_and_oplog: tombstone_existing_doc_pages start doc={}/{}", doc_id, record_id);
        let tombstoned = self.tombstone_existing_doc_pages(doc_id, record_id).await?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: tombstone_existing_doc_pages done count={}", tombstoned);
        if tombstoned > 1 {
            tracing::warn!(
                "[OpfsStorage] write_document_and_oplog: tombstoned {} live pages for {}/{} (expected 1)",
                tombstoned, doc_id, record_id
            );
        }
        tracing::trace!("[OpfsStorage] write_document_and_oplog: create DocEntry start");
        let doc_entry = DocEntry {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            bytes: bytes.to_vec(),
        };
        tracing::trace!("[OpfsStorage] write_document_and_oplog: encode doc entry start");
        let encoded_doc = postcard::to_allocvec(&doc_entry)
            .map_err(|e| VaultSyncError::Storage(format!("encode doc: {:?}", e)))?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: encode doc done len={}", encoded_doc.len());
        let doc_page_id = self.pages.doc_data.allocate_page_id().await?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: allocate doc page done id={}", doc_page_id);
        tracing::trace!("[OpfsStorage] write_document_and_oplog: write doc page start id={}", doc_page_id);
        self.pages.doc_data.write_page(doc_page_id, &encoded_doc).await?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: write doc page done");

        tracing::trace!("[OpfsStorage] write_document_and_oplog: encode oplog entry start");
        let encoded_entry = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode oplog: {:?}", e)))?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: encode oplog done len={}", encoded_entry.len());
        let oplog_page_id = self.pages.oplog.allocate_page_id().await?;
        tracing::trace!("[OpfsStorage] write_document_and_oplog: allocate oplog page done id={}", oplog_page_id);
        tracing::trace!("[OpfsStorage] write_document_and_oplog: write oplog page start");
        self.pages.oplog.write_page(oplog_page_id, &encoded_entry).await?;
        self.pages.oplog.adjust_pending_count(1).await
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
        self.pages.oplog.write_page(page_id, &encoded).await?;
        self.pages.oplog.adjust_pending_count(1).await
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[entry.clone()])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;
        self.pages.oplog.adjust_pending_count(1).await
    }

    async fn pending_count(&self, _namespace: &str) -> Result<usize, VaultSyncError> {
        Ok(self.pages.oplog.pending_count().await)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn vaultsync_core::storage::transaction::StorageTransaction>, VaultSyncError> {
        Ok(Box::new(crate::transaction::OpfsTransaction::new(&self.pages.oplog).await?))
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;

        // Delta page — do NOT update current_page. The base page stays canonical.
        self.pages.oplog.adjust_pending_count(-1).await?;
        self.pages.oplog.schedule_gc();

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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;

        self.pages.oplog.adjust_pending_count(-1).await?;
        self.pages.oplog.schedule_gc();

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

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        let all = read_all_entries::<(String, SyncState)>(&self.pages.sync_states, "read_sync_state").await?;
        Ok(all.into_iter().find(|(ns, _)| ns == namespace).map(|(_, s)| s))
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[(state.namespace.clone(), state.clone())])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.sync_states.allocate_page_id().await?;
        self.pages.sync_states.write_page(page_id, &encoded).await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        let all = read_all_entries::<(String, SchemaMeta)>(&self.pages.schemas, "read_schema").await?;
        Ok(all.into_iter().find(|(d, _)| d == doc_id).map(|(_, s)| s))
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(&[(meta.doc_id.clone(), meta.clone())])
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.schemas.allocate_page_id().await?;
        self.pages.schemas.write_page(page_id, &encoded).await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        read_all_entries::<MigrationRecord>(&self.pages.migrations, "read_migrations").await
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(record)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.migrations.allocate_page_id().await?;
        self.pages.migrations.write_page(page_id, &encoded).await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        let all = read_all_entries::<KeyRecord>(&self.pages.keys, "read_keys").await?;
        Ok(all.into_iter().filter(|k| k.namespace == namespace).collect())
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(key)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        let page_id = self.pages.keys.allocate_page_id().await?;
        self.pages.keys.write_page(page_id, &encoded).await
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
        self.pages.oplog.run_pending_gc().await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.pages.oplog.set_current_page(page_id).await?;
        self.pages.oplog.schedule_gc();

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
        self.pages.oplog.run_pending_gc().await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.pages.oplog.set_current_page(page_id).await?;
        self.pages.oplog.schedule_gc();

        Ok(removed)
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        self.pages.oplog.run_pending_gc().await?;
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
        let page_id = self.pages.oplog.allocate_page_id().await?;
        self.pages.oplog.write_page(page_id, &encoded).await?;

        // Full-state rewrite — mark as current and schedule deferred GC.
        self.pages.oplog.set_current_page(page_id).await?;
        self.pages.oplog.schedule_gc();

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
pub async fn read_all_oplog_entries(
    store: &PageStore,
) -> Result<Vec<OplogEntry>, VaultSyncError> {
    let current = store.current_page().await;
    let mut page_ids = store.list_page_ids("read_all_oplog_entries").await.unwrap_or_default();
    page_ids.sort_unstable();

    if current == 0 || page_ids.is_empty() {
        // No base page — fall back to generic read (append-only store)
        // Use the qualified path to avoid recursion.
        return self::read_all_entries::<OplogEntry>(store, "read_all_oplog_entries").await;
    }

    let deltas: Vec<PageId> = page_ids.iter().filter(|id| **id > current).copied().collect();
    let mut chain = VersionChain::new(current);
    for d in &deltas {
        chain.add_delta(*d);
    }
    let resolved = chain.resolve(store).await?;

    Ok(resolved)
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
