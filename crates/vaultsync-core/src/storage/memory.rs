use super::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug)]
pub struct InMemoryStorage {
    documents: RwLock<HashMap<(String, String), Vec<u8>>>,
    oplog: RwLock<Vec<OplogEntry>>,
    sync_states: RwLock<HashMap<String, SyncState>>,
    schemas: RwLock<HashMap<String, SchemaMeta>>,
    migrations: RwLock<Vec<MigrationRecord>>,
    keys: RwLock<Vec<KeyRecord>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            documents: RwLock::new(HashMap::new()),
            oplog: RwLock::new(Vec::new()),
            sync_states: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            migrations: RwLock::new(Vec::new()),
            keys: RwLock::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        self.documents
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .insert((doc_id.to_string(), record_id.to_string()), bytes.to_vec());
        Ok(())
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        Ok(self
            .documents
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .get(&(doc_id.to_string(), record_id.to_string()))
            .cloned())
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        self.documents
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .remove(&(doc_id.to_string(), record_id.to_string()));
        Ok(())
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let mut docs = self
            .documents
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        docs.insert((doc_id.to_string(), record_id.to_string()), bytes.to_vec());
        oplog.push(entry.clone());
        Ok(())
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let mut docs = self
            .documents
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        docs.remove(&(doc_id.to_string(), record_id.to_string()));
        oplog.push(entry.clone());
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let docs = self
            .documents
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let mut results = Vec::new();
        for ((did, rid), bytes) in docs.iter() {
            if did == doc_id {
                results.push((rid.clone(), bytes.clone()));
            }
        }
        Ok(results)
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        self.oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .push(entry.clone());
        Ok(())
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let oplog = self
            .oplog
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        Ok(oplog
            .iter()
            .filter(|e| {
                e.namespace == namespace
                    && e.sync_status.is_uploadable()
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let now = crate::time_utils::system_time_now_secs();
        for entry in oplog.iter_mut() {
            if entry.id == id {
                entry.sync_status = crate::oplog::entry::SyncStatus::Synced;
                entry.sequence = Some(sequence);
                entry.synced_at = Some(now);
            }
        }
        Ok(())
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        for entry in oplog.iter_mut() {
            if entry.id == id {
                entry.sync_status = crate::oplog::entry::SyncStatus::Failed;
            }
        }
        Ok(())
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let oplog = self
            .oplog
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        Ok(oplog
            .iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .cloned()
            .collect())
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        Ok(self
            .sync_states
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .get(namespace)
            .cloned())
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        self.sync_states
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .insert(state.namespace.clone(), state.clone());
        Ok(())
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        Ok(self
            .schemas
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .get(doc_id)
            .cloned())
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        self.schemas
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .insert(meta.doc_id.clone(), meta.clone());
        Ok(())
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        Ok(self
            .migrations
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .clone())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        self.migrations
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .push(record.clone());
        Ok(())
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        Ok(self
            .keys
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .iter()
            .filter(|k| k.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        self.keys
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?
            .push(key.clone());
        Ok(())
    }

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let now = crate::time_utils::system_time_now_ms();
        let threshold = now.saturating_sub(older_than_ms);
        let mut count = 0;
        for entry in oplog.iter_mut() {
            if entry.namespace == namespace
                && !matches!(entry.sync_status, crate::oplog::entry::SyncStatus::Synced)
                && entry.created_at < threshold
            {
                entry.sync_status = crate::oplog::entry::SyncStatus::Pending;
                count += 1;
            }
        }
        Ok(count)
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let now_secs = crate::time_utils::system_time_now_secs();
        let threshold = now_secs.saturating_sub(older_than_secs);
        let initial_len = oplog.len();
        oplog.retain(|entry| {
            !(entry.namespace == namespace
                && matches!(entry.sync_status, crate::oplog::entry::SyncStatus::Synced)
                && entry.synced_at.unwrap_or(0) < threshold)
        });
        Ok(initial_len - oplog.len())
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let oplog = self
            .oplog
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let now_ms = crate::time_utils::system_time_now_ms();
        let threshold = now_ms.saturating_sub(older_than_secs * 1000);
        let mut results = std::collections::HashSet::new();
        for entry in oplog.iter() {
            if entry.namespace == namespace
                && matches!(
                    entry.mutation_type,
                    crate::oplog::entry::MutationType::CrdtDelete
                )
                && matches!(entry.sync_status, crate::oplog::entry::SyncStatus::Synced)
                && entry.created_at < threshold
            {
                results.insert((entry.doc_id.clone(), entry.record_id.clone()));
            }
        }
        Ok(results.into_iter().collect())
    }

    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        for entry in oplog.iter_mut() {
            if entry.id == id {
                entry.encrypted_blob = Some(new_blob.to_vec());
                break;
            }
        }
        Ok(())
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let oplog = self
            .oplog
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let mut results = std::collections::HashSet::new();
        for entry in oplog.iter() {
            if entry.namespace == namespace {
                results.insert((entry.doc_id.clone(), entry.record_id.clone()));
            }
        }
        Ok(results.into_iter().collect())
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let oplog = self
            .oplog
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        Ok(oplog
            .iter()
            .filter(|e| {
                e.namespace == namespace
                    && e.doc_id == doc_id
                    && e.record_id == record_id
                    && matches!(e.sync_status, crate::oplog::entry::SyncStatus::Synced)
            })
            .cloned()
            .collect())
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        let mut oplog = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let initial_len = oplog.len();
        oplog.retain(|entry| {
            !(entry.namespace == namespace
                && entry.doc_id == doc_id
                && entry.record_id == record_id
                && matches!(entry.sync_status, crate::oplog::entry::SyncStatus::Synced)
                && entry.created_at < timestamp)
        });
        Ok(initial_len - oplog.len())
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let mut store = self
            .oplog
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        let before = store.len();
        store.retain(|entry| {
            !(entry.namespace == namespace
                && entry.sync_status == crate::oplog::entry::SyncStatus::Synced
                && entry.created_at < cutoff_ms)
        });
        Ok(before - store.len())
    }

    async fn write_batch_reconciliation(
        &self,
        documents: Vec<(String, String, Vec<u8>)>,
    ) -> Result<(), VaultSyncError> {
        let mut docs = self
            .documents
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        for (doc_id, record_id, bytes) in documents {
            docs.insert((doc_id, record_id), bytes);
        }
        Ok(())
    }
}
