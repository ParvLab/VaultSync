use std::sync::RwLock;
use std::collections::HashMap;
use async_trait::async_trait;
use crate::error::DriftError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use super::traits::{Storage, SchemaMeta, MigrationRecord, KeyRecord};

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
    async fn insert_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), DriftError> {
        self.documents.write().map_err(|e| DriftError::Storage(e.to_string()))?
            .insert((doc_id.to_string(), record_id.to_string()), bytes.to_vec());
        Ok(())
    }

    async fn get_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, DriftError> {
        Ok(self.documents.read().map_err(|e| DriftError::Storage(e.to_string()))?
            .get(&(doc_id.to_string(), record_id.to_string())).cloned())
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        self.documents.write().map_err(|e| DriftError::Storage(e.to_string()))?
            .remove(&(doc_id.to_string(), record_id.to_string()));
        Ok(())
    }

    async fn write_document_and_oplog(&self, doc_id: &str, record_id: &str, bytes: &[u8], entry: &OplogEntry) -> Result<(), DriftError> {
        let mut docs = self.documents.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        let mut oplog = self.oplog.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        docs.insert((doc_id.to_string(), record_id.to_string()), bytes.to_vec());
        oplog.push(entry.clone());
        Ok(())
    }

    async fn delete_document_and_oplog(&self, doc_id: &str, record_id: &str, entry: &OplogEntry) -> Result<(), DriftError> {
        let mut docs = self.documents.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        let mut oplog = self.oplog.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        docs.remove(&(doc_id.to_string(), record_id.to_string()));
        oplog.push(entry.clone());
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, DriftError> {
        let docs = self.documents.read().map_err(|e| DriftError::Storage(e.to_string()))?;
        let mut results = Vec::new();
        for ((did, rid), bytes) in docs.iter() {
            if did == doc_id {
                results.push((rid.clone(), bytes.clone()));
            }
        }
        Ok(results)
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), DriftError> {
        self.oplog.write().map_err(|e| DriftError::Storage(e.to_string()))?.push(entry.clone());
        Ok(())
    }

    async fn read_pending_oplog(&self, namespace: &str, limit: usize) -> Result<Vec<OplogEntry>, DriftError> {
        let oplog = self.oplog.read().map_err(|e| DriftError::Storage(e.to_string()))?;
        Ok(oplog.iter()
            .filter(|e| e.namespace == namespace && matches!(e.sync_status, crate::oplog::entry::SyncStatus::Pending))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), DriftError> {
        let mut oplog = self.oplog.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        for entry in oplog.iter_mut() {
            if entry.id == id {
                entry.sync_status = crate::oplog::entry::SyncStatus::Synced;
                entry.sequence = Some(sequence);
            }
        }
        Ok(())
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), DriftError> {
        let mut oplog = self.oplog.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        for entry in oplog.iter_mut() {
            if entry.id == id {
                entry.sync_status = crate::oplog::entry::SyncStatus::Failed;
            }
        }
        Ok(())
    }

    async fn read_oplog_after_sequence(&self, namespace: &str, seq: u64) -> Result<Vec<OplogEntry>, DriftError> {
        let oplog = self.oplog.read().map_err(|e| DriftError::Storage(e.to_string()))?;
        Ok(oplog.iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .cloned()
            .collect())
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, DriftError> {
        Ok(self.sync_states.read().map_err(|e| DriftError::Storage(e.to_string()))?
            .get(namespace).cloned())
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), DriftError> {
        self.sync_states.write().map_err(|e| DriftError::Storage(e.to_string()))?
            .insert(state.namespace.clone(), state.clone());
        Ok(())
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, DriftError> {
        Ok(self.schemas.read().map_err(|e| DriftError::Storage(e.to_string()))?
            .get(doc_id).cloned())
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), DriftError> {
        self.schemas.write().map_err(|e| DriftError::Storage(e.to_string()))?
            .insert(meta.doc_id.clone(), meta.clone());
        Ok(())
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, DriftError> {
        Ok(self.migrations.read().map_err(|e| DriftError::Storage(e.to_string()))?.clone())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), DriftError> {
        self.migrations.write().map_err(|e| DriftError::Storage(e.to_string()))?.push(record.clone());
        Ok(())
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, DriftError> {
        Ok(self.keys.read().map_err(|e| DriftError::Storage(e.to_string()))?
            .iter().filter(|k| k.namespace == namespace).cloned().collect())
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), DriftError> {
        self.keys.write().map_err(|e| DriftError::Storage(e.to_string()))?.push(key.clone());
        Ok(())
    }
}
