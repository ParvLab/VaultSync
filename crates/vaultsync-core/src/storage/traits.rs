use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Sqlite { path: String },
    InMemory,
    Wasm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMeta {
    pub doc_id: String,
    pub version: u64,
    pub schema_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub version: String,
    pub applied_at: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRecord {
    pub namespace: String,
    pub key_bytes: Vec<u8>,
    pub version: u64,
}

#[async_trait]
pub trait Storage: Send + Sync + std::fmt::Debug {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError>;
    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError>;
    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError>;
    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError>;

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError>;
    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError>;

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError>;
    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError>;
    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError>;
    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), VaultSyncError>;
    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError>;

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError>;
    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError>;

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError>;
    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError>;
    /// List all registered schemas.
    async fn list_schemas(&self) -> Result<Vec<SchemaMeta>, VaultSyncError> {
        Ok(Vec::new())
    }
    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError>;
    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError>;

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError>;
    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError>;

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError>;
    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError>;
    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError>;
    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError>;

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError>;
    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError>;
    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError>;

    /// Delete all Synced oplog entries created before `cutoff_ms`.
    /// MUST NOT delete Pending entries regardless of age.
    /// Returns count of entries deleted.
    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError>;

    /// Write a batch of document updates in a single operation.
    async fn write_batch_reconciliation(
        &self,
        documents: Vec<(String, String, Vec<u8>)>,
    ) -> Result<(), VaultSyncError> {
        for (doc_id, record_id, bytes) in documents {
            self.insert_document(&doc_id, &record_id, &bytes).await?;
        }
        Ok(())
    }

    /// Clone self as a boxed trait object.
    fn clone_box(&self) -> Box<dyn Storage> {
        panic!("clone_box not implemented for this Storage type");
    }

    /// Check if storage is healthy and accessible.
    fn is_healthy(&self) -> bool {
        true
    }
}
