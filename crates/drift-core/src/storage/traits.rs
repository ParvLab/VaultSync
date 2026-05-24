use async_trait::async_trait;
use crate::error::DriftError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use serde::{Serialize, Deserialize};

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
    async fn insert_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), DriftError>;
    async fn get_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, DriftError>;
    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError>;
    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, DriftError>;

    async fn write_document_and_oplog(&self, doc_id: &str, record_id: &str, bytes: &[u8], entry: &OplogEntry) -> Result<(), DriftError>;
    async fn delete_document_and_oplog(&self, doc_id: &str, record_id: &str, entry: &OplogEntry) -> Result<(), DriftError>;

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), DriftError>;
    async fn read_pending_oplog(&self, namespace: &str, limit: usize) -> Result<Vec<OplogEntry>, DriftError>;
    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), DriftError>;
    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), DriftError>;
    async fn read_oplog_after_sequence(&self, namespace: &str, seq: u64) -> Result<Vec<OplogEntry>, DriftError>;

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, DriftError>;
    async fn write_sync_state(&self, state: &SyncState) -> Result<(), DriftError>;

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, DriftError>;
    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), DriftError>;
    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, DriftError>;
    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), DriftError>;

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, DriftError>;
    async fn write_key(&self, key: &KeyRecord) -> Result<(), DriftError>;
}
