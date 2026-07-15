use std::collections::HashMap;

use crate::crdt::types::CrdtValue;
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::storage::transaction::StorageTransaction;
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

    /// Get the count of pending (unsynced) oplog entries.
    /// Default implementation falls back to scanning via `read_pending_oplog`.
    async fn pending_count(&self, namespace: &str) -> Result<usize, VaultSyncError> {
        Ok(self
            .read_pending_oplog(namespace, 50000)
            .await?
            .len())
    }

    /// Begin a new storage transaction for atomic batch operations.
    /// The default implementation is a no-op passthrough.
    async fn begin_transaction(&self) -> Result<Box<dyn StorageTransaction>, VaultSyncError> {
        Err(VaultSyncError::Storage("transactions not supported".into()))
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

// ──────────────────────────────────────────────────────────────────────────
// Replay types
// ──────────────────────────────────────────────────────────────────────────

/// Per-invocation replay context. Constructed at the call site (from metadata_store
/// or SyncStateStore cursor), never stored permanently on ReplayEngine.
#[derive(Debug, Clone)]
pub struct ReplayContext {
    /// Namespace to replay.
    pub namespace: String,
    /// Starting cursor for replay. 0 = full replay.
    pub cursor: u64,
    /// Optional replay mode for future cursor-based filtering.
    pub mode: ReplayMode,
}

impl ReplayContext {
    pub fn new(namespace: &str, cursor: u64) -> Self {
        Self {
            namespace: namespace.to_string(),
            cursor,
            mode: ReplayMode::Full,
        }
    }
}

/// Reason a record was skipped during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySkipReason {
    Cursor,
    Namespace,
    Tombstoned,
    DecodeFailure,
    CorruptPage,
    Duplicate,
}

/// Statistics about a replay operation.
/// Logged as a structured report to replace ad-hoc debug logs.
#[derive(Debug, Clone)]
pub struct ReplayReport {
    pub namespace: String,
    pub requested_cursor: u64,
    pub storage_cursor: u64,
    pub documents_scanned: u64,
    pub records_scanned: u64,
    pub pages_scanned: u64,
    pub decoded: u64,
    pub emitted: u64,
    /// Per-reason skip counters for diagnostic clarity.
    pub skipped: HashMap<ReplaySkipReason, u64>,
    pub elapsed_ms: u64,
}

impl ReplayReport {
    pub fn summary(&self) -> String {
        let skip_total: u64 = self.skipped.values().sum();
        format!(
            "ns={} requested={} storage_cursor={} documents={} records={} pages={} decoded={} emitted={} skipped={} elapsed={}ms",
            self.namespace,
            self.requested_cursor,
            self.storage_cursor,
            self.documents_scanned,
            self.records_scanned,
            self.pages_scanned,
            self.decoded,
            self.emitted,
            skip_total,
            self.elapsed_ms,
        )
    }
}

/// Receives individual mutation callbacks during replay.
/// The concrete implementation posts MUTATION frames to the BroadcastChannel.
pub trait MutationSink: Send + Sync {
    fn send_mutation(
        &self,
        doc_id: &str,
        record_id: &str,
        fields: &HashMap<String, CrdtValue>,
    );
    fn send_sync_done(&self, cursor: u64, count: u64);
}

/// Source of replay data for SessionProtocol.
/// Implemented by ReplayEngine (which delegates to StorageRuntime via DocumentReader).
/// SessionProtocol never accesses PageStore, ContentIndex, or OPFS directly.
#[async_trait]
pub trait ReplaySource: Send + Sync {
    /// Replay documents starting from the cursor in the given context through the sink.
    /// Context is constructed per-invocation (never stored permanently on the source).
    /// Returns a structured report with enumeration and timing diagnostics.
    async fn replay_since(
        &self,
        ctx: &ReplayContext,
        sink: &dyn MutationSink,
    ) -> ReplayReport;
}

/// Future-proofing: replay strategies that can extend beyond full replays.
/// Currently only Full is used; Cursor and WorkingSet are reserved for future use.
#[derive(Debug, Clone)]
pub enum ReplayMode {
    /// Emit all documents (current behavior).
    Full,
    /// Emit mutations after cursor Cursor(u64) — reserved.
    Cursor(u64),
    /// Emit only working-set documents — reserved.
    WorkingSet,
}

/// Future-proofing: filter documents emitted during replay.
/// Initially AllowAllFilter; later WorkingSetFilter can be injected without trait changes.
pub trait ReplayFilter: Send + Sync {
    fn should_emit(&self, namespace: &str, doc_id: &str) -> bool;
}

/// Pass-through filter that emits all documents.
pub struct AllowAllFilter;

impl ReplayFilter for AllowAllFilter {
    fn should_emit(&self, _namespace: &str, _doc_id: &str) -> bool {
        true
    }
}

/// Reads raw document data from the storage layer.
/// ReplayEngine depends on this interface instead of owning PageStore directly,
/// keeping replay logic independent of the physical storage layout.
#[async_trait]
pub trait DocumentReader: Send + Sync {
    /// Return every (doc_id, record_id, fields) tuple for the given namespace.
    async fn read_all_records(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String, HashMap<String, CrdtValue>)>, VaultSyncError>;
}
