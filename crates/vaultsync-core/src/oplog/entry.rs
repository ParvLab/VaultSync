use serde::{Deserialize, Serialize};

/// Identifies which code path created this mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOrigin {
    /// Unknown / default (backward compat).
    #[serde(rename = "Unknown")]
    Unknown,
    /// Client insert() method.
    #[serde(rename = "Insert")]
    Insert,
    /// Client update() method.
    #[serde(rename = "Update")]
    Update,
    /// Client delete() method.
    #[serde(rename = "Delete")]
    Delete,
    /// Crash-recovery at startup.
    #[serde(rename = "CrashRecovery")]
    CrashRecovery,
    /// Pull batch from server.
    #[serde(rename = "RemotePull")]
    RemotePull,
    /// Push mutation from coordinator subscription.
    #[serde(rename = "PushMutation")]
    PushMutation,
    /// P2P broadcast from another peer.
    #[serde(rename = "P2PBroadcast")]
    P2PBroadcast,
    /// Snapshot catch-up.
    #[serde(rename = "Snapshot")]
    Snapshot,
    /// Test utilities.
    #[serde(rename = "TestUtils")]
    TestUtils,
    /// Conformance / integration tests.
    #[serde(rename = "TestHarness")]
    TestHarness,
}

impl Default for MutationOrigin {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OplogEntry {
    pub id: String,
    pub replica_id: String,
    pub namespace: String,
    pub mutation_type: MutationType,
    pub doc_id: String,
    pub record_id: String,
    pub yrs_update: Vec<u8>,
    pub encrypted_blob: Option<Vec<u8>>,
    pub timestamp: u64,
    pub sequence: Option<u64>,
    pub sync_status: SyncStatus,
    pub synced_at: Option<u64>,
    pub created_at: u64,
    /// Schema version at time of mutation (0 = unknown / unversioned).
    #[serde(default)]
    pub schema_version: u64,
    /// Provenance: which code path created this entry.
    #[serde(default)]
    pub origin: MutationOrigin,
    /// Human-readable context (e.g. "EditorPage::save", "AutoSave::flush").
    #[serde(default)]
    pub origin_context: String,
}

impl OplogEntry {
    /// Create a new oplog entry with provenance tracking.
    ///
    /// Logs a `CREATE OPLOG` trace containing the entry id, status, origin,
    /// and (in debug builds) a stack backtrace to pinpoint the caller.
    pub fn new(
        id: String,
        replica_id: String,
        namespace: String,
        mutation_type: MutationType,
        doc_id: String,
        record_id: String,
        yrs_update: Vec<u8>,
        encrypted_blob: Option<Vec<u8>>,
        timestamp: u64,
        sequence: Option<u64>,
        sync_status: SyncStatus,
        synced_at: Option<u64>,
        created_at: u64,
        schema_version: u64,
        origin: MutationOrigin,
        origin_context: impl Into<String>,
    ) -> Self {
        let entry = Self {
            id,
            replica_id,
            namespace,
            mutation_type,
            doc_id,
            record_id,
            yrs_update,
            encrypted_blob,
            timestamp,
            sequence,
            sync_status,
            synced_at,
            created_at,
            schema_version,
            origin: origin.clone(),
            origin_context: origin_context.into(),
        };
        #[cfg(debug_assertions)]
        {
            let bt = std::backtrace::Backtrace::capture();
            tracing::debug!(
                "CREATE OPLOG id={} status={:?} origin={:?} ctx={}\n{}",
                entry.id,
                entry.sync_status,
                origin,
                entry.origin_context,
                bt,
            );
        }
        #[cfg(not(debug_assertions))]
        tracing::debug!(
            "CREATE OPLOG id={} status={:?} origin={:?} ctx={}",
            entry.id,
            entry.sync_status,
            origin,
            entry.origin_context,
        );
        entry
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    Pending,
    Synced,
    Failed,
    /// Applied locally but server confirmation pending.
    /// Same as Pending but explicitly marks the optimistic write.
    Optimistic,
}

impl SyncStatus {
    /// Returns `true` for statuses that should be picked up by the upload queue.
    /// Adding new uploadable variants requires updating this method.
    pub fn is_uploadable(&self) -> bool {
        matches!(self, SyncStatus::Pending | SyncStatus::Optimistic)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationType {
    CrdtUpdate,
    CrdtInsert,
    CrdtDelete,
    CrdtBatch,
}
