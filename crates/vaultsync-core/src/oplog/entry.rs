use serde::{Deserialize, Serialize};

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
