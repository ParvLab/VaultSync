use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use futures::Stream;

pub type SequenceId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedMutation {
    pub id: String,
    pub namespace: String,
    pub replica_id: String,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub schema_version: u64,
    pub key_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub sequence: SequenceId,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub key_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub namespace: String,
    pub public_key: Vec<u8>,
    pub schema_version: u64,
}

#[derive(Debug, Clone)]
pub enum CoordinatorError {
    NotAvailable,
    AuthFailed,
    SchemaMismatch,
    Timeout,
    Internal(String),
}

#[async_trait]
pub trait Coordinator: Send + Sync + std::fmt::Debug {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError>;
    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError>;
    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError>;
    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError>;
    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError>;
    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError>;
    async fn update_replica_key(
        &self,
        _namespace: &str,
        _replica_id: &str,
        _public_key: Vec<u8>,
        _key_version: u64,
    ) -> Result<(), CoordinatorError> {
        Ok(())
    }
    async fn get_replica_key(
        &self,
        _namespace: &str,
        _replica_id: &str,
    ) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        Ok(None)
    }
    async fn list_replicas(&self, _namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        Ok(vec![])
    }
    async fn get_snapshot(&self, _namespace: &str, _doc_id: &str, _record_id: &str)
        -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        Ok(None)
    }
    async fn store_snapshot(&self, _namespace: &str, _snapshot: &crate::crdt::snapshot::Snapshot)
        -> Result<(), CoordinatorError> {
        Ok(())
    }
    async fn compact_oplog(&self, _namespace: &str) -> Result<crate::sync::compaction::CompactionStats, CoordinatorError> {
        Ok(crate::sync::compaction::CompactionStats::default())
    }
    async fn list_snapshots(&self, _namespace: &str)
        -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        Ok(vec![])
    }
}
