use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use futures::Stream;

pub type SequenceId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedMutation {
    pub id: String,
    pub namespace: String,
    pub replica_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub sequence: SequenceId,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
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
}
