use async_trait::async_trait;
use drift_core::coordinator::traits::*;

#[derive(Debug)]
pub struct PostgresCoordinator;

impl PostgresCoordinator {
    pub async fn new(connection_string: &str) -> Self {
        tracing::info!("Connecting to Postgres: {}", connection_string);
        Self
    }
}

#[async_trait]
impl Coordinator for PostgresCoordinator {
    async fn push(&self, _namespace: &str, _mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> { Ok(vec![]) }
    async fn pull(&self, _namespace: &str, _after: SequenceId, _limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> { Ok(vec![]) }
    async fn register(&self, _namespace: &str, _info: ReplicaInfo) -> Result<(), CoordinatorError> { Ok(()) }
    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> { Ok(()) }
    async fn subscribe(&self, _namespace: &str, _from_sequence: SequenceId) -> Result<Box<dyn futures::Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        Ok(Box::new(futures::stream::empty()))
    }
    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> { Ok(0) }
}
