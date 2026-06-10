use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU32, Ordering};
use async_trait::async_trait;
use futures::Stream;
use super::traits::{Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId};

#[derive(Debug)]
pub struct FailoverCoordinator {
    candidates: Vec<Arc<dyn Coordinator>>,
    current: AtomicUsize,
    failure_count: AtomicU32,
    max_failures_before_failover: u32,
}

impl FailoverCoordinator {
    pub fn new(candidates: Vec<Arc<dyn Coordinator>>) -> Self {
        assert!(!candidates.is_empty(), "FailoverCoordinator requires at least one candidate");
        Self {
            candidates,
            current: AtomicUsize::new(0),
            failure_count: AtomicU32::new(0),
            max_failures_before_failover: 3,
        }
    }

    pub fn with_max_failures(mut self, n: u32) -> Self {
        self.max_failures_before_failover = n;
        self
    }

    pub fn active_index(&self) -> usize {
        self.current.load(Ordering::SeqCst)
    }



    fn record_failure(&self, idx: usize) -> bool {
        let current_idx = self.current.load(Ordering::SeqCst);
        if idx != current_idx {
            return false;
        }

        let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.max_failures_before_failover {
            let next_idx = (current_idx + 1) % self.candidates.len();
            if next_idx != current_idx {
                self.current.store(next_idx, Ordering::SeqCst);
                self.failure_count.store(0, Ordering::SeqCst);
                tracing::warn!("Failover triggered! Switching coordinator from index {} to {}", current_idx, next_idx);
                return true;
            }
        }
        false
    }

    fn record_success(&self, idx: usize) {
        if idx == self.current.load(Ordering::SeqCst) {
            self.failure_count.store(0, Ordering::SeqCst);
        }
    }
}

macro_rules! try_coord {
    ($self:expr, $method:ident ( $($args:expr),* )) => {{
        let mut retries = 0;
        let limit = $self.candidates.len();
        let start_idx = $self.active_index();
        loop {
            let idx = (start_idx + retries) % limit;
            let coord = $self.candidates[idx].clone();
            match coord.$method($($args),*).await {
                Ok(val) => {
                    $self.record_success(idx);
                    break Ok(val);
                }
                Err(e) => {
                    match e {
                        CoordinatorError::NotAvailable | CoordinatorError::Timeout => {
                            $self.record_failure(idx);
                            retries += 1;
                            if retries >= limit {
                                break Err(e);
                            }
                        }
                        other => {
                            break Err(other);
                        }
                    }
                }
            }
        }
    }};
}

#[async_trait]
impl Coordinator for FailoverCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        try_coord!(self, push(namespace, mutations.clone()))
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        try_coord!(self, pull(namespace, after, limit))
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        try_coord!(self, subscribe(namespace, from_sequence))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        try_coord!(self, register(namespace, info.clone()))
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        try_coord!(self, heartbeat(namespace, replica_id))
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        try_coord!(self, schema_version(namespace))
    }

    async fn update_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        key_version: u64,
    ) -> Result<(), CoordinatorError> {
        try_coord!(self, update_replica_key(namespace, replica_id, public_key.clone(), key_version))
    }

    async fn get_replica_key(&self, namespace: &str, replica_id: &str) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        try_coord!(self, get_replica_key(namespace, replica_id))
    }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        try_coord!(self, list_replicas(namespace))
    }

    async fn get_snapshot(&self, namespace: &str, doc_id: &str, record_id: &str)
        -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        try_coord!(self, get_snapshot(namespace, doc_id, record_id))
    }

    async fn store_snapshot(&self, namespace: &str, snapshot: &crate::crdt::snapshot::Snapshot)
        -> Result<(), CoordinatorError> {
        try_coord!(self, store_snapshot(namespace, snapshot))
    }

    async fn compact_oplog(&self, namespace: &str) -> Result<crate::sync::compaction::CompactionStats, CoordinatorError> {
        try_coord!(self, compact_oplog(namespace))
    }

    async fn list_snapshots(&self, namespace: &str)
        -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        try_coord!(self, list_snapshots(namespace))
    }
}
