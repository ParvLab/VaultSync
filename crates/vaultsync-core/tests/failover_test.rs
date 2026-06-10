use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_trait::async_trait;
use futures::Stream;
use vaultsync_core::coordinator::traits::{Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId};
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::coordinator::failover::FailoverCoordinator;

#[derive(Debug)]
struct PanickingCoordinator {
    inner: Arc<InMemoryCoordinator>,
    should_fail: Arc<AtomicBool>,
}

impl PanickingCoordinator {
    fn new(inner: Arc<InMemoryCoordinator>) -> Self {
        Self {
            inner,
            should_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_fail(&self, fail: bool) {
        self.should_fail.store(fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl Coordinator for PanickingCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.push(namespace, mutations).await
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.pull(namespace, after, limit).await
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.subscribe(namespace, from_sequence).await
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.register(namespace, info).await
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.heartbeat(namespace, replica_id).await
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.schema_version(namespace).await
    }

    async fn update_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        key_version: u64,
    ) -> Result<(), CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.update_replica_key(namespace, replica_id, public_key, key_version).await
    }

    async fn get_replica_key(&self, namespace: &str, replica_id: &str) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.get_replica_key(namespace, replica_id).await
    }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.list_replicas(namespace).await
    }
}

#[tokio::test]
async fn test_failover_on_primary_down() {
    let primary_inner = Arc::new(InMemoryCoordinator::new());
    let fallback_inner = Arc::new(InMemoryCoordinator::new());

    let primary = Arc::new(PanickingCoordinator::new(primary_inner));
    let fallback = Arc::new(PanickingCoordinator::new(fallback_inner));

    let failover = FailoverCoordinator::new(vec![primary.clone(), fallback.clone()]).with_max_failures(2);

    let ns = "test-failover-ns";
    let rep = ReplicaInfo {
        replica_id: "rep-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![1],
        schema_version: 0,
    };

    // Primary works initially
    failover.register(ns, rep.clone()).await.unwrap();
    assert_eq!(failover.active_index(), 0);

    // Primary goes down
    primary.set_fail(true);

    // First attempt fails, but active index doesn't switch yet because max_failures is 2
    let res = failover.register(ns, rep.clone()).await;
    assert!(res.is_ok());
    assert_eq!(failover.active_index(), 0);

    // Second attempt fails, which triggers the switch to fallback (idx 1)
    let res = failover.register(ns, rep).await;
    assert!(res.is_ok());
    assert_eq!(failover.active_index(), 1);
}

#[tokio::test]
async fn test_no_mutation_loss_on_failover() {
    let primary_inner = Arc::new(InMemoryCoordinator::new());
    let fallback_inner = Arc::new(InMemoryCoordinator::new());

    let primary = Arc::new(PanickingCoordinator::new(primary_inner));
    let fallback = Arc::new(PanickingCoordinator::new(fallback_inner));

    let failover = FailoverCoordinator::new(vec![primary.clone(), fallback.clone()]).with_max_failures(1);

    let ns = "test-failover-ns";
    let mut1 = EncryptedMutation {
        id: "m-1".to_string(),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1, 2],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };

    // Failover triggers immediately on primary failure
    primary.set_fail(true);

    let seqs = failover.push(ns, vec![mut1]).await.unwrap();
    assert_eq!(seqs.len(), 1);
    assert_eq!(failover.active_index(), 1);

    // Verify it is on fallback
    let pulled = fallback.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].id, "m-1");
}

#[tokio::test]
async fn test_failover_tracks_sequence() {
    let primary_inner = Arc::new(InMemoryCoordinator::new());
    let fallback_inner = Arc::new(InMemoryCoordinator::new());

    // Pre-populate fallback to have a higher sequence offset
    let dummy_muts = (0..5).map(|i| EncryptedMutation {
        id: format!("dummy-{}", i),
        namespace: "test-failover-ns".to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![0],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    }).collect::<Vec<_>>();
    fallback_inner.push("test-failover-ns", dummy_muts).await.unwrap();

    let primary = Arc::new(PanickingCoordinator::new(primary_inner));
    let fallback = Arc::new(PanickingCoordinator::new(fallback_inner));

    let failover = FailoverCoordinator::new(vec![primary.clone(), fallback.clone()]).with_max_failures(1);

    let ns = "test-failover-ns";
    let mut1 = EncryptedMutation {
        id: "m-1".to_string(),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1, 2],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };

    let seqs1 = failover.push(ns, vec![mut1]).await.unwrap();
    let seq1 = seqs1[0];

    // Primary goes down
    primary.set_fail(true);

    let mut2 = EncryptedMutation {
        id: "m-2".to_string(),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-2".to_string(),
        encrypted_blob: vec![3, 4],
        timestamp: 1001,
        schema_version: 0,
        key_version: 1,
    };

    // Should push to fallback and get a valid sequence > seq1
    let seqs2 = failover.push(ns, vec![mut2]).await.unwrap();
    let seq2 = seqs2[0];
    assert!(seq2 > seq1);
}

#[tokio::test]
async fn test_primary_recovers_stays_on_fallback() {
    let primary_inner = Arc::new(InMemoryCoordinator::new());
    let fallback_inner = Arc::new(InMemoryCoordinator::new());

    let primary = Arc::new(PanickingCoordinator::new(primary_inner));
    let fallback = Arc::new(PanickingCoordinator::new(fallback_inner));

    let failover = FailoverCoordinator::new(vec![primary.clone(), fallback.clone()]).with_max_failures(1);

    let ns = "test-failover-ns";

    // Switch to fallback
    primary.set_fail(true);
    failover.schema_version(ns).await.unwrap();
    assert_eq!(failover.active_index(), 1);

    // Primary recovers
    primary.set_fail(false);

    // Should stay on fallback
    failover.schema_version(ns).await.unwrap();
    assert_eq!(failover.active_index(), 1);
}

#[tokio::test]
async fn test_multiple_fallbacks_chain() {
    let primary = Arc::new(PanickingCoordinator::new(Arc::new(InMemoryCoordinator::new())));
    let fallback1 = Arc::new(PanickingCoordinator::new(Arc::new(InMemoryCoordinator::new())));
    let fallback2 = Arc::new(PanickingCoordinator::new(Arc::new(InMemoryCoordinator::new())));

    let failover = FailoverCoordinator::new(vec![primary.clone(), fallback1.clone(), fallback2.clone()]).with_max_failures(1);

    let ns = "test-failover-ns";

    // Primary dies
    primary.set_fail(true);
    // Switch to fallback1
    failover.schema_version(ns).await.unwrap();
    assert_eq!(failover.active_index(), 1);

    // fallback1 also dies
    fallback1.set_fail(true);
    // Switch to fallback2
    failover.schema_version(ns).await.unwrap();
    assert_eq!(failover.active_index(), 2);
}
