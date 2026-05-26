use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use async_trait::async_trait;
use futures::Stream;
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use drift_core::storage::traits::StorageConfig;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId, CoordinatorError};
use drift_core::coordinator::memory::InMemoryCoordinator;
use drift_core::sync::retry::RetryConfig;

#[derive(Debug)]
pub struct OfflineCoordinatorWrapper {
    inner: Arc<InMemoryCoordinator>,
    offline: Arc<AtomicBool>,
}

impl OfflineCoordinatorWrapper {
    pub fn new(inner: Arc<InMemoryCoordinator>) -> Self {
        Self {
            inner,
            offline: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_offline(&self, value: bool) {
        self.offline.store(value, Ordering::SeqCst);
    }
}

#[async_trait]
impl Coordinator for OfflineCoordinatorWrapper {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.push(namespace, mutations).await
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.pull(namespace, after, limit).await
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.subscribe(namespace, from_sequence).await
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.register(namespace, info).await
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.heartbeat(namespace, replica_id).await
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.schema_version(namespace).await
    }
}

fn test_retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: 5,
        initial_delay: Duration::from_millis(0),
        max_delay: Duration::from_millis(0),
        backoff_factor: 1.0,
        jitter: false,
    }
}

#[tokio::test]
async fn test_write_offline_then_reconnect() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let coord = Arc::new(OfflineCoordinatorWrapper::new(shared));
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let ns = "offline-reconnect-ns";

    let mut config = DriftConfig::default();
    config.namespace = ns.to_string();
    config.replica_id = "replica-offline-client".to_string();
    config.storage = StorageConfig::InMemory;
    config.retry = test_retry_config();
    config.sync_interval = Duration::from_secs(3600);

    // Initialize while online
    let client = DriftClient::new_with_keyring(config, coord.clone(), keyring).await.unwrap();

    // Now go offline
    coord.set_offline(true);

    // Insert while offline
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), CrdtValue::String("Offline Task".to_string()));
    client.insert("doc-1", "rec-1", fields).await.unwrap();

    // Verify it is written locally
    let local = client.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(local.get("title").unwrap(), &CrdtValue::String("Offline Task".to_string()));

    // Verify sync fails (returns 0 uploaded/downloaded due to offline status)
    let sync_res1 = client.force_sync().await.unwrap();
    assert_eq!(sync_res1, 0);
    assert_eq!(client.pending_uploads().await.unwrap(), 1);

    // Go online
    coord.set_offline(false);

    // Sync again: should succeed and upload
    let sync_res2 = client.force_sync().await.unwrap();
    assert_eq!(sync_res2, 2); // 1 upload + 1 download (self loopback download queue pulls it)
    assert_eq!(client.pending_uploads().await.unwrap(), 0);
}

#[tokio::test]
async fn test_missed_mutations_on_reconnect() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let coord_alice = Arc::new(OfflineCoordinatorWrapper::new(shared.clone()));
    let coord_bob = Arc::new(OfflineCoordinatorWrapper::new(shared.clone()));
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let ns = "missed-mutations-ns";

    // Alice is online
    let mut config_alice = DriftConfig::default();
    config_alice.namespace = ns.to_string();
    config_alice.replica_id = "alice".to_string();
    config_alice.storage = StorageConfig::InMemory;
    config_alice.retry = test_retry_config();
    config_alice.sync_interval = Duration::from_secs(3600);
    let client_alice = DriftClient::new_with_keyring(config_alice, coord_alice.clone(), keyring.clone()).await.unwrap();

    // Bob is online initially to initialize
    let mut config_bob = DriftConfig::default();
    config_bob.namespace = ns.to_string();
    config_bob.replica_id = "bob".to_string();
    config_bob.storage = StorageConfig::InMemory;
    config_bob.retry = test_retry_config();
    config_bob.sync_interval = Duration::from_secs(3600);
    let client_bob = DriftClient::new_with_keyring(config_bob, coord_bob.clone(), keyring.clone()).await.unwrap();

    // Bob goes offline
    coord_bob.set_offline(true);

    // Alice writes a mutation and syncs (Alice is online)
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), CrdtValue::String("Alice's item".to_string()));
    client_alice.insert("doc-1", "rec-1", fields).await.unwrap();
    let alice_sync = client_alice.force_sync().await.unwrap();
    assert_eq!(alice_sync, 2); // 1 uploaded + 1 downloaded (self loopback)

    // Bob tries to sync while offline: should get 0 updates and have nothing in his doc
    let bob_sync1 = client_bob.force_sync().await.unwrap();
    assert_eq!(bob_sync1, 0);
    assert!(client_bob.get("doc-1", "rec-1").await.unwrap().is_none());

    // Bob goes online
    coord_bob.set_offline(false);

    // Bob syncs: should pull Alice's mutation and have it
    let bob_sync2 = client_bob.force_sync().await.unwrap();
    assert_eq!(bob_sync2, 1); // 0 uploaded + 1 downloaded
    let local_bob = client_bob.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(local_bob.get("title").unwrap(), &CrdtValue::String("Alice's item".to_string()));
}

#[tokio::test]
async fn test_partial_offline_crdt_merge() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let coord_alice = Arc::new(OfflineCoordinatorWrapper::new(shared.clone()));
    let coord_bob = Arc::new(OfflineCoordinatorWrapper::new(shared.clone()));
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let ns = "crdt-merge-ns";

    // Alice config
    let mut config_alice = DriftConfig::default();
    config_alice.namespace = ns.to_string();
    config_alice.replica_id = "alice".to_string();
    config_alice.storage = StorageConfig::InMemory;
    config_alice.retry = test_retry_config();
    config_alice.sync_interval = Duration::from_secs(3600);
    let client_alice = DriftClient::new_with_keyring(config_alice, coord_alice.clone(), keyring.clone()).await.unwrap();

    // Bob config
    let mut config_bob = DriftConfig::default();
    config_bob.namespace = ns.to_string();
    config_bob.replica_id = "bob".to_string();
    config_bob.storage = StorageConfig::InMemory;
    config_bob.retry = test_retry_config();
    config_bob.sync_interval = Duration::from_secs(3600);
    let client_bob = DriftClient::new_with_keyring(config_bob, coord_bob.clone(), keyring.clone()).await.unwrap();

    // Both replicas go offline
    coord_alice.set_offline(true);
    coord_bob.set_offline(true);

    // Alice writes to doc-1 while offline
    let mut fields_a = HashMap::new();
    fields_a.insert("title".to_string(), CrdtValue::String("Alice's edit".to_string()));
    client_alice.insert("doc-1", "rec-1", fields_a).await.unwrap();

    // Bob writes to doc-1 concurrently while offline
    let mut fields_b = HashMap::new();
    fields_b.insert("description".to_string(), CrdtValue::String("Bob's description".to_string()));
    client_bob.insert("doc-1", "rec-1", fields_b).await.unwrap();

    // Both attempt sync, both return 0 because offline
    assert_eq!(client_alice.force_sync().await.unwrap(), 0);
    assert_eq!(client_bob.force_sync().await.unwrap(), 0);

    // Both go online
    coord_alice.set_offline(false);
    coord_bob.set_offline(false);

    // Alice syncs to upload Alice's edits
    let alice_sync = client_alice.force_sync().await.unwrap();
    assert!(alice_sync > 0);

    // Bob syncs to upload Bob's edits and download Alice's edits
    let bob_sync = client_bob.force_sync().await.unwrap();
    assert!(bob_sync > 0);

    // Alice syncs again to download Bob's edits
    let alice_sync2 = client_alice.force_sync().await.unwrap();
    assert!(alice_sync2 > 0);

    // Verify both replicas converged to the identical state containing both edits
    let map_a = client_alice.get("doc-1", "rec-1").await.unwrap().unwrap();
    let map_b = client_bob.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(map_a, map_b);
    assert_eq!(map_a.get("title").unwrap(), &CrdtValue::String("Alice's edit".to_string()));
    assert_eq!(map_a.get("description").unwrap(), &CrdtValue::String("Bob's description".to_string()));
}
