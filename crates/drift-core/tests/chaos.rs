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
struct ChaosCoordinator {
    inner: Arc<InMemoryCoordinator>,
    partitioned: Arc<AtomicBool>,
}

impl ChaosCoordinator {
    fn new(inner: Arc<InMemoryCoordinator>) -> Self {
        Self {
            inner,
            partitioned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_partitioned(&self, val: bool) {
        self.partitioned.store(val, Ordering::SeqCst);
    }
}

#[async_trait]
impl Coordinator for ChaosCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.push(namespace, mutations).await
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.pull(namespace, after, limit).await
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.subscribe(namespace, from_sequence).await
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.register(namespace, info).await
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.heartbeat(namespace, replica_id).await
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.schema_version(namespace).await
    }
}

fn test_retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: 10,
        initial_delay: Duration::from_millis(0),
        max_delay: Duration::from_millis(0),
        backoff_factor: 1.0,
        jitter: false,
    }
}

#[tokio::test]
async fn test_chaos_network_partition() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let coord_alice = Arc::new(ChaosCoordinator::new(shared.clone()));
    let coord_bob = Arc::new(ChaosCoordinator::new(shared.clone()));
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let ns = "chaos-partition-ns";

    let mut config_alice = DriftConfig::default();
    config_alice.namespace = ns.to_string();
    config_alice.replica_id = "alice".to_string();
    config_alice.storage = StorageConfig::InMemory;
    config_alice.retry = test_retry_config();
    config_alice.sync_interval = Duration::from_secs(3600);
    let client_alice = DriftClient::new_with_keyring(config_alice, coord_alice.clone(), keyring.clone()).await.unwrap();

    let mut config_bob = DriftConfig::default();
    config_bob.namespace = ns.to_string();
    config_bob.replica_id = "bob".to_string();
    config_bob.storage = StorageConfig::InMemory;
    config_bob.retry = test_retry_config();
    config_bob.sync_interval = Duration::from_secs(3600);
    let client_bob = DriftClient::new_with_keyring(config_bob, coord_bob.clone(), keyring.clone()).await.unwrap();

    // Partition the network for both
    coord_alice.set_partitioned(true);
    coord_bob.set_partitioned(true);

    // Write concurrently during partition
    let mut fields_a = HashMap::new();
    fields_a.insert("title".to_string(), CrdtValue::String("Alice's partition task".to_string()));
    client_alice.insert("doc-1", "rec-1", fields_a).await.unwrap();

    let mut fields_b = HashMap::new();
    fields_b.insert("description".to_string(), CrdtValue::String("Bob's partition description".to_string()));
    client_bob.insert("doc-1", "rec-1", fields_b).await.unwrap();

    // Verify sync returns 0 during partition
    assert_eq!(client_alice.force_sync().await.unwrap(), 0);
    assert_eq!(client_bob.force_sync().await.unwrap(), 0);

    // Heal partition
    coord_alice.set_partitioned(false);
    coord_bob.set_partitioned(false);

    // Sync to converge
    client_alice.force_sync().await.unwrap();
    client_bob.force_sync().await.unwrap();
    client_alice.force_sync().await.unwrap();

    // Assert convergence
    let map_a = client_alice.get("doc-1", "rec-1").await.unwrap().unwrap();
    let map_b = client_bob.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(map_a, map_b);
    assert_eq!(map_a.get("title").unwrap(), &CrdtValue::String("Alice's partition task".to_string()));
    assert_eq!(map_a.get("description").unwrap(), &CrdtValue::String("Bob's partition description".to_string()));
}

#[tokio::test]
async fn test_chaos_leader_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("chaos_leader.db").to_string_lossy().to_string();

    // Leader client (Client 1)
    let mut config1 = DriftConfig::default();
    config1.namespace = "chaos-leader-ns".to_string();
    config1.replica_id = "replica-1".to_string();
    config1.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config1.sync_interval = Duration::from_millis(40);
    let client1 = DriftClient::new(config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(client1.sync_status().await.unwrap().leader_status, Some(true));

    // Reader client (Client 2)
    let mut config2 = DriftConfig::default();
    config2.namespace = "chaos-leader-ns".to_string();
    config2.replica_id = "replica-2".to_string();
    config2.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config2.sync_interval = Duration::from_millis(40);
    let client2 = DriftClient::new(config2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(client2.sync_status().await.unwrap().leader_status, Some(false));

    // Simulate leader crash by dropping/shutting down Client 1
    client1.shutdown().await.unwrap();
    drop(client1);

    // Wait for Client 2 to detect heartbeat/lock timeout and promote
    tokio::time::sleep(Duration::from_millis(150)).await;

    let status2 = client2.sync_status().await.unwrap();
    assert_eq!(status2.leader_status, Some(true), "Client 2 should promote to leader after Client 1 crashes");

    client2.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_chaos_simultaneous_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("chaos_crash.db").to_string_lossy().to_string();

    let mut config = DriftConfig::default();
    config.namespace = "chaos-crash-ns".to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config.sync_interval = Duration::from_secs(3600);

    let client = DriftClient::new(config.clone()).await.unwrap();

    let mut fields = HashMap::new();
    fields.insert("key1".to_string(), CrdtValue::String("value before crash".to_string()));
    client.insert("doc-1", "rec-1", fields).await.unwrap();

    // Verify it is written locally
    assert_eq!(client.pending_uploads().await.unwrap(), 1);

    // Simulate sudden crash by dropping/shutting down the client immediately without syncing
    client.shutdown().await.unwrap();
    drop(client);

    // Re-initialize client on restart using the same SQLite db path
    let client_reborn = DriftClient::new(config).await.unwrap();

    // Verify data was restored from the persistent SQLite storage
    let restored = client_reborn.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(restored.get("key1").unwrap(), &CrdtValue::String("value before crash".to_string()));
    assert_eq!(client_reborn.pending_uploads().await.unwrap(), 1);

    client_reborn.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_chaos_corrupt_oplog_entry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("chaos_corrupt.db").to_string_lossy().to_string();

    let mut config = DriftConfig::default();
    config.namespace = "chaos-corrupt-ns".to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config.sync_interval = Duration::from_secs(3600);

    let client = DriftClient::new(config.clone()).await.unwrap();

    let mut fields = HashMap::new();
    fields.insert("key1".to_string(), CrdtValue::String("safe data".to_string()));
    client.insert("doc-1", "rec-1", fields).await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);

    // Manually corrupt the oplog table using SQLite raw connections
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        // Set yrs_update to garbage bytes that are not valid Yrs encoded updates
        conn.execute("UPDATE oplog SET yrs_update = ?1", [vec![255, 255, 255, 255]]).unwrap();
    }

    // Starting client should succeed (it handles corruption gracefully in recovery/startup without panicking)
    let client_reborn = DriftClient::new(config).await.unwrap();

    // The data shouldn't be loaded correctly, but the system must not panic or crash.
    let res = client_reborn.get("doc-1", "rec-1").await;
    // It should either return Ok(None) or Ok(Some) depending on document table state.
    // The key guarantee is that it did not panic during startup or lookup.
    assert!(res.is_ok());

    client_reborn.shutdown().await.unwrap();
}
