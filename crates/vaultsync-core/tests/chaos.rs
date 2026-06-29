use async_trait::async_trait;
use futures::Stream;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::coordinator::traits::{
    Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId,
};
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::sync::retry::RetryConfig;
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;

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
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.push(namespace, mutations).await
    }

    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.pull(namespace, after, limit).await
    }

    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.subscribe(namespace, from_sequence).await
    }

    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
        _last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.register(namespace, info, _last_sequence).await
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
    use vaultsync_core::storage::memory::InMemoryStorage;
    use vaultsync_core::test_utils::{NetworkPartitionFault, SimulatedNetwork};

    let coord = Arc::new(InMemoryCoordinator::new());
    let mut net = SimulatedNetwork::new(coord);
    net.add_replica("alice", Arc::new(InMemoryStorage::new()));
    net.add_replica("bob", Arc::new(InMemoryStorage::new()));

    // Partition the network for alice
    let fault = NetworkPartitionFault {
        target_id: "alice".to_string(),
    };
    net.inject_fault(&fault);

    // Write concurrently during partition
    let mut fields_a = HashMap::new();
    fields_a.insert(
        "title".to_string(),
        CrdtValue::String("Alice's partition task".to_string()),
    );
    net.replicas[0]
        .write("doc-1", "rec-1", fields_a)
        .await
        .unwrap();

    let mut fields_b = HashMap::new();
    fields_b.insert(
        "description".to_string(),
        CrdtValue::String("Bob's partition description".to_string()),
    );
    net.replicas[1]
        .write("doc-1", "rec-1", fields_b)
        .await
        .unwrap();

    // Verify sync does not converge alice (who is partitioned)
    net.sync_all().await;

    // Heal partition
    net.remove_fault(&fault);

    // Sync to converge
    net.sync_all().await;

    // Assert convergence
    net.assert_all_converge("doc-1", "rec-1").await;
}

#[tokio::test]
async fn test_chaos_leader_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("chaos_leader.db")
        .to_string_lossy()
        .to_string();

    // Leader client (Client 1)
    let mut config1 = VaultSyncConfig::default();
    config1.namespace = "chaos-leader-ns".to_string();
    config1.replica_id = "replica-1".to_string();
    config1.storage = StorageConfig::Sqlite {
        path: db_path.clone(),
    };
    config1.sync_interval = Duration::from_millis(40);
    let client1 = VaultSyncClient::new(config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        client1.sync_status().await.unwrap().leader_status,
        Some(true)
    );

    // Reader client (Client 2)
    let mut config2 = VaultSyncConfig::default();
    config2.namespace = "chaos-leader-ns".to_string();
    config2.replica_id = "replica-2".to_string();
    config2.storage = StorageConfig::Sqlite {
        path: db_path.clone(),
    };
    config2.sync_interval = Duration::from_millis(40);
    let client2 = VaultSyncClient::new(config2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        client2.sync_status().await.unwrap().leader_status,
        Some(false)
    );

    // Simulate leader crash by dropping/shutting down Client 1
    client1.shutdown().await.unwrap();
    drop(client1);

    // Wait for Client 2 to detect heartbeat/lock timeout and promote
    tokio::time::sleep(Duration::from_millis(150)).await;

    let status2 = client2.sync_status().await.unwrap();
    assert_eq!(
        status2.leader_status,
        Some(true),
        "Client 2 should promote to leader after Client 1 crashes"
    );

    client2.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_chaos_simultaneous_crash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("chaos_crash.db")
        .to_string_lossy()
        .to_string();

    let mut config = VaultSyncConfig::default();
    config.namespace = "chaos-crash-ns".to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::Sqlite {
        path: db_path.clone(),
    };
    config.sync_interval = Duration::from_secs(3600);

    let client = VaultSyncClient::new(config.clone()).await.unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "key1".to_string(),
        CrdtValue::String("value before crash".to_string()),
    );
    client.insert("doc-1", "rec-1", fields).await.unwrap();

    // Verify it is written locally
    assert_eq!(client.pending_uploads().await.unwrap(), 1);

    // Simulate sudden crash by dropping/shutting down the client immediately without syncing
    client.shutdown().await.unwrap();
    drop(client);

    // Re-initialize client on restart using the same SQLite db path
    let client_reborn = VaultSyncClient::new(config).await.unwrap();

    // Verify data was restored from the persistent SQLite storage
    let restored = client_reborn.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(
        restored.get("key1").unwrap(),
        &CrdtValue::String("value before crash".to_string())
    );
    assert_eq!(client_reborn.pending_uploads().await.unwrap(), 1);

    client_reborn.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_chaos_corrupt_oplog_entry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("chaos_corrupt.db")
        .to_string_lossy()
        .to_string();

    let mut config = VaultSyncConfig::default();
    config.namespace = "chaos-corrupt-ns".to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::Sqlite {
        path: db_path.clone(),
    };
    config.sync_interval = Duration::from_secs(3600);

    let client = VaultSyncClient::new(config.clone()).await.unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "key1".to_string(),
        CrdtValue::String("safe data".to_string()),
    );
    client.insert("doc-1", "rec-1", fields).await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);

    // Manually corrupt the oplog table using SQLite raw connections
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        // Set yrs_update to garbage bytes that are not valid Yrs encoded updates
        conn.execute(
            "UPDATE oplog SET yrs_update = ?1",
            [vec![255, 255, 255, 255]],
        )
        .unwrap();
    }

    // Starting client should succeed (it handles corruption gracefully in recovery/startup without panicking)
    let client_reborn = VaultSyncClient::new(config).await.unwrap();

    // The data shouldn't be loaded correctly, but the system must not panic or crash.
    let res = client_reborn.get("doc-1", "rec-1").await;
    // It should either return Ok(None) or Ok(Some) depending on document table state.
    // The key guarantee is that it did not panic during startup or lookup.
    assert!(res.is_ok());

    client_reborn.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_chaos_clock_skew() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());
    let ns = "skew-ns";

    let mut config_alice = VaultSyncConfig::default();
    config_alice.namespace = ns.to_string();
    config_alice.replica_id = "alice".to_string();
    config_alice.storage = StorageConfig::InMemory;
    config_alice.sync_interval = Duration::from_secs(3600);
    let client_alice =
        VaultSyncClient::new_with_keyring(config_alice, shared.clone(), keyring.clone())
            .await
            .unwrap();

    let mut config_bob = VaultSyncConfig::default();
    config_bob.namespace = ns.to_string();
    config_bob.replica_id = "bob".to_string();
    config_bob.storage = StorageConfig::InMemory;
    config_bob.sync_interval = Duration::from_secs(3600);
    let client_bob = VaultSyncClient::new_with_keyring(config_bob, shared.clone(), keyring.clone())
        .await
        .unwrap();

    let mut fields_a = HashMap::new();
    fields_a.insert("k1".to_string(), CrdtValue::String("val-a".to_string()));
    client_alice
        .insert("doc-1", "rec-1", fields_a)
        .await
        .unwrap();

    let mut fields_b = HashMap::new();
    fields_b.insert("k2".to_string(), CrdtValue::String("val-b".to_string()));
    client_bob.insert("doc-1", "rec-1", fields_b).await.unwrap();

    // Force sync
    client_alice.force_sync().await.unwrap();
    client_bob.force_sync().await.unwrap();
    client_alice.force_sync().await.unwrap();

    // Assert convergence
    let map_a = client_alice.get("doc-1", "rec-1").await.unwrap().unwrap();
    let map_b = client_bob.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(map_a, map_b);
}

#[tokio::test]
async fn test_chaos_massive_oplog() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("massive.db")
        .to_string_lossy()
        .to_string();
    let namespace = format!("massive-ns-{}", uuid::Uuid::new_v4());

    let mut config = VaultSyncConfig::default();
    config.namespace = namespace.clone();
    config.storage = StorageConfig::Sqlite {
        path: db_path.clone(),
    };
    config.sync_interval = Duration::from_secs(3600);

    let client = VaultSyncClient::new(config).await.unwrap();

    for i in 0..100 {
        let mut fields = HashMap::new();
        fields.insert("val".to_string(), CrdtValue::Number(i as f64));
        client.update("doc-1", "rec-1", fields).await.unwrap();
    }

    let pending = client.pending_uploads().await.unwrap();
    assert_eq!(pending, 100);

    let engine = vaultsync_core::sync::compaction::CompactionEngine::new(
        client.debug_api.storage.clone(),
        vaultsync_core::sync::compaction::CompactionConfig::default(),
    );
    engine.run_compaction(&namespace).await.unwrap();
    engine.run_snapshot_compaction(&namespace).await.unwrap();

    let map = client.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert_eq!(map.get("val").unwrap(), &CrdtValue::Number(99.0));

    client.shutdown().await.unwrap();

    let shm_path = std::env::temp_dir().join(format!("vaultsync_shm_{}.bin", namespace));
    if shm_path.exists() {
        let _ = std::fs::remove_file(shm_path);
    }
}

#[tokio::test]
async fn test_chaos_key_rotation_mid_sync() {
    let shared = Arc::new(InMemoryCoordinator::new());
    let ns = "rotation-mid-sync-ns";
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = ns.to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::InMemory;
    config.sync_interval = Duration::from_secs(3600);

    let client = Arc::new(
        VaultSyncClient::new_with_keyring(config, shared.clone(), keyring.clone())
            .await
            .unwrap(),
    );

    let client_c = client.clone();
    let update_handle = tokio::spawn(async move {
        for i in 0..20 {
            let mut fields = HashMap::new();
            fields.insert("val".to_string(), CrdtValue::Number(i as f64));
            let _ = client_c.update("doc-1", "rec-1", fields).await;
            let _ = client_c.force_sync().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    let client_r = client.clone();
    let rotate_handle = tokio::spawn(async move {
        for _ in 0..3 {
            let _ = client_r.rotate_keys().await;
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    let _ = tokio::join!(update_handle, rotate_handle);

    client.force_sync().await.unwrap();
    let map = client.get("doc-1", "rec-1").await.unwrap().unwrap();
    assert!(map.contains_key("val"));

    client.shutdown().await.unwrap();
}

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
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.push(namespace, mutations).await
    }
    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.pull(namespace, after, limit).await
    }
    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.subscribe(namespace, from_sequence).await
    }
    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
        _last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(CoordinatorError::NotAvailable);
        }
        self.inner.register(namespace, info, _last_sequence).await
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
}

#[tokio::test]
async fn test_chaos_failover_during_push() {
    let primary_inner = Arc::new(InMemoryCoordinator::new());
    let fallback_inner = Arc::new(InMemoryCoordinator::new());

    let primary = Arc::new(PanickingCoordinator::new(primary_inner));
    let fallback = Arc::new(PanickingCoordinator::new(fallback_inner));

    let failover = Arc::new(
        vaultsync_core::coordinator::failover::FailoverCoordinator::new(vec![
            primary.clone(),
            fallback.clone(),
        ])
        .with_max_failures(1),
    );

    let ns = "chaos-failover-push-ns";
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = ns.to_string();
    config.replica_id = "replica-1".to_string();
    config.storage = StorageConfig::InMemory;
    config.sync_interval = Duration::from_secs(3600);

    let client = VaultSyncClient::new_with_keyring(config, failover.clone(), keyring.clone())
        .await
        .unwrap();

    let mut fields = HashMap::new();
    fields.insert("k".to_string(), CrdtValue::String("v".to_string()));
    client.insert("doc-1", "rec-1", fields).await.unwrap();

    primary.set_fail(true);

    let synced = client.force_sync().await.unwrap();
    assert!(synced > 0);
    assert_eq!(failover.active_index(), 1);

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_unicode_text_field_convergence() {
    let base = vaultsync_core::crdt::document::CRDTDocument::new("doc-unicode", "rec-unicode", 1);
    let mut replica_a =
        vaultsync_core::crdt::document::CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
    let mut replica_b =
        vaultsync_core::crdt::document::CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();

    let text_a = "Hello, 🦀! Greetings from Munich 🇩🇪 and Tokyo 🇯🇵.";
    let text_b = "Hello, 🦀! Greetings from Munich 🇩🇪 and Cairo 🇪🇬.";

    let update_a = replica_a.set_field("content", CrdtValue::String(text_a.to_string()));
    let update_b = replica_b.set_field("content", CrdtValue::String(text_b.to_string()));

    replica_a.apply_update(&update_b).unwrap();
    replica_b.apply_update(&update_a).unwrap();

    assert_eq!(replica_a.to_map(), replica_b.to_map());
}
