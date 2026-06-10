use std::time::Duration;
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::ipc::shared_memory::{SharedMemory, encode_sync_state, decode_sync_state};
use vaultsync_core::sync::state::{SyncState, ConnectionStatus};

#[tokio::test]
async fn test_single_leader_elected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multitab.db").to_string_lossy().to_string();

    let mut config1 = VaultSyncConfig::default();
    config1.namespace = "multitab-ns".to_string();
    config1.replica_id = "replica-1".to_string();
    config1.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config1.sync_interval = Duration::from_millis(50);

    let mut config2 = VaultSyncConfig::default();
    config2.namespace = "multitab-ns".to_string();
    config2.replica_id = "replica-2".to_string();
    config2.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config2.sync_interval = Duration::from_millis(50);

    let mut config3 = VaultSyncConfig::default();
    config3.namespace = "multitab-ns".to_string();
    config3.replica_id = "replica-3".to_string();
    config3.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config3.sync_interval = Duration::from_millis(50);

    // Initialize client 1
    let client1 = VaultSyncClient::new(config1).await.unwrap();
    // Wait for client 1 to settle as leader
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Initialize client 2 & 3
    let client2 = VaultSyncClient::new(config2).await.unwrap();
    let client3 = VaultSyncClient::new(config3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify leader status
    let status1 = client1.sync_status().await.unwrap();
    let status2 = client2.sync_status().await.unwrap();
    let status3 = client3.sync_status().await.unwrap();

    let leaders_count = [status1.leader_status, status2.leader_status, status3.leader_status]
        .iter()
        .filter(|&&s| s == Some(true))
        .count();

    assert_eq!(leaders_count, 1, "Exactly one client must be elected leader");

    // Clean up
    client1.shutdown().await.unwrap();
    client2.shutdown().await.unwrap();
    client3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_leader_drop_promotes_new_leader() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multitab-drop.db").to_string_lossy().to_string();

    let mut config1 = VaultSyncConfig::default();
    config1.namespace = "multitab-drop-ns".to_string();
    config1.replica_id = "replica-1".to_string();
    config1.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config1.sync_interval = Duration::from_millis(40);

    let mut config2 = VaultSyncConfig::default();
    config2.namespace = "multitab-drop-ns".to_string();
    config2.replica_id = "replica-2".to_string();
    config2.storage = StorageConfig::Sqlite { path: db_path.clone() };
    config2.sync_interval = Duration::from_millis(40);

    // Initialize client 1 (should become leader)
    let client1 = VaultSyncClient::new(config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(client1.sync_status().await.unwrap().leader_status, Some(true));

    // Initialize client 2 (should be reader)
    let client2 = VaultSyncClient::new(config2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(client2.sync_status().await.unwrap().leader_status, Some(false));

    // Drop/Shutdown client 1 (leader)
    client1.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Client 2 should be promoted to leader
    let status2_after = client2.sync_status().await.unwrap();
    assert_eq!(status2_after.leader_status, Some(true), "Client 2 should be promoted to leader after Client 1 is dropped");

    // Clean up
    client2.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_readers_receive_via_shared_memory() {
    // Test shared memory sync state encoding, writing, reading, and decoding
    let shmem = SharedMemory::create(1024).unwrap();

    let state = SyncState {
        namespace: "shmem-ns".to_string(),
        replica_id: "replica-leader".to_string(),
        last_synced_sequence: 1234,
        connection_status: ConnectionStatus::Connected,
        leader_status: Some(true),
        last_connected_at: Some(99999),
        last_sync_at: Some(88888),
        schema_version: 5,
    };

    let encoded = encode_sync_state(&state).unwrap();
    shmem.write(&encoded).unwrap();

    let read_bytes = shmem.read().unwrap();
    let decoded = decode_sync_state(&read_bytes).unwrap();

    assert_eq!(decoded.namespace, "shmem-ns");
    assert_eq!(decoded.replica_id, "replica-leader");
    assert_eq!(decoded.last_synced_sequence, 1234);
    assert_eq!(decoded.connection_status, ConnectionStatus::Connected);
    assert_eq!(decoded.leader_status, Some(true));
    assert_eq!(decoded.schema_version, 5);
}
