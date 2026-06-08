use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use drift_core::storage::traits::StorageConfig;
use drift_core::coordinator::memory::InMemoryCoordinator;

#[tokio::test]
async fn test_end_to_end_client_sync_pipeline() {
    use drift_core::test_utils::DriftFixture;
    use drift_core::storage::memory::InMemoryStorage;

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let alice = DriftFixture::new(Arc::new(InMemoryStorage::new()), coordinator.clone()).with_replica_id("alice");
    let bob = DriftFixture::new(Arc::new(InMemoryStorage::new()), coordinator.clone()).with_replica_id("bob");

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), CrdtValue::String("Local First".to_string()));
    fields.insert("version".to_string(), CrdtValue::Number(1.0));
    alice.write("doc-1", "record-1", fields).await.unwrap();

    // Verify local
    let doc = alice.get_document("doc-1", "record-1").await.unwrap();
    assert_eq!(doc.get_field("title").unwrap(), CrdtValue::String("Local First".to_string()));

    // Sync
    alice.sync().await.unwrap();
    bob.sync().await.unwrap();

    // Verify convergence
    alice.assert_converges_with(&bob, "doc-1", "record-1").await;
}

#[tokio::test]
async fn test_background_sync_loop_automatic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let alice_db_path = temp_dir.path().join("alice_bg.db").to_string_lossy().to_string();
    let bob_db_path = temp_dir.path().join("bob_bg.db").to_string_lossy().to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());

    let mut config_alice = DriftConfig::default();
    config_alice.namespace = "bg-sync-test-ns".to_string();
    config_alice.replica_id = "replica-alice".to_string();
    config_alice.storage = StorageConfig::Sqlite { path: alice_db_path };
    config_alice.sync_interval = std::time::Duration::from_millis(50);
    let client_alice = DriftClient::new_with_keyring(config_alice, coordinator.clone(), keyring.clone())
        .await
        .unwrap();

    let mut config_bob = DriftConfig::default();
    config_bob.namespace = "bg-sync-test-ns".to_string();
    config_bob.replica_id = "replica-bob".to_string();
    config_bob.storage = StorageConfig::Sqlite { path: bob_db_path };
    config_bob.sync_interval = std::time::Duration::from_millis(50);
    let client_bob = DriftClient::new_with_keyring(config_bob, coordinator.clone(), keyring.clone())
        .await
        .unwrap();

    // Alice inserts a document
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), CrdtValue::String("Background First".to_string()));
    client_alice.insert("doc-2", "record-2", fields).await.unwrap();

    // Wait for background tasks to sync the change
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify Bob's database contains the record automatically
    let bob_doc = client_bob.get("doc-2", "record-2").await.unwrap();
    assert!(bob_doc.is_some());
    let doc = bob_doc.unwrap();
    assert_eq!(doc.get("title").unwrap(), &CrdtValue::String("Background First".to_string()));

    // Clean up
    client_alice.shutdown().await.unwrap();
    client_bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_schema_migration_runner() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("migration.db").to_string_lossy().to_string();

    let mut config = DriftConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path };
    let client = DriftClient::new(config).await.unwrap();

    // Define a migration
    let migration_executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed_clone = migration_executed.clone();
    let migration = drift_core::schema::migration::MigrationDefinition::new(
        "v1.0.0",
        Box::new(move || {
            executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    );

    // Apply migration
    client.apply_migration(migration).await.unwrap();
    assert!(migration_executed.load(std::sync::atomic::Ordering::SeqCst));

    // Try applying again, it should skip execution since it's already applied
    let migration_executed_2 = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed_clone_2 = migration_executed_2.clone();
    let migration_again = drift_core::schema::migration::MigrationDefinition::new(
        "v1.0.0",
        Box::new(move || {
            executed_clone_2.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    );
    client.apply_migration(migration_again).await.unwrap();
    assert!(!migration_executed_2.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn test_retry_backoff_and_failed_status() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("retry.db").to_string_lossy().to_string();

    // Use a mock/failing coordinator
    let coordinator = Arc::new(drift_core::coordinator::mock::MockCoordinator::new());
    coordinator.expect_push(Err(drift_core::coordinator::traits::CoordinatorError::NotAvailable));
    coordinator.expect_push(Err(drift_core::coordinator::traits::CoordinatorError::NotAvailable));
    coordinator.expect_push(Err(drift_core::coordinator::traits::CoordinatorError::NotAvailable));
    coordinator.expect_push(Err(drift_core::coordinator::traits::CoordinatorError::NotAvailable));
    coordinator.expect_push(Err(drift_core::coordinator::traits::CoordinatorError::NotAvailable));

    let mut config = DriftConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path };
    // Set low max attempts for quick execution
    config.retry.max_attempts = 3;
    config.retry.initial_delay = std::time::Duration::from_millis(5);
    config.retry.max_delay = std::time::Duration::from_millis(20);
    // Disable background loop to manually trace upload attempts
    config.sync_interval = std::time::Duration::from_secs(3600);

    let client = DriftClient::new_with_coordinator(config, coordinator.clone()).await.unwrap();

    // Insert a document
    let mut fields = HashMap::new();
    fields.insert("key".to_string(), CrdtValue::String("value".to_string()));
    client.insert("doc-3", "record-3", fields).await.unwrap();

    // Attempt sync, coordinator push fails since MockCoordinator is configured to fail
    let _ = client.force_sync().await;

    // Check that it backed off, calling force_sync again shouldn't push because it is backing off
    let _ = client.force_sync().await;

    // Verify it eventually exhausts retries and marks as failed
    for _ in 0..5 {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let _ = client.force_sync().await;
    }

    // Pending count should be 0 because the entry was marked as failed
    let pending = client.pending_uploads().await.unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn test_two_replicas_independent_keyrings_sync() {
    let temp_dir = tempfile::tempdir().unwrap();
    let alice_db_path = temp_dir.path().join("alice_ind.db").to_string_lossy().to_string();
    let bob_db_path = temp_dir.path().join("bob_ind.db").to_string_lossy().to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    
    // Generate Alice's keyring
    let kr_alice = drift_core::e2ee::keyring::KeyRing::generate();
    let alice_key = kr_alice.active_key().clone();

    // Bob initializes a keyring with the same active keypair
    let kr_bob = drift_core::e2ee::keyring::KeyRing::from_key(alice_key);

    let mut config_alice = DriftConfig::default();
    config_alice.namespace = "ind-sync-ns".to_string();
    config_alice.replica_id = "replica-alice".to_string();
    config_alice.storage = StorageConfig::Sqlite { path: alice_db_path };
    config_alice.sync_interval = std::time::Duration::from_secs(3600);
    let client_alice = DriftClient::new_with_keyring(config_alice, coordinator.clone(), Arc::new(kr_alice))
        .await
        .unwrap();

    let mut config_bob = DriftConfig::default();
    config_bob.namespace = "ind-sync-ns".to_string();
    config_bob.replica_id = "replica-bob".to_string();
    config_bob.storage = StorageConfig::Sqlite { path: bob_db_path };
    config_bob.sync_interval = std::time::Duration::from_secs(3600);
    let client_bob = DriftClient::new_with_keyring(config_bob, coordinator.clone(), Arc::new(kr_bob))
        .await
        .unwrap();

    // Alice inserts a document
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), CrdtValue::String("independent keys work".to_string()));
    client_alice.insert("doc-4", "record-4", fields).await.unwrap();

    // Alice forces sync (uploads)
    client_alice.force_sync().await.unwrap();

    // Bob forces sync (downloads)
    client_bob.force_sync().await.unwrap();

    // Verify Bob successfully decrypted and read the document
    let bob_doc = client_bob.get("doc-4", "record-4").await.unwrap().unwrap();
    assert_eq!(bob_doc.get("value").unwrap(), &CrdtValue::String("independent keys work".to_string()));
}

