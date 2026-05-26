use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use drift_core::storage::traits::StorageConfig;
use drift_core::coordinator::memory::InMemoryCoordinator;

#[tokio::test]
async fn test_end_to_end_client_sync_pipeline() {
    let temp_dir = tempfile::tempdir().unwrap();
    let alice_db_path = temp_dir.path().join("alice.db").to_string_lossy().to_string();
    let bob_db_path = temp_dir.path().join("bob.db").to_string_lossy().to_string();

    // 1. Initialize a shared coordinator and keyring (shared namespace key)
    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());

    // 2. Initialize Alice's client
    let mut config_alice = DriftConfig::default();
    config_alice.namespace = "sync-test-ns".to_string();
    config_alice.replica_id = "replica-alice".to_string();
    config_alice.storage = StorageConfig::Sqlite { path: alice_db_path };
    config_alice.sync_interval = std::time::Duration::from_secs(3600);
    let client_alice = DriftClient::new_with_keyring(config_alice, coordinator.clone(), keyring.clone())
        .await
        .unwrap();

    // 3. Initialize Bob's client
    let mut config_bob = DriftConfig::default();
    config_bob.namespace = "sync-test-ns".to_string();
    config_bob.replica_id = "replica-bob".to_string();
    config_bob.storage = StorageConfig::Sqlite { path: bob_db_path };
    config_bob.sync_interval = std::time::Duration::from_secs(3600);
    let client_bob = DriftClient::new_with_keyring(config_bob, coordinator.clone(), keyring.clone())
        .await
        .unwrap();

    // 4. Set up Bob's subscription listener
    let bob_received_changes = Arc::new(Mutex::new(Vec::new()));
    let bob_changes_clone = bob_received_changes.clone();
    let _sub_handle = client_bob.subscribe("doc-1", Box::new(move |doc_id, record_id, fields| {
        let mut changes = bob_changes_clone.lock().unwrap();
        changes.push((doc_id.to_string(), record_id.to_string(), fields.clone()));
    }));

    // 5. Alice inserts a document
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), CrdtValue::String("Local First".to_string()));
    fields.insert("version".to_string(), CrdtValue::Number(1.0));
    client_alice.insert("doc-1", "record-1", fields).await.unwrap();

    // Verify Alice's local database contains the record
    let alice_doc = client_alice.get("doc-1", "record-1").await.unwrap().unwrap();
    assert_eq!(alice_doc.get("title").unwrap(), &CrdtValue::String("Local First".to_string()));
    assert_eq!(alice_doc.get("version").unwrap(), &CrdtValue::Number(1.0));

    // Verify Bob does not have it yet
    let bob_doc_before = client_bob.get("doc-1", "record-1").await.unwrap();
    assert!(bob_doc_before.is_none());

    // 6. Alice pushes mutations to the coordinator
    let pending_count = client_alice.pending_uploads().await.unwrap();
    println!("Alice's pending uploads count: {}", pending_count);
    let uploaded = client_alice.force_sync().await.unwrap();
    assert_eq!(uploaded, 2); // 1 uploaded + 1 downloaded (self loopback)

    // 7. Bob pulls mutations from the coordinator, decrypts them, and applies them
    let downloaded = client_bob.force_sync().await.unwrap();
    assert_eq!(downloaded, 1); // 0 uploaded + 1 downloaded

    // 8. Verify Bob's database contains the record
    let bob_doc_opt = client_bob.get("doc-1", "record-1").await.unwrap();
    println!("Bob doc opt: {:?}", bob_doc_opt);
    let bob_doc_after = bob_doc_opt.unwrap();
    assert_eq!(bob_doc_after.get("title").unwrap(), &CrdtValue::String("Local First".to_string()));
    assert_eq!(bob_doc_after.get("version").unwrap(), &CrdtValue::Number(1.0));

    // 9. Verify Bob's subscription callback fired
    let changes = bob_received_changes.lock().unwrap();
    assert_eq!(changes.len(), 1);
    let (ref doc_id, ref record_id, ref fields) = changes[0];
    assert_eq!(doc_id, "doc-1");
    assert_eq!(record_id, "record-1");
    assert_eq!(fields.get("title").unwrap(), &CrdtValue::String("Local First".to_string()));

    drop(changes);

    // 10. Bob updates the document
    let mut update_fields = HashMap::new();
    update_fields.insert("title".to_string(), CrdtValue::String("Local First Sync Engine".to_string()));
    update_fields.insert("version".to_string(), CrdtValue::Number(2.0));
    client_bob.update("doc-1", "record-1", update_fields).await.unwrap();

    // Bob syncs to upload the update
    let bob_uploaded = client_bob.force_sync().await.unwrap();
    assert_eq!(bob_uploaded, 2); // 1 uploaded + 1 downloaded (self loopback)

    // Alice syncs to download the update
    let alice_downloaded = client_alice.force_sync().await.unwrap();
    assert_eq!(alice_downloaded, 1); // 0 uploaded + 1 downloaded

    // Verify Alice's local database contains the updated record
    let alice_doc_updated = client_alice.get("doc-1", "record-1").await.unwrap().unwrap();
    assert_eq!(alice_doc_updated.get("title").unwrap(), &CrdtValue::String("Local First Sync Engine".to_string()));
    assert_eq!(alice_doc_updated.get("version").unwrap(), &CrdtValue::Number(2.0));

    // 11. Bob performs a soft-delete (tombstone)
    client_bob.delete("doc-1", "record-1").await.unwrap();

    // Verify Bob's get returns None
    assert!(client_bob.get("doc-1", "record-1").await.unwrap().is_none());

    // Bob syncs to upload the deletion tombstone
    let bob_deleted = client_bob.force_sync().await.unwrap();
    assert_eq!(bob_deleted, 2); // 1 uploaded + 1 downloaded (self loopback)

    // Alice syncs to pull the deletion tombstone
    let alice_deleted = client_alice.force_sync().await.unwrap();
    assert_eq!(alice_deleted, 1); // 0 uploaded + 1 downloaded

    // Verify Alice's get now also returns None
    assert!(client_alice.get("doc-1", "record-1").await.unwrap().is_none());
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

