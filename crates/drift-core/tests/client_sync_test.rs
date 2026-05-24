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
    let client_alice = DriftClient::new_with_keyring(config_alice, coordinator.clone(), keyring.clone())
        .await
        .unwrap();

    // 3. Initialize Bob's client
    let mut config_bob = DriftConfig::default();
    config_bob.namespace = "sync-test-ns".to_string();
    config_bob.replica_id = "replica-bob".to_string();
    config_bob.storage = StorageConfig::Sqlite { path: bob_db_path };
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
