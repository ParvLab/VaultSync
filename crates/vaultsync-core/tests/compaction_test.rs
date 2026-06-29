use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::sync::compaction::{CompactionConfig, CompactionEngine};
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;

#[tokio::test]
async fn test_snapshot_compaction_reduces_oplog() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("compaction.db")
        .to_string_lossy()
        .to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = "compaction-ns".to_string();
    config.replica_id = "compaction-replica".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path };
    config.sync_interval = Duration::from_secs(3600); // disable auto-sync

    let client =
        VaultSyncClient::new_with_keyring(config.clone(), coordinator.clone(), keyring.clone())
            .await
            .unwrap();

    // 1. Insert a document
    let mut fields = HashMap::new();
    fields.insert("counter".to_string(), CrdtValue::Number(0.0));
    client.insert("doc-1", "record-1", fields).await.unwrap();

    // 2. Perform 150 updates to generate many oplog entries
    for i in 1..=150 {
        let mut fields = HashMap::new();
        fields.insert("counter".to_string(), CrdtValue::Number(i as f64));
        client.update("doc-1", "record-1", fields).await.unwrap();
    }

    // 3. Sync to mark all oplog entries as 'Synced'
    while client.pending_uploads().await.unwrap() > 0 {
        client.force_sync().await.unwrap();
    }

    // Retrieve storage from client using internal access or direct SQLite read
    // Since client has its storage, let's create a CompactionEngine with a low threshold
    let mut comp_config = CompactionConfig::default();
    comp_config.snapshot_entry_threshold = 50; // Threshold lower than 150 entries

    // We can extract/access the client storage using vaultsync_core's internal structure if it's pub,
    // or by opening another SQLiteStorage or InMemoryStorage. But wait, since we configured
    // config.storage as Sqlite, let's create another Storage reader or let the client run it.
    // Wait, let's just create a new CompactionEngine pointing to the same database!
    let storage = vaultsync_core::storage::sqlite::SQLiteStorage::new(
        &temp_dir.path().join("compaction.db").to_string_lossy(),
    )
    .unwrap();
    let comp_engine = CompactionEngine::new(Arc::new(storage), comp_config);

    // 4. Verify oplog synced entries count is high
    let active_docs = comp_engine
        .run_snapshot_compaction("compaction-ns")
        .await
        .unwrap();
    assert_eq!(active_docs.snapshots_collapsed, 1);
    assert!(active_docs.oplog_removed >= 100);

    // 5. Verify the state still converges perfectly
    let doc_val = client.get("doc-1", "record-1").await.unwrap().unwrap();
    assert_eq!(doc_val.get("counter").unwrap(), &CrdtValue::Number(150.0));
}

#[tokio::test]
async fn test_tombstone_gc_removes_deleted_docs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("tombstone.db")
        .to_string_lossy()
        .to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = "tombstone-ns".to_string();
    config.replica_id = "tombstone-replica".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path };
    config.sync_interval = Duration::from_secs(3600);

    let client =
        VaultSyncClient::new_with_keyring(config.clone(), coordinator.clone(), keyring.clone())
            .await
            .unwrap();

    // 1. Insert a document
    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        CrdtValue::String("GC Target".to_string()),
    );
    client.insert("doc-gc", "record-gc", fields).await.unwrap();

    // 2. Soft-delete the document
    client.delete("doc-gc", "record-gc").await.unwrap();

    // Verify it is soft-deleted (get returns None because it filters deleted)
    let get_before = client.get("doc-gc", "record-gc").await.unwrap();
    assert!(get_before.is_none());

    // 3. Sync to mark deletions as synced
    client.force_sync().await.unwrap();

    // Sleep a tiny bit to make sure SystemTime progresses past the creation timestamp
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Create CompactionEngine with 0 grace period so it is GC'd immediately
    let storage = vaultsync_core::storage::sqlite::SQLiteStorage::new(
        &temp_dir.path().join("tombstone.db").to_string_lossy(),
    )
    .unwrap();
    let mut comp_config = CompactionConfig::default();
    comp_config.tombstone_grace_secs = 0;
    let comp_engine = CompactionEngine::new(Arc::new(storage), comp_config);

    // Run tombstone GC compaction
    let stats = comp_engine.run_compaction("tombstone-ns").await.unwrap();
    assert_eq!(stats.docs_removed, 1);
}

#[tokio::test]
async fn test_key_rotation_re_encrypts_oplog() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("rotation.db")
        .to_string_lossy()
        .to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = "rotation-ns".to_string();
    config.replica_id = "rotation-replica".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path };
    config.sync_interval = Duration::from_secs(3600);

    let client =
        VaultSyncClient::new_with_keyring(config.clone(), coordinator.clone(), keyring.clone())
            .await
            .unwrap();

    // 1. Write an update (this generates a pending, encrypted oplog entry)
    let mut fields = HashMap::new();
    fields.insert(
        "secret".to_string(),
        CrdtValue::String("encrypted content".to_string()),
    );
    client
        .insert("doc-rot", "record-rot", fields)
        .await
        .unwrap();

    // 2. Rotate keys (this re-encrypts the pending oplog entry)
    let active_v1 = client.export_public_key().await.unwrap();
    client.rotate_keys().await.unwrap();
    let active_v2 = client.export_public_key().await.unwrap();
    assert_ne!(active_v1, active_v2);

    // 3. Sync to push re-encrypted oplog entry
    let synced = client.force_sync().await.unwrap();
    assert!(synced > 0);

    // 4. Verify we can still decrypt and read the converges state
    let doc_val = client.get("doc-rot", "record-rot").await.unwrap().unwrap();
    assert_eq!(
        doc_val.get("secret").unwrap(),
        &CrdtValue::String("encrypted content".to_string())
    );
}

#[tokio::test]
async fn test_key_rotation_uses_random_nonces() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir
        .path()
        .join("nonces.db")
        .to_string_lossy()
        .to_string();

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

    let mut config = VaultSyncConfig::default();
    config.namespace = "nonces-ns".to_string();
    config.replica_id = "nonces-replica".to_string();
    config.storage = StorageConfig::Sqlite { path: db_path };
    config.sync_interval = Duration::from_secs(3600);

    let client =
        VaultSyncClient::new_with_keyring(config.clone(), coordinator.clone(), keyring.clone())
            .await
            .unwrap();

    let _metrics = client.metrics.clone(); // arbitrary fetch to access client

    // Let's encrypt the same plaintext twice using client's active encryptor
    let plaintext = b"confidential document text";
    let ciphertext1 = client
        .rotate_keys()
        .await
        .map(|_| {
            let _enc = keyring.active_key();
            let enc_helper = vaultsync_core::e2ee::keyring::E2eeEncryptor::new(keyring.clone());
            enc_helper
                .encrypt_symmetric(plaintext, "nonces-ns")
                .unwrap()
        })
        .unwrap();

    let ciphertext2 = {
        let enc_helper = vaultsync_core::e2ee::keyring::E2eeEncryptor::new(keyring.clone());
        enc_helper
            .encrypt_symmetric(plaintext, "nonces-ns")
            .unwrap()
    };

    // Since a random nonce is prepended, the ciphertext bytes must differ even for the same key and same plaintext!
    assert_ne!(ciphertext1, ciphertext2);

    // Decrypt both ciphertexts and check they successfully yield the original plaintext!
    let dec_helper = vaultsync_core::e2ee::keyring::E2eeDecryptor::new(keyring.clone());
    let dec1 = dec_helper
        .decrypt_symmetric(&ciphertext1, "nonces-ns")
        .unwrap();
    let dec2 = dec_helper
        .decrypt_symmetric(&ciphertext2, "nonces-ns")
        .unwrap();
    assert_eq!(dec1, plaintext);
    assert_eq!(dec2, plaintext);
}

mod oplog_cleanup {
    use std::sync::Arc;
    use std::time::Duration;
    use vaultsync_core::{
        oplog::cleanup::OplogCleanup,
        oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus},
        storage::memory::InMemoryStorage,
        storage::traits::Storage,
    };

    #[tokio::test]
    async fn test_cleanup_removes_old_synced_entries() {
        let storage = Arc::new(InMemoryStorage::new());
        let cleanup = OplogCleanup::new(storage.clone());

        // Insert old synced entry (timestamp 0 = very old)
        let old_entry = OplogEntry {
            id: "old-synced".to_string(),
            replica_id: "replica-1".to_string(),
            namespace: "ns".to_string(),
            mutation_type: MutationType::CrdtInsert,
            doc_id: "doc-1".to_string(),
            record_id: "rec-1".to_string(),
            yrs_update: vec![1, 2, 3],
            encrypted_blob: None,
            timestamp: 0,
            sequence: Some(1),
            sync_status: SyncStatus::Synced,
            synced_at: Some(0),
            created_at: 0, // epoch
            origin: MutationOrigin::Unknown,
            origin_context: String::new(),
        };
        storage.append_oplog(&old_entry).await.unwrap();

        // Run compaction with 1-day retention
        let removed = cleanup
            .compact("ns", Duration::from_secs(86400))
            .await
            .unwrap();
        assert_eq!(removed, 1, "Should have removed 1 old synced entry");

        let remaining = storage.read_pending_oplog("ns", 100).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_never_removes_pending_entries() {
        let storage = Arc::new(InMemoryStorage::new());
        let cleanup = OplogCleanup::new(storage.clone());

        // Insert very old pending entry
        let old_pending = OplogEntry {
            id: "old-pending".to_string(),
            replica_id: "replica-1".to_string(),
            namespace: "ns".to_string(),
            mutation_type: MutationType::CrdtInsert,
            doc_id: "doc-1".to_string(),
            record_id: "rec-1".to_string(),
            yrs_update: vec![1, 2, 3],
            encrypted_blob: None,
            timestamp: 0,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: 0,
            origin: MutationOrigin::Unknown,
            origin_context: String::new(),
        };
        storage.append_oplog(&old_pending).await.unwrap();

        let removed = cleanup
            .compact("ns", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(removed, 0, "Pending entries must NEVER be removed");

        let pending = storage.read_pending_oplog("ns", 100).await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_cleanup_returns_correct_count() {
        let storage = Arc::new(InMemoryStorage::new());
        let cleanup = OplogCleanup::new(storage.clone());

        // 5 old synced + 3 old pending + 2 new synced
        for i in 0..5 {
            let e = OplogEntry {
                id: format!("synced-old-{i}"),
                replica_id: "replica-1".to_string(),
                namespace: "ns".to_string(),
                mutation_type: MutationType::CrdtInsert,
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                yrs_update: vec![1, 2, 3],
                encrypted_blob: None,
                timestamp: 0,
                sequence: Some(i),
                sync_status: SyncStatus::Synced,
                synced_at: Some(0),
                created_at: 0,
                origin: MutationOrigin::Unknown,
                origin_context: String::new(),
            };
            storage.append_oplog(&e).await.unwrap();
        }
        for i in 5..8 {
            let e = OplogEntry {
                id: format!("pending-old-{i}"),
                replica_id: "replica-1".to_string(),
                namespace: "ns".to_string(),
                mutation_type: MutationType::CrdtInsert,
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                yrs_update: vec![1, 2, 3],
                encrypted_blob: None,
                timestamp: 0,
                sequence: None,
                sync_status: SyncStatus::Pending,
                synced_at: None,
                created_at: 0,
                origin: MutationOrigin::Unknown,
                origin_context: String::new(),
            };
            storage.append_oplog(&e).await.unwrap();
        }
        for i in 8..10 {
            let e = OplogEntry {
                id: format!("synced-new-{i}"),
                replica_id: "replica-1".to_string(),
                namespace: "ns".to_string(),
                mutation_type: MutationType::CrdtInsert,
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                yrs_update: vec![1, 2, 3],
                encrypted_blob: None,
                timestamp: u64::MAX,
                sequence: Some(i),
                sync_status: SyncStatus::Synced,
                synced_at: Some(u64::MAX),
                created_at: u64::MAX, // future
                origin: MutationOrigin::Unknown,
                origin_context: String::new(),
            };
            storage.append_oplog(&e).await.unwrap();
        }

        let removed = cleanup
            .compact("ns", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(removed, 5, "Only old synced entries should be removed");
    }
}
