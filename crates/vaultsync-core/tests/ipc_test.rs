use std::sync::Arc;
use vaultsync_core::crdt::document::CRDTDocument;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::ipc::crash_recovery::CrashRecovery;
use vaultsync_core::ipc::leader_election::LeaderElection;
use vaultsync_core::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::memory::InMemoryStorage;
use vaultsync_core::storage::traits::{Storage, StorageConfig};
use vaultsync_core::sync::compaction::{CompactionConfig, CompactionEngine};

#[tokio::test]
async fn test_leader_election_acquire_and_release() {
    let ns = "test-ns-acquire-release";
    let le = LeaderElection::new(ns, &StorageConfig::InMemory);
    assert!(!le.is_leader());

    let acquired = le.try_acquire().expect("acquire succeeded");
    assert!(acquired);
    assert!(le.is_leader());

    le.release();
    assert!(!le.is_leader());
}

#[tokio::test]
async fn test_leader_election_second_acquire_fails() {
    let ns = "test-ns-second-acquire";
    let le1 = LeaderElection::new(ns, &StorageConfig::InMemory);
    let le2 = LeaderElection::new(ns, &StorageConfig::InMemory);

    let acquired1 = le1.try_acquire().expect("first acquire succeeded");
    assert!(acquired1);

    let acquired2 = le2.try_acquire().expect("second acquire attempted");
    assert!(!acquired2);

    le1.release();

    let acquired2_again = le2
        .try_acquire()
        .expect("second acquire succeeded after release");
    assert!(acquired2_again);

    le2.release();
}

#[tokio::test]
async fn test_crash_recovery_requeues_stale_pending() {
    let storage = Arc::new(InMemoryStorage::new());
    let ns = "test-ns-recovery";

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // 1. Insert a failed entry created 100 seconds ago (stale)
    let stale_entry = OplogEntry {
        id: "stale-id".to_string(),
        replica_id: "replica-1".to_string(),
        namespace: ns.to_string(),
        mutation_type: MutationType::CrdtUpdate,
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        yrs_update: vec![1, 2, 3],
        encrypted_blob: None,
        timestamp: now - 100_000,
        sequence: None,
        sync_status: SyncStatus::Failed,
        synced_at: None,
        created_at: now - 100_000,
    };
    storage.append_oplog(&stale_entry).await.unwrap();

    // 2. Insert a failed entry created 10 seconds ago (not stale)
    let fresh_entry = OplogEntry {
        id: "fresh-id".to_string(),
        replica_id: "replica-1".to_string(),
        namespace: ns.to_string(),
        mutation_type: MutationType::CrdtUpdate,
        doc_id: "doc-2".to_string(),
        record_id: "rec-2".to_string(),
        yrs_update: vec![4, 5, 6],
        encrypted_blob: None,
        timestamp: now - 10_000,
        sequence: None,
        sync_status: SyncStatus::Failed,
        synced_at: None,
        created_at: now - 10_000,
    };
    storage.append_oplog(&fresh_entry).await.unwrap();

    // 3. Run recovery
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());
    let recovery = CrashRecovery::new(storage.clone(), ns.to_string());
    let recovered_count = recovery.recover(keyring).await.expect("recovery runs");
    assert_eq!(recovered_count, 1);

    // 4. Verify stale entry is Pending, fresh entry is still Failed
    let pending = storage.read_pending_oplog(ns, 10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "stale-id");
}

#[tokio::test]
async fn test_compaction_removes_synced_entries() {
    let storage = Arc::new(InMemoryStorage::new());
    let ns = "test-ns-compaction";

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // 1. Insert a synced entry from 10 days ago (stale, max_age is 7 days)
    let stale_entry = OplogEntry {
        id: "stale-id".to_string(),
        replica_id: "replica-1".to_string(),
        namespace: ns.to_string(),
        mutation_type: MutationType::CrdtUpdate,
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        yrs_update: vec![1, 2, 3],
        encrypted_blob: None,
        timestamp: (now_secs - 10 * 86400) * 1000,
        sequence: Some(42),
        sync_status: SyncStatus::Synced,
        synced_at: Some(now_secs - 10 * 86400),
        created_at: (now_secs - 10 * 86400) * 1000,
    };
    storage.append_oplog(&stale_entry).await.unwrap();

    // 2. Insert a synced entry from 2 days ago (fresh)
    let fresh_entry = OplogEntry {
        id: "fresh-id".to_string(),
        replica_id: "replica-1".to_string(),
        namespace: ns.to_string(),
        mutation_type: MutationType::CrdtUpdate,
        doc_id: "doc-2".to_string(),
        record_id: "rec-2".to_string(),
        yrs_update: vec![4, 5, 6],
        encrypted_blob: None,
        timestamp: (now_secs - 2 * 86400) * 1000,
        sequence: Some(43),
        sync_status: SyncStatus::Synced,
        synced_at: Some(now_secs - 2 * 86400),
        created_at: (now_secs - 2 * 86400) * 1000,
    };
    storage.append_oplog(&fresh_entry).await.unwrap();

    // 3. Run compaction
    let engine = CompactionEngine::new(storage.clone(), CompactionConfig::default());
    let stats = engine.run_compaction(ns).await.expect("compaction runs");
    assert_eq!(stats.oplog_removed, 1);
    assert_eq!(stats.docs_removed, 0);
}

#[tokio::test]
async fn test_compaction_removes_tombstoned_docs() {
    let storage = Arc::new(InMemoryStorage::new());
    let ns = "test-ns-tombstone";

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // 1. Create a tombstoned document in storage
    let mut doc = CRDTDocument::new("doc-id-1", "rec-id-1", 0);
    doc.set_field("_deleted", CrdtValue::Boolean(true));
    let snapshot = doc.to_snapshot();
    storage
        .insert_document("doc-id-1", "rec-id-1", &snapshot)
        .await
        .unwrap();

    // 2. Write the synced CrdtDelete oplog entry created 2 days ago (grace is 24h)
    let delete_entry = OplogEntry {
        id: "del-op-1".to_string(),
        replica_id: "replica-1".to_string(),
        namespace: ns.to_string(),
        mutation_type: MutationType::CrdtDelete,
        doc_id: "doc-id-1".to_string(),
        record_id: "rec-id-1".to_string(),
        yrs_update: snapshot,
        encrypted_blob: None,
        timestamp: now_ms - 2 * 86400_000,
        sequence: Some(101),
        sync_status: SyncStatus::Synced,
        synced_at: Some((now_ms - 2 * 86400_000) / 1000),
        created_at: now_ms - 2 * 86400_000,
    };
    storage.append_oplog(&delete_entry).await.unwrap();

    // Verify document exists before compaction
    let doc_before = storage.get_document("doc-id-1", "rec-id-1").await.unwrap();
    assert!(doc_before.is_some());

    // 3. Run compaction
    let engine = CompactionEngine::new(storage.clone(), CompactionConfig::default());
    let stats = engine.run_compaction(ns).await.expect("compaction runs");
    assert_eq!(stats.docs_removed, 1);

    // Verify document is physically deleted after compaction
    let doc_after = storage.get_document("doc-id-1", "rec-id-1").await.unwrap();
    assert!(doc_after.is_none());
}
