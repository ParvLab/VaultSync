use crate::oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus};
use crate::storage::compaction::CompactionPolicy;
use crate::storage::lifecycle::SegmentState;
use crate::storage::manager::{DefaultStorageManager, StorageManager, WriteOptions};
use crate::storage::memory::InMemoryStorage;
use crate::storage::resource_manager::{MemoryBudget, MemoryTier, ResourceManager};
use crate::storage::traits::Storage;
use std::sync::Arc;
use std::time::Duration;

fn make_entry(doc_id: &str, record_id: &str, ns: &str) -> OplogEntry {
    OplogEntry::new(
        format!("{}-{}", doc_id, record_id),
        "test".to_string(),
        ns.to_string(),
        MutationType::CrdtInsert,
        ns.to_string(),
        record_id.to_string(),
        vec![],
        None,
        1000,
        None,
        SyncStatus::Pending,
        None,
        1000,
        0,
        MutationOrigin::TestHarness,
        "integration_test",
    )
}

fn make_delete_entry(doc_id: &str, record_id: &str, ns: &str) -> OplogEntry {
    OplogEntry::new(
        format!("del-{}-{}", doc_id, record_id),
        "test".to_string(),
        ns.to_string(),
        MutationType::CrdtDelete,
        ns.to_string(),
        record_id.to_string(),
        vec![],
        None,
        500,
        None,
        SyncStatus::Synced,
        Some(600),
        500,
        0,
        MutationOrigin::TestHarness,
        "integration_test",
    )
}

// ===== StorageManager CRUD =====

#[tokio::test]
async fn test_write_and_read_back() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "doc1", &vec![10, 20, 30], WriteOptions::default()).await.unwrap();
    let result = manager.read_document("ns1", "doc1").await.unwrap();
    assert_eq!(result.unwrap(), vec![10, 20, 30]);
}

#[tokio::test]
async fn test_delete_removes_page() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "doc1", &vec![10, 20], WriteOptions::default()).await.unwrap();
    assert!(manager.read_document("ns1", "doc1").await.unwrap().is_some());

    manager.delete_document("ns1", "doc1", WriteOptions::default()).await.unwrap();
    assert!(manager.read_document("ns1", "doc1").await.unwrap().is_none());
}

#[tokio::test]
async fn test_overwrite_document() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "doc1", &vec![1, 2], WriteOptions::default()).await.unwrap();
    manager.write_document("ns1", "doc1", &vec![3, 4, 5], WriteOptions::default()).await.unwrap();
    let result = manager.read_document("ns1", "doc1").await.unwrap();
    assert_eq!(result.unwrap(), vec![3, 4, 5]);
}

#[tokio::test]
async fn test_list_documents() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "r1", &vec![10], WriteOptions::default()).await.unwrap();
    manager.write_document("ns1", "r2", &vec![20], WriteOptions::default()).await.unwrap();
    let list = manager.list_documents("ns1").await.unwrap();
    assert_eq!(list.len(), 2);
}

// ===== Compaction Lifecycle =====

#[tokio::test]
async fn test_compact_creates_snapshots() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry1 = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry1).await.unwrap();

    let entry2 = make_entry("d2", "r2", "ns1");
    storage.write_document_and_oplog("ns1", "r2", &vec![20, 30], &entry2).await.unwrap();

    let del = make_delete_entry("d1", "r1", "ns1");
    storage.delete_document_and_oplog("ns1", "r1", &del).await.unwrap();

    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        max_tombstone_ratio: 0.0,
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.compact_namespace("ns1").await.unwrap();
    assert_eq!(stats.documents_compacted, 1);
    assert_eq!(stats.tombstones_removed, 1);
    assert!(!stats.snapshots_created.is_empty());
    assert_eq!(stats.snapshots_created[0].checksum, crc32fast::hash(&vec![20, 30]));
}

#[tokio::test]
async fn test_compact_skips_when_no_tombstones() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry).await.unwrap();

    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.compact_namespace("ns1").await.unwrap();
    assert_eq!(stats.documents_compacted, 0);
    assert_eq!(stats.tombstones_removed, 0);
}

#[tokio::test]
async fn test_compact_respects_tombstone_ratio() {
    let storage = Arc::new(InMemoryStorage::new());

    for i in 0..10 {
        let entry = make_entry(&format!("d{}", i), &format!("r{}", i), "ns1");
        storage.write_document_and_oplog(
            "ns1", &format!("r{}", i), &vec![i as u8], &entry
        ).await.unwrap();
    }

    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        max_tombstone_ratio: 0.8,
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.compact_namespace("ns1").await.unwrap();
    assert_eq!(stats.tombstones_removed, 0);
}

// ===== Lifecycle Cleanup =====

#[tokio::test]
async fn test_lifecycle_removes_old_tombstones() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry1 = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry1).await.unwrap();

    let entry2 = make_entry("d2", "r2", "ns1");
    storage.write_document_and_oplog("ns1", "r2", &vec![20], &entry2).await.unwrap();

    let del = make_delete_entry("d1", "r1", "ns1");
    storage.delete_document_and_oplog("ns1", "r1", &del).await.unwrap();

    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let lifecycle = manager.run_lifecycle("ns1").await.unwrap();
    assert_eq!(lifecycle.segments_cleaned, 1);
    assert_eq!(lifecycle.events[0].new_state, SegmentState::Deleted);
}

#[tokio::test]
async fn test_lifecycle_preserves_active_docs() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry1 = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry1).await.unwrap();

    let entry2 = make_entry("d2", "r2", "ns1");
    storage.write_document_and_oplog("ns1", "r2", &vec![20], &entry2).await.unwrap();

    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let lifecycle = manager.run_lifecycle("ns1").await.unwrap();
    assert_eq!(lifecycle.segments_cleaned, 0);

    assert!(manager.read_document("ns1", "r1").await.unwrap().is_some());
    assert!(manager.read_document("ns1", "r2").await.unwrap().is_some());
}

// ===== Resource Manager Integration =====

#[tokio::test]
async fn test_resource_manager_tracks_access() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "d1", &vec![0u8; 1024], WriteOptions::default()).await.unwrap();
    manager.read_document("ns1", "d1").await.unwrap();

    let stats = manager.run_resource_sweep().await.unwrap();
    assert_eq!(stats.pages_tracked, 1);
    assert!(stats.hot_bytes > 0);
}

#[tokio::test]
async fn test_resource_sweep_drops_tier() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    manager.write_document("ns1", "d1", &vec![0u8; 4096], WriteOptions::default()).await.unwrap();

    let stats = manager.run_resource_sweep().await.unwrap();
    assert!(stats.demotions_last_sweep >= 1 || stats.promotions_last_sweep >= 1);
}

#[tokio::test]
async fn test_resource_manager_eviction_candidates() {
    let rm = ResourceManager::new(MemoryBudget::default());

    rm.record_access("d1", "r1", 4096, 1000);
    rm.record_access("d2", "r2", 8192, 2000);
    rm.record_access("d3", "r3", 1024, 3000);

    rm.set_pinned("d1", "r1", true);

    let candidates = rm.eviction_candidates(MemoryTier::ResidentHot, 8192, 200_000);
    assert!(candidates.len() > 0);
    let ids: Vec<&str> = candidates.iter().map(|(id, _, _)| id.as_str()).collect();
    assert!(!ids.contains(&"d1"));
}

// ===== Segment Seal =====

#[tokio::test]
async fn test_lifecycle_engine_should_seal() {
    let engine = crate::storage::lifecycle::LifecycleEngine::new();
    assert!(engine.should_seal(4_000_000, 4_000_000));
    assert!(!engine.should_seal(3_000_000, 4_000_000));
    assert!(!engine.should_seal(0, 4_000_000));
}

#[tokio::test]
async fn test_lifecycle_engine_records_events() {
    let engine = crate::storage::lifecycle::LifecycleEngine::new();
    engine.record_event("doc1", "rec1", SegmentState::Active);
    engine.record_event("doc2", "rec2", SegmentState::Sealed);
    engine.record_event("doc3", "rec3", SegmentState::Deleted);

    let events = engine.recent_events(2);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].new_state, SegmentState::Sealed);
    assert_eq!(events[1].new_state, SegmentState::Deleted);
}

#[tokio::test]
async fn test_lifecycle_engine_records_timestamps() {
    let engine = crate::storage::lifecycle::LifecycleEngine::new();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    engine.record_event("doc1", "rec1", SegmentState::Active);
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let events = engine.recent_events(1);
    assert!(events[0].timestamp >= before);
    assert!(events[0].timestamp <= after);
}

// ===== Multi-Tab Simulation =====

#[tokio::test]
async fn test_multi_tab_same_storage() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        ..Default::default()
    };
    let manager1 = DefaultStorageManager::new(storage.clone(), policy.clone());
    let manager2 = DefaultStorageManager::new(storage.clone(), policy.clone());

    manager1.write_document("ns1", "d1", &vec![1, 2], WriteOptions::default()).await.unwrap();
    manager2.write_document("ns1", "d2", &vec![3, 4], WriteOptions::default()).await.unwrap();

    let r1 = manager1.read_document("ns1", "d1").await.unwrap();
    let r2 = manager1.read_document("ns1", "d2").await.unwrap();
    let r3 = manager2.read_document("ns1", "d1").await.unwrap();
    let r4 = manager2.read_document("ns1", "d2").await.unwrap();

    assert_eq!(r1.unwrap(), vec![1, 2]);
    assert_eq!(r2.unwrap(), vec![3, 4]);
    assert_eq!(r3.unwrap(), vec![1, 2]);
    assert_eq!(r4.unwrap(), vec![3, 4]);
}

#[tokio::test]
async fn test_multi_tab_concurrent_delete() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        ..Default::default()
    };
    let manager1 = DefaultStorageManager::new(storage.clone(), policy.clone());
    let manager2 = DefaultStorageManager::new(storage.clone(), policy.clone());

    manager1.write_document("ns1", "d1", &vec![10], WriteOptions::default()).await.unwrap();
    manager1.write_document("ns1", "d2", &vec![20], WriteOptions::default()).await.unwrap();

    manager2.delete_document("ns1", "d1", WriteOptions::default()).await.unwrap();

    assert!(manager1.read_document("ns1", "d1").await.unwrap().is_none());
    assert!(manager2.read_document("ns1", "d2").await.unwrap().is_some());
}

// ===== Compaction + Lifecycle Integration =====

#[tokio::test]
async fn test_compact_then_lifecycle() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry1 = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry1).await.unwrap();

    let entry2 = make_entry("d2", "r2", "ns1");
    storage.write_document_and_oplog("ns1", "r2", &vec![20], &entry2).await.unwrap();

    let del = make_delete_entry("d1", "r1", "ns1");
    storage.delete_document_and_oplog("ns1", "r1", &del).await.unwrap();

    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        max_tombstone_ratio: 0.0,
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let compaction = manager.compact_namespace("ns1").await.unwrap();
    assert!(compaction.documents_compacted >= 1);

    let lifecycle = manager.run_lifecycle("ns1").await.unwrap();
    assert!(lifecycle.segments_cleaned >= 1);
}

#[tokio::test]
async fn test_compact_then_read_surviving_docs() {
    let storage = Arc::new(InMemoryStorage::new());
    let entry1 = make_entry("d1", "r1", "ns1");
    storage.write_document_and_oplog("ns1", "r1", &vec![10], &entry1).await.unwrap();

    let entry2 = make_entry("d2", "r2", "ns1");
    storage.write_document_and_oplog("ns1", "r2", &vec![20, 30], &entry2).await.unwrap();

    let entry3 = make_entry("d3", "r3", "ns1");
    storage.write_document_and_oplog("ns1", "r3", &vec![40], &entry3).await.unwrap();

    let del = make_delete_entry("d1", "r1", "ns1");
    storage.delete_document_and_oplog("ns1", "r1", &del).await.unwrap();

    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        max_tombstone_ratio: 0.0,
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let compaction = manager.compact_namespace("ns1").await.unwrap();
    assert_eq!(compaction.documents_compacted, 2);

    let r1 = manager.read_document("ns1", "r1").await.unwrap();
    let r2 = manager.read_document("ns1", "r2").await.unwrap();
    let r3 = manager.read_document("ns1", "r3").await.unwrap();

    assert!(r1.is_none());
    assert_eq!(r2.unwrap(), vec![20, 30]);
    assert_eq!(r3.unwrap(), vec![40]);
}

// ===== Resource Manager: Multi-Sweep Scenarios =====

#[tokio::test]
async fn test_resource_sweep_multiple_passes() {
    let rm = ResourceManager::new(MemoryBudget::default());

    rm.record_access("d1", "r1", 4096, 1000);
    rm.record_access("d2", "r2", 8192, 1000);

    rm.sweep(1000);

    rm.record_access("d1", "r1", 4096, 1000);
    rm.sweep(2000);

    rm.record_access("d1", "r1", 4096, 3000);
    rm.sweep(3000);

    let tier1 = rm.get_tier("d1", "r1");
    let tier2 = rm.get_tier("d2", "r2");
    assert!(tier1 != tier2 || tier1 == Some(MemoryTier::ResidentHot));
}

#[tokio::test]
async fn test_resource_sweep_respects_budget() {
    let budget = MemoryBudget {
        active_bytes: 1024,
        hot_bytes: 2048,
        warm_bytes: 4096,
        ..Default::default()
    };
    let rm = ResourceManager::new(budget);

    for i in 0..20 {
        rm.record_access(&format!("d{}", i), "r1", 512, 1000 + i as u64);
    }

    let stats = rm.sweep(1000);
    assert!(stats.pages_tracked <= 20);
}

#[tokio::test]
async fn test_resource_sweep_pinned_not_evicted() {
    let rm = ResourceManager::new(MemoryBudget::default());

    rm.record_access("d1", "r1", 4096, 1000);
    rm.record_access("d2", "r2", 8192, 2000);
    rm.set_pinned("d1", "r1", true);

    let candidates = rm.eviction_candidates(MemoryTier::ResidentHot, 8192, 200_000);
    let ids: Vec<&str> = candidates.iter().map(|(id, _, _)| id.as_str()).collect();
    assert!(!ids.contains(&"d1"));
}

// ===== Edge Cases =====

#[tokio::test]
async fn test_empty_manager_stats() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.stats().await.unwrap();
    assert_eq!(stats.total_pages, 0);
}

#[tokio::test]
async fn test_compact_empty_namespace() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        ..Default::default()
    };
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.compact_namespace("empty_ns").await.unwrap();
    assert_eq!(stats.documents_compacted, 0);
    assert_eq!(stats.tombstones_removed, 0);
}

#[tokio::test]
async fn test_lifecycle_empty_namespace() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let stats = manager.run_lifecycle("empty_ns").await.unwrap();
    assert_eq!(stats.segments_cleaned, 0);
}

#[tokio::test]
async fn test_read_nonexistent_document() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let result = manager.read_document("ns1", "nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_nonexistent_document() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let result = manager.delete_document("ns1", "nonexistent", WriteOptions::default()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_large_document() {
    let storage = Arc::new(InMemoryStorage::new());
    let policy = CompactionPolicy::default();
    let manager = DefaultStorageManager::new(storage, policy);

    let big = vec![0xAB; 1024 * 1024];
    manager.write_document("ns1", "big", &big, WriteOptions::default()).await.unwrap();
    let result = manager.read_document("ns1", "big").await.unwrap();
    assert_eq!(result.unwrap(), big);
}

// ===== Compaction Engine Decision Logic =====

#[tokio::test]
async fn test_compaction_engine_decision_tombstone_ratio() {
    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(0),
        max_tombstone_ratio: 0.25,
        ..Default::default()
    };
    let engine = crate::storage::compaction::CompactionEngine::new(policy);

    let decision = engine.should_compact(10, 2);
    assert!(!decision.should_run);

    let decision = engine.should_compact(10, 5);
    assert!(decision.should_run);
}

#[tokio::test]
async fn test_compaction_engine_decision_empty_storage() {
    let policy = CompactionPolicy::default();
    let engine = crate::storage::compaction::CompactionEngine::new(policy);

    let decision = engine.should_compact(0, 0);
    assert!(!decision.should_run);
}

#[tokio::test]
async fn test_compaction_engine_mark_run_throttles() {
    let policy = CompactionPolicy {
        min_compaction_interval: Duration::from_secs(600),
        max_tombstone_ratio: 0.25,
        ..Default::default()
    };
    let engine = crate::storage::compaction::CompactionEngine::new(policy);

    let decision = engine.should_compact(10, 10);
    assert!(decision.should_run);

    engine.mark_run();

    let decision = engine.should_compact(10, 10);
    assert!(!decision.should_run);
}
