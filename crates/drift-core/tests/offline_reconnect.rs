use std::collections::HashMap;
use std::sync::Arc;
use drift_core::{
    coordinator::memory::InMemoryCoordinator,
    crdt::types::CrdtValue,
    storage::memory::InMemoryStorage,
    test_utils::{SimulatedNetwork, NetworkPartitionFault},
};

fn s(k: &str, v: &str) -> HashMap<String, CrdtValue> {
    let mut m = HashMap::new();
    m.insert(k.to_string(), CrdtValue::String(v.to_string()));
    m
}

#[tokio::test]
async fn test_write_offline_then_reconnect() {
    let coord = Arc::new(InMemoryCoordinator::new());
    let mut net = SimulatedNetwork::new(coord);
    net.add_replica("replica-offline-client", Arc::new(InMemoryStorage::new()));

    let fault = NetworkPartitionFault { target_id: "replica-offline-client".to_string() };
    net.inject_fault(&fault);

    net.replicas[0].write("doc-1", "rec-1", s("title", "Offline Task")).await.unwrap();

    // Verify written locally
    let doc = net.replicas[0].get_document("doc-1", "rec-1").await.unwrap();
    assert_eq!(doc.get_field("title").unwrap(), CrdtValue::String("Offline Task".to_string()));

    // Verify sync does not flush
    net.sync_all().await;
    let pending = net.replicas[0].fixture.storage.read_pending_oplog("test-ns", 10).await.unwrap().len();
    assert_eq!(pending, 1);

    // Reconnect
    net.remove_fault(&fault);

    // Sync
    net.sync_all().await;
    let pending_after = net.replicas[0].fixture.storage.read_pending_oplog("test-ns", 10).await.unwrap().len();
    assert_eq!(pending_after, 0);
}

#[tokio::test]
async fn test_missed_mutations_on_reconnect() {
    let coord = Arc::new(InMemoryCoordinator::new());
    let mut net = SimulatedNetwork::new(coord);
    net.add_replica("alice", Arc::new(InMemoryStorage::new()));
    net.add_replica("bob", Arc::new(InMemoryStorage::new()));

    let fault = NetworkPartitionFault { target_id: "bob".to_string() };
    net.inject_fault(&fault);

    net.replicas[0].write("doc-1", "rec-1", s("title", "Alice's item")).await.unwrap();
    net.sync_all().await;

    // Bob has not received it yet
    assert!(net.replicas[1].get_document("doc-1", "rec-1").await.is_none());

    // Reconnect Bob
    net.remove_fault(&fault);

    // Sync
    net.sync_all().await;

    // Verify bob has it now
    let doc = net.replicas[1].get_document("doc-1", "rec-1").await.unwrap();
    assert_eq!(doc.get_field("title").unwrap(), CrdtValue::String("Alice's item".to_string()));
}
