use std::collections::HashMap;
use std::sync::Arc;
use vaultsync_core::{
    coordinator::memory::InMemoryCoordinator,
    crdt::types::CrdtValue,
    storage::memory::InMemoryStorage,
    test_utils::{NetworkPartitionFault, SimulatedNetwork},
};

fn s(k: &str, v: &str) -> HashMap<String, CrdtValue> {
    let mut m = HashMap::new();
    m.insert(k.to_string(), CrdtValue::String(v.to_string()));
    m
}

#[tokio::test]
async fn test_two_replica_basic_convergence() {
    let mut net = SimulatedNetwork::new(Arc::new(InMemoryCoordinator::new()));
    net.add_replica("alice", Arc::new(InMemoryStorage::new()));
    net.add_replica("bob", Arc::new(InMemoryStorage::new()));

    net.replicas[0]
        .write("todos", "t:1", s("title", "buy milk"))
        .await
        .unwrap();
    net.sync_all().await;
    net.replicas[1]
        .write("todos", "t:2", s("title", "get eggs"))
        .await
        .unwrap();
    net.sync_all().await;

    net.assert_all_converge("todos", "t:1").await;
}

#[tokio::test]
async fn test_three_replica_offline_convergence() {
    let mut net = SimulatedNetwork::new(Arc::new(InMemoryCoordinator::new()));
    net.add_replica("alice", Arc::new(InMemoryStorage::new()));
    net.add_replica("bob", Arc::new(InMemoryStorage::new()));
    net.add_replica("carol", Arc::new(InMemoryStorage::new()));

    net.replicas[0]
        .write("docs", "d:1", s("text", "hello"))
        .await
        .unwrap();
    net.sync_all().await;

    net.replicas[1].disconnect();
    net.replicas[1]
        .write("docs", "d:2", s("text", "from bob"))
        .await
        .unwrap();

    net.replicas[2].disconnect();
    net.replicas[2]
        .write("docs", "d:1", s("text", "hello world"))
        .await
        .unwrap();

    net.replicas[1].reconnect().await;
    net.replicas[2].reconnect().await;
    net.sync_all().await;

    net.assert_all_converge("docs", "d:1").await;
}

#[tokio::test]
async fn test_50_offline_writes_converge_after_reconnect() {
    let mut net = SimulatedNetwork::new(Arc::new(InMemoryCoordinator::new()));
    net.add_replica("device_a", Arc::new(InMemoryStorage::new()));
    net.add_replica("device_b", Arc::new(InMemoryStorage::new()));

    net.replicas[0].disconnect();
    for i in 0..50u64 {
        net.replicas[0]
            .write("items", &format!("item:{i}"), s("v", &format!("val_{i}")))
            .await
            .unwrap();
    }

    net.replicas[1]
        .write("items", "item:99", s("v", "from_b"))
        .await
        .unwrap();
    net.sync_all().await;

    net.replicas[0].reconnect().await;
    net.sync_all().await;

    net.assert_all_converge("items", "item:0").await;
    net.assert_all_converge("items", "item:99").await;
}

#[tokio::test]
async fn test_network_partition_fault() {
    let mut net = SimulatedNetwork::new(Arc::new(InMemoryCoordinator::new()));
    net.add_replica("r1", Arc::new(InMemoryStorage::new()));
    net.add_replica("r2", Arc::new(InMemoryStorage::new()));

    let fault = NetworkPartitionFault {
        target_id: "r1".to_string(),
    };
    net.inject_fault(&fault);

    // r1 is disconnected; r2 can write
    net.replicas[1]
        .write("data", "d:1", s("k", "v"))
        .await
        .unwrap();
    net.sync_all().await;

    // r1 is still disconnected — should not have d:1 yet
    // (state is partitioned)

    let fault2 = NetworkPartitionFault {
        target_id: "r1".to_string(),
    };
    net.remove_fault(&fault2);

    net.sync_all().await;
    net.assert_all_converge("data", "d:1").await;
}
