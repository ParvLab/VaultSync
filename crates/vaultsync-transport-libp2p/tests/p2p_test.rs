use std::time::Duration;
use tokio::sync::mpsc;
use vaultsync_transport_libp2p::{LibP2pTransport, PendingMutation};

#[tokio::test]
async fn test_mutation_gossip_roundtrip() {
    let ns = "test-gossip-ns";
    let (incoming_tx1, _incoming_rx1) = mpsc::channel(10);
    let (incoming_tx2, mut incoming_rx2) = mpsc::channel(10);

    // Node 1 listens on TCP ephemeral port
    let (node1, handle1) =
        LibP2pTransport::new(ns, Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx1)
            .await
            .unwrap();

    // Node 2 listens on TCP ephemeral port
    let (node2, handle2) =
        LibP2pTransport::new(ns, Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx2)
            .await
            .unwrap();

    tokio::spawn(node1.run());
    tokio::spawn(node2.run());

    // Wait for listening addresses to populate
    tokio::time::sleep(Duration::from_millis(500)).await;

    let addresses1 = handle1.listen_addresses();
    assert!(
        !addresses1.is_empty(),
        "Node 1 should be listening on at least one address"
    );
    let target_addr = addresses1[0].clone();

    // Explicitly dial Node 1 from Node 2 to bypass mDNS discovery unreliability on loopback
    handle2.dial(target_addr).await.unwrap();

    // Give some time for TCP connection and Gossipsub mesh link to establish
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create a mutation to broadcast
    let mutation = PendingMutation {
        id: "mut-1".to_string(),
        namespace: ns.to_string(),
        sequence: 0,
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![42, 43, 44],
        timestamp: 123456789,
        key_version: 1,
        replica_id: "test-replica".to_string(),
    };

    // Broadcast mutation
    handle1.broadcast_mutation(mutation.clone()).await.unwrap();

    // Wait for the mutation to be received
    let timeout = Duration::from_secs(5);
    let received_mut = tokio::time::timeout(timeout, incoming_rx2.recv())
        .await
        .expect("Failed to receive gossiped mutation within timeout")
        .expect("Channel closed");

    assert_eq!(received_mut.id, "mut-1");
    assert_eq!(received_mut.encrypted_blob, vec![42, 43, 44]);
}

#[tokio::test]
async fn test_namespace_isolation() {
    let (incoming_tx1, _incoming_rx1) = mpsc::channel(10);
    let (incoming_tx2, mut incoming_rx2) = mpsc::channel(10);

    // Node 1 is on ns-1
    let (node1, handle1) = LibP2pTransport::new(
        "ns-1",
        Some("/ip4/127.0.0.1/tcp/0".to_string()),
        incoming_tx1,
    )
    .await
    .unwrap();

    // Node 2 is on ns-2
    let (node2, handle2) = LibP2pTransport::new(
        "ns-2",
        Some("/ip4/127.0.0.1/tcp/0".to_string()),
        incoming_tx2,
    )
    .await
    .unwrap();

    tokio::spawn(node1.run());
    tokio::spawn(node2.run());

    // Wait for listening addresses to populate
    tokio::time::sleep(Duration::from_millis(500)).await;

    let addresses1 = handle1.listen_addresses();
    assert!(!addresses1.is_empty());
    let target_addr = addresses1[0].clone();

    // Explicitly dial Node 1 from Node 2 to connect them
    handle2.dial(target_addr).await.unwrap();

    // Give some time for connection to establish
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mutation = PendingMutation {
        id: "mut-2".to_string(),
        namespace: "ns-1".to_string(),
        sequence: 0,
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1],
        timestamp: 12345,
        key_version: 1,
        replica_id: "test-replica".to_string(),
    };

    // Broadcast from Node 1 (ns-1)
    handle1.broadcast_mutation(mutation.clone()).await.unwrap();

    // Node 2 (ns-2) should not receive it since they are on different topics/namespaces
    let receive_attempt = tokio::time::timeout(Duration::from_secs(2), incoming_rx2.recv()).await;
    assert!(
        receive_attempt.is_err(),
        "Node on ns-2 received a message from ns-1"
    );
}

#[tokio::test]
async fn test_hybrid_p2p_and_coordinator_convergence() {
    use std::sync::Arc;
    use vaultsync_coordinator_memory::coordinator::InMemoryCoordinator;
    use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation};

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let namespace = "hybrid-test-ns";

    // --- P2P nodes (Replica A and B on same LAN mesh) ---
    let (incoming_tx_a, _rx_a) = mpsc::channel(10);
    let (incoming_tx_b, mut rx_b) = mpsc::channel(10);

    let (node_a, handle_a) = LibP2pTransport::new(
        namespace,
        Some("/ip4/127.0.0.1/tcp/0".to_string()),
        incoming_tx_a,
    )
    .await
    .unwrap();

    let (node_b, handle_b) = LibP2pTransport::new(
        namespace,
        Some("/ip4/127.0.0.1/tcp/0".to_string()),
        incoming_tx_b,
    )
    .await
    .unwrap();

    tokio::spawn(node_a.run());
    tokio::spawn(node_b.run());

    tokio::time::sleep(Duration::from_millis(500)).await;

    // B dials A to establish mesh connection
    let addr_a = handle_a.listen_addresses()[0].clone();
    handle_b.dial(addr_a).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- The Mutation ---
    let mutation = EncryptedMutation {
        id: "hybrid-mut-1".to_string(),
        replica_id: "replica-a".to_string(),
        namespace: namespace.to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![0xDE, 0xAD, 0xBE, 0xEF],
        timestamp: 1_000_000,
        schema_version: 1,
        key_version: 1,
    };

    // --- Path 1: Gossip to B via libp2p ---
    let pending = PendingMutation {
        id: mutation.id.clone(),
        namespace: mutation.namespace.clone(),
        sequence: 0,
        doc_id: mutation.doc_id.clone(),
        record_id: mutation.record_id.clone(),
        encrypted_blob: mutation.encrypted_blob.clone(),
        timestamp: mutation.timestamp,
        key_version: mutation.key_version,
        replica_id: mutation.replica_id.clone(),
    };
    handle_a.broadcast_mutation(pending).await.unwrap();

    let received_by_b = tokio::time::timeout(Duration::from_secs(5), rx_b.recv())
        .await
        .expect("B did not receive mutation via P2P within 5s")
        .unwrap();
    assert_eq!(
        received_by_b.id, "hybrid-mut-1",
        "B got wrong mutation via P2P"
    );

    // --- Path 2: Push to coordinator (e.g. replica C pulls it) ---
    coordinator
        .push(namespace, vec![mutation.clone()])
        .await
        .unwrap();
    let pulled = coordinator.pull(namespace, 0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1, "Coordinator should have 1 mutation");
    assert_eq!(
        pulled[0].id, "hybrid-mut-1",
        "C got wrong mutation via coordinator"
    );

    // --- Convergence check ---
    assert_eq!(
        received_by_b.encrypted_blob, pulled[0].encrypted_blob,
        "P2P blob and coordinator blob must be identical"
    );
}
