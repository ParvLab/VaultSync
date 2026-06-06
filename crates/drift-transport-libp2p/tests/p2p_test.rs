use std::time::Duration;
use tokio::sync::mpsc;
use drift_transport_libp2p::{LibP2pTransport, PendingMutation};

#[tokio::test]
async fn test_mutation_gossip_roundtrip() {

    let ns = "test-gossip-ns";
    let (incoming_tx1, _incoming_rx1) = mpsc::channel(10);
    let (incoming_tx2, mut incoming_rx2) = mpsc::channel(10);

    // Node 1 listens on TCP ephemeral port
    let (node1, handle1) = LibP2pTransport::new(ns, Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx1)
        .await
        .unwrap();

    // Node 2 listens on TCP ephemeral port
    let (node2, handle2) = LibP2pTransport::new(ns, Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx2)
        .await
        .unwrap();

    tokio::spawn(node1.run());
    tokio::spawn(node2.run());

    // Wait for listening addresses to populate
    tokio::time::sleep(Duration::from_millis(500)).await;

    let addresses1 = handle1.listen_addresses();
    assert!(!addresses1.is_empty(), "Node 1 should be listening on at least one address");
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
    let (node1, handle1) = LibP2pTransport::new("ns-1", Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx1)
        .await
        .unwrap();

    // Node 2 is on ns-2
    let (node2, handle2) = LibP2pTransport::new("ns-2", Some("/ip4/127.0.0.1/tcp/0".to_string()), incoming_tx2)
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
    };

    // Broadcast from Node 1 (ns-1)
    handle1.broadcast_mutation(mutation.clone()).await.unwrap();

    // Node 2 (ns-2) should not receive it since they are on different topics/namespaces
    let receive_attempt = tokio::time::timeout(Duration::from_secs(2), incoming_rx2.recv()).await;
    assert!(receive_attempt.is_err(), "Node on ns-2 received a message from ns-1");
}
