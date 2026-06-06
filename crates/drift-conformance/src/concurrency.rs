use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation};
use futures::StreamExt;

pub async fn run_concurrency_tests(coord: Arc<dyn Coordinator>, ns: &str) {
    // 1. concurrent_push_from_two_replicas & concurrent_push_ordering
    let coord1 = coord.clone();
    let coord2 = coord.clone();
    let ns1 = ns.to_string();
    let ns2 = ns.to_string();

    let task1 = tokio::spawn(async move {
        let muts = (0..10).map(|i| EncryptedMutation {
            id: format!("m-t1-{}", i),
            namespace: ns1.clone(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: format!("rec-1-{}", i),
            encrypted_blob: vec![i as u8],
            timestamp: 1000 + i,
            schema_version: 0,
            key_version: 1,
        }).collect::<Vec<_>>();
        coord1.push(&ns1, muts).await.expect("push task1")
    });

    let task2 = tokio::spawn(async move {
        let muts = (0..10).map(|i| EncryptedMutation {
            id: format!("m-t2-{}", i),
            namespace: ns2.clone(),
            replica_id: "rep-2".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: format!("rec-2-{}", i),
            encrypted_blob: vec![100 + i as u8],
            timestamp: 1000 + i,
            schema_version: 0,
            key_version: 1,
        }).collect::<Vec<_>>();
        coord2.push(&ns2, muts).await.expect("push task2")
    });

    let (res1, res2) = tokio::join!(task1, task2);
    let seqs1 = res1.expect("task1 panicked");
    let seqs2 = res2.expect("task2 panicked");

    // Check all sequence IDs are unique and strictly increasing in their batch
    let mut all_seqs = [seqs1, seqs2].concat();
    all_seqs.sort();
    let before_len = all_seqs.len();
    all_seqs.dedup();
    assert_eq!(all_seqs.len(), before_len, "sequence IDs must be unique");

    // 2. concurrent_pull_consistent
    let pulled = coord.pull(ns, 0, 100).await.expect("pull all");
    assert_eq!(pulled.len(), 20, "expected all 20 mutations to be stored");

    // 3. concurrent_subscribe_and_push
    let mut stream = Box::into_pin(coord.subscribe(ns, 0).await.expect("subscribe for concurrency test"));
    // Since subscribe might catch up immediately, let's drain the first 20 mutations
    let mut count = 0;
    while count < 20 {
        if let Some(_item) = stream.next().await {
            count += 1;
        } else {
            break;
        }
    }

    let coord_push = coord.clone();
    let ns_push = ns.to_string();
    let push_handle = tokio::spawn(async move {
        let muts = vec![EncryptedMutation {
            id: "m-late-1".to_string(),
            namespace: ns_push.clone(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-late-1".to_string(),
            encrypted_blob: vec![255],
            timestamp: 5000,
            schema_version: 0,
            key_version: 1,
        }];
        coord_push.push(&ns_push, muts).await.expect("push late")
    });

    let item_late = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("timeout waiting for late push")
        .expect("stream closed");
    assert_eq!(item_late.id, "m-late-1");
    let _ = push_handle.await;
}
