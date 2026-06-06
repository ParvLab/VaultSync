use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation};
use futures::StreamExt;

pub async fn run_subscribe_tests(coord: Arc<dyn Coordinator>, ns: &str, other_ns: &str) {
    // 1. subscribe_receives_push
    let mut stream = Box::into_pin(coord.subscribe(ns, 0).await.expect("subscribe from 0"));

    let mut1 = EncryptedMutation {
        id: format!("{}-sub-1", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1, 2, 3],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };
    
    let seqs = coord.push(ns, vec![mut1.clone()]).await.expect("push mutation");
    let seq = seqs[0];

    // Read from stream
    let item = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("timeout waiting for stream item")
        .expect("stream ended prematurely");
    assert_eq!(item.id, mut1.id);
    assert_eq!(item.sequence, seq);

    // 2. subscribe_from_sequence_skips_old
    let mut stream_skip = Box::into_pin(coord.subscribe(ns, seq).await.expect("subscribe after seq"));
    
    let mut2 = EncryptedMutation {
        id: format!("{}-sub-2", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-2".to_string(),
        encrypted_blob: vec![4, 5, 6],
        timestamp: 1001,
        schema_version: 0,
        key_version: 1,
    };
    let seqs2 = coord.push(ns, vec![mut2.clone()]).await.expect("push second mutation");
    let seq2 = seqs2[0];

    let item2 = tokio::time::timeout(std::time::Duration::from_secs(3), stream_skip.next())
        .await
        .expect("timeout waiting for skipped stream item")
        .expect("stream ended");
    assert_eq!(item2.id, mut2.id);
    assert_eq!(item2.sequence, seq2);

    // 3. subscribe_multiple_replicas
    let mut stream_a = Box::into_pin(coord.subscribe(ns, seq2).await.expect("stream a"));
    let mut stream_b = Box::into_pin(coord.subscribe(ns, seq2).await.expect("stream b"));

    let mut3 = EncryptedMutation {
        id: format!("{}-sub-3", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-3".to_string(),
        encrypted_blob: vec![7, 8, 9],
        timestamp: 1002,
        schema_version: 0,
        key_version: 1,
    };
    coord.push(ns, vec![mut3.clone()]).await.expect("push third mutation");

    let item_a = tokio::time::timeout(std::time::Duration::from_secs(3), stream_a.next())
        .await
        .expect("timeout stream a")
        .expect("stream a ended");
    let item_b = tokio::time::timeout(std::time::Duration::from_secs(3), stream_b.next())
        .await
        .expect("timeout stream b")
        .expect("stream b ended");
    assert_eq!(item_a.id, mut3.id);
    assert_eq!(item_b.id, mut3.id);

    // 4. subscribe_namespace_isolation
    let mut stream_other = Box::into_pin(coord.subscribe(other_ns, 0).await.expect("subscribe other"));
    let mut4 = EncryptedMutation {
        id: format!("{}-sub-4", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-4".to_string(),
        encrypted_blob: vec![0],
        timestamp: 1003,
        schema_version: 0,
        key_version: 1,
    };
    coord.push(ns, vec![mut4]).await.expect("push fourth mutation");

    // Wait a short amount of time to ensure stream_other doesn't receive anything
    let opt = tokio::time::timeout(std::time::Duration::from_millis(500), stream_other.next()).await;
    if let Ok(ref res) = opt {
        println!("DEBUG: stream_other received item: {:?}", res);
    }
    assert!(opt.is_err(), "expected no item on stream_other due to isolation");
}
