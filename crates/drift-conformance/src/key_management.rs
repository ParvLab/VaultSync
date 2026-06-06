use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};

pub async fn run_key_management_tests(coord: Arc<dyn Coordinator>, ns: &str) {
    let rep_id = "rep-key-1";

    // 1. get_unregistered_key_returns_none
    let missing = coord.get_replica_key(ns, rep_id).await.expect("get missing key");
    assert!(missing.is_none(), "expected missing replica key to be None");

    // Register replica first so it exists in tables/databases that enforce foreign keys or schemas (like SQLite)
    let rep = ReplicaInfo {
        replica_id: rep_id.to_string(),
        namespace: ns.to_string(),
        public_key: vec![],
        schema_version: 0,
    };
    coord.register(ns, rep).await.expect("register replica first");

    // 2. update_and_get_replica_key
    let key_bytes = vec![1, 3, 5, 7, 9];
    coord.update_replica_key(ns, rep_id, key_bytes.clone(), 1).await.expect("update replica key version 1");

    let retrieved = coord.get_replica_key(ns, rep_id).await.expect("get replica key")
        .expect("expected key to exist");
    assert_eq!(retrieved.0, key_bytes);
    assert_eq!(retrieved.1, 1);

    // 3. update_key_version_increments
    let key_bytes_2 = vec![2, 4, 6, 8, 10];
    coord.update_replica_key(ns, rep_id, key_bytes_2.clone(), 2).await.expect("update replica key version 2");

    let retrieved_2 = coord.get_replica_key(ns, rep_id).await.expect("get updated replica key")
        .expect("expected key to exist");
    assert_eq!(retrieved_2.0, key_bytes_2);
    assert_eq!(retrieved_2.1, 2);

    // 4. key_version_in_mutation
    let mut1 = EncryptedMutation {
        id: format!("{}-km-1", ns),
        namespace: ns.to_string(),
        replica_id: rep_id.to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![100],
        timestamp: 1000,
        schema_version: 0,
        key_version: 2,
    };
    let seqs = coord.push(ns, vec![mut1.clone()]).await.expect("push key_version 2 mutation");
    let seq = seqs[0];

    let pulled = coord.pull(ns, seq - 1, 1).await.expect("pull key_version 2 mutation");
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].key_version, 2);
}
