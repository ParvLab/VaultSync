use std::sync::Arc;
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation};

pub async fn run_push_pull_tests(coord: Arc<dyn Coordinator>, ns: &str) {
    // 1. pull_empty_returns_empty
    let empty = coord.pull(ns, 0, 10).await.expect("pull empty");
    assert!(empty.is_empty(), "expected empty mutations list at start");

    // 2. push_single_mutation & pull_preserves_blob & pull_preserves_key_version
    let mut1 = EncryptedMutation {
        id: format!("{}-m-1", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![10, 20, 30],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };
    let seqs = coord.push(ns, vec![mut1.clone()]).await.expect("push single");
    assert_eq!(seqs.len(), 1, "expected 1 sequence ID returned");
    let seq1 = seqs[0];

    let pulled = coord.pull(ns, 0, 10).await.expect("pull single");
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].id, mut1.id);
    assert_eq!(pulled[0].sequence, seq1);
    assert_eq!(pulled[0].encrypted_blob, mut1.encrypted_blob);
    assert_eq!(pulled[0].key_version, mut1.key_version);

    // 3. push_batch_mutations & sequence_strictly_increasing & push_returns_n_sequence_ids
    let muts = vec![
        EncryptedMutation {
            id: format!("{}-m-2", ns),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-2".to_string(),
            encrypted_blob: vec![40, 50],
            timestamp: 1001,
            schema_version: 0,
            key_version: 1,
        },
        EncryptedMutation {
            id: format!("{}-m-3", ns),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-3".to_string(),
            encrypted_blob: vec![60, 70],
            timestamp: 1002,
            schema_version: 0,
            key_version: 1,
        },
    ];
    let seqs2 = coord.push(ns, muts.clone()).await.expect("push batch");
    assert_eq!(seqs2.len(), 2);
    assert!(seqs2[0] > seq1, "seqs2[0] must be > seq1");
    assert!(seqs2[1] > seqs2[0], "seqs2[1] must be > seqs2[0]");

    // 4. pull_from_sequence
    let pulled_after = coord.pull(ns, seq1, 10).await.expect("pull after seq1");
    assert_eq!(pulled_after.len(), 2);
    assert_eq!(pulled_after[0].id, muts[0].id);
    assert_eq!(pulled_after[1].id, muts[1].id);

    // 5. pull_with_limit
    let pulled_limit = coord.pull(ns, 0, 2).await.expect("pull with limit");
    assert_eq!(pulled_limit.len(), 2);
    assert_eq!(pulled_limit[0].id, mut1.id);
    assert_eq!(pulled_limit[1].id, muts[0].id);

    // 6. push_idempotent_id (idempotency checks)
    let dup_mut = EncryptedMutation {
        id: format!("{}-m-1", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![99, 99],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };
    // Should be idempotent or ignore dup
    let res = coord.push(ns, vec![dup_mut]).await;
    // Some coordinators return the existing sequence IDs or succeed. We check that we don't have duplicate records in pull.
    if res.is_ok() {
        let pulled_all = coord.pull(ns, 0, 10).await.expect("pull all");
        let occurrences = pulled_all.iter().filter(|m| m.id == format!("{}-m-1", ns)).count();
        assert_eq!(occurrences, 1, "mutation ID must be unique (idempotent / deduplicated)");
    }
}
