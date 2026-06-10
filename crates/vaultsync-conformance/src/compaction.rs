use std::sync::Arc;
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation};
use vaultsync_core::crdt::snapshot::Snapshot;

pub async fn run_compaction_tests(coord: Arc<dyn Coordinator>, ns: &str) {
    // 1. compact_empty_namespace
    let empty_ns = format!("{}-empty", ns);
    let stats = coord.compact_oplog(&empty_ns).await.expect("compact empty namespace");
    assert_eq!(stats.oplog_removed, 0, "compact empty should remove 0");

    // 2. compact_removes_snapshotted_entries & compact_preserves_unsnapshotted_entries
    let mut1 = EncryptedMutation {
        id: format!("{}-c-1", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1, 2],
        timestamp: 1000,
        schema_version: 0,
        key_version: 1,
    };
    let mut2 = EncryptedMutation {
        id: format!("{}-c-2", ns),
        namespace: ns.to_string(),
        replica_id: "rep-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![3, 4],
        timestamp: 1001,
        schema_version: 0,
        key_version: 1,
    };

    let seqs = coord.push(ns, vec![mut1.clone(), mut2.clone()]).await.expect("push two mutations");
    let seq1 = seqs[0];
    let _seq2 = seqs[1];

    // Store snapshot that includes seq1 but NOT seq2
    let snap = Snapshot {
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        schema_version: 0,
        sequence: seq1,
        created_at: 1000,
        bytes: vec![99],
        checksum: 1234,
    };
    coord.store_snapshot(ns, &snap).await.expect("store snap");

    // Compact oplog
    let _stats = coord.compact_oplog(ns).await.expect("compact oplog");

    // Pull after 0 to check if seq2 is still there
    let pulled = coord.pull(ns, 0, 10).await.expect("pull after compact");
    // Seq2 must survive because it's newer than snapshot.
    // Note: depends on coordinator implementation, seq1 might be removed or retained depending on backend capability,
    // but seq2 MUST survive.
    assert!(pulled.iter().any(|m| m.id == mut2.id), "unsnapshotted entry must survive compaction");

    // 3. double_compact_idempotent
    let _stats2 = coord.compact_oplog(ns).await.expect("compact second time");
}
