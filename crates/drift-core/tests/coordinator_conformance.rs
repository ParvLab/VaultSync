use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};
use drift_core::coordinator::memory::InMemoryCoordinator;

pub async fn run_coordinator_conformance_suite(coord: Arc<dyn Coordinator>) {
    let ns = "coord-conformance-ns";

    // 1. Register replica
    let replica = ReplicaInfo {
        replica_id: "rep-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![1, 2, 3],
        schema_version: 0,
    };
    coord.register(ns, replica).await.unwrap();

    // 2. Heartbeat
    coord.heartbeat(ns, "rep-1").await.unwrap();

    // 3. Schema Version
    let version = coord.schema_version(ns).await.unwrap();
    assert_eq!(version, 0);

    // 4. Push mutations and Pull mutations roundtrip
    let muts = vec![
        EncryptedMutation {
            id: "m-1".to_string(),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-1".to_string(),
            encrypted_blob: vec![10, 20],
            timestamp: 1000,
            schema_version: 0,
        },
        EncryptedMutation {
            id: "m-2".to_string(),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-2".to_string(),
            encrypted_blob: vec![30, 40],
            timestamp: 2000,
            schema_version: 0,
        },
    ];

    let seqs = coord.push(ns, muts).await.unwrap();
    assert_eq!(seqs.len(), 2);
    // Sequences must be strictly increasing
    assert!(seqs[1] > seqs[0]);

    // Pull after sequence 0
    let pending = coord.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, "m-1");
    assert_eq!(pending[0].sequence, seqs[0]);
    assert_eq!(pending[0].encrypted_blob, vec![10, 20]);
    assert_eq!(pending[1].id, "m-2");
    assert_eq!(pending[1].sequence, seqs[1]);
    assert_eq!(pending[1].encrypted_blob, vec![30, 40]);

    // Pull after sequence seqs[0] (should return only m-2)
    let pending_after = coord.pull(ns, seqs[0], 10).await.unwrap();
    assert_eq!(pending_after.len(), 1);
    assert_eq!(pending_after[0].id, "m-2");

    // Pull with limit 1
    let pending_limit = coord.pull(ns, 0, 1).await.unwrap();
    assert_eq!(pending_limit.len(), 1);
    assert_eq!(pending_limit[0].id, "m-1");
}

#[tokio::test]
async fn test_in_memory_coordinator_conformance() {
    let coord = Arc::new(InMemoryCoordinator::new());
    run_coordinator_conformance_suite(coord).await;
}
