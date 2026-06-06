use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, ReplicaInfo};

pub async fn run_register_tests(coord: Arc<dyn Coordinator>, ns: &str) {
    // 1. schema_version_default_zero
    let v = coord.schema_version(ns).await.expect("schema version default");
    assert_eq!(v, 0, "fresh namespace should have schema version 0");

    // 2. register_new_replica
    let rep1 = ReplicaInfo {
        replica_id: "rep-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![1, 2, 3],
        schema_version: 0,
    };
    coord.register(ns, rep1.clone()).await.expect("register rep-1");

    // 3. register_is_idempotent
    coord.register(ns, rep1).await.expect("register rep-1 again");

    // 4. register_different_replicas
    let rep2 = ReplicaInfo {
        replica_id: "rep-2".to_string(),
        namespace: ns.to_string(),
        public_key: vec![4, 5, 6],
        schema_version: 0,
    };
    coord.register(ns, rep2).await.expect("register rep-2");

    // 5. heartbeat_succeeds
    coord.heartbeat(ns, "rep-1").await.expect("heartbeat rep-1");
    coord.heartbeat(ns, "rep-2").await.expect("heartbeat rep-2");
}
