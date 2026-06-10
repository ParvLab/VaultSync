#![no_main]
use libfuzzer_sys::fuzz_target;
use vaultsync_core::crdt::document::CRDTDocument;
use vaultsync_core::crdt::types::CrdtValue;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum FuzzOp {
    Set { key: String, val: String },
    Delete { key: String },
}

fuzz_target!(|data: &[u8]| {
    if let Ok(ops) = bincode::deserialize::<Vec<FuzzOp>>(data) {
        let base = CRDTDocument::new("doc-1", "rec-1", 0);
        let base_snapshot = base.to_snapshot();
        
        let mut replica_a = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
        let mut replica_b = CRDTDocument::from_snapshot(&base_snapshot).unwrap();

        let mut updates_a = Vec::new();
        let mut updates_b = Vec::new();

        for (i, op) in ops.into_iter().enumerate() {
            if i % 2 == 0 {
                let u = match op {
                    FuzzOp::Set { key, val } => replica_a.set_field(&key, CrdtValue::String(val)),
                    FuzzOp::Delete { key } => replica_a.delete_field(&key),
                };
                updates_a.push(u);
            } else {
                let u = match op {
                    FuzzOp::Set { key, val } => replica_b.set_field(&key, CrdtValue::String(val)),
                    FuzzOp::Delete { key } => replica_b.delete_field(&key),
                };
                updates_b.push(u);
            }
        }

        // Merge updates
        for u in &updates_b {
            let _ = replica_a.apply_update(u);
        }
        for u in &updates_a {
            let _ = replica_b.apply_update(u);
        }

        // Verify convergence
        let map_a = replica_a.to_map();
        let map_b = replica_b.to_map();
        assert_eq!(map_a, map_b);
    }
});
