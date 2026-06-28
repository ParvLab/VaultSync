use crate::crdt::types::CrdtValue;
use crate::error::VaultSyncError;
use std::collections::HashMap;
use yrs::any::Any;
use yrs::updates::decoder::Decode;
use yrs::{Array, Doc, GetString, Map, MapRef, Out, ReadTxn, Transact, Update, WriteTxn};

pub struct CRDTDocument {
    pub doc_id: String,
    pub record_id: String,
    pub schema_version: u64,
    inner: Doc,
    root: MapRef,
}

impl CRDTDocument {
    pub const MAX_SNAPSHOT_SIZE: usize = 100 * 1024 * 1024; // 100 MB

    pub fn new(doc_id: &str, record_id: &str, schema_version: u64) -> Self {
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        let root = txn.get_or_insert_map("root");
        root.insert(&mut txn, "doc_id", doc_id);
        root.insert(&mut txn, "record_id", record_id);
        root.insert(&mut txn, "schema_version", schema_version as f64);
        drop(txn);
        Self {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            schema_version,
            inner: doc,
            root,
        }
    }

    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, VaultSyncError> {
        if bytes.len() > Self::MAX_SNAPSHOT_SIZE {
            return Err(VaultSyncError::DocumentTooLarge(
                bytes.len(),
                Self::MAX_SNAPSHOT_SIZE,
            ));
        }
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        let root = txn.get_or_insert_map("root");
        let update = Update::decode_v1(bytes)
            .map_err(|e| VaultSyncError::Crdt(format!("failed to decode snapshot: {e}")))?;
        txn.apply_update(update);
        drop(txn);

        let txn = doc.transact();
        let doc_id = root
            .get(&txn, "doc_id")
            .and_then(|v| extract_string(&v))
            .unwrap_or_default();
        let record_id = root
            .get(&txn, "record_id")
            .and_then(|v| extract_string(&v))
            .unwrap_or_default();
        let schema_version = root
            .get(&txn, "schema_version")
            .and_then(|v| match v {
                Out::Any(Any::Number(n)) => Some(n as u64),
                _ => None,
            })
            .unwrap_or(0);
        drop(txn);

        Ok(Self {
            doc_id,
            record_id,
            schema_version,
            inner: doc,
            root,
        })
    }

    pub fn to_snapshot(&self) -> Vec<u8> {
        let txn = self.inner.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    }

    pub fn apply_update(&mut self, update: &[u8]) -> Result<(), VaultSyncError> {
        if update.len() > Self::MAX_SNAPSHOT_SIZE {
            return Err(VaultSyncError::DocumentTooLarge(
                update.len(),
                Self::MAX_SNAPSHOT_SIZE,
            ));
        }
        let start = crate::time_utils::PlatformInstant::now();
        let span = tracing::info_span!("crdt.merge", doc_id = self.doc_id.as_str());
        let _enter = span.enter();

        let decoded = Update::decode_v1(update)
            .map_err(|e| VaultSyncError::Crdt(format!("failed to decode update: {e}")))?;
        let mut txn = self.inner.transact_mut();
        txn.apply_update(decoded);
        drop(txn);

        let duration_us = start.elapsed().as_micros() as f64;
        crate::telemetry::metrics::get_metrics().record_crdt_merge_time(&self.doc_id, duration_us);
        Ok(())
    }

    pub fn get_field(&self, field: &str) -> Option<CrdtValue> {
        let txn = self.inner.transact();
        self.root
            .get(&txn, field)
            .map(|out| out_to_crdt_value(&out, &txn))
    }

    pub fn set_field(&mut self, field: &str, value: CrdtValue) -> Vec<u8> {
        let sv = self.state_vector();
        let mut txn = self.inner.transact_mut();
        let any_val = crdt_value_to_any(&value);
        self.root.insert(&mut txn, field, any_val);
        drop(txn);
        self.inner.transact().encode_diff_v1(&sv)
    }

    pub fn capture_incremental_update<F>(&mut self, f: F) -> Vec<u8>
    where
        F: FnOnce(&mut Self),
    {
        let sv = self.state_vector();
        f(self);
        self.inner.transact().encode_diff_v1(&sv)
    }

    pub fn delete_field(&mut self, field: &str) -> Vec<u8> {
        let sv = self.state_vector();
        let mut txn = self.inner.transact_mut();
        self.root.remove(&mut txn, field);
        drop(txn);
        self.inner.transact().encode_diff_v1(&sv)
    }

    pub fn encode_update(&self) -> Vec<u8> {
        let txn = self.inner.transact();
        let sv = yrs::StateVector::default();
        txn.encode_diff_v1(&sv)
    }

    pub fn is_consistent(&self) -> bool {
        let txn = self.inner.transact();
        self.root.contains_key(&txn, "doc_id") && self.root.contains_key(&txn, "record_id")
    }

    pub fn to_map(&self) -> HashMap<String, CrdtValue> {
        let txn = self.inner.transact();
        let mut map = HashMap::new();
        for (key, value) in self.root.iter(&txn) {
            map.insert(key.to_string(), out_to_crdt_value(&value, &txn));
        }
        map
    }

    pub fn inner_doc(&self) -> &Doc {
        &self.inner
    }

    pub fn state_vector(&self) -> yrs::StateVector {
        let txn = self.inner.transact();
        txn.state_vector()
    }
}

fn extract_string(out: &Out) -> Option<String> {
    if let Out::Any(Any::String(s)) = out {
        Some(s.to_string())
    } else {
        None
    }
}

fn out_to_crdt_value(out: &Out, txn: &yrs::Transaction) -> CrdtValue {
    match out {
        Out::Any(any) => any_to_crdt_value(any),
        Out::YText(text) => CrdtValue::String(text.get_string(txn)),
        Out::YArray(arr) => {
            let mut items = Vec::new();
            for item in arr.iter(txn) {
                items.push(out_to_crdt_value(&item, txn));
            }
            CrdtValue::Array(items)
        }
        Out::YMap(map) => {
            let mut fields = HashMap::new();
            for (k, v) in map.iter(txn) {
                fields.insert(k.to_string(), out_to_crdt_value(&v, txn));
            }
            CrdtValue::Map(fields)
        }
        _ => CrdtValue::Null,
    }
}

fn any_to_crdt_value(any: &Any) -> CrdtValue {
    match any {
        Any::String(s) => CrdtValue::String(s.to_string()),
        Any::Number(n) => CrdtValue::Number(*n),
        Any::Bool(b) => CrdtValue::Boolean(*b),
        Any::Array(arr) => CrdtValue::Array(arr.iter().map(|a| any_to_crdt_value(a)).collect()),
        Any::Map(map) => {
            let fields: HashMap<String, CrdtValue> = map
                .iter()
                .map(|(k, v)| (k.to_string(), any_to_crdt_value(v)))
                .collect();
            CrdtValue::Map(fields)
        }
        Any::Null => CrdtValue::Null,
        _ => CrdtValue::Null,
    }
}

fn crdt_value_to_any(val: &CrdtValue) -> Any {
    match val {
        CrdtValue::String(s) => Any::String(s.clone().into()),
        CrdtValue::Number(n) => Any::Number(*n),
        CrdtValue::Boolean(b) => Any::Bool(*b),
        CrdtValue::Array(items) => Any::Array(items.iter().map(crdt_value_to_any).collect()),
        CrdtValue::Map(fields) => {
            let map: HashMap<String, Any> = fields
                .iter()
                .map(|(k, v)| (k.clone(), crdt_value_to_any(v)))
                .collect();
            Any::Map(map.into())
        }
        CrdtValue::Null => Any::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn any_crdt_value_flat() -> impl Strategy<Value = CrdtValue> {
        prop_oneof![
            any::<String>().prop_map(CrdtValue::String),
            any::<f64>().prop_map(CrdtValue::Number),
            any::<bool>().prop_map(CrdtValue::Boolean),
            Just(CrdtValue::Null),
        ]
    }

    fn any_crdt_value() -> impl Strategy<Value = CrdtValue> {
        any_crdt_value_flat().prop_recursive(
            4,  // max depth
            16, // max size
            3,  // max items per vec/map
            |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..3).prop_map(CrdtValue::Array),
                    prop::collection::hash_map(any::<String>(), inner, 0..3)
                        .prop_map(CrdtValue::Map),
                ]
            },
        )
    }

    #[derive(Debug, Clone)]
    enum PropOp {
        Set { key: String, value: CrdtValue },
        Delete { key: String },
    }

    fn any_prop_op() -> impl Strategy<Value = PropOp> {
        let keys = prop_oneof![
            Just("key_a".to_string()),
            Just("key_b".to_string()),
            Just("key_c".to_string()),
            Just("key_d".to_string()),
            any::<String>(),
        ];
        prop_oneof![
            (keys.clone(), any_crdt_value()).prop_map(|(key, value)| PropOp::Set { key, value }),
            keys.prop_map(|key| PropOp::Delete { key }),
        ]
    }

    proptest! {
        #[test]
        fn prop_convergence(
            ops in prop::collection::vec((0..3usize, any_prop_op()), 1..50)
        ) {
            let base = CRDTDocument::new("doc1", "rec1", 1);
            let base_snapshot = base.to_snapshot();
            let mut replicas = vec![
                CRDTDocument::from_snapshot(&base_snapshot).unwrap(),
                CRDTDocument::from_snapshot(&base_snapshot).unwrap(),
                CRDTDocument::from_snapshot(&base_snapshot).unwrap(),
            ];

            let mut updates = Vec::new();
            for (idx, op) in ops {
                let replica = &mut replicas[idx];
                let update = match op {
                    PropOp::Set { key, value } => replica.set_field(&key, value),
                    PropOp::Delete { key } => replica.delete_field(&key),
                };
                updates.push(update);
            }

            // Apply all updates to all replicas
            for replica in &mut replicas {
                for update in &updates {
                    replica.apply_update(update).unwrap();
                }
            }

            // Verify all replicas converged
            let map0 = replicas[0].to_map();
            let map1 = replicas[1].to_map();
            let map2 = replicas[2].to_map();
            assert_eq!(map0, map1);
            assert_eq!(map1, map2);
        }

        #[test]
        fn prop_idempotence(
            op in any_prop_op()
        ) {
            let mut doc1 = CRDTDocument::new("doc1", "rec1", 1);
            let mut doc2 = CRDTDocument::new("doc1", "rec1", 1);

            let update = match op {
                PropOp::Set { key, value } => doc1.set_field(&key, value),
                PropOp::Delete { key } => doc1.delete_field(&key),
            };

            // Apply once
            doc2.apply_update(&update).unwrap();
            let map_once = doc2.to_map();

            // Apply twice
            doc2.apply_update(&update).unwrap();
            let map_twice = doc2.to_map();

            assert_eq!(map_once, map_twice);
        }

        #[test]
        fn prop_commutativity(
            op1 in any_prop_op(),
            op2 in any_prop_op()
        ) {
            let base = CRDTDocument::new("doc1", "rec1", 1);

            let mut branch_a = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            let mut branch_b = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();

            let update_a = match op1 {
                PropOp::Set { key, value } => branch_a.set_field(&key, value),
                PropOp::Delete { key } => branch_a.delete_field(&key),
            };

            let update_b = match op2 {
                PropOp::Set { key, value } => branch_b.set_field(&key, value),
                PropOp::Delete { key } => branch_b.delete_field(&key),
            };

            // Merge update_a then update_b
            let mut doc_ab = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            doc_ab.apply_update(&update_a).unwrap();
            doc_ab.apply_update(&update_b).unwrap();

            // Merge update_b then update_a
            let mut doc_ba = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            doc_ba.apply_update(&update_b).unwrap();
            doc_ba.apply_update(&update_a).unwrap();

            assert_eq!(doc_ab.to_map(), doc_ba.to_map());
        }

        #[test]
        fn prop_associativity(
            op1 in any_prop_op(),
            op2 in any_prop_op(),
            op3 in any_prop_op()
        ) {
            let base = CRDTDocument::new("doc1", "rec1", 1);

            let mut branch_a = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            let mut branch_b = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            let mut branch_c = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();

            let update_a = match op1 {
                PropOp::Set { key, value } => branch_a.set_field(&key, value),
                PropOp::Delete { key } => branch_a.delete_field(&key),
            };

            let update_b = match op2 {
                PropOp::Set { key, value } => branch_b.set_field(&key, value),
                PropOp::Delete { key } => branch_b.delete_field(&key),
            };

            let update_c = match op3 {
                PropOp::Set { key, value } => branch_c.set_field(&key, value),
                PropOp::Delete { key } => branch_c.delete_field(&key),
            };

            let mut doc_a = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            doc_a.apply_update(&update_a).unwrap();

            let mut doc_b = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            doc_b.apply_update(&update_b).unwrap();

            let mut doc_c = CRDTDocument::from_snapshot(&base.to_snapshot()).unwrap();
            doc_c.apply_update(&update_c).unwrap();

            // Grouping 1: (A merge B) merge C
            let mut doc_ab = CRDTDocument::from_snapshot(&doc_a.to_snapshot()).unwrap();
            doc_ab.apply_update(&doc_b.to_snapshot()).unwrap();
            doc_ab.apply_update(&doc_c.to_snapshot()).unwrap();

            // Grouping 2: A merge (B merge C)
            let mut doc_bc = CRDTDocument::from_snapshot(&doc_b.to_snapshot()).unwrap();
            doc_bc.apply_update(&doc_c.to_snapshot()).unwrap();

            let mut doc_a_bc = CRDTDocument::from_snapshot(&doc_a.to_snapshot()).unwrap();
            doc_a_bc.apply_update(&doc_bc.to_snapshot()).unwrap();

            assert_eq!(doc_ab.to_map(), doc_a_bc.to_map());
        }
    }
}
