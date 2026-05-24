use std::collections::HashMap;
use yrs::{Doc, Map, MapRef, Out, ReadTxn, Transact, Update, WriteTxn, Array, GetString};
use yrs::updates::decoder::Decode;
use yrs::any::Any;
use crate::crdt::types::CrdtValue;
use crate::error::DriftError;

pub struct CRDTDocument {
    pub doc_id: String,
    pub record_id: String,
    pub schema_version: u64,
    inner: Doc,
    root: MapRef,
}

impl CRDTDocument {
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

    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, DriftError> {
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        let root = txn.get_or_insert_map("root");
        let update = Update::decode_v1(bytes)
            .map_err(|e| DriftError::Crdt(format!("failed to decode snapshot: {e}")))?;
        txn.apply_update(update);
        drop(txn);

        let txn = doc.transact();
        let doc_id = root.get(&txn, "doc_id")
            .and_then(|v| extract_string(&v))
            .unwrap_or_default();
        let record_id = root.get(&txn, "record_id")
            .and_then(|v| extract_string(&v))
            .unwrap_or_default();
        let schema_version = root.get(&txn, "schema_version")
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

    pub fn apply_update(&mut self, update: &[u8]) -> Result<(), DriftError> {
        let decoded = Update::decode_v1(update)
            .map_err(|e| DriftError::Crdt(format!("failed to decode update: {e}")))?;
        let mut txn = self.inner.transact_mut();
        txn.apply_update(decoded);
        Ok(())
    }

    pub fn get_field(&self, field: &str) -> Option<CrdtValue> {
        let txn = self.inner.transact();
        self.root.get(&txn, field).map(|out| out_to_crdt_value(&out, &txn))
    }

    pub fn set_field(&mut self, field: &str, value: CrdtValue) -> Vec<u8> {
        let mut txn = self.inner.transact_mut();
        let any_val = crdt_value_to_any(&value);
        self.root.insert(&mut txn, field, any_val);
        txn.encode_update_v1()
    }

    pub fn delete_field(&mut self, field: &str) -> Vec<u8> {
        let mut txn = self.inner.transact_mut();
        self.root.remove(&mut txn, field);
        txn.encode_update_v1()
    }

    pub fn encode_update(&self) -> Vec<u8> {
        let txn = self.inner.transact();
        let sv = yrs::StateVector::default();
        txn.encode_diff_v1(&sv)
    }

    pub fn is_consistent(&self) -> bool {
        let txn = self.inner.transact();
        self.root.contains_key(&txn, "doc_id")
            && self.root.contains_key(&txn, "record_id")
    }

    pub fn to_map(&self) -> HashMap<String, CrdtValue> {
        let txn = self.inner.transact();
        let mut map = HashMap::new();
        for (key, value) in self.root.iter(&txn) {
            map.insert(key.to_string(), out_to_crdt_value(&value, &txn));
        }
        map
    }

    pub fn inner_doc(&self) -> &Doc { &self.inner }

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
        Any::Array(arr) => {
            CrdtValue::Array(arr.iter().map(|a| any_to_crdt_value(a)).collect())
        }
        Any::Map(map) => {
            let fields: HashMap<String, CrdtValue> = map.iter()
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
        CrdtValue::Array(items) => {
            Any::Array(items.iter().map(crdt_value_to_any).collect())
        }
        CrdtValue::Map(fields) => {
            let map: HashMap<String, Any> = fields.iter()
                .map(|(k, v)| (k.clone(), crdt_value_to_any(v)))
                .collect();
            Any::Map(map.into())
        }
        CrdtValue::Null => Any::Null,
    }
}
