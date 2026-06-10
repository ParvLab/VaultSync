use std::collections::{HashSet, HashMap};
use crate::crdt::types::CrdtValue;
use crate::crdt::document::CRDTDocument;

pub struct CrdtObserver {
    watched: HashSet<(String, String)>,
    last_states: HashMap<(String, String), HashMap<String, CrdtValue>>,
}

impl CrdtObserver {
    pub fn new() -> Self {
        Self {
            watched: HashSet::new(),
            last_states: HashMap::new(),
        }
    }

    pub fn watch(&mut self, doc_id: &str, record_id: &str) {
        self.watched.insert((doc_id.to_string(), record_id.to_string()));
    }

    pub fn unwatch(&mut self, doc_id: &str, record_id: &str) {
        self.watched.remove(&(doc_id.to_string(), record_id.to_string()));
        self.last_states.remove(&(doc_id.to_string(), record_id.to_string()));
    }

    pub fn detect_changes(&mut self, doc_id: &str, doc: &CRDTDocument) -> Vec<String> {
        let key = (doc_id.to_string(), doc.record_id.clone());
        if !self.watched.contains(&key) {
            return Vec::new();
        }
        let current = doc.to_map();
        let changed = if let Some(prev) = self.last_states.get(&key) {
            let mut changes = Vec::new();
            for (field, value) in &current {
                if prev.get(field) != Some(value) {
                    changes.push(field.clone());
                }
            }
            for field in prev.keys() {
                if !current.contains_key(field) {
                    changes.push(field.clone());
                }
            }
            changes
        } else {
            current.keys().cloned().collect()
        };
        self.last_states.insert(key, current);
        changed
    }

    pub fn watched_count(&self) -> usize {
        self.watched.len()
    }
}
