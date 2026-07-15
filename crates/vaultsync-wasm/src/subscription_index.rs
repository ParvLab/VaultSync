use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::crdt::types::CrdtValue;
use wasm_bindgen::prelude::*;

/// Unified SubscriptionIndex used by both leader (DocumentRuntime) and follower (MirrorRuntime).
///
/// Provides O(1) doc→subscriber dispatch so mutations only notify listeners for the affected doc.
/// No namespace index yet — that's added in Phase 6 (NamespaceManager).
#[derive(Debug)]
pub struct SubscriptionIndex {
    by_document: Mutex<HashMap<String, Vec<(u64, String, js_sys::Function)>>>,
    by_handle: Mutex<HashMap<u64, (String, String)>>,
    next_id: AtomicU64,
}

impl SubscriptionIndex {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            by_document: Mutex::new(HashMap::new()),
            by_handle: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
    }

    /// Subscribe to changes for a specific doc_id + record_id.
    /// Returns a handle ID for unsubscription.
    pub fn subscribe(
        &self,
        doc_id: &str,
        record_id: &str,
        callback: js_sys::Function,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut docs = self.by_document.lock().unwrap();
            docs.entry(doc_id.to_string())
                .or_default()
                .push((id, record_id.to_string(), callback));
        }
        {
            let mut handles = self.by_handle.lock().unwrap();
            handles.insert(id, (doc_id.to_string(), record_id.to_string()));
        }
        id
    }

    /// Unsubscribe a specific handle. Removes from both indexes.
    pub fn unsubscribe(&self, handle_id: u64) {
        let key = {
            let mut handles = self.by_handle.lock().unwrap();
            handles.remove(&handle_id)
        };
        if let Some((doc_id, record_id)) = key {
            let mut docs = self.by_document.lock().unwrap();
            if let Some(callbacks) = docs.get_mut(&doc_id) {
                callbacks.retain(|(id, rec, _)| *id != handle_id || *rec != record_id);
                if callbacks.is_empty() {
                    docs.remove(&doc_id);
                }
            }
        }
    }

    /// Fire subscription callbacks for a specific doc_id + record_id with the given fields.
    /// Invokes callbacks registered for this exact doc+record combination OR
    /// callbacks registered for the wildcard record_id "" (all records for a doc).
    pub fn fire(
        &self,
        doc_id: &str,
        record_id: &str,
        fields: &HashMap<String, CrdtValue>,
    ) {
        let callbacks = {
            let docs = self.by_document.lock().unwrap();
            docs.get(doc_id).cloned().unwrap_or_default()
        };
        if callbacks.is_empty() {
            engine_trace!(
                "[fire] doc={} record={} subscribers=0 hop=subscription_notify — no subscribers",
                doc_id, record_id,
            );
            return;
        }
        let json_str = crate::util::fields_to_json_string(fields).unwrap_or_else(|_| "{}".to_string());
        let record_id_js = JsValue::from_str(record_id);
        let json_js = JsValue::from_str(&json_str);
        let mut fired = 0u64;
        for (_, rec, cb) in &callbacks {
            if rec == record_id || rec.is_empty() {
                let _ = cb.call2(&JsValue::NULL, &record_id_js, &json_js);
                fired += 1;
            }
        }
        engine_trace!(
            "[fire] doc={} record={} subscribers={} fired={} hop=subscription_notify",
            doc_id, record_id, callbacks.len(), fired,
        );
    }

    /// Check if any subscribers exist for a given doc_id.
    pub fn has_subscribers(&self, doc_id: &str) -> bool {
        let docs = self.by_document.lock().unwrap();
        docs.contains_key(doc_id)
    }

    /// Total number of unique doc_ids with subscribers.
    pub fn subscriber_count(&self) -> usize {
        let docs = self.by_document.lock().unwrap();
        docs.len()
    }
}
