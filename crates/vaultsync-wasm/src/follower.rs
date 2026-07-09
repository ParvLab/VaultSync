use crate::metrics::RuntimeMetrics;
use crate::runtime::DocumentStore;
use std::collections::HashMap;
use std::sync::Arc;
use vaultsync_core::crdt::types::CrdtValue;
use wasm_bindgen::prelude::*;

/// Phase 3: MirrorRuntime — lightweight runtime for follower tabs.
/// No storage, no coordinator, no recovery.
/// Shares DocumentStore — reads/writes go to the shared in-memory store.
#[derive(Debug)]
pub struct MirrorRuntime {
    pub document_store: Arc<std::sync::Mutex<DocumentStore>>,
    pub runtime_gen: std::sync::atomic::AtomicU64,
    pub bus_gen: std::sync::atomic::AtomicU64,
    pub cursor: std::sync::atomic::AtomicU64,
    pub pending_count: std::sync::atomic::AtomicU64,
    pub last_heartbeat: std::sync::atomic::AtomicU64,
    pub metrics: Arc<RuntimeMetrics>,
    pub tab_id: String,
    /// Phase 4: Subscription callbacks — doc_id → Vec<(subscription_id, callback)>
    pub subscriptions: Arc<std::sync::Mutex<HashMap<String, Vec<(u64, js_sys::Function)>>>>,
}

impl MirrorRuntime {
    pub fn new(tab_id: &str, doc_store: Arc<std::sync::Mutex<DocumentStore>>) -> Arc<Self> {
        Arc::new(Self {
            document_store: doc_store,
            runtime_gen: std::sync::atomic::AtomicU64::new(0),
            bus_gen: std::sync::atomic::AtomicU64::new(0),
            cursor: std::sync::atomic::AtomicU64::new(0),
            pending_count: std::sync::atomic::AtomicU64::new(0),
            last_heartbeat: std::sync::atomic::AtomicU64::new(0),
            metrics: Arc::new(RuntimeMetrics::default()),
            tab_id: tab_id.to_string(),
            subscriptions: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Get a document record from the shared DocumentStore.
    pub fn get(&self, doc_id: &str, record_id: &str) -> Option<HashMap<String, CrdtValue>> {
        let store = self.document_store.lock().unwrap();
        store.get_record(doc_id, record_id).cloned()
    }

    /// Find all records for a given doc_id.
    pub fn query_doc(&self, doc_id: &str) -> Vec<HashMap<String, CrdtValue>> {
        let store = self.document_store.lock().unwrap();
        store.query_doc(doc_id).into_iter().cloned().collect()
    }

    /// Apply a mutation directly to the shared DocumentStore.
    pub fn apply_mutation(&self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        let mut store = self.document_store.lock().unwrap();
        store.set_field(doc_id, record_id, field, value);
    }

    /// Set an entire record in the shared DocumentStore.
    pub fn set_record(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) {
        let mut store = self.document_store.lock().unwrap();
        for (field, value) in fields {
            store.set_field(doc_id, record_id, &field, value);
        }
    }

    /// Remove a record from the shared DocumentStore.
    pub fn remove_record(&self, doc_id: &str, record_id: &str) {
        let mut store = self.document_store.lock().unwrap();
        store.delete_document(doc_id, record_id);
    }

    /// Update heartbeat from leader's HEARTBEAT message.
    pub fn update_heartbeat(&self, runtime_gen: u64, bus_gen: u64, cursor: u64, pending_count: u64) {
        self.runtime_gen.store(runtime_gen, std::sync::atomic::Ordering::Release);
        self.bus_gen.store(bus_gen, std::sync::atomic::Ordering::Release);
        self.cursor.store(cursor, std::sync::atomic::Ordering::Release);
        self.pending_count.store(pending_count, std::sync::atomic::Ordering::Release);
        self.last_heartbeat.store(js_sys::Date::now() as u64, std::sync::atomic::Ordering::Release);
    }

    /// Check if leader is alive (heartbeat within timeout).
    pub fn leader_alive(&self, timeout_ms: u64) -> bool {
        let now = js_sys::Date::now() as u64;
        let last = self.last_heartbeat.load(std::sync::atomic::Ordering::Acquire);
        last > 0 && now.saturating_sub(last) < timeout_ms
    }

    /// Clear all state (used when leader changes).
    pub fn clear(&self) {
        let mut store = self.document_store.lock().unwrap();
        store.documents.clear();
        store.access_counts.clear();
        drop(store);
        self.runtime_gen.store(0, std::sync::atomic::Ordering::Release);
        self.bus_gen.store(0, std::sync::atomic::Ordering::Release);
        self.cursor.store(0, std::sync::atomic::Ordering::Release);
        self.pending_count.store(0, std::sync::atomic::Ordering::Release);
        self.last_heartbeat.store(0, std::sync::atomic::Ordering::Release);
    }

    /// Phase 4f: Check if leader has timed out and signal JS to promote.
    /// Returns true if leader is gone and the tab should reinitialize as leader.
    pub fn promote_to_leader(&self) -> bool {
        if !self.leader_alive(5000) {
            self.metrics.promotion_candidate_attempts.fetch_add(
                1, std::sync::atomic::Ordering::Relaxed
            );
            engine_info!("[MirrorRuntime] leader timeout — signaling promotion");
            true
        } else {
            false
        }
    }

    /// Number of documents in store.
    pub fn cached_count(&self) -> usize {
        self.document_store.lock().unwrap().documents.len()
    }

    /// Subscribe to changes for a specific doc_id.
    pub fn subscribe(&self, doc_id: &str, callback: js_sys::Function) -> u64 {
        static SUB_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = SUB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut subs = self.subscriptions.lock().unwrap();
        subs.entry(doc_id.to_string()).or_default().push((id, callback));
        id
    }

    /// Unsubscribe a specific subscription.
    pub fn unsubscribe(&self, doc_id: &str, sub_id: u64) {
        let mut subs = self.subscriptions.lock().unwrap();
        if let Some(callbacks) = subs.get_mut(doc_id) {
            callbacks.retain(|(id, _)| *id != sub_id);
        }
    }

    /// Fire subscription callbacks for a document mutation.
    pub fn fire_subscription(&self, doc_id: &str, record_id: &str, fields: &HashMap<String, CrdtValue>) {
        let callbacks = {
            let subs = self.subscriptions.lock().unwrap();
            subs.get(doc_id).cloned().unwrap_or_default()
        };
        if callbacks.is_empty() {
            return;
        }
        let json_str = crate::client::fields_to_json_string(fields).unwrap_or_else(|_| "{}".to_string());
        let record_id_js = JsValue::from_str(record_id);
        let json_js = JsValue::from_str(&json_str);
        for (_id, cb) in &callbacks {
            let _ = cb.call2(&JsValue::NULL, &record_id_js, &json_js);
        }
    }
}
