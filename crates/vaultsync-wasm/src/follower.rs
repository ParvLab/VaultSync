use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::fmt;
use std::sync::{Arc, Mutex};

use wasm_bindgen::JsValue;
use vaultsync_core::crdt::types::CrdtValue;

use crate::metrics::RuntimeMetrics;
use crate::runtime::DocumentStore;
use crate::subscription_index::SubscriptionIndex;

/// Phase 3: MirrorRuntime — lightweight runtime for follower tabs.
/// No storage, no coordinator, no recovery.
/// Shares DocumentStore — reads/writes go to the shared in-memory store.
pub struct MirrorRuntime {
    pub document_store: Arc<std::sync::Mutex<DocumentStore>>,
    pub runtime_gen: AtomicU64,
    pub bus_gen: AtomicU64,
    pub cursor: AtomicU64,
    pub pending_count: AtomicU64,
    pub last_heartbeat: AtomicU64,
    pub metrics: Arc<RuntimeMetrics>,
    pub tab_id: String,
    pub subscription_index: Arc<SubscriptionIndex>,
    /// Sprint B: Set when PROTO|LEFT received from leader. Fast leader-loss detection.
    pub leader_left: AtomicBool,
    /// Sprint 2: Count of mutations received during replay (reset on SYNC_BEGIN).
    pub replay_mutation_count: AtomicU64,
    /// Phase 4: Set when Web Lock acquired (LeaderEvent::Acquired fires).
    /// Used by promote_to_leader() alongside leader_left and heartbeat timeout.
    pub promotion_ready: AtomicBool,
    /// Phase 2: Parameters for promote(), stored here so they survive
    /// the MirrorRuntime→VaultSyncRuntime handoff without external refs.
    pub promote_ns: Mutex<String>,
    pub promote_url: Mutex<String>,
    pub promote_auth: Mutex<Option<String>>,
    pub promote_db: Mutex<Option<String>>,
    pub promote_backend: Mutex<Option<String>>,
    /// Guard against double promotion.
    pub is_promoting: AtomicBool,
}

impl fmt::Debug for MirrorRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirrorRuntime")
            .field("tab_id", &self.tab_id)
            .field("runtime_gen", &self.runtime_gen)
            .field("bus_gen", &self.bus_gen)
            .field("cursor", &self.cursor)
            .field("pending_count", &self.pending_count)
            .field("last_heartbeat", &self.last_heartbeat)
            .field("leader_left", &self.leader_left)
            .field("promotion_ready", &self.promotion_ready)
            .field("is_promoting", &self.is_promoting)
            .finish()
    }
}

impl MirrorRuntime {
    pub fn new(tab_id: &str, doc_store: Arc<std::sync::Mutex<DocumentStore>>) -> Arc<Self> {
        Arc::new(Self {
            document_store: doc_store,
            runtime_gen: AtomicU64::new(0),
            bus_gen: AtomicU64::new(0),
            cursor: AtomicU64::new(0),
            pending_count: AtomicU64::new(0),
            last_heartbeat: AtomicU64::new(0),
            metrics: Arc::new(RuntimeMetrics::default()),
            tab_id: tab_id.to_string(),
            subscription_index: SubscriptionIndex::new(),
            leader_left: AtomicBool::new(false),
            replay_mutation_count: AtomicU64::new(0),
            promotion_ready: AtomicBool::new(false),
            promote_ns: Mutex::new(String::new()),
            promote_url: Mutex::new(String::new()),
            promote_auth: Mutex::new(None),
            promote_db: Mutex::new(None),
            promote_backend: Mutex::new(None),
            is_promoting: AtomicBool::new(false),
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
        engine_trace!(
            "[mirror] doc={} record={} field={} hop=doc_store_update",
            doc_id, record_id, field,
        );
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

    /// Phase 4: Drain all documents from MirrorRuntime's DocumentStore into
    /// the new leader Runtime's DocumentStore. This preserves in-memory state
    /// during follower→leader promotion so no data is lost.
    pub fn drain_into(&self, target: &Arc<std::sync::Mutex<DocumentStore>>) {
        let mut src = self.document_store.lock().unwrap();
        let mut dst = target.lock().unwrap();
        for (doc_id, records) in src.documents.drain() {
            for (record_id, fields) in records {
                for (field, value) in fields {
                    dst.set_field(&doc_id, &record_id, &field, value);
                }
            }
        }
        for (doc_id, (count, last_decay)) in src.access_counts.drain() {
            dst.access_counts.insert(doc_id, (count, last_decay));
        }
    }

    /// Sprint B: Set when PROTO|LEFT is received from leader.
    /// Enables fast leader-loss detection without heartbeat timeout.
    pub fn set_leader_left(&self) {
        self.leader_left.store(true, Ordering::Release);
    }

    /// Clear all state (used when leader changes).
    pub fn clear(&self) {
        let mut store = self.document_store.lock().unwrap();
        store.documents.clear();
        store.access_counts.clear();
        drop(store);
        self.runtime_gen.store(0, Ordering::Release);
        self.bus_gen.store(0, Ordering::Release);
        self.cursor.store(0, Ordering::Release);
        self.pending_count.store(0, Ordering::Release);
        self.last_heartbeat.store(0, Ordering::Release);
        self.leader_left.store(false, Ordering::Release);
    }

    /// Sprint B: Check if leader has left or timed out and signal JS to promote.
    /// Returns true if leader is gone and the tab should reinitialize as leader.
    /// Uses PROTO|LEFT for fast detection, heartbeat timeout as fallback.
    pub fn promote_to_leader(&self) -> bool {
        if self.leader_left.load(Ordering::Acquire) {
            self.metrics.promotion_candidate_attempts.fetch_add(
                1, Ordering::Relaxed
            );
            engine_info!("[MirrorRuntime] leader LEFT received — signaling promotion");
            return true;
        }
        if self.promotion_ready.load(Ordering::Acquire) {
            self.metrics.promotion_candidate_attempts.fetch_add(
                1, Ordering::Relaxed
            );
            engine_info!("[MirrorRuntime] Web Lock acquired — promotion ready");
            return true;
        }
        if !self.leader_alive(5000) {
            self.metrics.promotion_candidate_attempts.fetch_add(
                1, Ordering::Relaxed
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

    /// Subscribe to changes for a specific doc_id + record_id.
    pub fn subscribe(&self, doc_id: &str, record_id: &str, callback: js_sys::Function) -> u64 {
        self.subscription_index.subscribe(doc_id, record_id, callback)
    }

    /// Unsubscribe a specific subscription handle.
    pub fn unsubscribe(&self, handle_id: u64) {
        self.subscription_index.unsubscribe(handle_id);
    }

    /// Fire subscription callbacks for a document mutation.
    pub fn fire_subscription(&self, doc_id: &str, record_id: &str, fields: &HashMap<String, CrdtValue>) {
        self.subscription_index.fire(doc_id, record_id, fields);
    }
}
