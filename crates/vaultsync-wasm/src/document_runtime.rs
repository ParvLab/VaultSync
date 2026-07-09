use crate::memory::SegmentedLruCache;
use crate::metrics::RuntimeMetrics;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::crdt::types::CrdtValue;
use wasm_bindgen::prelude::*;

/// Phase 5: DocumentRuntime — owns all document lifecycle state.
///
/// Responsibilities:
/// - DocumentStore (in-memory document state, source of truth for cache)
/// - Subscriptions (JS callbacks for doc_id changes)
/// - Document cache (SegmentedLRU for hot documents)
/// - Working set integration
/// - Eviction of cold documents
///
/// Separated from StorageRuntime and SyncRuntime because document lifecycle
/// is neither pure storage nor pure sync — it's the application-facing layer.
pub struct DocumentRuntime {
    /// In-memory document store: doc_id → record_id → field → CrdtValue
    pub store: Arc<Mutex<DocumentStore>>,
    /// Subscriptions: doc_id → Vec<(sub_id, record_id, callback)>
    pub subscriptions: Arc<Mutex<HashMap<String, Vec<(u64, String, js_sys::Function)>>>>,
    /// Document cache (SegmentedLRU)
    pub cache: Arc<Mutex<SegmentedLruCache<String, HashMap<String, CrdtValue>>>>,
    /// Metrics
    pub metrics: Arc<RuntimeMetrics>,
    /// Hot document tracking
    pub access_counts: Arc<Mutex<HashMap<String, (f64, u64)>>>,
    /// Total documents loaded
    pub loaded_count: AtomicU64,
    /// Eviction count
    pub eviction_count: AtomicU64,
}

#[derive(Debug)]
pub struct DocumentStore {
    pub documents: HashMap<String, HashMap<String, HashMap<String, CrdtValue>>>,
}

impl DocumentStore {
    pub fn new() -> Self {
        Self { documents: HashMap::new() }
    }

    pub fn set_field(&mut self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        self.documents
            .entry(doc_id.to_string())
            .or_default()
            .entry(record_id.to_string())
            .or_default()
            .insert(field.to_string(), value);
    }

    pub fn set_record(&mut self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) {
        self.documents
            .entry(doc_id.to_string())
            .or_default()
            .insert(record_id.to_string(), fields);
    }

    pub fn delete_field(&mut self, doc_id: &str, record_id: &str, field: &str) {
        if let Some(doc) = self.documents.get_mut(doc_id) {
            if let Some(rec) = doc.get_mut(record_id) {
                rec.remove(field);
            }
        }
    }

    pub fn delete_record(&mut self, doc_id: &str, record_id: &str) {
        if let Some(doc) = self.documents.get_mut(doc_id) {
            doc.remove(record_id);
        }
    }

    pub fn get_record(&self, doc_id: &str, record_id: &str) -> Option<&HashMap<String, CrdtValue>> {
        self.documents.get(doc_id)?.get(record_id)
    }

    pub fn query_doc(&self, doc_id: &str) -> Vec<&HashMap<String, CrdtValue>> {
        self.documents
            .get(doc_id)
            .map(|recs| recs.values().collect())
            .unwrap_or_default()
    }

    pub fn has_doc(&self, doc_id: &str) -> bool {
        self.documents.contains_key(doc_id)
    }

    pub fn doc_count(&self) -> usize {
        self.documents.len()
    }

    pub fn record_count(&self, doc_id: &str) -> usize {
        self.documents.get(doc_id).map(|r| r.len()).unwrap_or(0)
    }
}

impl DocumentRuntime {
    const CACHE_CAPACITY: usize = 5000;
    const CACHE_HOT_FRACTION: f64 = 0.2;

    pub fn new(metrics: Arc<RuntimeMetrics>) -> Arc<Self> {
        Arc::new(Self {
            store: Arc::new(Mutex::new(DocumentStore::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            cache: Arc::new(Mutex::new(SegmentedLruCache::new(
                Self::CACHE_CAPACITY,
                Self::CACHE_HOT_FRACTION,
            ))),
            metrics,
            access_counts: Arc::new(Mutex::new(HashMap::new())),
            loaded_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
        })
    }

    // ── Read operations ──

    pub fn get(&self, doc_id: &str, record_id: &str) -> Option<HashMap<String, CrdtValue>> {
        // Check cache first
        let cache_key = format!("{}:{}", doc_id, record_id);
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(fields) = cache.get(&cache_key) {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Some(fields.clone());
            }
        }
        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Fall back to store
        let store = self.store.lock().unwrap();
        store.get_record(doc_id, record_id).cloned()
    }

    pub fn query_doc(&self, doc_id: &str) -> Vec<HashMap<String, CrdtValue>> {
        let store = self.store.lock().unwrap();
        store.query_doc(doc_id).into_iter().cloned().collect()
    }

    // ── Write operations ──

    pub fn set_field(&self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        let mut store = self.store.lock().unwrap();
        store.set_field(doc_id, record_id, field, value.clone());
        self.record_access(doc_id);

        // Update cache
        let cache_key = format!("{}:{}", doc_id, record_id);
        if let Some(record) = store.get_record(doc_id, record_id) {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(cache_key, record.clone());
        }

        // Fire subscriptions
        self.fire_subscription(doc_id, record_id);
    }

    pub fn set_record(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) {
        let mut store = self.store.lock().unwrap();
        store.set_record(doc_id, record_id, fields.clone());
        self.record_access(doc_id);

        let cache_key = format!("{}:{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        cache.insert(cache_key, fields.clone());

        self.fire_subscription(doc_id, record_id);
    }

    pub fn delete_field(&self, doc_id: &str, record_id: &str, field: &str) {
        let mut store = self.store.lock().unwrap();
        store.delete_field(doc_id, record_id, field);
        self.fire_subscription(doc_id, record_id);
    }

    pub fn delete_record(&self, doc_id: &str, record_id: &str) {
        let mut store = self.store.lock().unwrap();
        store.delete_record(doc_id, record_id);
        let cache_key = format!("{}:{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        cache.remove(&cache_key);
        self.fire_subscription(doc_id, record_id);
    }

    // ── Cache operations ──

    pub fn cache_record(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) {
        let cache_key = format!("{}:{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        cache.insert(cache_key, fields);
        self.loaded_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn evict(&self, doc_id: &str, record_id: &str) {
        let cache_key = format!("{}:{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        cache.remove(&cache_key);
        self.eviction_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_stats(&self) -> (usize, usize) {
        let len = self.cache.lock().unwrap().len();
        (len, Self::CACHE_CAPACITY)
    }

    // ── Subscription operations ──

    pub fn subscribe(&self, doc_id: &str, record_id: &str, callback: js_sys::Function) -> u64 {
        static SUB_ID: AtomicU64 = AtomicU64::new(1);
        let id = SUB_ID.fetch_add(1, Ordering::Relaxed);
        let mut subs = self.subscriptions.lock().unwrap();
        subs.entry(doc_id.to_string()).or_default().push((id, record_id.to_string(), callback));
        id
    }

    pub fn unsubscribe(&self, doc_id: &str, sub_id: u64) {
        let mut subs = self.subscriptions.lock().unwrap();
        if let Some(callbacks) = subs.get_mut(doc_id) {
            callbacks.retain(|(id, _, _)| *id != sub_id);
        }
    }

    pub fn fire_subscription(&self, doc_id: &str, record_id: &str) {
        let callbacks = {
            let subs = self.subscriptions.lock().unwrap();
            subs.get(doc_id).cloned().unwrap_or_default()
        };
        if callbacks.is_empty() {
            return;
        }

        // Get current fields
        let record = {
            let store = self.store.lock().unwrap();
            store.get_record(doc_id, record_id).cloned()
        };

        if let Some(fields) = record {
            let json_str = crate::client::fields_to_json_string(&fields).unwrap_or_else(|_| "{}".to_string());
            let record_id_js = JsValue::from_str(record_id);
            let json_js = JsValue::from_str(&json_str);
            for (_id, _rec_id, cb) in &callbacks {
                let _ = cb.call2(&JsValue::NULL, &record_id_js, &json_js);
            }
        }
    }

    // ── Hot document tracking ──

    pub fn record_access(&self, doc_id: &str) {
        const DECAY_INTERVAL_MS: u64 = 60_000;
        const DECAY_FACTOR: f64 = 0.5;
        let now = js_sys::Date::now() as u64;

        let mut counts = self.access_counts.lock().unwrap();
        let (count, last_decay) = counts
            .entry(doc_id.to_string())
            .or_insert((0.0, now));

        if now - *last_decay >= DECAY_INTERVAL_MS {
            *count *= DECAY_FACTOR;
            *last_decay = now;
        }
        *count += 1.0;
    }

    pub fn hot_documents(&self, n: usize) -> Vec<String> {
        let counts = self.access_counts.lock().unwrap();
        let mut docs: Vec<(String, f64)> = counts
            .iter()
            .map(|(k, (v, _))| (k.clone(), *v))
            .collect();
        docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        docs.into_iter().take(n).map(|(k, _)| k).collect()
    }

    pub fn clear(&self) {
        let mut store = self.store.lock().unwrap();
        store.documents.clear();
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
        let mut counts = self.access_counts.lock().unwrap();
        counts.clear();
        let mut subs = self.subscriptions.lock().unwrap();
        subs.clear();
    }
}
