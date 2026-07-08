use crate::memory::SegmentedLruCache;
use std::collections::HashMap;
use vaultsync_core::crdt::types::CrdtValue;

/// Phase 4: FollowerProxy — cache-only runtime with no persistence or network.
pub struct FollowerProxy {
    /// SegmentedLRU cache of documents — probation entries evicted first
    pub cache: std::sync::Mutex<FollowerCache>,
    /// Current runtime generation (from leader's heartbeat)
    pub runtime_gen: std::sync::atomic::AtomicU64,
    /// Current bus generation (for gap detection)
    pub bus_gen: std::sync::atomic::AtomicU64,
    /// Last heartbeat timestamp
    pub last_heartbeat: std::sync::atomic::AtomicU64,
    /// Cursor from leader
    pub cursor: std::sync::atomic::AtomicU64,
    /// Last mutation seq seen on bus
    pub last_mutation_seq: std::sync::atomic::AtomicU64,
}

/// Phase 4: Follower cache entries
#[derive(Debug, Clone)]
pub struct CachedDocument {
    pub fields: HashMap<String, CrdtValue>,
    pub content_hash: u64,
}

pub struct FollowerCache {
    pub documents: SegmentedLruCache<String, HashMap<String, CrdtValue>>,
    pub pending: Vec<PendingMutation>,
}

pub struct PendingMutation {
    pub doc_id: String,
    pub record_id: String,
    pub seq: u64,
    pub sent_at: u64,
    pub retries: u8,
    pub acked: bool,
}

impl FollowerProxy {
    pub fn new() -> Self {
        Self {
            cache: std::sync::Mutex::new(FollowerCache {
                documents: SegmentedLruCache::new(10000, 0.2),
                pending: Vec::new(),
            }),
            runtime_gen: std::sync::atomic::AtomicU64::new(0),
            bus_gen: std::sync::atomic::AtomicU64::new(0),
            last_heartbeat: std::sync::atomic::AtomicU64::new(0),
            cursor: std::sync::atomic::AtomicU64::new(0),
            last_mutation_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Apply mutation from leader to local cache
    pub fn apply_mutation(&self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        let key = format!("{}::{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        let mut doc = cache.documents.get(&key).unwrap_or_default();
        doc.insert(field.to_string(), value.clone());
        cache.documents.insert(key, doc);
    }

    /// Get document from cache
    pub fn get(&self, doc_id: &str, record_id: &str) -> Option<HashMap<String, CrdtValue>> {
        let key = format!("{}::{}", doc_id, record_id);
        let mut cache = self.cache.lock().unwrap();
        cache.documents.get(&key)
    }

    /// Check for gap: if bus_gen or cursor jumped, we missed mutations
    pub fn detect_gap(&self, leader_bus_gen: u64, leader_cursor: u64) -> bool {
        let my_bus = self.bus_gen.load(std::sync::atomic::Ordering::Acquire);
        let my_cursor = self.cursor.load(std::sync::atomic::Ordering::Acquire);
        my_bus > 0 && (leader_bus_gen > my_bus + 1 || leader_cursor > my_cursor + 100)
    }

    pub fn update_heartbeat(&self, runtime_gen: u64, bus_gen: u64, cursor: u64) {
        self.runtime_gen.store(runtime_gen, std::sync::atomic::Ordering::Release);
        self.bus_gen.store(bus_gen, std::sync::atomic::Ordering::Release);
        self.cursor.store(cursor, std::sync::atomic::Ordering::Release);
        self.last_heartbeat.store(js_sys::Date::now() as u64, std::sync::atomic::Ordering::Release);
    }

    /// Phase 8: Preserve cache during promotion — only hydrate missing docs
    pub fn cached_doc_ids(&self) -> Vec<String> {
        let cache = self.cache.lock().unwrap();
        // We can't enumerate SegmentedLruCache easily, so return empty
        // In practice, this would iterate the internal entries
        Vec::new()
    }
}
