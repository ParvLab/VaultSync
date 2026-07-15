use crate::memory::SegmentedLruCache;
use crate::metrics::RuntimeMetrics;
use crate::migration::PagesDir;
use crate::scheduler::{RuntimeScheduler, Task, TaskType};
use crate::upload_scheduler::{UploadAction, UploadScheduler};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use vaultsync_core::crdt::types::CrdtValue;

/// Phase 2: Runtime generations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RuntimeGeneration(pub u64);

impl Default for RuntimeGeneration {
    fn default() -> Self { Self(0) }
}

impl std::fmt::Display for RuntimeGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Phase 2: Bus generation for gap detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusGeneration(pub u64);

impl Default for BusGeneration {
    fn default() -> Self { Self(0) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeStatus {
    Initializing,
    Ready,
    Leader,
    Follower,
    Offline,
    Recovering,
}

impl RuntimeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Leader => "Leader",
            Self::Follower => "Mirror",
            Self::Recovering => "Recovering",
            Self::Initializing => "Connecting",
            Self::Ready => "Connecting",
            Self::Offline => "Offline",
        }
    }
}

impl Default for RuntimeStatus {
    fn default() -> Self { Self::Initializing }
}

#[derive(Debug, Clone)]
pub struct FollowerRecord {
    pub tab_id: String,
    pub last_heartbeat: u64,
    pub protocol_version: u16,
    pub capabilities: Vec<String>,
}

/// Phase 2: DocumentStore — in-memory document state (source of truth)
#[derive(Debug)]
pub struct DocumentStore {
    // doc_id → record_id → field_name → CrdtValue
    pub documents: HashMap<String, HashMap<String, HashMap<String, CrdtValue>>>,
    /// Access counter for hot-document tracking (LFU with decay)
    pub access_counts: HashMap<String, (f64, u64)>, // (count, last_decay_tick)
}

impl DocumentStore {
    pub fn new() -> Self {
        Self {
            documents: HashMap::new(),
            access_counts: HashMap::new(),
        }
    }

    pub fn set_field(&mut self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        let doc = self
            .documents
            .entry(doc_id.to_string())
            .or_default();
        let rec = doc
            .entry(record_id.to_string())
            .or_default();
        rec.insert(field.to_string(), value);
    }

    pub fn delete_field(&mut self, doc_id: &str, record_id: &str, field: &str) {
        if let Some(doc) = self.documents.get_mut(doc_id) {
            if let Some(rec) = doc.get_mut(record_id) {
                rec.remove(field);
            }
        }
    }

    pub fn delete_document(&mut self, doc_id: &str, record_id: &str) {
        if let Some(doc) = self.documents.get_mut(doc_id) {
            doc.remove(record_id);
        }
    }

    pub fn get_field(&self, doc_id: &str, record_id: &str, field: &str) -> Option<&CrdtValue> {
        self.documents
            .get(doc_id)?
            .get(record_id)?
            .get(field)
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

    /// Record document access for hot-doc tracking (LFU with decay)
    pub fn record_access(&mut self, doc_id: &str) {
        const DECAY_INTERVAL_MS: u64 = 60_000;
        const DECAY_FACTOR: f64 = 0.5;
        let now = js_sys::Date::now() as u64;

        let (count, last_decay) = self
            .access_counts
            .entry(doc_id.to_string())
            .or_insert((0.0, now));

        // Apply decay if interval has passed
        if now - *last_decay >= DECAY_INTERVAL_MS {
            *count *= DECAY_FACTOR;
            *last_decay = now;
        }
        *count += 1.0;
    }

    /// Top-N hot documents by access frequency
    pub fn hot_documents(&self, n: usize) -> Vec<String> {
        let mut docs: Vec<(String, f64)> = self
            .access_counts
            .iter()
            .map(|(k, (v, _))| (k.clone(), *v))
            .collect();
        docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        docs.into_iter().take(n).map(|(k, _)| k).collect()
    }
}

/// Phase 2: MetadataStore
#[derive(Debug)]
pub struct MetadataStore {
    pub runtime_gen: RuntimeGeneration,
    pub bus_gen: BusGeneration,
    pub cursor: u64,
    pub pending_count: usize,
    pub status: RuntimeStatus,
    /// Phase 4 v2: Leader info (used by followers)
    pub leader_tab: Option<String>,
    pub leader_gen: u64,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            runtime_gen: RuntimeGeneration(0),
            bus_gen: BusGeneration(0),
            cursor: 0,
            pending_count: 0,
            status: RuntimeStatus::Initializing,
            leader_tab: None,
            leader_gen: 0,
        }
    }
}

/// Phase 2: PresenceStore
#[derive(Debug)]
pub struct PresenceStore {
    pub followers: HashMap<String, FollowerRecord>,
    pub self_tab_id: String,
}

impl PresenceStore {
    pub fn new(tab_id: String) -> Self {
        Self {
            followers: HashMap::new(),
            self_tab_id: tab_id,
        }
    }

    pub fn register_follower(&mut self, tab_id: String, version: u16, capabilities: Vec<String>) {
        self.followers.insert(
            tab_id.clone(),
            FollowerRecord {
                tab_id,
                last_heartbeat: js_sys::Date::now() as u64,
                protocol_version: version,
                capabilities,
            },
        );
    }

    pub fn update_heartbeat(&mut self, tab_id: &str) -> bool {
        if let Some(record) = self.followers.get_mut(tab_id) {
            record.last_heartbeat = js_sys::Date::now() as u64;
            true
        } else {
            false
        }
    }

    pub fn remove_follower(&mut self, tab_id: &str) {
        self.followers.remove(tab_id);
    }

    pub fn timed_out_followers(&self, timeout_ms: u64) -> Vec<String> {
        let now = js_sys::Date::now() as u64;
        self.followers
            .iter()
            .filter(|(_, r)| now - r.last_heartbeat > timeout_ms)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

/// Phase 2: PendingStore
#[derive(Debug)]
pub struct PendingEntry {
    pub seq: u64,
    pub doc_id: String,
    pub record_id: String,
}

#[derive(Debug)]
pub struct PendingStore {
    pub pending: Vec<PendingEntry>,
    pub ack_tracker: HashMap<u64, Vec<String>>, // seq → tab_ids that have ACKed
}

impl PendingStore {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            ack_tracker: HashMap::new(),
        }
    }
}

/// Phase 2: IndexStore — in-memory views of ContentIndex
#[derive(Debug)]
pub struct IndexStore {
    pub lookup_cache: SegmentedLruCache<String, u64>, // key → page_id
}

impl IndexStore {
    pub fn new() -> Self {
        Self {
            lookup_cache: SegmentedLruCache::new(5000, 0.2),
        }
    }
}

/// Phase 7: WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub seq: u64,
    pub action: String,
    pub doc_id: String,
    pub record_id: String,
    pub data: Vec<u8>,
    pub applied: bool,
}

/// Phase 7: WAL (append-only, checkpoint-based cleanup)
#[derive(Debug)]
pub struct WriteAheadLog {
    entries: Vec<WalEntry>,
    last_checkpoint: u64,
    max_entries_before_checkpoint: usize,
}

impl WriteAheadLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            last_checkpoint: 0,
            max_entries_before_checkpoint: max_entries,
        }
    }

    pub fn append(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn mark_applied(&mut self, seq: u64) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.seq == seq) {
            entry.applied = true;
        }
    }

    /// Checkpoint: remove applied entries, keep only unapplied
    pub fn checkpoint(&mut self) {
        self.entries.retain(|e| !e.applied);
        self.last_checkpoint = js_sys::Date::now() as u64;
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.entries.len() >= self.max_entries_before_checkpoint
    }

    pub fn unapplied_entries(&self) -> Vec<&WalEntry> {
        self.entries.iter().filter(|e| !e.applied).collect()
    }

    pub fn last_checkpoint_time(&self) -> u64 {
        self.last_checkpoint
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Phase 4: RuntimeSnapshot — single BC message for entire runtime state.
/// Replaces doc-by-doc SNAPSHOT_METADATA/SNAPSHOT_DOCUMENT for initial hydration.
/// NOTE: REMOVED in Phase 5 — followers read from OPFS directly.

/// Phase 2: Main Runtime struct
/// Phase 3: Runtime owns storage references — all reads go through Runtime, not OPFS directly.
pub struct Runtime {
    pub scheduler: RuntimeScheduler,
    pub metrics: RuntimeMetrics,
    pub document_store: Arc<Mutex<DocumentStore>>,
    pub metadata_store: std::sync::Mutex<MetadataStore>,
    pub presence_store: std::sync::Mutex<PresenceStore>,
    pub pending_store: std::sync::Mutex<PendingStore>,
    pub index_store: std::sync::Mutex<IndexStore>,
    pub wal: std::sync::Mutex<WriteAheadLog>,
    /// Phase 5: UploadScheduler — single entry point for uploads
    pub upload_scheduler: UploadScheduler,
    pub start_time: js_sys::Date,
}

impl Runtime {
    pub fn new(tab_id: String) -> Arc<Self> {
        let doc_store = Arc::new(Mutex::new(DocumentStore::new()));
        let runtime = Arc::new(Self {
            scheduler: RuntimeScheduler::new(),
            metrics: RuntimeMetrics::default(),
            document_store: doc_store.clone(),
            metadata_store: std::sync::Mutex::new(MetadataStore::new()),
            presence_store: std::sync::Mutex::new(PresenceStore::new(tab_id)),
            pending_store: std::sync::Mutex::new(PendingStore::new()),
            index_store: std::sync::Mutex::new(IndexStore::new()),
            wal: std::sync::Mutex::new(WriteAheadLog::new(100)),
            upload_scheduler: UploadScheduler::new(200),
            start_time: js_sys::Date::new_0(),
        });

        // Schedule periodic tasks
        runtime.scheduler.schedule(Task::new(TaskType::Heartbeat).with_budget(1));
        runtime.scheduler.schedule(Task::new(TaskType::GarbageCollect).with_deadline(60_000));

        runtime
    }

    pub fn set_field(&self, doc_id: &str, record_id: &str, field: &str, value: CrdtValue) {
        let mut store = self.document_store.lock().unwrap();
        store.set_field(doc_id, record_id, field, value.clone());
        store.record_access(doc_id);

        // WAL-first: log mutation before anything else
        let mut wal = self.wal.lock().unwrap();
        let seq = {
            let meta = self.metadata_store.lock().unwrap();
            meta.cursor + 1
        };
        wal.append(WalEntry {
            seq,
            action: "set_field".into(),
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            data: Vec::new(),
            applied: false,
        });
        if wal.needs_checkpoint() {
            wal.checkpoint();
        }
        drop(wal);

        // Update cursor
        {
            let mut meta = self.metadata_store.lock().unwrap();
            meta.cursor = seq;
            meta.pending_count += 1;
        }

        // Phase 5: Schedule via UploadScheduler (single entry point for uploads)
        let mut fields = std::collections::HashMap::new();
        fields.insert(field.to_string(), value);
        self.upload_scheduler.schedule(UploadAction::Insert, doc_id, record_id, fields);

        // Schedule upload task
        self.scheduler.schedule(Task::new(TaskType::Upload).with_budget(50));
    }

    pub fn delete_field(&self, doc_id: &str, record_id: &str, field: &str) {
        self.document_store.lock().unwrap().delete_field(doc_id, record_id, field);
        let mut wal = self.wal.lock().unwrap();
        let seq = {
            let meta = self.metadata_store.lock().unwrap();
            meta.cursor + 1
        };
        wal.append(WalEntry {
            seq,
            action: "delete_field".into(),
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            data: Vec::new(),
            applied: false,
        });
        drop(wal);
        self.metadata_store.lock().unwrap().cursor = seq;

        // Phase 5: Schedule delete via UploadScheduler
        self.upload_scheduler.schedule_delete(doc_id, record_id);
        self.scheduler.schedule(Task::new(TaskType::Upload).with_budget(50));
    }

    pub fn record_document_access(&self, doc_id: &str) {
        self.document_store.lock().unwrap().record_access(doc_id);
    }

    pub fn hot_documents(&self, n: usize) -> Vec<String> {
        self.document_store.lock().unwrap().hot_documents(n)
    }

    pub fn uptime_ms(&self) -> u64 {
        (js_sys::Date::now() - self.start_time.get_time()) as u64
    }

    pub fn is_leader(&self) -> bool {
        self.metadata_store.lock().unwrap().status == RuntimeStatus::Leader
    }

    pub fn set_status(&self, status: RuntimeStatus) {
        self.metadata_store.lock().unwrap().status = status;
    }

    pub fn current_status(&self) -> RuntimeStatus {
        self.metadata_store.lock().unwrap().status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_runtime() -> Arc<Runtime> {
        Runtime::new("test-tab".to_string())
    }

    #[test]
    fn document_store_set_and_get() {
        let mut store = DocumentStore::new();
        store.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        let val = store.get_field("doc1", "rec1", "title");
        assert!(val.is_some());
        assert_eq!(val.unwrap(), &CrdtValue::Text("hello".into()));
    }

    #[test]
    fn document_store_delete_field() {
        let mut store = DocumentStore::new();
        store.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        store.delete_field("doc1", "rec1", "title");
        assert!(store.get_field("doc1", "rec1", "title").is_none());
    }

    #[test]
    fn document_store_delete_document() {
        let mut store = DocumentStore::new();
        store.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        store.delete_document("doc1", "rec1");
        assert!(store.get_field("doc1", "rec1", "title").is_none());
    }

    #[test]
    fn document_store_multiple_fields() {
        let mut store = DocumentStore::new();
        store.set_field("doc1", "rec1", "a", CrdtValue::Integer(1));
        store.set_field("doc1", "rec1", "b", CrdtValue::Text("two".into()));
        assert_eq!(
            store.get_field("doc1", "rec1", "a"),
            Some(&CrdtValue::Integer(1))
        );
        assert_eq!(
            store.get_field("doc1", "rec1", "b"),
            Some(&CrdtValue::Text("two".into()))
        );
    }

    #[test]
    fn document_store_access_tracking() {
        let mut store = DocumentStore::new();
        store.set_field("doc1", "rec1", "x", CrdtValue::Text("val".into()));
        store.record_access("doc1");
        store.record_access("doc1");
        let counts = store.access_counts.get("doc1").unwrap();
        assert!(counts.0 >= 2.0);
    }

    #[test]
    fn wal_append_and_checkpoint() {
        let mut wal = WriteAheadLog::new(10);
        wal.append(WalEntry {
            seq: 1,
            action: "set_field".into(),
            doc_id: "doc1".into(),
            record_id: "rec1".into(),
            data: vec![],
            applied: false,
        });
        wal.append(WalEntry {
            seq: 2,
            action: "set_field".into(),
            doc_id: "doc1".into(),
            record_id: "rec2".into(),
            data: vec![],
            applied: false,
        });
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.unapplied_entries().len(), 2);

        wal.mark_applied(1);
        assert_eq!(wal.unapplied_entries().len(), 1);

        wal.checkpoint();
        assert_eq!(wal.len(), 1);
    }

    #[test]
    fn wal_needs_checkpoint() {
        let mut wal = WriteAheadLog::new(3);
        for i in 0..3 {
            wal.append(WalEntry {
                seq: i,
                action: "set_field".into(),
                doc_id: "doc1".into(),
                record_id: "rec1".into(),
                data: vec![],
                applied: false,
            });
        }
        assert!(wal.needs_checkpoint());
    }

    #[test]
    fn runtime_set_field_increments_cursor() {
        let rt = make_runtime();
        rt.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        let meta = rt.metadata_store.lock().unwrap();
        assert!(meta.cursor > 0);
    }

    #[test]
    fn runtime_delete_field_removes_value() {
        let rt = make_runtime();
        rt.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        rt.delete_field("doc1", "rec1", "title");
        let store = rt.document_store.lock().unwrap();
        assert!(store.get_field("doc1", "rec1", "title").is_none());
    }

    #[test]
    fn runtime_hot_documents() {
        let rt = make_runtime();
        rt.set_field("doc1", "rec1", "a", CrdtValue::Text("x".into()));
        rt.set_field("doc2", "rec1", "a", CrdtValue::Text("y".into()));
        rt.set_field("doc3", "rec1", "a", CrdtValue::Text("z".into()));
        // Each set_field calls record_access, so all 3 should be hot
        let hot = rt.hot_documents(10);
        assert!(hot.contains(&"doc1".to_string()));
        assert!(hot.contains(&"doc2".to_string()));
        assert!(hot.contains(&"doc3".to_string()));
    }

    #[test]
    fn runtime_status_transitions() {
        let rt = make_runtime();
        assert_eq!(rt.current_status(), RuntimeStatus::Initializing);
        rt.set_status(RuntimeStatus::Leader);
        assert_eq!(rt.current_status(), RuntimeStatus::Leader);
        assert!(rt.is_leader());
        rt.set_status(RuntimeStatus::Follower);
        assert_eq!(rt.current_status(), RuntimeStatus::Follower);
        assert!(!rt.is_leader());
    }

    #[test]
    fn index_store_creation() {
        let store = IndexStore::new();
        assert_eq!(store.lookup_cache.len(), 0);
    }

    #[test]
    fn metadata_store_defaults() {
        let ms = MetadataStore::new();
        assert_eq!(ms.runtime_gen.0, 0);
        assert_eq!(ms.bus_gen.0, 0);
        assert_eq!(ms.cursor, 0);
    }

    #[test]
    fn runtime_wal_integration() {
        let rt = make_runtime();
        rt.set_field("doc1", "rec1", "title", CrdtValue::Text("hello".into()));
        {
            let wal = rt.wal.lock().unwrap();
            assert_eq!(wal.len(), 1);
            let entry = &wal.entries[0];
            assert_eq!(entry.doc_id, "doc1");
            assert_eq!(entry.record_id, "rec1");
            assert_eq!(entry.action, "set_field");
        }
        rt.set_field("doc1", "rec1", "body", CrdtValue::Text("world".into()));
        {
            let wal = rt.wal.lock().unwrap();
            assert_eq!(wal.len(), 2);
        }
    }
}
