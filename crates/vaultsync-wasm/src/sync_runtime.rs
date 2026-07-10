use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;

use crate::follower::MirrorRuntime;
use crate::metrics::RuntimeMetrics;
use vaultsync_core::runtime_state::RuntimeLifecycle;

/// PendingIndex — tracks un-uploaded mutations by seq, doc_id, record_id.
///
/// Owned by SyncRuntime, not StorageRuntime. Mutations pending upload
/// are sync state, not storage state.
#[derive(Debug)]
pub struct PendingIndex {
    /// seq → (doc_id, record_id)
    pending: std::collections::BTreeMap<u64, (String, String)>,
    /// doc_id → set of seqs (for quick cancellation)
    by_doc: HashMap<String, HashSet<u64>>,
    /// Count of pending entries
    count: usize,
}

impl PendingIndex {
    pub fn new() -> Self {
        Self {
            pending: std::collections::BTreeMap::new(),
            by_doc: HashMap::new(),
            count: 0,
        }
    }

    /// Record a pending mutation.
    pub fn record(&mut self, seq: u64, doc_id: String, record_id: String) {
        self.pending.insert(seq, (doc_id.clone(), record_id.clone()));
        self.by_doc.entry(doc_id).or_default().insert(seq);
        self.count = self.pending.len();
    }

    /// Mark a mutation as synced (remove from pending).
    pub fn mark_synced(&mut self, seq: u64) {
        if let Some((doc_id, _)) = self.pending.remove(&seq) {
            if let Some(seqs) = self.by_doc.get_mut(&doc_id) {
                seqs.remove(&seq);
                if seqs.is_empty() {
                    self.by_doc.remove(&doc_id);
                }
            }
            self.count = self.pending.len();
        }
    }

    /// Cancel all pending mutations for a document.
    pub fn cancel_doc(&mut self, doc_id: &str) {
        if let Some(seqs) = self.by_doc.remove(doc_id) {
            for seq in &seqs {
                self.pending.remove(seq);
            }
            self.count = self.pending.len();
        }
    }

    /// Get the oldest pending mutation.
    pub fn oldest(&self) -> Option<u64> {
        self.pending.keys().next().copied()
    }

    /// Number of pending mutations.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if a specific seq is pending.
    pub fn contains(&self, seq: u64) -> bool {
        self.pending.contains_key(&seq)
    }

    /// Drain all pending entries into a vec (for retry).
    pub fn drain_all(&mut self) -> Vec<(u64, String, String)> {
        let result: Vec<_> = self.pending.iter().map(|(k, v)| (*k, v.0.clone(), v.1.clone())).collect();
        self.pending.clear();
        self.by_doc.clear();
        self.count = 0;
        result
    }

    /// Reset from a list of entries (on recovery).
    pub fn reset_from(&mut self, entries: Vec<(u64, String, String)>) {
        self.pending.clear();
        self.by_doc.clear();
        for (seq, doc_id, record_id) in entries {
            self.pending.insert(seq, (doc_id.clone(), record_id.clone()));
            self.by_doc.entry(doc_id).or_default().insert(seq);
        }
        self.count = self.pending.len();
    }
}

/// RecoveryState — tracks recovery progress for SyncRuntime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryState {
    NotStarted,
    InProgress,
    Completed,
    Failed(String),
}

impl Default for RecoveryState {
    fn default() -> Self { Self::NotStarted }
}

/// SyncRuntime — owns all synchronization state.
///
/// No storage knowledge beyond reading/writing documents through StorageRuntime.
/// Owns:
/// - cursor: last known sequence
/// - generation: coordinator generation
/// - pending_index: un-uploaded mutations
/// - coordinator: WebSocket coordinator (optional)
/// - recovery: recovery state
/// - mirror: MirrorRuntime for follower mode (optional)
#[derive(Debug)]
pub struct SyncRuntime {
    pub cursor: AtomicU64,
    pub generation: RwLock<String>,
    pub pending_index: Mutex<PendingIndex>,
    pub recovery: Mutex<RecoveryState>,
    pub mirror: Mutex<Option<Arc<MirrorRuntime>>>,
    pub coord_connected: AtomicU64,
}

impl SyncRuntime {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cursor: AtomicU64::new(0),
            generation: RwLock::new(String::new()),
            pending_index: Mutex::new(PendingIndex::new()),
            recovery: Mutex::new(RecoveryState::NotStarted),
            mirror: Mutex::new(None),
            coord_connected: AtomicU64::new(0),
        })
    }

    // ── Cursor operations ──

    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Acquire)
    }

    pub fn set_cursor(&self, seq: u64) {
        self.cursor.store(seq, Ordering::Release);
    }

    pub fn advance_cursor(&self) -> u64 {
        self.cursor.fetch_add(1, Ordering::AcqRel)
    }

    // ── Generation operations ──

    pub fn generation(&self) -> String {
        self.generation.read().unwrap().clone()
    }

    pub fn set_generation(&self, gen: String) {
        *self.generation.write().unwrap() = gen;
    }

    // ── Pending operations ──

    pub fn record_pending(&self, seq: u64, doc_id: String, record_id: String) {
        self.pending_index.lock().unwrap().record(seq, doc_id, record_id);
    }

    pub fn mark_synced(&self, seq: u64) {
        self.pending_index.lock().unwrap().mark_synced(seq);
    }

    pub fn pending_count(&self) -> usize {
        self.pending_index.lock().unwrap().count()
    }

    // ── Recovery operations ──

    pub fn set_recovery(&self, state: RecoveryState) {
        *self.recovery.lock().unwrap() = state;
    }

    pub fn recovery_state(&self) -> RecoveryState {
        self.recovery.lock().unwrap().clone()
    }

    pub fn is_recovery_complete(&self) -> bool {
        matches!(self.recovery.lock().unwrap().clone(), RecoveryState::Completed)
    }

    // ── Mirror operations ──

    pub fn set_mirror(&self, mirror: Arc<MirrorRuntime>) {
        *self.mirror.lock().unwrap() = Some(mirror);
    }

    pub fn mirror(&self) -> Option<Arc<MirrorRuntime>> {
        self.mirror.lock().unwrap().clone()
    }

    pub fn is_follower(&self) -> bool {
        self.mirror.lock().unwrap().is_some()
    }

    /// Check if coordinator is healthy (last connected within timeout).
    pub fn coordinator_alive(&self, timeout_ms: u64) -> bool {
        let last = self.coord_connected.load(Ordering::Acquire);
        last > 0 && (js_sys::Date::now() as u64).saturating_sub(last) < timeout_ms
    }

    pub fn mark_coord_connected(&self) {
        self.coord_connected.store(js_sys::Date::now() as u64, Ordering::Release);
    }
}

#[async_trait]
impl RuntimeLifecycle for SyncRuntime {
    async fn boot(&self) -> Result<(), String> { Ok(()) }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> {
        // Warm phase: pending index loaded, ready for replay
        Ok(())
    }
    async fn idle(&self) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> {
        self.pending_index.lock().unwrap().drain_all();
        Ok(())
    }
}
