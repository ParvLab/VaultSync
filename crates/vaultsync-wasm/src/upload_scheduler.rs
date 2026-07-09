use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::crdt::types::CrdtValue;

/// Phase 5: Upload action type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadAction {
    Insert,
    Update,
    Delete,
}

/// Phase 5: A single pending upload entry
#[derive(Debug, Clone)]
pub struct PendingUpload {
    pub doc_id: String,
    pub record_id: String,
    pub fields: HashMap<String, CrdtValue>,
    pub action: UploadAction,
}

/// Phase 5: UploadScheduler — single entry point for mutations.
/// Debounces by 200ms, batches multiple mutations into a single flush.
///
/// Flow:
///   1. Runtime::set_field()/delete_field() calls upload_scheduler.schedule()
///   2. UploadScheduler records the mutation and the timestamp
///   3. client.rs calls upload_scheduler.prepare_flush() periodically
///   4. If debounce period has elapsed, returns all pending mutations as a batch
///   5. Client processes the batch through VaultSyncClient
#[derive(Debug)]
pub struct UploadScheduler {
    pending: Arc<Mutex<Vec<PendingUpload>>>,
    debounce_ms: u64,
    last_schedule: AtomicU64,
}

impl UploadScheduler {
    pub fn new(debounce_ms: u64) -> Self {
        Self {
            pending: Arc::new(Mutex::new(Vec::new())),
            debounce_ms,
            last_schedule: AtomicU64::new(0),
        }
    }

    /// Schedule a mutation for upload.
    pub fn schedule(&self, action: UploadAction, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) {
        self.pending.lock().unwrap().push(PendingUpload {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            fields,
            action,
        });
        self.last_schedule.store(js_sys::Date::now() as u64, Ordering::Release);
    }

    /// Schedule a delete mutation (convenience).
    pub fn schedule_delete(&self, doc_id: &str, record_id: &str) {
        self.schedule(UploadAction::Delete, doc_id, record_id, HashMap::new());
    }

    /// Check if debounce period has elapsed since last schedule.
    /// If true, flush() will return the pending batch.
    pub fn should_flush(&self) -> bool {
        let elapsed = (js_sys::Date::now() as u64)
            .saturating_sub(self.last_schedule.load(Ordering::Acquire));
        elapsed >= self.debounce_ms
    }

    /// Drain all pending uploads into a batch.
    /// Returns empty vec if debounce period hasn't elapsed yet.
    pub fn flush(&self) -> Vec<PendingUpload> {
        if !self.should_flush() {
            return Vec::new();
        }
        let mut pending = self.pending.lock().unwrap();
        pending.drain(..).collect()
    }

    /// Number of pending uploads.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Timestamp (ms since epoch) of the last schedule() call.
    pub fn last_schedule_time(&self) -> u64 {
        self.last_schedule.load(Ordering::Acquire)
    }

    /// Milliseconds remaining until next flush is allowed.
    pub fn debounce_remaining(&self) -> u64 {
        let elapsed = (js_sys::Date::now() as u64)
            .saturating_sub(self.last_schedule.load(Ordering::Acquire));
        if elapsed >= self.debounce_ms {
            0
        } else {
            self.debounce_ms - elapsed
        }
    }
}
