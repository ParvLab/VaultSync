use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::VaultSyncError;

use crate::page_store::PageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoragePriority {
    Critical = 0,
    Immediate = 1,
    High = 2,
    Normal = 3,
    Background = 4,
}

#[derive(Debug, Clone)]
pub struct StorageTransaction {
    pub ops: Vec<StorageOp>,
    pub started_at: u64,
}

impl StorageTransaction {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            started_at: js_sys::Date::now() as u64,
        }
    }

    pub fn push(&mut self, op: StorageOp) {
        self.ops.push(op);
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn elapsed_ms(&self) -> u64 {
        (js_sys::Date::now() as u64).saturating_sub(self.started_at)
    }
}

#[derive(Debug, Clone)]
pub enum StorageOp {
    WritePage {
        store: String,
        page_id: PageId,
        data: Vec<u8>,
    },
    ReadPage {
        store: String,
        page_id: PageId,
    },
    TombstonePage {
        store: String,
        page_id: PageId,
    },
    DeletePage {
        store: String,
        page_id: PageId,
    },
    AllocatePageId {
        store: String,
    },
    FlushManifest {
        store: String,
    },
    Checkpoint {
        seq: u64,
        count: usize,
    },
    Compaction {
        namespace: String,
    },
}

impl StorageOp {
    pub fn priority(&self) -> StoragePriority {
        match self {
            StorageOp::WritePage { .. } => StoragePriority::High,
            StorageOp::ReadPage { .. } => StoragePriority::Immediate,
            StorageOp::TombstonePage { .. } => StoragePriority::Normal,
            StorageOp::DeletePage { .. } => StoragePriority::Normal,
            StorageOp::AllocatePageId { .. } => StoragePriority::Immediate,
            StorageOp::FlushManifest { .. } => StoragePriority::Normal,
            StorageOp::Checkpoint { .. } => StoragePriority::Background,
            StorageOp::Compaction { .. } => StoragePriority::Background,
        }
    }

    pub fn store_name(&self) -> &str {
        match self {
            StorageOp::WritePage { store, .. } => store,
            StorageOp::ReadPage { store, .. } => store,
            StorageOp::TombstonePage { store, .. } => store,
            StorageOp::DeletePage { store, .. } => store,
            StorageOp::AllocatePageId { store, .. } => store,
            StorageOp::FlushManifest { store, .. } => store,
            StorageOp::Checkpoint { .. } => "_system",
            StorageOp::Compaction { .. } => "compaction",
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueuedOp {
    pub op: StorageOp,
    pub priority: StoragePriority,
    pub enqueued_at: u64,
    pub caller: &'static str,
}

/// StorageScheduler — serializes all storage operations through a priority queue.
///
/// In WASM (single-threaded), ops are enqueued and drained via process_pending()
/// called from the event loop. The worker pattern is simulated — there is no
/// background thread, but the queue enforces ordering and batching.
#[derive(Debug)]
pub struct StorageScheduler {
    queue: Arc<Mutex<VecDeque<QueuedOp>>>,
    processing: AtomicBool,
    state: AtomicU8,
    startup_guard: AtomicBool,
    metrics: Arc<crate::metrics::RuntimeMetrics>,
}

impl StorageScheduler {
    pub fn new(metrics: Arc<crate::metrics::RuntimeMetrics>) -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            processing: AtomicBool::new(false),
            state: AtomicU8::new(0),
            startup_guard: AtomicBool::new(true),
            metrics,
        }
    }

    /// Enqueue a storage operation with default priority.
    pub fn enqueue(&self, op: StorageOp, caller: &'static str) {
        let priority = op.priority();
        let queued = QueuedOp {
            op,
            priority,
            enqueued_at: js_sys::Date::now() as u64,
            caller,
        };
        self.queue.lock().unwrap().push_back(queued);
    }

    /// Enqueue with explicit priority.
    pub fn enqueue_with_priority(&self, op: StorageOp, priority: StoragePriority, caller: &'static str) {
        let queued = QueuedOp {
            op,
            priority,
            enqueued_at: js_sys::Date::now() as u64,
            caller,
        };
        self.queue.lock().unwrap().push_back(queued);
    }

    /// Drain all pending ops (respecting priority order) into a batch for processing.
    /// Returns the highest-priority batch. O(n) sort on each call.
    pub fn drain_pending(&self) -> Vec<QueuedOp> {
        let mut queue = self.queue.lock().unwrap();
        if queue.is_empty() {
            return Vec::new();
        }
        let mut items: Vec<QueuedOp> = queue.drain(..).collect();
        items.sort_by_key(|item| item.priority);
        items
    }

    /// Number of pending operations.
    pub fn pending_count(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// Whether ops are waiting.
    pub fn has_pending(&self) -> bool {
        !self.queue.lock().unwrap().is_empty()
    }

    /// Mark scheduler as processing (prevents re-entrant drains).
    pub fn try_begin_process(&self) -> bool {
        self.processing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// End processing.
    pub fn end_process(&self) {
        self.processing.store(false, Ordering::Release);
    }

    /// Check if scheduler is in the middle of processing.
    pub fn is_processing(&self) -> bool {
        self.processing.load(Ordering::Acquire)
    }

    /// Set/clear startup guard — blocks compaction during startup.
    pub fn set_startup_guard(&self, active: bool) {
        self.startup_guard.store(active, Ordering::Release);
    }

    pub fn is_startup_guard_active(&self) -> bool {
        self.startup_guard.load(Ordering::Acquire)
    }

    /// Scheduler state: 0=Idle, 1=Writing, 2=Compacting, 3=Checkpointing
    pub fn set_state(&self, state: u8) {
        self.state.store(state, Ordering::Release);
    }

    pub fn current_state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// Start a transaction (batch of ops to execute atomically).
    pub fn begin_transaction(&self) -> StorageTransaction {
        StorageTransaction::new()
    }

    /// Enqueue all ops from a transaction with a single priority.
    pub fn commit_transaction(&self, tx: StorageTransaction, caller: &'static str) {
        for op in tx.ops {
            self.enqueue(op, caller);
        }
    }

    /// Clear all pending ops (on shutdown).
    pub fn clear(&self) {
        self.queue.lock().unwrap().clear();
    }
}
