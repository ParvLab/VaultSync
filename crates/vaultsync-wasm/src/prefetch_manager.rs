use crate::access_tracker::AccessTracker;
use crate::broadcast_manager::BroadcastManager;
use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime;
use std::sync::Arc;

/// Phase 4: PrefetchManager — ranks hot documents via AccessTracker and pushes HOT_DOCUMENTS.
pub struct PrefetchManager {
    tracker: std::sync::Mutex<AccessTracker>,
    runtime: Arc<Runtime>,
    broadcast: Arc<BroadcastManager>,
    metrics: Arc<RuntimeMetrics>,
    top_n: usize,
}

impl PrefetchManager {
    pub fn new(
        runtime: Arc<Runtime>,
        broadcast: Arc<BroadcastManager>,
        metrics: Arc<RuntimeMetrics>,
        top_n: usize,
    ) -> Self {
        Self {
            tracker: std::sync::Mutex::new(AccessTracker::new(60_000, 0.5)),
            runtime,
            broadcast,
            metrics,
            top_n,
        }
    }

    pub fn record_access(&self, doc_id: &str) {
        self.tracker.lock().unwrap().record_access(doc_id);
    }

    pub fn push_hot_documents(&self) {
        let hot = self.tracker.lock().unwrap().top_n(self.top_n);
        if hot.is_empty() { return; }

        let bus_gen = self.runtime.metadata_store.lock().unwrap().bus_gen;
        let doc_list = hot.join(",");
        let msg = format!("HOT_DOCUMENTS|{}|{}", bus_gen.0, doc_list);

        if self.broadcast.send_raw(&msg).is_ok() {
            self.metrics.hot_docs_pushed.fetch_add(hot.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn top_n(&self) -> Vec<String> {
        self.tracker.lock().unwrap().top_n(self.top_n)
    }
}

// Add send_raw to BroadcastManager — it's needed by PrefetchManager
impl BroadcastManager {
    pub fn send_raw(&self, msg: &str) -> Result<(), String> {
        self.channel.send(msg).map_err(|e| format!("{:?}", e))
    }
}
