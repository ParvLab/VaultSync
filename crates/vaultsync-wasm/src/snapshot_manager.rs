use crate::broadcast_manager::BroadcastManager;
use crate::runtime::Runtime;
use std::sync::Arc;

/// Phase 4: SnapshotManager — handles SNAPSHOT_METADATA and lazy REQUEST_DOCUMENT.
pub struct SnapshotManager {
    runtime: Arc<Runtime>,
    broadcast: Arc<BroadcastManager>,
}

impl SnapshotManager {
    pub fn new(runtime: Arc<Runtime>, broadcast: Arc<BroadcastManager>) -> Self {
        Self { runtime, broadcast }
    }

    /// Send snapshot metadata (doc IDs only, no content) to a newly attached follower.
    pub fn send_snapshot_metadata(&self) {
        self.broadcast.broadcast_snapshot_metadata();
    }

    /// Handle a REQUEST_DOCUMENT from a follower — sends single doc via BC.
    pub fn handle_request_document(&self, doc_id: &str, record_id: &str) -> Option<String> {
        let store = self.runtime.document_store.lock().unwrap();
        let fields = store.get_record(doc_id, record_id)?;
        let fields_json = serde_json::to_string(fields).ok()?;
        Some(format!("SNAPSHOT_DOCUMENT|{}|{}|{}", doc_id, record_id, fields_json))
    }
}
