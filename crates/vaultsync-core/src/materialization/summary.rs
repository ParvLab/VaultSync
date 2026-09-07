use serde::Serialize;

/// Structured diagnostic output from a materialization cycle.
#[derive(Debug, Clone, Serialize)]
pub struct MaterializeSummary {
    /// Number of documents in durable storage at baseline.
    pub baseline_docs: u64,
    /// Number of remote updates applied during coordinator replay.
    pub remote_updates_applied: u64,
    /// Number of pending intents restored from the oplog.
    pub pending_intents_restored: u64,
    /// Number of semantic mutation replays executed.
    pub semantic_replays: u64,
    /// Number of conflicts detected during materialization.
    pub conflicts_detected: u64,
    /// Wall-clock time for the full materialization cycle.
    pub elapsed_ms: u64,
}

impl MaterializeSummary {
    pub fn is_empty(&self) -> bool {
        self.pending_intents_restored == 0 && self.semantic_replays == 0
    }
}
