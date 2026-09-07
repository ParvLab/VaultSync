use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;

/// A pending intent to be replayed during materialization.
///
/// Contains a reference to the original oplog entry.
/// The entry is guaranteed to have sync_status == Optimistic.
#[derive(Debug, Clone)]
pub struct PendingIntent {
    pub entry: OplogEntry,
}

/// Immutable execution plan produced by the Planner.
///
/// Once created, this plan must not be mutated.
/// It may be inspected, logged, serialized, or hashed.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Number of remote mutations already replayed (cursor value).
    pub replay_remote: u64,
    /// Pending intents to re-execute, sorted deterministically.
    pub pending_intents: Vec<PendingIntent>,
}

impl ExecutionPlan {
    pub fn is_empty(&self) -> bool {
        self.pending_intents.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.pending_intents.len()
    }
}

/// Builds an immutable ExecutionPlan from collected pending entries.
///
/// The planner is responsible for:
/// - Deterministic ordering (by created_at, then doc_id)
/// - Ensuring the plan is valid before returning it
#[async_trait::async_trait]
pub trait Planner: Send + Sync {
    async fn build(&self, entries: Vec<OplogEntry>, cursor: u64) -> Result<ExecutionPlan, VaultSyncError>;
}

/// Default planner that sorts entries deterministically.
pub struct DefaultPlanner;

#[async_trait::async_trait]
impl Planner for DefaultPlanner {
    async fn build(&self, mut entries: Vec<OplogEntry>, cursor: u64) -> Result<ExecutionPlan, VaultSyncError> {
        // Sort by created_at ASC, then doc_id ASC, then record_id ASC for determinism.
        entries.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
                .then_with(|| a.record_id.cmp(&b.record_id))
        });

        let pending_intents = entries
            .into_iter()
            .map(|entry| PendingIntent { entry })
            .collect();

        Ok(ExecutionPlan {
            replay_remote: cursor,
            pending_intents,
        })
    }
}
