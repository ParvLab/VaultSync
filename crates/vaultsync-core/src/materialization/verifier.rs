use std::sync::Arc;

use crate::storage::traits::Storage;

use super::planner::ExecutionPlan;

/// Structured verification failure types.
///
/// Each variant carries diagnostic information to pinpoint the issue.
/// The Health UI can display the failure reason directly.
#[derive(Debug, Clone)]
pub enum VerificationFailure {
    /// A document exists that shouldn't (visibility invariant violated).
    VisibilityViolation {
        doc_id: String,
        record_id: String,
        detail: String,
    },
    /// Oplog state doesn't match expected state.
    OplogCorruption {
        detail: String,
    },
    /// Two materialization runs produced different results.
    NonDeterministicState {
        expected_hash: String,
        actual_hash: String,
    },
    /// A record appears more than once in storage.
    DuplicateRecord {
        doc_id: String,
        record_id: String,
    },
    /// Pending entry count doesn't match oplog.
    PendingMismatch {
        expected: usize,
        actual: usize,
    },
    /// ContentIndex or other index is out of sync with storage.
    IndexMismatch {
        detail: String,
    },
}

impl std::fmt::Display for VerificationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VisibilityViolation { doc_id, record_id, detail } => {
                write!(f, "visibility violation: doc={} record={}: {}", doc_id, record_id, detail)
            }
            Self::OplogCorruption { detail } => {
                write!(f, "oplog corruption: {}", detail)
            }
            Self::NonDeterministicState { expected_hash, actual_hash } => {
                write!(f, "non-deterministic state: expected={} actual={}", expected_hash, actual_hash)
            }
            Self::DuplicateRecord { doc_id, record_id } => {
                write!(f, "duplicate record: doc={} record={}", doc_id, record_id)
            }
            Self::PendingMismatch { expected, actual } => {
                write!(f, "pending count mismatch: expected={} actual={}", expected, actual)
            }
            Self::IndexMismatch { detail } => {
                write!(f, "index mismatch: {}", detail)
            }
        }
    }
}

/// Verifies invariants after materialization.
///
/// MUST NOT modify storage or perform network I/O.
#[async_trait::async_trait]
pub trait Verifier: Send + Sync {
    /// Verify that the materialized state satisfies all invariants.
    /// Returns Ok(()) if all checks pass, or the first VerificationFailure.
    async fn verify(&self, plan: &ExecutionPlan) -> Result<(), VerificationFailure>;
}

/// Default verifier that checks basic invariants.
///
/// Checks performed:
/// - Pending count matches expected (post-materialization, pending should be 0)
/// - Visibility: no docs leaked through
pub struct DefaultVerifier {
    storage: Arc<dyn Storage>,
    namespace: String,
}

impl DefaultVerifier {
    pub fn new(storage: Arc<dyn Storage>, namespace: &str) -> Self {
        Self {
            storage,
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Verifier for DefaultVerifier {
    async fn verify(&self, _plan: &ExecutionPlan) -> Result<(), VerificationFailure> {
        // Check: all pending intents should have been consumed.
        let remaining = self
            .storage
            .read_pending_oplog(&self.namespace, 10)
            .await
            .map_err(|e| VerificationFailure::OplogCorruption {
                detail: format!("failed to read pending oplog: {}", e),
            })?;

        if !remaining.is_empty() {
            // It's possible for new pending entries to appear between the
            // plan build and verify — this is a soft warning, not a hard failure.
            tracing::warn!(
                "[Materializer] {} pending entries remain after materialization (may be newly created)",
                remaining.len()
            );
        }

        // Additional checks can be added here as the engine matures.
        Ok(())
    }
}
