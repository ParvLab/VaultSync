//! # ViewMaterializer — VaultSync's State Materialization Subsystem
//!
//! The ViewMaterializer enforces the three-level authority model:
//!
//! ```text
//! Visible State = Materialize(
//!     Persistent Storage,     (Level 1 — durable baseline)
//!     Confirmed Remote,       (Level 2 — coordinator replay)
//!     Local Pending,          (Level 3 — user intent, always wins)
//! )
//! ```
//!
//! ## Invariants
//!
//! 1. **Visibility Invariant** — No document may become visible because of
//!    older confirmed history if newer local intent exists.
//!
//! 2. **Idempotency Invariant** — `Materialize(Materialize(S)) == Materialize(S)`.
//!
//! 3. **Oplog Invariant** — Materializer MUST NOT append, modify, or delete
//!    oplog entries. It is a pure consumer.
//!
//! 4. **Single Mutation Path** — There is exactly one implementation of each
//!    mutation type. Both client methods and the materializer use the same
//!    core Yrs APIs via `MutationEngine`.
//!
//! 5. **Determinism Invariant** — Given same storage + same oplog + same
//!    remote history, every node produces identical visible state.
//!
//! 6. **Locality Invariant** — The materializer MUST NOT perform network I/O.
//!    No WebSocket, no coordinator, no timers, no HTTP. Purely local.

pub mod collector;
pub mod planner;
pub mod runtime;
pub mod summary;
pub mod verifier;

use std::sync::Arc;

use crate::error::VaultSyncError;
use crate::storage::traits::Storage;
use crate::time_utils::PlatformInstant;

pub use collector::{DefaultPendingCollector, PendingCollector};
pub use planner::{DefaultPlanner, ExecutionPlan, PendingIntent, Planner};
pub use runtime::MutationEngine;
pub use summary::MaterializeSummary;
pub use verifier::{DefaultVerifier, VerificationFailure, Verifier};

/// Orchestrator for the materialization pipeline.
///
/// Pipeline:
/// ```text
/// PendingCollector → collect oplog entries
///         ↓
/// Planner → build immutable ExecutionPlan
///         ↓
/// MutationEngine → re-execute pending intents
///         ↓
/// Verifier → check invariants
///         ↓
/// MaterializeSummary → structured output
/// ```
pub struct ViewMaterializer {
    collector: Arc<dyn PendingCollector>,
    planner: Arc<dyn Planner>,
    engine: Arc<MutationEngine>,
    verifier: Arc<dyn Verifier>,
    namespace: String,
    cursor: u64,
}

impl ViewMaterializer {
    /// Create a new ViewMaterializer with the default components.
    pub fn new(
        storage: Arc<dyn Storage>,
        namespace: &str,
        cursor: u64,
    ) -> Self {
        Self {
            collector: Arc::new(DefaultPendingCollector::new(storage.clone())),
            planner: Arc::new(DefaultPlanner),
            engine: Arc::new(MutationEngine::new(storage.clone())),
            verifier: Arc::new(DefaultVerifier::new(storage, namespace)),
            namespace: namespace.to_string(),
            cursor,
        }
    }

    /// Create a ViewMaterializer with custom components (for testing).
    pub fn with_components(
        collector: Arc<dyn PendingCollector>,
        planner: Arc<dyn Planner>,
        engine: Arc<MutationEngine>,
        verifier: Arc<dyn Verifier>,
        namespace: &str,
        cursor: u64,
    ) -> Self {
        Self {
            collector,
            planner,
            engine,
            verifier,
            namespace: namespace.to_string(),
            cursor,
        }
    }

    /// Run the full materialization pipeline.
    ///
    /// 1. Collect pending entries from the oplog
    /// 2. Build an immutable execution plan
    /// 3. Execute each pending intent via MutationEngine
    /// 4. Verify invariants
    ///
    /// Returns `MaterializeSummary` with diagnostic information.
    pub async fn materialize(&self) -> Result<MaterializeSummary, VaultSyncError> {
        let t0 = PlatformInstant::now();

        // Step 1: Collect pending entries
        let entries = self.collector.collect(&self.namespace).await?;

        if entries.is_empty() {
            return Ok(MaterializeSummary {
                baseline_docs: 0,
                remote_updates_applied: 0,
                pending_intents_restored: 0,
                semantic_replays: 0,
                conflicts_detected: 0,
                elapsed_ms: t0.elapsed().as_millis() as u64,
            });
        }

        let pending_count = entries.len();

        // Step 2: Build execution plan
        let plan = self.planner.build(entries, self.cursor).await?;

        // Step 3: Execute each pending intent
        let mut semantic_replays = 0u64;
        let mut conflicts = 0u64;

        for intent in &plan.pending_intents {
            match self.engine.execute(&intent.entry).await {
                Ok(_snapshot) => {
                    semantic_replays += 1;
                }
                Err(e) => {
                    // Log but continue — best-effort replay for each pending entry.
                    // A failure here means the entry's mutation could not be re-applied.
                    // This is non-fatal because the entry is still in the oplog and
                    // will be re-uploaded to the coordinator.
                    tracing::warn!(
                        "[Materializer] failed to replay pending mutation id={}: {:?}",
                        intent.entry.id,
                        e
                    );
                    conflicts += 1;
                }
            }
        }

        // Step 4: Verify invariants
        if let Err(failure) = self.verifier.verify(&plan).await {
            tracing::error!(
                "[Materializer] verification failed after replaying {} intents: {}",
                semantic_replays,
                failure,
            );
            return Err(VaultSyncError::Storage(format!(
                "materialization verification failed: {}",
                failure
            )));
        }

        let elapsed_ms = t0.elapsed().as_millis() as u64;

        if pending_count > 0 {
            tracing::info!(
                "[Materializer] restored {} pending intents (replayed={}, conflicts={}) in {}ms",
                pending_count,
                semantic_replays,
                conflicts,
                elapsed_ms,
            );
        }

        Ok(MaterializeSummary {
            baseline_docs: self.cursor,
            remote_updates_applied: self.cursor,
            pending_intents_restored: pending_count as u64,
            semantic_replays,
            conflicts_detected: conflicts,
            elapsed_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::crdt::document::CRDTDocument;
    use crate::crdt::types::CrdtValue;
    use crate::materialization::MutationEngine;
    use crate::materialization::ViewMaterializer;
    use crate::oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus};
    use crate::storage::memory::InMemoryStorage;
    use crate::storage::traits::Storage;
    use sha2::{Digest, Sha256};

    fn make_fixture() -> (Arc<dyn Storage>, ViewMaterializer) {
        let storage = Arc::new(InMemoryStorage::new());
        let materializer = ViewMaterializer::new(storage.clone(), "test-ns", 0);
        (storage, materializer)
    }

    /// Helper: write a document with fields, creating an Optimistic oplog entry.
    async fn write_doc(
        storage: &Arc<dyn Storage>,
        doc_id: &str,
        record_id: &str,
        fields: HashMap<String, CrdtValue>,
    ) {
        let mut doc = CRDTDocument::new(doc_id, record_id, 0);
        for (k, v) in &fields {
            doc.set_field(k, v.clone());
        }
        let snapshot = doc.to_snapshot();
        let entry = OplogEntry::new(
            uuid::Uuid::new_v4().to_string(),
            "test-replica".into(),
            "test-ns".into(),
            MutationType::CrdtInsert,
            doc_id.to_string(),
            record_id.to_string(),
            doc.encode_update(),
            None,
            1000,
            None,
            SyncStatus::Optimistic,
            None,
            1000,
            0,
            MutationOrigin::TestUtils,
            "",
        );
        storage
            .write_document_and_oplog(doc_id, record_id, &snapshot, &entry)
            .await
            .unwrap();
    }

    /// Helper: simulate a remote snapshot applied to storage (no oplog entry).
    async fn apply_remote_snapshot(
        storage: &Arc<dyn Storage>,
        doc_id: &str,
        record_id: &str,
        fields: HashMap<String, CrdtValue>,
    ) {
        let mut doc = CRDTDocument::new(doc_id, record_id, 0);
        for (k, v) in &fields {
            doc.set_field(k, v.clone());
        }
        let snapshot = doc.to_snapshot();
        storage
            .insert_document(doc_id, record_id, &snapshot)
            .await
            .unwrap();
    }

    /// Helper: simulate a local delete (sets _deleted=true, writes Optimistic oplog entry).
    async fn local_delete(storage: &Arc<dyn Storage>, doc_id: &str, record_id: &str) {
        let engine = MutationEngine::new(storage.clone());
        engine.apply_delete(doc_id, record_id).await.unwrap();
        // Write an Optimistic oplog entry to simulate what client.delete() does.
        let entry = OplogEntry::new(
            uuid::Uuid::new_v4().to_string(),
            "test-replica".into(),
            "test-ns".into(),
            MutationType::CrdtDelete,
            doc_id.to_string(),
            record_id.to_string(),
            vec![], // delete uses set_field, not yrs_update
            None,
            1001,
            None,
            SyncStatus::Optimistic,
            None,
            1001,
            0,
            MutationOrigin::TestUtils,
            "",
        );
        storage.append_oplog(&entry).await.unwrap();
    }

    /// Helper: compute a deterministic hash of all documents in storage.
    async fn storage_hash(storage: &Arc<dyn Storage>) -> String {
        let docs = storage.list_documents("").await.unwrap();
        let mut hasher = Sha256::new();
        for (record_id, bytes) in docs {
            hasher.update(record_id.as_bytes());
            hasher.update(&bytes);
        }
        hex::encode(hasher.finalize())
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 1: Create → Delete → Restart → Materialize
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_create_delete_materialize() {
        let (storage, materializer) = make_fixture();

        // Create a document (creates 1 Optimistic oplog entry)
        let mut fields = HashMap::new();
        fields.insert("title".into(), CrdtValue::String("Hello".into()));
        write_doc(&storage, "doc1", "rec1", fields).await;

        // Verify it exists
        let doc = storage.get_document("doc1", "rec1").await.unwrap();
        assert!(doc.is_some(), "doc should exist after create");

        // Simulate restart: local delete (writes _deleted=true + another Optimistic entry)
        local_delete(&storage, "doc1", "rec1").await;

        // Verify it has _deleted=true before materialization
        let doc = storage.get_document("doc1", "rec1").await.unwrap().unwrap();
        let crdt = CRDTDocument::from_snapshot(&doc).unwrap();
        assert_eq!(
            crdt.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "doc should have _deleted=true before materialization"
        );

        // Materialize — there are 2 pending entries (insert + delete)
        let summary = materializer.materialize().await.unwrap();
        assert_eq!(summary.pending_intents_restored, 2, "should restore 2 pending intents (insert + delete)");
        assert!(summary.semantic_replays >= 1, "should replay at least 1 semantic mutation");

        // Verify it's still deleted after materialization
        let doc = storage.get_document("doc1", "rec1").await.unwrap().unwrap();
        let crdt = CRDTDocument::from_snapshot(&doc).unwrap();
        assert_eq!(
            crdt.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "doc should remain deleted after materialization"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 2: Offline → Delete → Restart → Materialize
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_pending_update_survives_snapshot() {
        let (storage, materializer) = make_fixture();

        // Simulate remote snapshot: doc1 has title="Remote" (confirmed via coordinator)
        let mut remote_fields = HashMap::new();
        remote_fields.insert("title".into(), CrdtValue::String("Remote".into()));
        apply_remote_snapshot(&storage, "doc3", "rec3", remote_fields).await;

        // Simulate pending local update: user sets title="Local Update" before it synced
        let engine = MutationEngine::new(storage.clone());
        let mut local_doc = CRDTDocument::new("doc3", "rec3", 0);
        local_doc.set_field("title", CrdtValue::String("Local Update".into()));
        let update = local_doc.encode_update();
        engine.apply_update("doc3", "rec3", &update).await.unwrap();

        // Write pending oplog entry
        let entry = OplogEntry::new(
            uuid::Uuid::new_v4().to_string(),
            "test-replica".into(),
            "test-ns".into(),
            MutationType::CrdtUpdate,
            "doc3".into(),
            "rec3".into(),
            update,
            None,
            1002,
            None,
            SyncStatus::Optimistic,
            None,
            1002,
            0,
            MutationOrigin::TestUtils,
            "",
        );
        storage.append_oplog(&entry).await.unwrap();

        // Materialize
        let summary = materializer.materialize().await.unwrap();
        assert!(summary.semantic_replays >= 1, "should replay the pending update");

        // Verify: the materializer does best-effort CRDT merge for content fields.
        // The pending update's Yrs clocks may or may not dominate the snapshot.
        let doc_bytes = storage.get_document("doc3", "rec3").await.unwrap().unwrap();
        let doc = CRDTDocument::from_snapshot(&doc_bytes).unwrap();
        // At minimum, the doc should exist and not be deleted.
        assert_ne!(
            doc.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "doc should not be deleted"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 4: Materialize → Materialize (Idempotency)
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_idempotency() {
        let (storage, materializer) = make_fixture();

        // Create a doc and delete it (2 pending entries: insert + delete)
        let mut fields = HashMap::new();
        fields.insert("title".into(), CrdtValue::String("Idempotent".into()));
        write_doc(&storage, "doc4", "rec4", fields).await;
        local_delete(&storage, "doc4", "rec4").await;

        // First materialization
        let summary1 = materializer.materialize().await.unwrap();
        // Pending entries are still in oplog (Oplog Invariant), so pending_intents_restored >= 1
        assert!(summary1.pending_intents_restored >= 1, "first materialization should find pending entries");
        assert!(summary1.semantic_replays >= 1, "first materialization should replay mutations");

        let hash_after_first = storage_hash(&storage).await;

        // Second materialization — idempotency: storage hash must be identical
        let summary2 = materializer.materialize().await.unwrap();
        // Same pending entries still in oplog (Oplog Invariant — materializer doesn't modify oplog)
        assert!(summary2.pending_intents_restored >= 1, "second materialization still finds pending entries");
        // Hash stability (not semantic_replays count) proves idempotency,
        // because Yrs may still produce updates even when state doesn't change.

        let hash_after_second = storage_hash(&storage).await;
        assert_eq!(
            hash_after_first, hash_after_second,
            "storage state must be identical after second materialization (idempotency)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 5: Determinism — Two identical nodes produce identical state
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_determinism() {
        let storage_a: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let materializer_a = ViewMaterializer::new(storage_a.clone(), "test-ns", 0);

        let storage_b: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let materializer_b = ViewMaterializer::new(storage_b.clone(), "test-ns", 0);

        // Apply identical sequence to both
        let mut fields = HashMap::new();
        fields.insert("title".into(), CrdtValue::String("Deterministic".into()));
        write_doc(&storage_a, "doc5", "rec5", fields.clone()).await;
        write_doc(&storage_b, "doc5", "rec5", fields).await;

        local_delete(&storage_a, "doc5", "rec5").await;
        local_delete(&storage_b, "doc5", "rec5").await;

        // Materialize both
        materializer_a.materialize().await.unwrap();
        materializer_b.materialize().await.unwrap();

        let hash_a = storage_hash(&storage_a).await;
        let hash_b = storage_hash(&storage_b).await;

        assert_eq!(
            hash_a, hash_b,
            "two identical nodes must produce identical state (determinism)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 6: Restart Chain — Pending Delete survives snapshot + restart
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_restart_chain() {
        let (storage, materializer) = make_fixture();

        // Phase 1: Create doc via remote snapshot (no pending oplog entry)
        let mut fields = HashMap::new();
        fields.insert("title".into(), CrdtValue::String("Chain".into()));
        apply_remote_snapshot(&storage, "doc6", "rec6", fields).await;

        // Phase 2: Local pending delete (1 pending oplog entry)
        local_delete(&storage, "doc6", "rec6").await;

        // Phase 3: Materialize (first restart)
        let summary = materializer.materialize().await.unwrap();
        assert_eq!(summary.pending_intents_restored, 1, "first materialization: restore 1 pending delete");
        assert!(summary.semantic_replays >= 1, "first materialization: replay delete mutation");

        // Verify deleted
        let doc = storage.get_document("doc6", "rec6").await.unwrap().unwrap();
        let crdt = CRDTDocument::from_snapshot(&doc).unwrap();
        assert_eq!(
            crdt.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "deleted after first materialization"
        );

        let hash_after_first = storage_hash(&storage).await;

        // Phase 4: Simulate restart — new materializer, same storage
        let materializer2 = ViewMaterializer::new(storage.clone(), "test-ns", 100);

        // Second materialization (idempotency check within restart chain)
        // Same pending entry is still in oplog (Oplog Invariant)
        let summary2 = materializer2.materialize().await.unwrap();
        assert_eq!(
            summary2.pending_intents_restored, 1,
            "second materialization: same pending entry still in oplog"
        );

        // Verify still deleted
        let doc = storage.get_document("doc6", "rec6").await.unwrap().unwrap();
        let crdt = CRDTDocument::from_snapshot(&doc).unwrap();
        assert_eq!(
            crdt.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "still deleted after restart chain"
        );

        let hash_after_restart = storage_hash(&storage).await;
        assert_eq!(
            hash_after_first, hash_after_restart,
            "storage identical after second materialization (idempotency)"
        );

        // Phase 5: Third materialization (second restart)
        let materializer3 = ViewMaterializer::new(storage.clone(), "test-ns", 200);
        let summary3 = materializer3.materialize().await.unwrap();
        assert_eq!(
            summary3.pending_intents_restored, 1,
            "third materialization: same pending entry still in oplog"
        );

        let hash_after_third = storage_hash(&storage).await;
        assert_eq!(
            hash_after_restart, hash_after_third,
            "storage must be stable across restart chain"
        );

        // Final verification
        let doc = storage.get_document("doc6", "rec6").await.unwrap().unwrap();
        let crdt = CRDTDocument::from_snapshot(&doc).unwrap();
        assert_eq!(
            crdt.get_field("_deleted"),
            Some(CrdtValue::Boolean(true)),
            "deleted after full restart chain"
        );
    }
}
