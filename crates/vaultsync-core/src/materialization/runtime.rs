use std::sync::Arc;

use crate::crdt::document::CRDTDocument;
use crate::crdt::types::CrdtValue;
use crate::error::VaultSyncError;
use crate::oplog::entry::{MutationType, OplogEntry};
use crate::storage::traits::Storage;

/// Shared Yrs mutation engine.
///
/// This is the SINGLE mutation path for both:
/// - `VaultSyncClient::delete/insert/update` (via delegation)
/// - `ViewMaterializer` (pending intent replay)
///
/// Every mutation type has exactly one implementation.
/// The engine operates on storage only — it does NOT:
/// - Write oplog entries
/// - Fire subscriptions
/// - Contact the coordinator
/// - Perform network I/O
pub struct MutationEngine {
    storage: Arc<dyn Storage>,
}

impl MutationEngine {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Execute a single pending mutation from its oplog entry.
    ///
    /// Returns the complete snapshot bytes that were written to storage.
    pub async fn execute(&self, entry: &OplogEntry) -> Result<Vec<u8>, VaultSyncError> {
        match entry.mutation_type {
            MutationType::CrdtDelete => {
                self.apply_delete(&entry.doc_id, &entry.record_id).await
            }
            MutationType::CrdtUpdate => {
                self.apply_update(&entry.doc_id, &entry.record_id, &entry.yrs_update).await
            }
            MutationType::CrdtInsert => {
                self.apply_insert(&entry.doc_id, &entry.record_id, &entry.yrs_update, entry.schema_version).await
            }
            MutationType::CrdtBatch => {
                Err(VaultSyncError::Storage("batch mutations are not individually replayable".into()))
            }
        }
    }

    /// Apply a delete mutation: set `_deleted = true` on the document.
    ///
    /// Uses the same Yrs API (`doc.set_field`) as `VaultSyncClient::delete`.
    pub async fn apply_delete(&self, doc_id: &str, record_id: &str) -> Result<Vec<u8>, VaultSyncError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            // If the doc doesn't exist (e.g., deleted by another path), nothing to do.
            None => return Ok(Vec::new()),
        };

        // Same Yrs API as client.delete() — generates a FRESH clock for _deleted=true.
        doc.set_field("_deleted", CrdtValue::Boolean(true));
        let snapshot = doc.to_snapshot();

        self.storage
            .insert_document(doc_id, record_id, &snapshot)
            .await?;

        Ok(snapshot)
    }

    /// Apply an update mutation: apply the incremental Yrs update.
    pub async fn apply_update(&self, doc_id: &str, record_id: &str, yrs_update: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => return Err(VaultSyncError::Storage(format!(
                "cannot apply update to nonexistent doc {} record {}",
                doc_id, record_id
            ))),
        };

        doc.apply_update(yrs_update)?;
        let snapshot = doc.to_snapshot();

        self.storage
            .insert_document(doc_id, record_id, &snapshot)
            .await?;

        Ok(snapshot)
    }

    /// Apply an insert mutation: create the document and apply the initial Yrs update.
    pub async fn apply_insert(&self, doc_id: &str, record_id: &str, yrs_update: &[u8], schema_version: u64) -> Result<Vec<u8>, VaultSyncError> {
        // For inserts, we always create a fresh document and apply the update.
        // If the doc already exists (e.g., from remote replay), the mutation
        // is applied on top — CRDT merge handles it deterministically.
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(doc_id, record_id, schema_version),
        };

        doc.apply_update(yrs_update)?;
        let snapshot = doc.to_snapshot();

        self.storage
            .insert_document(doc_id, record_id, &snapshot)
            .await?;

        Ok(snapshot)
    }
}
