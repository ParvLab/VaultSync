use std::sync::Arc;
use std::sync::Mutex;
use crate::error::VaultSyncError;
use crate::crdt::document::CRDTDocument;
use crate::oplog::entry::OplogEntry;
use crate::storage::traits::Storage;
use crate::subscription::engine::SubscriptionEngine;

pub struct Reconciler {
    storage: Arc<dyn Storage>,
    subscriptions: Arc<Mutex<SubscriptionEngine>>,
}

impl Reconciler {
    pub fn new(storage: Arc<dyn Storage>, subscriptions: Arc<Mutex<SubscriptionEngine>>) -> Self {
        Self { storage, subscriptions }
    }

    pub async fn apply_remote_update(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let existing = self.storage.get_document(&entry.doc_id, &entry.record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(&entry.doc_id, &entry.record_id, 0),
        };
        doc.apply_update(&entry.yrs_update)?;
        let snapshot = doc.to_snapshot();
        self.storage.insert_document(&entry.doc_id, &entry.record_id, &snapshot).await?;
        let state = doc.to_map();
        self.subscriptions.lock().unwrap().fire(&entry.doc_id, &entry.record_id, &state);
        Ok(())
    }

    pub async fn apply_encrypted_update(
        &self,
        entry: &OplogEntry,
        plaintext_update: &[u8],
    ) -> Result<(), VaultSyncError> {
        let existing = self.storage.get_document(&entry.doc_id, &entry.record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(&entry.doc_id, &entry.record_id, 0),
        };
        doc.apply_update(plaintext_update)?;
        let snapshot = doc.to_snapshot();
        self.storage.insert_document(&entry.doc_id, &entry.record_id, &snapshot).await?;
        let state = doc.to_map();
        self.subscriptions.lock().unwrap().fire(&entry.doc_id, &entry.record_id, &state);
        Ok(())
    }

    pub async fn apply_batch(&self, entries: &[OplogEntry]) -> Result<(), VaultSyncError> {
        if entries.is_empty() {
            return Ok(());
        }

        use std::collections::HashMap;
        let mut unique_docs = HashMap::new();
        for entry in entries {
            let key = (entry.doc_id.clone(), entry.record_id.clone());
            unique_docs.entry(key).or_insert_with(Vec::new).push(entry);
        }

        let mut updated_docs = Vec::with_capacity(unique_docs.len());
        let mut final_states = Vec::with_capacity(unique_docs.len());

        for ((doc_id, record_id), doc_entries) in unique_docs {
            let existing = self.storage.get_document(&doc_id, &record_id).await?;
            let mut doc = match existing {
                Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
                None => CRDTDocument::new(&doc_id, &record_id, 0),
            };

            for entry in &doc_entries {
                doc.apply_update(&entry.yrs_update)?;
            }

            let snapshot = doc.to_snapshot();
            updated_docs.push((doc_id.clone(), record_id.clone(), snapshot));

            let state = doc.to_map();
            final_states.push((doc_id, record_id, state));
        }

        self.storage.write_batch_reconciliation(updated_docs).await?;

        let subs = self.subscriptions.lock().unwrap();
        for (doc_id, record_id, state) in final_states {
            subs.fire(&doc_id, &record_id, &state);
        }

        Ok(())
    }
}
