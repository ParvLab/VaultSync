use std::sync::Arc;
use std::sync::Mutex;
use crate::error::DriftError;
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

    pub async fn apply_remote_update(&self, entry: &OplogEntry) -> Result<(), DriftError> {
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
    ) -> Result<(), DriftError> {
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

    pub async fn apply_batch(&self, entries: &[OplogEntry]) -> Result<(), DriftError> {
        for entry in entries {
            self.apply_remote_update(entry).await?;
        }
        Ok(())
    }
}
