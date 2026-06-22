use crate::crdt::document::CRDTDocument;
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::storage::traits::Storage;
use crate::subscription::engine::SubscriptionEngine;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;

const DEDUP_CACHE_SIZE: usize = 10_000;

pub struct Reconciler {
    storage: Arc<dyn Storage>,
    subscriptions: Arc<Mutex<SubscriptionEngine>>,
    seen_ids: Mutex<(HashSet<String>, VecDeque<String>)>,
}

impl Reconciler {
    pub fn new(storage: Arc<dyn Storage>, subscriptions: Arc<Mutex<SubscriptionEngine>>) -> Self {
        Self {
            storage,
            subscriptions,
            seen_ids: Mutex::new((HashSet::with_capacity(DEDUP_CACHE_SIZE), VecDeque::with_capacity(DEDUP_CACHE_SIZE))),
        }
    }

    fn is_deduped(&self, id: &str) -> bool {
        self.seen_ids.lock().unwrap().0.contains(id)
    }

    fn mark_dedup(&self, id: &str) {
        let mut guard = self.seen_ids.lock().unwrap();
        guard.0.insert(id.to_string());
        guard.1.push_back(id.to_string());
        if guard.1.len() > DEDUP_CACHE_SIZE {
            if let Some(oldest) = guard.1.pop_front() {
                guard.0.remove(&oldest);
            }
        }
    }

    pub async fn apply_remote_update(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        if self.is_deduped(&entry.id) {
            tracing::debug!(mutation_id = %entry.id, "Dedup: skipping already-applied mutation");
            return Ok(());
        }
        let existing = self
            .storage
            .get_document(&entry.doc_id, &entry.record_id)
            .await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(&entry.doc_id, &entry.record_id, 0),
        };
        doc.apply_update(&entry.yrs_update)?;
        let snapshot = doc.to_snapshot();
        self.storage
            .insert_document(&entry.doc_id, &entry.record_id, &snapshot)
            .await?;
        self.mark_dedup(&entry.id);
        let state = doc.to_map();
        self.subscriptions
            .lock()
            .unwrap()
            .fire(&entry.doc_id, &entry.record_id, &state);
        Ok(())
    }

    pub async fn apply_encrypted_update(
        &self,
        entry: &OplogEntry,
        plaintext_update: &[u8],
    ) -> Result<(), VaultSyncError> {
        if self.is_deduped(&entry.id) {
            tracing::debug!(mutation_id = %entry.id, "Dedup: skipping already-applied encrypted mutation");
            return Ok(());
        }
        let existing = self
            .storage
            .get_document(&entry.doc_id, &entry.record_id)
            .await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(&entry.doc_id, &entry.record_id, 0),
        };
        doc.apply_update(plaintext_update)?;
        let snapshot = doc.to_snapshot();
        self.storage
            .insert_document(&entry.doc_id, &entry.record_id, &snapshot)
            .await?;
        self.mark_dedup(&entry.id);
        let state = doc.to_map();
        self.subscriptions
            .lock()
            .unwrap()
            .fire(&entry.doc_id, &entry.record_id, &state);
        Ok(())
    }

    pub async fn apply_batch(&self, entries: &[OplogEntry]) -> Result<(), VaultSyncError> {
        tracing::info!("[reconciler] apply_batch start count={}", entries.len());
        if entries.is_empty() {
            return Ok(());
        }

        use std::collections::HashMap;
        let mut unique_docs = HashMap::new();
        let mut uncommitted_ids: Vec<String> = Vec::new();
        for entry in entries {
            if self.is_deduped(&entry.id) {
                tracing::debug!(mutation_id = %entry.id, "Dedup: skipping already-applied batch mutation");
                continue;
            }
            let key = (entry.doc_id.clone(), entry.record_id.clone());
            unique_docs.entry(key).or_insert_with(Vec::new).push(entry);
            uncommitted_ids.push(entry.id.clone());
        }

        if unique_docs.is_empty() {
            return Ok(());
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

        tracing::info!(
            "[reconciler] write_batch_reconciliation start docs={}",
            updated_docs.len()
        );
        self.storage
            .write_batch_reconciliation(updated_docs)
            .await?;
        tracing::info!("[reconciler] write_batch_reconciliation done");

        for id in uncommitted_ids {
            self.mark_dedup(&id);
        }

        tracing::info!(
            "[reconciler] firing {} subscription notifications",
            final_states.len()
        );
        let subs = self.subscriptions.lock().unwrap();
        for (doc_id, record_id, state) in final_states {
            subs.fire(&doc_id, &record_id, &state);
        }
        tracing::info!("[reconciler] apply_batch done");

        Ok(())
    }
}
