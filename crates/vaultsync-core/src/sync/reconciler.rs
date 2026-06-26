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

    fn is_deduped(&self, id: &str, record_id: &str) -> bool {
        let already_seen = self.seen_ids.lock().unwrap().0.contains(id);
        tracing::debug!(
            "[dedup] id={} record={} already_seen={}",
            id,
            record_id,
            already_seen
        );
        already_seen
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

    pub async fn apply_remote_update(&self, source: &str, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        tracing::info!(
            "[reconciler] ENTER source={} seq={:?} doc={} record={} id={} update_bytes={}",
            source,
            entry.sequence,
            entry.doc_id,
            entry.record_id,
            entry.id,
            entry.yrs_update.len(),
        );
        if self.is_deduped(&entry.id, &entry.record_id) {
            tracing::info!(
                "[reconciler] EXIT source={} dedup=true id={}",
                source, entry.id,
            );
            return Ok(());
        }
        let existing = self
            .storage
            .get_document(&entry.doc_id, &entry.record_id)
            .await?;
        let doc_exists = existing.is_some();
        let old_len = existing.as_ref().map_or(0, |b| b.len());
        let old_snapshot = existing.clone();
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(&entry.doc_id, &entry.record_id, 0),
        };
        tracing::info!(
            "[reconciler] doc_load source={} exists={} old_len={}",
            source, doc_exists, old_len,
        );
        doc.apply_update(&entry.yrs_update)?;
        let snapshot = doc.to_snapshot();
        tracing::info!(
            "[reconciler] apply_update source={} update_bytes={} new_snapshot_len={}",
            source, entry.yrs_update.len(), snapshot.len(),
        );
        self.storage
            .insert_document(&entry.doc_id, &entry.record_id, &snapshot)
            .await?;
        self.mark_dedup(&entry.id);
        let changed = match &old_snapshot {
            Some(old) => old.as_slice() != snapshot.as_slice(),
            None => true,
        };
        tracing::info!(
            "[reconciler] snapshot_cmp source={} old_len={} new_len={} changed={}",
            source, old_len, snapshot.len(), changed,
        );
        if changed {
            let listeners = self.subscriptions.lock().unwrap().listener_count(&entry.doc_id);
            tracing::info!(
                "[reconciler.fire] seq={} mutation={} doc={} record={} changed={} listeners={}",
                entry.sequence.unwrap_or(0),
                entry.id,
                entry.doc_id,
                entry.record_id,
                changed,
                listeners,
            );
            let state = doc.to_map();
            self.subscriptions
                .lock()
                .unwrap()
                .fire(&entry.doc_id, &entry.record_id, &state);
        }
        tracing::info!(
            "[reconciler] EXIT source={} changed={} id={}",
            source, changed, entry.id,
        );
        Ok(())
    }

    pub async fn apply_encrypted_update(
        &self,
        source: &str,
        entry: &OplogEntry,
        plaintext_update: &[u8],
    ) -> Result<(), VaultSyncError> {
        tracing::info!(
            "[reconciler] ENTER source={} encrypted doc={} record={} id={} update_bytes={}",
            source, entry.doc_id, entry.record_id, entry.id, plaintext_update.len(),
        );
        if self.is_deduped(&entry.id, &entry.record_id) {
            tracing::info!(
                "[reconciler] EXIT source={} dedup=true id={}",
                source, entry.id,
            );
            return Ok(());
        }
        let existing = self
            .storage
            .get_document(&entry.doc_id, &entry.record_id)
            .await?;
        let old_snapshot = existing.clone();
        let old_len = old_snapshot.as_ref().map_or(0, |b| b.len());
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
        let changed = match &old_snapshot {
            Some(old) => old.as_slice() != snapshot.as_slice(),
            None => true,
        };
        tracing::info!(
            "[reconciler] snapshot_cmp source={} encrypted old_len={} new_len={} changed={}",
            source, old_len, snapshot.len(), changed,
        );
        if changed {
            let listeners = self.subscriptions.lock().unwrap().listener_count(&entry.doc_id);
            tracing::info!(
                "[reconciler.fire] seq={} mutation={} doc={} record={} changed={} listeners={}",
                entry.sequence.unwrap_or(0),
                entry.id,
                entry.doc_id,
                entry.record_id,
                changed,
                listeners,
            );
            let state = doc.to_map();
            self.subscriptions
                .lock()
                .unwrap()
                .fire(&entry.doc_id, &entry.record_id, &state);
        }
        tracing::info!(
            "[reconciler] EXIT source={} encrypted changed={} id={}",
            source, changed, entry.id,
        );
        Ok(())
    }

    pub async fn apply_batch(&self, source: &str, entries: &[OplogEntry]) -> Result<(), VaultSyncError> {
        tracing::debug!("[reconciler] apply_batch start source={} count={}", source, entries.len());
        if entries.is_empty() {
            return Ok(());
        }

        use std::collections::HashMap;
        let mut unique_docs = HashMap::new();
        let mut uncommitted_ids: Vec<String> = Vec::new();
        for entry in entries {
            if self.is_deduped(&entry.id, &entry.record_id) {
                continue;
            }
            let key = (entry.doc_id.clone(), entry.record_id.clone());
            unique_docs.entry(key).or_insert_with(Vec::new).push(entry);
            uncommitted_ids.push(entry.id.clone());
        }

        if unique_docs.is_empty() {
            return Ok(());
        }

        let docs_in_batch = unique_docs.len();
        let mut updated_docs = Vec::with_capacity(docs_in_batch);
        let mut final_states = Vec::with_capacity(docs_in_batch);

        for ((doc_id, record_id), doc_entries) in unique_docs {
            let existing = self.storage.get_document(&doc_id, &record_id).await?;
            let old_snapshot = existing.clone();
            let mut doc = match existing {
                Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
                None => CRDTDocument::new(&doc_id, &record_id, 0),
            };

            for entry in &doc_entries {
                doc.apply_update(&entry.yrs_update)?;
            }

            let snapshot = doc.to_snapshot();
            updated_docs.push((doc_id.clone(), record_id.clone(), snapshot.clone()));

            let changed = match &old_snapshot {
                Some(old) => old.as_slice() != snapshot.as_slice(),
                None => true,
            };
            let mutation_id = doc_entries.first().map(|e| e.id.as_str()).unwrap_or("");
            let seq = doc_entries.first().map(|e| e.sequence.unwrap_or(0)).unwrap_or(0);
            tracing::info!(
                "[reconciler.reconcile] doc={} record={} changed={} snap_len={} mutation={} seq={}",
                doc_id, record_id, changed, snapshot.len(), mutation_id, seq,
            );
            if changed {
                let state = doc.to_map();
                final_states.push((doc_id, record_id, state));
            }
        }

        tracing::debug!(
            "[reconciler] write_batch_reconciliation start docs={}",
            updated_docs.len()
        );
        self.storage
            .write_batch_reconciliation(updated_docs)
            .await?;
        tracing::debug!("[reconciler] write_batch_reconciliation done");

        for id in uncommitted_ids {
            self.mark_dedup(&id);
        }

        tracing::info!(
            "[reconciler.batch] docs_in_batch={} changed_count={}",
            docs_in_batch,
            final_states.len(),
        );
        let fired_count = final_states.len();
        let mut subs = self.subscriptions.lock().unwrap();
        for (doc_id, record_id, state) in &final_states {
            tracing::info!(
                "[subscription.enqueue] doc={} record={} listeners={}",
                doc_id, record_id, subs.listener_count(doc_id),
            );
            subs.fire(doc_id, record_id, state);
        }
        tracing::info!(
            "[subscription.batch] fired={}",
            fired_count,
        );
        tracing::debug!("[reconciler] apply_batch done");

        Ok(())
    }
}
