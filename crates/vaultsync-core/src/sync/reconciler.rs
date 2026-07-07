use crate::crdt::document::CRDTDocument;
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::storage::traits::Storage;
use crate::subscription::engine::{FireSource, SubscriptionEngine};
use crate::telemetry::log_data;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::level_filters::LevelFilter;
use yrs::updates::decoder::Decode;
use yrs::Update;
use sha2::{Digest, Sha256};

const DEDUP_CACHE_SIZE: usize = 10_000;

pub struct Reconciler {
    storage: Arc<dyn Storage>,
    subscriptions: Arc<Mutex<SubscriptionEngine>>,
    seen_ids: Mutex<(HashSet<String>, VecDeque<String>)>,
    fire_suppressed: AtomicBool,
}

impl Reconciler {
    pub fn new(storage: Arc<dyn Storage>, subscriptions: Arc<Mutex<SubscriptionEngine>>) -> Self {
        Self {
            storage,
            subscriptions,
            seen_ids: Mutex::new((HashSet::with_capacity(DEDUP_CACHE_SIZE), VecDeque::with_capacity(DEDUP_CACHE_SIZE))),
            fire_suppressed: AtomicBool::new(false),
        }
    }

    pub fn set_fire_suppressed(&self, suppressed: bool) {
        self.fire_suppressed.store(suppressed, Ordering::SeqCst);
    }

    pub fn is_fire_suppressed(&self) -> bool {
        self.fire_suppressed.load(Ordering::SeqCst)
    }

    /// Fire a single notification for a doc/record after replay. Reads the current
    /// state from storage and fires through the subscription engine.
    pub async fn fire_doc(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let snapshot = self.storage.get_document(doc_id, record_id).await?;
        let doc = match snapshot {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => return Ok(()),
        };
        let state = doc.to_map();
        let mut subs = self.subscriptions.lock().unwrap();
        let listeners = subs.listener_count(doc_id);
        tracing::trace!(
            "[reconciler.fire_doc] doc={} record={} listeners={}",
            doc_id, record_id, listeners,
        );
        subs.fire(FireSource::ReconcilerPull, doc_id, record_id, &state);
        Ok(())
    }

    fn is_deduped(&self, id: &str, record_id: &str) -> bool {
        let already_seen = self.seen_ids.lock().unwrap().0.contains(id);
        tracing::trace!(
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
        let content_hash = hex::encode(&Sha256::digest(&entry.yrs_update)[..8]);
        tracing::trace!(
            "[reconciler] ENTER source={} seq={:?} doc={} record={} id={} sha256={} update_bytes={}",
            source,
            entry.sequence,
            entry.doc_id,
            entry.record_id,
            entry.id,
            content_hash,
            entry.yrs_update.len(),
        );
        if self.is_deduped(&entry.id, &entry.record_id) {
            tracing::trace!(
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
        tracing::trace!(
            "[reconciler] doc_load source={} exists={} old_len={}",
            source, doc_exists, old_len,
        );

        let old_sv = doc.state_vector();
        let old_entries: Vec<_> = old_sv.iter().map(|(c, cl)| (c, cl)).collect();

        let before_diag = log_data(LevelFilter::TRACE, || {
            let m = doc.to_map();
            (
                m.get("title").map(|v| v.to_truncated(80)).unwrap_or_default(),
                m.get("body").map(|v| v.to_truncated(80)).unwrap_or_default(),
                m.get("updatedAt").map(|v| v.to_truncated(80)).unwrap_or_default(),
            )
        });
        tracing::trace!(
            "[reconciler] doc_before source={} doc={} record={} title={} body={} updatedAt={}",
            source, entry.doc_id, entry.record_id,
            before_diag.as_ref().map(|d| d.0.as_str()).unwrap_or("-"),
            before_diag.as_ref().map(|d| d.1.as_str()).unwrap_or("-"),
            before_diag.as_ref().map(|d| d.2.as_str()).unwrap_or("-"),
        );

        if entry.yrs_update.is_empty() {
            tracing::error!(
                "[reconciler] LEN_ZERO source={} id={} doc={} record={} seq={:?}",
                source, entry.id, entry.doc_id, entry.record_id, entry.sequence,
            );
        }

        let redundant = {
            let decoded = Update::decode_v1(&entry.yrs_update);
            match decoded {
                Ok(decoded_update) => {
                    let update_sv = decoded_update.state_vector();
                    let update_entries = log_data(LevelFilter::TRACE, || {
                        update_sv.iter().map(|(c, cl)| (c, cl)).collect::<Vec<_>>()
                    });
                    tracing::trace!(
                        "[reconciler] incoming_update source={} state_vector={:?}",
                        source, update_entries,
                    );
                    update_sv.iter().all(|(client_id, clock)| {
                        old_sv.get(client_id) >= *clock
                    })
                }
                Err(_) => {
                    tracing::trace!(
                        "[reconciler] incoming_update source={} decode_failed=true",
                        source,
                    );
                    false
                }
            }
        };

        // Yrs can panic on stale updates in release wasm (panic=abort).
        let before_hash = log_data(LevelFilter::TRACE, || {
            let snap = doc.to_snapshot();
            hex::encode(&Sha256::digest(&snap)[..8])
        });

        if !redundant {
            doc.apply_update(&entry.yrs_update)?;
        } else {
            tracing::trace!(
                "[reconciler] skip_apply redundant=true source={} id={}",
                source, entry.id,
            );
        }

        let after_diag = log_data(LevelFilter::TRACE, || {
            let m = doc.to_map();
            (
                m.get("title").map(|v| v.to_truncated(80)).unwrap_or_default(),
                m.get("body").map(|v| v.to_truncated(80)).unwrap_or_default(),
                m.get("updatedAt").map(|v| v.to_truncated(80)).unwrap_or_default(),
            )
        });
        tracing::trace!(
            "[reconciler] doc_after source={} doc={} record={} title={} body={} updatedAt={}",
            source, entry.doc_id, entry.record_id,
            after_diag.as_ref().map(|d| d.0.as_str()).unwrap_or("-"),
            after_diag.as_ref().map(|d| d.1.as_str()).unwrap_or("-"),
            after_diag.as_ref().map(|d| d.2.as_str()).unwrap_or("-"),
        );

        let new_sv = doc.state_vector();
        let new_entries: Vec<_> = new_sv.iter().map(|(c, cl)| (c, cl)).collect();
        let snapshot = doc.to_snapshot();
        let after_hash = log_data(LevelFilter::TRACE, || {
            hex::encode(&Sha256::digest(&snapshot)[..8])
        });
        let doc_advanced = old_sv != new_sv;
        let doc_ptr_val = &doc as *const CRDTDocument as usize;
        let inner_doc_ptr_val = doc.inner_doc() as *const yrs::Doc as usize;

        tracing::trace!(
            "[reconciler] crt_diag source={} id={} incoming={} before={:?} after={:?} snapshot_changed={} redundant={} sv_entries_old={} sv_entries_new={} doc_advanced={} doc_ptr=0x{:x} inner_doc_ptr=0x{:x}",
            source, entry.id, content_hash, before_hash, after_hash,
            before_hash != after_hash, redundant,
            old_entries.len(), new_entries.len(), doc_advanced,
            doc_ptr_val, inner_doc_ptr_val,
        );

        if !doc_advanced && redundant && !entry.yrs_update.is_empty() {
            // Normal case: push is a duplicate of an already-applied mutation.
            // Expected during steady-state sync — the server echoes mutations
            // that were already pulled via batch or received via another path.
            tracing::debug!(
                "[reconciler] DUPLICATE_MUTATION source={} id={} doc={} record={} update_len={} redundant={} doc_advanced={} client_count_before={} client_count_after={} incoming={}",
                source, entry.id, entry.doc_id, entry.record_id, entry.yrs_update.len(),
                redundant, doc_advanced, old_entries.len(), new_entries.len(), content_hash,
            );
        }
        if !doc_advanced && !redundant && !entry.yrs_update.is_empty() {
            // Suspicious: mutation was not redundant (claimed new state)
            // yet document didn't advance. Indicates potential causal gap,
            // clock skew, or CRDT merge issue.
            tracing::warn!(
                "[reconciler] BLIND_SPOT source={} id={} doc={} record={} update_len={} redundant={} doc_advanced={} client_count_before={} client_count_after={} incoming={}",
                source, entry.id, entry.doc_id, entry.record_id, entry.yrs_update.len(),
                redundant, doc_advanced, old_entries.len(), new_entries.len(), content_hash,
            );
        }

        tracing::trace!(
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
        tracing::trace!(
            "[reconciler] snapshot_cmp source={} old_len={} new_len={} changed={}",
            source, old_len, snapshot.len(), changed,
        );
        if changed {
            if self.fire_suppressed.load(Ordering::SeqCst) {
                tracing::trace!(
                    "[reconciler.fire] SUPPRESSED source={} id={} doc={} record={}",
                    source, entry.id, entry.doc_id, entry.record_id,
                );
            } else {
                let listeners = self.subscriptions.lock().unwrap().listener_count(&entry.doc_id);
                tracing::trace!(
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
                    .fire(FireSource::ReconcilerPush, &entry.doc_id, &entry.record_id, &state);
            }
        }
        tracing::trace!(
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
        let content_hash = hex::encode(&Sha256::digest(plaintext_update)[..8]);
        tracing::trace!(
            "[reconciler] ENTER source={} encrypted doc={} record={} id={} sha256={} update_bytes={}",
            source, entry.doc_id, entry.record_id, entry.id, content_hash, plaintext_update.len(),
        );

        let encrypted_update_entries: Option<Vec<(u64, u32)>> = log_data(LevelFilter::TRACE, || {
            if let Ok(decoded) = Update::decode_v1(plaintext_update) {
                let sv = decoded.state_vector();
                let entries: Vec<_> = sv.iter().map(|(&c, &cl)| (c, cl)).collect();
                entries
            } else {
                Vec::new()
            }
        });
        tracing::trace!(
            "[reconciler] incoming_encrypted_update source={} state_vector={:?}",
            source, encrypted_update_entries,
        );

        if self.is_deduped(&entry.id, &entry.record_id) {
            tracing::trace!(
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

        let old_sv = doc.state_vector();
        let encrypted_redundant = if let Ok(decoded) = Update::decode_v1(plaintext_update) {
            let update_sv = decoded.state_vector();
            update_sv.iter().all(|(client_id, clock)| {
                old_sv.get(client_id) >= *clock
            })
        } else {
            false
        };

        if !encrypted_redundant {
            doc.apply_update(plaintext_update)?;
        } else {
            tracing::trace!(
                "[reconciler] skip_apply_enc redundant=true source={} id={}",
                source, entry.id,
            );
        }

        let snapshot = doc.to_snapshot();
        self.storage
            .insert_document(&entry.doc_id, &entry.record_id, &snapshot)
            .await?;
        self.mark_dedup(&entry.id);
        let changed = match &old_snapshot {
            Some(old) => old.as_slice() != snapshot.as_slice(),
            None => true,
        };
        tracing::trace!(
            "[reconciler] snapshot_cmp source={} encrypted old_len={} new_len={} changed={}",
            source, old_len, snapshot.len(), changed,
        );
        if changed {
            if self.fire_suppressed.load(Ordering::SeqCst) {
                tracing::trace!(
                    "[reconciler.fire] SUPPRESSED source={} encrypted id={} doc={} record={}",
                    source, entry.id, entry.doc_id, entry.record_id,
                );
            } else {
                let listeners = self.subscriptions.lock().unwrap().listener_count(&entry.doc_id);
                tracing::trace!(
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
                    .fire(FireSource::ReconcilerPush, &entry.doc_id, &entry.record_id, &state);
            }
        }
        tracing::trace!(
            "[reconciler] EXIT source={} encrypted changed={} id={}",
            source, changed, entry.id,
        );
        Ok(())
    }

    pub async fn apply_batch(&self, source: &str, entries: &[OplogEntry]) -> Result<(), VaultSyncError> {
        tracing::trace!("[reconciler] apply_batch start source={} count={}", source, entries.len());
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
            tracing::trace!(
                "[reconciler.reconcile] doc={} record={} changed={} snap_len={} mutation={} seq={}",
                doc_id, record_id, changed, snapshot.len(), mutation_id, seq,
            );
            if changed {
                let state = doc.to_map();
                final_states.push((doc_id, record_id, state));
            }
        }

        tracing::trace!(
            "[reconciler] write_batch_reconciliation start docs={}",
            updated_docs.len()
        );
        self.storage
            .write_batch_reconciliation(updated_docs)
            .await?;

        for id in uncommitted_ids {
            self.mark_dedup(&id);
        }

        tracing::trace!(
            "[reconciler.batch] docs_in_batch={} changed_count={} fire_suppressed={}",
            docs_in_batch,
            final_states.len(),
            self.fire_suppressed.load(Ordering::SeqCst),
        );
        if self.fire_suppressed.load(Ordering::SeqCst) {
            tracing::trace!(
                "[reconciler.batch] SUPPRESSED source={} changed_count={}",
                source, final_states.len(),
            );
        } else {
            let mut subs = self.subscriptions.lock().unwrap();
            for (doc_id, record_id, state) in &final_states {
                tracing::trace!(
                    "[subscription.enqueue] doc={} record={} listeners={}",
                    doc_id, record_id, subs.listener_count(doc_id),
                );
                subs.fire(FireSource::ReconcilerPull, doc_id, record_id, state);
            }
            tracing::trace!(
                "[subscription.batch] fired={}",
                final_states.len(),
            );
        }

        Ok(())
    }
}
