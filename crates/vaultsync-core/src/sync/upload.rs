use crate::connection_manager::ConnectionManager;
use crate::coordinator::traits::{Coordinator, CoordinatorError, EncryptedMutation};
use crate::error::VaultSyncError;
use crate::oplog::log::OpLog;
use crate::sync::sync_state_store::SyncStateStore;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::telemetry::metrics::VaultSyncMetrics;

static UPLOAD_ATTEMPT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct UploadConfig {
    pub batch_size: usize,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self { batch_size: 50 }
    }
}

pub struct UploadQueue {
    oplog: Arc<OpLog>,
    coordinator: Arc<dyn Coordinator>,
    config: UploadConfig,
    retry_engine: std::sync::Mutex<crate::sync::retry::RetryEngine>,
    metrics: Arc<VaultSyncMetrics>,
    connection_manager: Mutex<Option<Arc<ConnectionManager>>>,
    sync_state_store: Option<Arc<dyn SyncStateStore>>,
}

impl UploadQueue {
    pub fn new(
        oplog: Arc<OpLog>,
        coordinator: Arc<dyn Coordinator>,
        config: UploadConfig,
        retry_config: crate::sync::retry::RetryConfig,
        metrics: Arc<VaultSyncMetrics>,
    ) -> Self {
        Self {
            oplog,
            coordinator,
            config,
            retry_engine: std::sync::Mutex::new(crate::sync::retry::RetryEngine::new(retry_config)),
            metrics,
            connection_manager: Mutex::new(None),
            sync_state_store: None,
        }
    }

    /// Attach a SyncStateStore so the upload pipeline can advance cursor after a successful push.
    pub fn with_sync_state_store(mut self, store: Arc<dyn SyncStateStore>) -> Self {
        self.sync_state_store = Some(store);
        self
    }

    pub fn set_connection_manager(&self, cm: Option<Arc<ConnectionManager>>) {
        *self.connection_manager.lock().unwrap() = cm;
    }

    pub fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        self.connection_manager.lock().unwrap().clone()
    }

    pub async fn process_batch(&self) -> Result<usize, VaultSyncError> {
        let attempt = UPLOAD_ATTEMPT.fetch_add(1, Ordering::Relaxed);
        let started = crate::time_utils::system_time_now_ms();

        // Skip if ConnectionManager reports offline — avoids NotAvailable round-trips
        if let Some(ref cm) = *self.connection_manager.lock().unwrap() {
            if !cm.is_online() {
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] EXIT attempt={} reason=cm_offline cm={:?}",
                    attempt, cm.state(),
                );
                return Ok(0);
            }
        }
        let raw_entries = self.oplog.read_pending(self.config.batch_size).await?;
        let raw_count = raw_entries.len();
        if raw_entries.is_empty() {
            let pending = self.oplog.count_pending().await.unwrap_or(0);
            if pending > 0 {
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] EXIT attempt={} reason=no_entries pending_count={}",
                    attempt, pending,
                );
            } else {
                tracing::trace!(target: "upload_trace",
                    "[UPLOAD] EXIT attempt={} reason=no_entries pending_count=0",
                    attempt,
                );
            }
            return Ok(0);
        }

        // Log entry details for diagnosis
        for e in &raw_entries {
            tracing::info!(target: "upload_trace",
                "[UPLOAD] READ attempt={} id={} type={:?} status={:?} seq={:?}",
                attempt, e.id, e.mutation_type, e.sync_status, e.sequence,
            );
        }

        // Filter out entries that cannot be retried yet based on the retry engine
        let entries: Vec<_> = {
            let engine = self.retry_engine.lock().unwrap();
            raw_entries
                .into_iter()
                .filter(|e| engine.can_retry(&e.id))
                .collect()
        };

        if entries.is_empty() {
            tracing::info!(target: "upload_trace",
                "[UPLOAD] EXIT attempt={} reason=throttled raw={}",
                attempt, raw_count,
            );
            return Ok(0);
        }

        tracing::info!(target: "upload_trace",
            "[UPLOAD] FILTER attempt={} raw={} selected={}",
            attempt, raw_count, entries.len(),
        );

        let first_ids: Vec<&str> = entries.iter().take(3).map(|e| e.id.as_str()).collect();
        let first_sha = entries
            .first()
            .map(|e| hex::encode(&Sha256::digest(&e.yrs_update)[..8]))
            .unwrap_or_default();
        tracing::info!(
            "[upload_batch] pending_before={} selected={} first_ids=[{}] sha256_first={}",
            raw_count, entries.len(), first_ids.join(","), first_sha,
        );

        for e in &entries {
            let yrs_sha256 = hex::encode(&Sha256::digest(&e.yrs_update)[..8]);
            let doc_id = &e.doc_id;
            let record_id = &e.record_id;
            tracing::trace!(
                "[upload_queue] ENTRY id={} doc={} record={} yrs_update_len={} yrs_update_sha256={}",
                e.id, doc_id, record_id, e.yrs_update.len(), yrs_sha256,
            );
        }

        let mutations: Vec<EncryptedMutation> = entries
            .iter()
            .map(|e| {
                let key_version = if let Some(ref blob) = e.encrypted_blob {
                    if blob.len() >= 8 {
                        u64::from_le_bytes(blob[..8].try_into().unwrap())
                    } else {
                        1
                    }
                } else {
                    1
                };
                EncryptedMutation {
                    id: e.id.clone(),
                    namespace: e.namespace.clone(),
                    replica_id: e.replica_id.clone(),
                    doc_id: e.doc_id.clone(),
                    record_id: e.record_id.clone(),
                    encrypted_blob: e.encrypted_blob.clone().unwrap_or_default(),
                    timestamp: e.timestamp,
                    schema_version: 0,
                    key_version,
                }
            })
            .collect();

        let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
        tracing::info!(target: "upload_trace",
            "[UPLOAD] PUSH attempt={} count={} ids=[{}]",
            attempt, entries.len(), ids.join(","),
        );
        let push_t0 = crate::time_utils::system_time_now_ms();
        let push_result = self
            .coordinator
            .push(&self.oplog.namespace(), mutations)
            .await;
        let push_elapsed = crate::time_utils::system_time_now_ms() - push_t0;
        match push_result
        {
            Ok(sequences) => {
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] ACK attempt={} sequences={:?} elapsed={}ms",
                    attempt, sequences, push_elapsed,
                );
                self.metrics
                    .set_connection_status(&self.oplog.namespace(), true);

                let before = self.oplog.count_pending().await.unwrap_or(0);
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] MARK_START attempt={} before={}",
                    attempt, before,
                );

                // Use a transaction for atomic batch marking
                match self.oplog.begin_transaction().await {
                    Ok(mut tx) => {
                        for (entry, seq) in entries.iter().zip(sequences.iter()) {
                            if let Err(e) = tx.mark_synced(&entry.id, *seq).await {
                                tracing::warn!("[upload_queue] tx.mark_synced error: {:?}", e);
                                // Fallback: mark individually
                                for (entry, seq) in entries.iter().zip(sequences.iter()) {
                                    self.oplog.mark_synced(&entry.id, *seq).await?;
                                }
                                break;
                            }
                        }
                        match tx.commit().await {
                            Ok(()) => {
                                tracing::trace!(
                                    "[upload_queue] tx.commit done count={}",
                                    entries.len(),
                                );
                            }
                            Err(e) => {
                                tracing::warn!("[upload_queue] tx.commit error: {:?}", e);
                                // Fallback: mark individually
                                for (entry, seq) in entries.iter().zip(sequences.iter()) {
                                    self.oplog.mark_synced(&entry.id, *seq).await?;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Transaction not supported (e.g., InMemory storage) — mark individually
                        for (entry, seq) in entries.iter().zip(sequences.iter()) {
                            self.oplog.mark_synced(&entry.id, *seq).await?;
                        }
                    }
                }

                let after = self.oplog.count_pending().await.unwrap_or(0);
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] MARK_DONE attempt={} before={} after={}",
                    attempt, before, after,
                );

                // Advance cursor to the max acknowledged sequence so locally-generated
                // mutations advance the cursor even without push-echo from coordinator.
                if let Some(max_seq) = sequences.iter().max().copied() {
                    if let Some(ref store) = self.sync_state_store {
                        let cursor_before = store.cursor().await.unwrap_or(0);
                        if max_seq > cursor_before {
                            store.set_cursor(max_seq, "upload::mark_done").await?;
                            tracing::info!(target: "upload_trace",
                                "[UPLOAD] CURSOR attempt={} cursor {} -> {} (max_seq={})",
                                attempt, cursor_before, max_seq, max_seq,
                            );
                        }
                    }
                }

                // VERIFY: compare count_pending vs read_pending for inconsistency detection
                let v_pending = self.oplog.count_pending().await.unwrap_or(0);
                let v_read = self.oplog.read_pending(self.config.batch_size).await.unwrap_or_default().len();
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] VERIFY attempt={} acked={} pending_count={} read_pending={}",
                    attempt, entries.len(), v_pending, v_read,
                );

                // Verify: re-read pending count to confirm mark_synced persisted
                match self.oplog.count_pending().await {
                    Ok(remaining) => {
                        tracing::debug!(
                            "[upload_queue] AFTER_MARK_SYNCED pending={} marked={}",
                            remaining, entries.len(),
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[upload_queue] AFTER_MARK_SYNCED error={:?}",
                            e,
                        );
                    }
                }
                {
                    let t0 = crate::time_utils::system_time_now_ms();
                    tracing::trace!("[upload_queue] retry_engine success lock begin");
                    let mut engine = self.retry_engine.lock().unwrap();
                    tracing::trace!(
                        "[upload_queue] retry_engine success lock acquired elapsed={}ms",
                        crate::time_utils::system_time_now_ms() - t0,
                    );
                    for entry in &entries {
                        engine.record_success(&entry.id);
                    }
                }
                self.metrics.record_upload(entries.len());
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] DONE attempt={} acked={} elapsed={}ms",
                    attempt, entries.len(),
                    crate::time_utils::system_time_now_ms() - started,
                );
                Ok(entries.len())
            }
            Err(CoordinatorError::NotAvailable) => {
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] EXIT attempt={} reason=notavail elapsed={}ms",
                    attempt, push_elapsed,
                );
                tracing::warn!(
                    "[upload_queue] PUSH_FAIL reason=NotAvailable elapsed={}ms",
                    push_elapsed,
                );
                self.metrics
                    .set_connection_status(&self.oplog.namespace(), false);
                self.metrics.record_sync_error();
                let failed_ids: Vec<(String, u32)> = {
                    let t0 = crate::time_utils::system_time_now_ms();
                    tracing::trace!("[upload_queue] retry_engine notavail lock begin");
                    let mut engine = self.retry_engine.lock().unwrap();
                    tracing::trace!(
                        "[upload_queue] retry_engine notavail lock acquired elapsed={}ms",
                        crate::time_utils::system_time_now_ms() - t0,
                    );
                    let mut exhausted = Vec::new();
                    for entry in &entries {
                        let remaining = engine.record_failure(&entry.id);
                        if remaining.is_none() {
                            exhausted.push((entry.id.clone(), engine.failure_count(&entry.id)));
                        }
                    }
                    exhausted
                };
                for (id, retry_count) in &failed_ids {
                    self.oplog
                        .mark_failed(id, &format!("Coordinator not available ({} retries exhausted)", retry_count))
                        .await?;
                }
                Ok(0)
            }
            Err(e) => {
                tracing::info!(target: "upload_trace",
                    "[UPLOAD] EXIT attempt={} reason=error err={:?} elapsed={}ms",
                    attempt, e, push_elapsed,
                );
                tracing::warn!(
                    "[upload_queue] PUSH_FAIL reason={:?} elapsed={}ms",
                    e, push_elapsed,
                );
                self.metrics
                    .set_connection_status(&self.oplog.namespace(), false);
                self.metrics.record_sync_error();
                let err_msg = format!("upload failed: {e:?}");
                let failed_ids: Vec<String> = {
                    let t0 = crate::time_utils::system_time_now_ms();
                    tracing::trace!("[upload_queue] retry_engine error lock begin");
                    let mut engine = self.retry_engine.lock().unwrap();
                    tracing::trace!(
                        "[upload_queue] retry_engine error lock acquired elapsed={}ms",
                        crate::time_utils::system_time_now_ms() - t0,
                    );
                    let mut exhausted = Vec::new();
                    for entry in &entries {
                        if engine.record_failure(&entry.id).is_none() {
                            exhausted.push(entry.id.clone());
                        }
                    }
                    exhausted
                };
                for id in failed_ids {
                    self.oplog.mark_failed(&id, &err_msg).await?;
                }
                Err(VaultSyncError::Coordinator(err_msg))
            }
        }
    }

    pub async fn pending_count(&self) -> Result<usize, VaultSyncError> {
        self.oplog.count_pending().await
    }
}
