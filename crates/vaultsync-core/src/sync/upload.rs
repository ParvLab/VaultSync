use crate::coordinator::traits::{Coordinator, CoordinatorError, EncryptedMutation};
use crate::error::VaultSyncError;
use crate::oplog::log::OpLog;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::telemetry::metrics::VaultSyncMetrics;

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
        }
    }

    pub async fn process_batch(&self) -> Result<usize, VaultSyncError> {
        let raw_entries = self.oplog.read_pending(self.config.batch_size).await?;
        let raw_count = raw_entries.len();
        if raw_entries.is_empty() {
            tracing::debug!("[upload_queue] BATCH_START read_pending=0 after_filter=0");
            return Ok(0);
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
            tracing::debug!("[upload_queue] BATCH_START read_pending={} after_filter=0 (all throttled)", raw_count);
            return Ok(0);
        }

        tracing::debug!(
            "[upload_queue] BATCH_START read_pending={} after_filter={}",
            raw_count,
            entries.len(),
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
        tracing::debug!(
            "[upload_queue] PUSH_SEND count={} ids=[{}]",
            mutations.len(),
            ids.join(","),
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
                tracing::debug!(
                    "[upload_queue] PUSH_ACK sequences={:?} elapsed={}ms",
                    sequences, push_elapsed,
                );
                self.metrics
                    .set_connection_status(&self.oplog.namespace(), true);
                for (entry, seq) in entries.iter().zip(sequences.iter()) {
                    let t0 = crate::time_utils::system_time_now_ms();
                    tracing::trace!(
                        "[upload_queue] mark_synced begin id={} seq={}",
                        entry.id, *seq,
                    );
                    self.oplog.mark_synced(&entry.id, *seq).await?;
                    tracing::trace!(
                        "[upload_queue] mark_synced done id={} elapsed={}ms",
                        entry.id,
                        crate::time_utils::system_time_now_ms() - t0,
                    );
                }
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
                tracing::debug!(
                    "[upload_queue] BATCH_DONE success={}",
                    entries.len(),
                );
                Ok(entries.len())
            }
            Err(CoordinatorError::NotAvailable) => {
                tracing::warn!(
                    "[upload_queue] PUSH_FAIL reason=NotAvailable elapsed={}ms",
                    push_elapsed,
                );
                self.metrics
                    .set_connection_status(&self.oplog.namespace(), false);
                self.metrics.record_sync_error();
                let failed_ids: Vec<String> = {
                    let t0 = crate::time_utils::system_time_now_ms();
                    tracing::trace!("[upload_queue] retry_engine notavail lock begin");
                    let mut engine = self.retry_engine.lock().unwrap();
                    tracing::trace!(
                        "[upload_queue] retry_engine notavail lock acquired elapsed={}ms",
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
                    self.oplog
                        .mark_failed(&id, "Coordinator not available: retry limit reached")
                        .await?;
                }
                Ok(0)
            }
            Err(e) => {
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
