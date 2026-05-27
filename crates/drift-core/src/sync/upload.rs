use std::sync::Arc;
use crate::error::DriftError;
use crate::oplog::log::OpLog;
use crate::coordinator::traits::{Coordinator, EncryptedMutation, CoordinatorError};

use crate::telemetry::metrics::DriftMetrics;

#[derive(Debug, Clone)]
pub struct UploadConfig {
    pub batch_size: usize,
}

impl Default for UploadConfig {
    fn default() -> Self { Self { batch_size: 50 } }
}

pub struct UploadQueue {
    oplog: Arc<OpLog>,
    coordinator: Arc<dyn Coordinator>,
    config: UploadConfig,
    retry_engine: std::sync::Mutex<crate::sync::retry::RetryEngine>,
    metrics: Arc<DriftMetrics>,
}

impl UploadQueue {
    pub fn new(
        oplog: Arc<OpLog>,
        coordinator: Arc<dyn Coordinator>,
        config: UploadConfig,
        retry_config: crate::sync::retry::RetryConfig,
        metrics: Arc<DriftMetrics>,
    ) -> Self {
        Self {
            oplog,
            coordinator,
            config,
            retry_engine: std::sync::Mutex::new(crate::sync::retry::RetryEngine::new(retry_config)),
            metrics,
        }
    }

    pub async fn process_batch(&self) -> Result<usize, DriftError> {
        let raw_entries = self.oplog.read_pending(self.config.batch_size).await?;
        if raw_entries.is_empty() {
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
            return Ok(0);
        }

        let mutations: Vec<EncryptedMutation> = entries.iter().map(|e| EncryptedMutation {
            id: e.id.clone(),
            namespace: e.namespace.clone(),
            replica_id: e.replica_id.clone(),
            doc_id: e.doc_id.clone(),
            record_id: e.record_id.clone(),
            encrypted_blob: e.encrypted_blob.clone().unwrap_or_default(),
            timestamp: e.timestamp,
            schema_version: 0,
        }).collect();

        let span = tracing::info_span!("transport.send", batch_size = mutations.len(), namespace = self.oplog.namespace());
        let _enter = span.enter();

        match self.coordinator.push(&self.oplog.namespace(), mutations).await {
            Ok(sequences) => {
                self.metrics.set_connection_status(&self.oplog.namespace(), true);
                for (entry, seq) in entries.iter().zip(sequences.iter()) {
                    self.oplog.mark_synced(&entry.id, *seq).await?;
                }
                {
                    let mut engine = self.retry_engine.lock().unwrap();
                    for entry in &entries {
                        engine.record_success(&entry.id);
                    }
                }
                self.metrics.record_upload(entries.len());
                Ok(entries.len())
            }
            Err(CoordinatorError::NotAvailable) => {
                self.metrics.set_connection_status(&self.oplog.namespace(), false);
                self.metrics.record_sync_error();
                let failed_ids: Vec<String> = {
                    let mut engine = self.retry_engine.lock().unwrap();
                    let mut exhausted = Vec::new();
                    for entry in &entries {
                        if engine.record_failure(&entry.id).is_none() {
                            exhausted.push(entry.id.clone());
                        }
                    }
                    exhausted
                };
                for id in failed_ids {
                    self.oplog.mark_failed(&id, "Coordinator not available: retry limit reached").await?;
                }
                Ok(0)
            }
            Err(e) => {
                self.metrics.set_connection_status(&self.oplog.namespace(), false);
                self.metrics.record_sync_error();
                let err_msg = format!("upload failed: {e:?}");
                let failed_ids: Vec<String> = {
                    let mut engine = self.retry_engine.lock().unwrap();
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
                Err(DriftError::Coordinator(err_msg))
            }
        }
    }

    pub async fn pending_count(&self) -> Result<usize, DriftError> {
        self.oplog.count_pending().await
    }
}
