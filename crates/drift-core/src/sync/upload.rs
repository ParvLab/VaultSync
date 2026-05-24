use std::sync::Arc;
use crate::error::DriftError;
use crate::oplog::log::OpLog;
use crate::coordinator::traits::{Coordinator, EncryptedMutation, CoordinatorError};

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
}

impl UploadQueue {
    pub fn new(oplog: Arc<OpLog>, coordinator: Arc<dyn Coordinator>, config: UploadConfig) -> Self {
        Self { oplog, coordinator, config }
    }

    pub async fn process_batch(&self) -> Result<usize, DriftError> {
        let entries = self.oplog.read_pending(self.config.batch_size).await?;
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

        match self.coordinator.push(&self.oplog.namespace(), mutations).await {
            Ok(sequences) => {
                for (entry, seq) in entries.iter().zip(sequences.iter()) {
                    self.oplog.mark_synced(&entry.id, *seq).await?;
                }
                Ok(entries.len())
            }
            Err(CoordinatorError::NotAvailable) => Ok(0),
            Err(e) => Err(DriftError::Coordinator(format!("upload failed: {e:?}"))),
        }
    }

    pub async fn pending_count(&self) -> Result<usize, DriftError> {
        self.oplog.count_pending().await
    }
}
