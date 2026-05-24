use std::sync::Arc;
use crate::error::DriftError;
use crate::coordinator::traits::{Coordinator, CoordinatorError};
use crate::e2ee::keyring::E2eeDecryptor;
use crate::oplog::entry::{OplogEntry, MutationType, SyncStatus};
use crate::sync::reconciler::Reconciler;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub batch_size: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self { Self { batch_size: 50 } }
}

pub struct DownloadQueue {
    coordinator: Arc<dyn Coordinator>,
    namespace: String,
    last_sequence: std::sync::atomic::AtomicU64,
    config: DownloadConfig,
    reconciler: Arc<Reconciler>,
    decryptor: Arc<E2eeDecryptor>,
}

impl DownloadQueue {
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        namespace: &str,
        last_sequence: u64,
        config: DownloadConfig,
        reconciler: Arc<Reconciler>,
        decryptor: Arc<E2eeDecryptor>,
    ) -> Self {
        Self {
            coordinator,
            namespace: namespace.to_string(),
            last_sequence: std::sync::atomic::AtomicU64::new(last_sequence),
            config,
            reconciler,
            decryptor,
        }
    }

    pub async fn process_batch(&self) -> Result<usize, DriftError> {
        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        match self.coordinator.pull(&self.namespace, after, self.config.batch_size).await {
            Ok(mutations) => {
                let count = mutations.len();
                let namespace_pk = self.decryptor.keyring().active_key().public_key;

                for m in &mutations {
                    // Decrypt yrs_update bytes from coordinator mutation
                    let decrypted_bytes = self.decryptor.decrypt(&m.encrypted_blob, &namespace_pk)?;

                    let entry = OplogEntry {
                        id: m.id.clone(),
                        namespace: m.namespace.clone(),
                        replica_id: "".to_string(), // Coordinator doesn't expose sender replica ID in PendingMutation
                        mutation_type: MutationType::CrdtUpdate,
                        doc_id: m.doc_id.clone(),
                        record_id: m.record_id.clone(),
                        yrs_update: decrypted_bytes,
                        encrypted_blob: Some(m.encrypted_blob.clone()),
                        timestamp: m.timestamp,
                        sequence: Some(m.sequence),
                        sync_status: SyncStatus::Synced,
                        synced_at: None,
                        created_at: m.timestamp,
                    };

                    // Apply to storage and fire subscriptions
                    self.reconciler.apply_remote_update(&entry).await?;
                }

                if let Some(last) = mutations.last() {
                    self.last_sequence.store(last.sequence, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(count)
            }
            Err(CoordinatorError::NotAvailable) => Ok(0),
            Err(e) => Err(DriftError::Coordinator(format!("download failed: {e:?}"))),
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(std::sync::atomic::Ordering::SeqCst)
    }
}
