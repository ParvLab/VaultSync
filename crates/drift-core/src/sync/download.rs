use std::sync::Arc;
use crate::error::DriftError;
use crate::coordinator::traits::{Coordinator, CoordinatorError};
use crate::e2ee::keyring::E2eeDecryptor;
use crate::oplog::entry::{OplogEntry, MutationType, SyncStatus};
use crate::sync::reconciler::Reconciler;
use crate::telemetry::metrics::DriftMetrics;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub batch_size: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self { Self { batch_size: 50 } }
}

pub struct DownloadQueue {
    coordinator: Arc<dyn Coordinator>,
    storage: Arc<dyn crate::storage::traits::Storage>,
    namespace: String,
    last_sequence: std::sync::atomic::AtomicU64,
    config: DownloadConfig,
    reconciler: Arc<Reconciler>,
    decryptor: Arc<E2eeDecryptor>,
    metrics: Arc<DriftMetrics>,
}

impl DownloadQueue {
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        storage: Arc<dyn crate::storage::traits::Storage>,
        namespace: &str,
        last_sequence: u64,
        config: DownloadConfig,
        reconciler: Arc<Reconciler>,
        decryptor: Arc<E2eeDecryptor>,
        metrics: Arc<DriftMetrics>,
    ) -> Self {
        Self {
            coordinator,
            storage,
            namespace: namespace.to_string(),
            last_sequence: std::sync::atomic::AtomicU64::new(last_sequence),
            config,
            reconciler,
            decryptor,
            metrics,
        }
    }

    pub async fn process_batch(&self) -> Result<usize, DriftError> {
        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        match self.coordinator.pull(&self.namespace, after, self.config.batch_size).await {
            Ok(mutations) => {
                let count = mutations.len();
                for m in &mutations {
                    let decrypted_bytes = self.decryptor.decrypt_symmetric(&m.encrypted_blob, &self.namespace)?;

                    let entry = OplogEntry {
                        id: m.id.clone(),
                        namespace: m.namespace.clone(),
                        replica_id: "".to_string(),
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

                    self.reconciler.apply_remote_update(&entry).await?;
                }

                if let Some(last) = mutations.last() {
                    let new_seq = last.sequence;
                    self.last_sequence.store(new_seq, std::sync::atomic::Ordering::SeqCst);

                    let mut state = match self.storage.read_sync_state(&self.namespace).await? {
                        Some(s) => s,
                        None => crate::sync::state::SyncState {
                            namespace: self.namespace.clone(),
                            replica_id: "".to_string(),
                            last_synced_sequence: new_seq,
                            connection_status: crate::sync::state::ConnectionStatus::Connected,
                            leader_status: Some(true),
                            last_connected_at: None,
                            last_sync_at: None,
                            schema_version: 0,
                        }
                    };
                    state.last_synced_sequence = new_seq;
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                    state.last_sync_at = Some(now_ms);
                    self.storage.write_sync_state(&state).await?;

                    // Record sync lag using last mutation's timestamp
                    let lag = now_ms.saturating_sub(last.timestamp);
                    self.metrics.record_sync_lag(lag as f64);
                }
                if count > 0 {
                    self.metrics.record_download(count);
                }
                Ok(count)
            }
            Err(CoordinatorError::NotAvailable) => {
                self.metrics.record_sync_error();
                Ok(0)
            }
            Err(e) => {
                self.metrics.record_sync_error();
                Err(DriftError::Coordinator(format!("download failed: {e:?}")))
            }
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(std::sync::atomic::Ordering::SeqCst)
    }
}
