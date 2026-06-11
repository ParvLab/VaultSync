use crate::coordinator::traits::{Coordinator, CoordinatorError};
use crate::e2ee::keyring::E2eeDecryptor;
use crate::error::VaultSyncError;
use crate::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use crate::sync::reconciler::Reconciler;
use crate::telemetry::metrics::VaultSyncMetrics;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub batch_size: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self { batch_size: 50 }
    }
}

pub struct DownloadQueue {
    coordinator: Arc<dyn Coordinator>,
    storage: Arc<dyn crate::storage::traits::Storage>,
    namespace: String,
    last_sequence: std::sync::atomic::AtomicU64,
    config: DownloadConfig,
    reconciler: Arc<Reconciler>,
    decryptor: Arc<E2eeDecryptor>,
    metrics: Arc<VaultSyncMetrics>,
    max_clock_skew: std::time::Duration,
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
        metrics: Arc<VaultSyncMetrics>,
        max_clock_skew: std::time::Duration,
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
            max_clock_skew,
        }
    }

    pub async fn process_batch(&self) -> Result<usize, VaultSyncError> {
        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        let span = tracing::info_span!(
            "transport.receive",
            batch_size = self.config.batch_size,
            namespace = self.namespace.as_str()
        );
        let _enter = span.enter();

        match self
            .coordinator
            .pull(&self.namespace, after, self.config.batch_size)
            .await
        {
            Ok(mutations) => {
                self.metrics.set_connection_status(&self.namespace, true);
                let count = mutations.len();
                let mut entries = Vec::with_capacity(count);

                for m in &mutations {
                    let local_time = crate::time_utils::system_time_now_ms();
                    let skew = if m.timestamp > local_time {
                        m.timestamp - local_time
                    } else {
                        local_time - m.timestamp
                    };
                    if skew > self.max_clock_skew.as_millis() as u64 {
                        tracing::warn!(
                            mutation_id = m.id.as_str(),
                            timestamp = m.timestamp,
                            local_time = local_time,
                            "Rejecting mutation due to clock skew exceeding threshold"
                        );
                        continue;
                    }

                    if m.encrypted_blob.len() >= 8 {
                        let header_version =
                            u64::from_le_bytes(m.encrypted_blob[..8].try_into().unwrap());
                        if header_version != m.key_version {
                            tracing::warn!(
                                blob_version = header_version,
                                mutation_version = m.key_version,
                                "Key version mismatch in mutation payload"
                            );
                        }
                    }
                    let decrypted_bytes = self
                        .decryptor
                        .decrypt_symmetric(&m.encrypted_blob, &self.namespace)?;

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
                    entries.push(entry);
                }

                if !entries.is_empty() {
                    let merge_remote_span =
                        tracing::info_span!("crdt.merge_remote_batch", count = entries.len());
                    let _merge_guard = merge_remote_span.enter();
                    self.reconciler.apply_batch(&entries).await?;
                }

                if let Some(last) = mutations.last() {
                    let new_seq = last.sequence;
                    self.last_sequence
                        .store(new_seq, std::sync::atomic::Ordering::SeqCst);

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
                        },
                    };
                    state.last_synced_sequence = new_seq;
                    let now_ms = crate::time_utils::system_time_now_ms();
                    state.last_sync_at = Some(now_ms);
                    self.storage.write_sync_state(&state).await?;

                    // Record sync lag using last mutation's timestamp
                    let lag = now_ms.saturating_sub(last.timestamp);
                    self.metrics.record_sync_lag(lag as f64);
                    self.metrics
                        .record_download_lag(&self.namespace, lag as f64);
                }
                if count > 0 {
                    self.metrics.record_download(count);
                }
                Ok(count)
            }
            Err(CoordinatorError::NotAvailable) => {
                self.metrics.set_connection_status(&self.namespace, false);
                self.metrics.record_sync_error();
                Ok(0)
            }
            Err(e) => {
                self.metrics.set_connection_status(&self.namespace, false);
                self.metrics.record_sync_error();
                Err(VaultSyncError::Coordinator(format!(
                    "download failed: {e:?}"
                )))
            }
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn process_p2p_mutation(
        &self,
        m: crate::coordinator::traits::PendingMutation,
    ) -> Result<(), VaultSyncError> {
        let local_time = crate::time_utils::system_time_now_ms();
        let skew = if m.timestamp > local_time {
            m.timestamp - local_time
        } else {
            local_time - m.timestamp
        };
        if skew > self.max_clock_skew.as_millis() as u64 {
            return Err(VaultSyncError::ClockSkew(m.timestamp, local_time));
        }

        if m.encrypted_blob.len() >= 8 {
            let header_version = u64::from_le_bytes(m.encrypted_blob[..8].try_into().unwrap());
            if header_version != m.key_version {
                tracing::warn!(
                    blob_version = header_version,
                    mutation_version = m.key_version,
                    "Key version mismatch in P2P mutation payload"
                );
            }
        }
        let decrypted_bytes = self
            .decryptor
            .decrypt_symmetric(&m.encrypted_blob, &self.namespace)?;

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
            sequence: None,
            sync_status: SyncStatus::Synced,
            synced_at: None,
            created_at: m.timestamp,
        };

        self.reconciler.apply_remote_update(&entry).await?;
        Ok(())
    }
}
