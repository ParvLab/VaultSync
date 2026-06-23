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
    /// If the cursor is behind by more than this many sequences,
    /// attempt to fetch a snapshot instead of replaying all mutations.
    /// 0 = disabled.
    pub snapshot_threshold: u64,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            snapshot_threshold: 500,
        }
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
    self_replica_id: String,
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
        self_replica_id: String,
    ) -> Self {
        tracing::info!("[download_queue::new] cursor={}", last_sequence);

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
            self_replica_id,
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

        tracing::info!("[download_queue] pulling after={}", after);

        // If cursor is far behind, try snapshot-first catch-up
        if self.config.snapshot_threshold > 0 && after > 0 {
            match self.try_fetch_snapshot().await {
                Ok(true) => {
                    let new_after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
                    if new_after > after {
                        tracing::info!(
                            "[download_queue] snapshot catch-up: cursor {} -> {}",
                            after,
                            new_after
                        );
                    }
                }
                _ => {}
            }
        }

        let pull_result = self
            .coordinator
            .pull(&self.namespace, after, self.config.batch_size)
            .await;

        match pull_result {
            Ok(mutations) => {
                self.metrics.set_connection_status(&self.namespace, true);
                let count = mutations.len();

                tracing::info!("[download_queue] received {} mutations", count);
                let mut entries = Vec::with_capacity(count);

                for m in &mutations {
                    // Skip our own mutations to prevent echo loops
                    if m.replica_id == self.self_replica_id {
                        tracing::debug!(
                            mutation_id = m.id.as_str(),
                            "Skipping own mutation to prevent echo loop"
                        );
                        continue;
                    }
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

                    // Persist cursor to storage FIRST
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
                            generation_id: String::new(),
                        },
                    };
                    state.last_synced_sequence = new_seq;
                    let now_ms = crate::time_utils::system_time_now_ms();
                    state.last_sync_at = Some(now_ms);
                    self.storage.write_sync_state(&state).await?;

                    // THEN advance in-memory cursor (only after persistence succeeds)
                    self.last_sequence
                        .store(new_seq, std::sync::atomic::Ordering::SeqCst);
                    tracing::info!(
                        "[download_queue] cursor advanced to {}",
                        new_seq
                    );

                    // Record sync lag using last mutation's timestamp
                    let lag = now_ms.saturating_sub(last.timestamp);
                    self.metrics.record_sync_lag(lag as f64);
                    self.metrics
                        .record_download_lag(&self.namespace, lag as f64);
                }
                if count > 0 {
                    self.metrics.record_download(count);
                }
                tracing::info!("[download_queue] process_batch done count={}", count);
                if count == 0 && after > 0 {
                    tracing::warn!("[download_queue] cursor={} but pull returned empty", after);
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
                tracing::warn!("[download_queue] pull failed: {:?}", e);
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
        // Skip our own mutations to prevent echo loops
        if m.replica_id == self.self_replica_id {
            tracing::debug!(
                mutation_id = m.id.as_str(),
                "Skipping own P2P mutation to prevent echo loop"
            );
            return Ok(());
        }
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

    pub fn reset_cursor(&self, to: u64) {
        self.last_sequence.store(to, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(
            "[download_queue] cursor reset to {}",
            to
        );
    }

    /// Try to fetch and apply a snapshot when the cursor is far behind.
    ///
    /// Returns `true` if a snapshot was applied and cursor advanced.
    pub async fn try_fetch_snapshot(&self) -> Result<bool, VaultSyncError> {
        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        if self.config.snapshot_threshold == 0 {
            return Ok(false);
        }

        let snapshots = match self.coordinator.list_snapshots(&self.namespace).await {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };

        // Find the snapshot with the highest sequence > cursor
        let best = snapshots
            .into_iter()
            .filter(|s| s.sequence > after)
            .max_by_key(|s| s.sequence);

        match best {
            Some(snapshot) => {
                tracing::info!(
                    "[download_queue] applying snapshot doc={} record={} seq={}",
                    snapshot.doc_id,
                    snapshot.record_id,
                    snapshot.sequence
                );

                let entry = OplogEntry {
                    id: format!("snap-{}", snapshot.sequence),
                    replica_id: String::new(),
                    namespace: self.namespace.clone(),
                    mutation_type: crate::oplog::entry::MutationType::CrdtUpdate,
                    doc_id: snapshot.doc_id.clone(),
                    record_id: snapshot.record_id.clone(),
                    yrs_update: snapshot.bytes.clone(),
                    encrypted_blob: None,
                    timestamp: snapshot.created_at,
                    sequence: Some(snapshot.sequence),
                    sync_status: crate::oplog::entry::SyncStatus::Synced,
                    synced_at: None,
                    created_at: snapshot.created_at,
                };
                self.reconciler.apply_remote_update(&entry).await?;
                self.metrics.record_snapshot_applied();

                // Advance cursor past the snapshot sequence
                let mut state = match self.storage.read_sync_state(&self.namespace).await? {
                    Some(s) => s,
                    None => crate::sync::state::SyncState {
                        namespace: self.namespace.clone(),
                        replica_id: "".to_string(),
                        last_synced_sequence: snapshot.sequence,
                        connection_status: crate::sync::state::ConnectionStatus::Connected,
                        leader_status: Some(true),
                        last_connected_at: None,
                        last_sync_at: None,
                        schema_version: 0,
                        generation_id: String::new(),
                    },
                };
                state.last_synced_sequence = snapshot.sequence;
                let now_ms = crate::time_utils::system_time_now_ms();
                state.last_sync_at = Some(now_ms);
                self.storage.write_sync_state(&state).await?;
                let old = self.last_sequence.swap(snapshot.sequence, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    "[download_queue] cursor advanced via snapshot {} -> {}",
                    old,
                    snapshot.sequence
                );
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Process a single pushed mutation from the coordinator subscription
    /// or WebSocket transport. Decrypts, applies via reconciler, advances
    /// cursor, and persists state.
    pub async fn process_push_mutation(
        &self,
        m: crate::coordinator::traits::PendingMutation,
    ) -> Result<(), VaultSyncError> {
        // Skip our own mutations to prevent echo loops
        if m.replica_id == self.self_replica_id {
            tracing::debug!(
                mutation_id = m.id.as_str(),
                "Skipping own push mutation to prevent echo loop"
            );
            return Ok(());
        }
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
                    "Key version mismatch in push mutation payload"
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

        self.reconciler.apply_remote_update(&entry).await?;
        self.metrics.record_push_received();

        // Advance cursor to the pushed mutation's sequence
        let new_seq = m.sequence;
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
                generation_id: String::new(),
            },
        };
        state.last_synced_sequence = new_seq;
        let now_ms = crate::time_utils::system_time_now_ms();
        state.last_sync_at = Some(now_ms);
        self.storage.write_sync_state(&state).await?;

        // Only advance in-memory cursor after persistence succeeds
        let old = self.last_sequence.swap(new_seq, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(
            "[download_queue] push cursor advanced {} -> {} (mutation={})",
            old,
            new_seq,
            m.id
        );

        let lag = now_ms.saturating_sub(m.timestamp);
        self.metrics.record_sync_lag(lag as f64);
        self.metrics
            .record_download_lag(&self.namespace, lag as f64);

        Ok(())
    }
}
