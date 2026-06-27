use crate::coordinator::traits::{Coordinator, CoordinatorError};
use crate::e2ee::keyring::E2eeDecryptor;
use crate::error::VaultSyncError;
use crate::oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus};
use crate::sync::reconciler::Reconciler;
use yrs::updates::decoder::Decode;
use yrs::Update;
use sha2::{Digest, Sha256};
use crate::telemetry::metrics::VaultSyncMetrics;
use std::sync::Arc;

/// Result of processing a single push mutation.
/// Used by the download worker to decide whether to backfill via pull.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// Push sequence was exactly cursor + 1 (no gap). No pull needed.
    Contiguous,
    /// Push sequence skipped ahead (gap detected). Pull recommended.
    GapDetected,
    /// Push was stale (seq <= cursor) or an own-mutation echo. No pull needed.
    Stale,
}

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
        tracing::trace!("[download_queue::new] cursor={}", last_sequence);

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
        let span = tracing::info_span!(
            "transport.receive",
            batch_size = self.config.batch_size,
            namespace = self.namespace.as_str()
        );
        let _enter = span.enter();

        // ── Generation check: if server restarted, reset cursor ──
        self.check_generation().await?;

        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        tracing::trace!("[download_queue] pulling after={}", after);

        // If cursor is far behind, try snapshot-first catch-up
        if self.config.snapshot_threshold > 0 && after > 0 {
            match self.try_fetch_snapshot().await {
                Ok(true) => {
                    let new_after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
                    if new_after > after {
                        tracing::debug!(
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

                tracing::trace!("[download_queue] received {} mutations", count);
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

                    // No clock skew check for pull mutations — coordinator already validated them
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

        // ── Phase 2 diagnostic: state vector + hash of decrypted push payload ──
        let content_hash = hex::encode(&Sha256::digest(&decrypted_bytes)[..8]);
        if let Ok(decoded) = Update::decode_v1(&decrypted_bytes) {
            let sv = decoded.state_vector();
            let entries: Vec<_> = sv.iter().map(|(c, cl)| (c, cl)).collect();
            tracing::info!(
                "[download_queue] push_decrypted source=push seq={} sha256={} state_vector={:?} len={}",
                m.sequence, content_hash, entries, decrypted_bytes.len(),
            );
        }

        let entry = OplogEntry::new(
                        m.id.clone(),
                        "".to_string(),
                        m.namespace.clone(),
                        MutationType::CrdtUpdate,
                        m.doc_id.clone(),
                        m.record_id.clone(),
                        decrypted_bytes,
                        Some(m.encrypted_blob.clone()),
                        m.timestamp,
                        Some(m.sequence),
                        SyncStatus::Synced,
                        None,
                        m.timestamp,
                        MutationOrigin::RemotePull,
                        "",
                    );
                    entries.push(entry);
                }

                if !entries.is_empty() {
                    let merge_remote_span =
                        tracing::info_span!("crdt.merge_remote_batch", count = entries.len());
                    let _merge_guard = merge_remote_span.enter();
                    self.reconciler.apply_batch("pull", &entries).await?;
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
                    tracing::debug!(
                        "[download_queue] write_sync_state cursor={} gen={} (from process_batch pull)",
                        state.last_synced_sequence,
                        state.generation_id
                    );

                    // THEN advance in-memory cursor (only after persistence succeeds)
                    self.last_sequence
                        .store(new_seq, std::sync::atomic::Ordering::SeqCst);
                    tracing::trace!(
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
                tracing::debug!(
                    "[download_queue] process_batch done count={} t={}",
                    count,
                    crate::time_utils::system_time_now_ms(),
                );
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

        let entry = OplogEntry::new(
            m.id.clone(),
            "".to_string(),
            m.namespace.clone(),
            MutationType::CrdtUpdate,
            m.doc_id.clone(),
            m.record_id.clone(),
            decrypted_bytes,
            Some(m.encrypted_blob.clone()),
            m.timestamp,
            None,
            SyncStatus::Synced,
            None,
            m.timestamp,
            MutationOrigin::P2PBroadcast,
            "",
        );

        self.reconciler.apply_remote_update("p2p", &entry).await?;
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

                let entry = OplogEntry::new(
                    format!("snap-{}", snapshot.sequence),
                    String::new(),
                    self.namespace.clone(),
                    MutationType::CrdtUpdate,
                    snapshot.doc_id.clone(),
                    snapshot.record_id.clone(),
                    snapshot.bytes.clone(),
                    None,
                    snapshot.created_at,
                    Some(snapshot.sequence),
                    SyncStatus::Synced,
                    None,
                    snapshot.created_at,
                    MutationOrigin::Snapshot,
                    "",
                );
                self.reconciler.apply_remote_update("snapshot", &entry).await?;
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
                tracing::debug!(
                    "[download_queue] write_sync_state cursor={} gen={} (from snapshot)",
                    state.last_synced_sequence,
                    state.generation_id
                );
                let old = self.last_sequence.swap(snapshot.sequence, std::sync::atomic::Ordering::SeqCst);
                tracing::trace!(
                    "[download_queue] cursor advanced via snapshot {} -> {}",
                    old,
                    snapshot.sequence
                );
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Verify server generation matches local state. If mismatch is detected, log
    /// but do NOT reset cursor — that responsibility belongs to `initialize()`.
    /// This is a defense-in-depth verification layer only.
    async fn check_generation(&self) -> Result<(), VaultSyncError> {
        let server_gen = self.coordinator.generation_id().await;
        if server_gen.is_empty() {
            return Ok(());
        }
        if let Some(state) = self.storage.read_sync_state(&self.namespace).await? {
            if state.generation_id != server_gen {
                tracing::warn!(
                    "[download_queue] generation mismatch: local={} server={} (verify only, cursor NOT reset here)",
                    state.generation_id,
                    server_gen
                );
            }
        }
        Ok(())
    }

    /// Process a single pushed mutation from the coordinator subscription
    /// or WebSocket transport. Decrypts, applies via reconciler, advances
    /// cursor, and persists state.
    ///
    /// Returns [`PushOutcome::Contiguous`] when no pull is needed (push filled
    /// the next expected slot), or [`PushOutcome::GapDetected`] when a backfill
    /// pull is recommended.
    pub async fn process_push_mutation(
        &self,
        m: crate::coordinator::traits::PendingMutation,
    ) -> Result<PushOutcome, VaultSyncError> {
        // Skip our own mutations to prevent echo loops
        if m.replica_id == self.self_replica_id {
            tracing::debug!(
                mutation_id = m.id.as_str(),
                "Skipping own push mutation to prevent echo loop"
            );
            return Ok(PushOutcome::Stale);
        }

        // No clock skew check for push mutations — coordinator already validated them
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

        let entry = OplogEntry::new(
            m.id.clone(),
            "".to_string(),
            m.namespace.clone(),
            MutationType::CrdtUpdate,
            m.doc_id.clone(),
            m.record_id.clone(),
            decrypted_bytes,
            Some(m.encrypted_blob.clone()),
            m.timestamp,
            Some(m.sequence),
            SyncStatus::Synced,
            None,
            m.timestamp,
            MutationOrigin::PushMutation,
            "",
        );

        // ── Cursor gate: skip if already processed (seq <= last_sequence) ──
        let cursor_before = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        tracing::info!(
            "[download_queue] push seq={} cursor={} stale={} id={}",
            m.sequence, cursor_before, m.sequence <= cursor_before, m.id,
        );
        if m.sequence <= cursor_before {
            tracing::debug!(
                "[download_queue] cursor gate: skip stale push seq={} (cursor={})",
                m.sequence, cursor_before,
            );
            return Ok(PushOutcome::Stale);
        }

        self.reconciler.apply_remote_update("push", &entry).await?;
        self.metrics.record_push_received();

        // Advance cursor to the pushed mutation's sequence (monotonic: never go backwards)
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
        let now_ms = crate::time_utils::system_time_now_ms();
        state.last_synced_sequence = new_seq;
        state.last_sync_at = Some(now_ms);
        self.storage.write_sync_state(&state).await?;
        tracing::debug!(
            "[download_queue] write_sync_state cursor={} gen={} (from push mutation)",
            state.last_synced_sequence,
            state.generation_id
        );

        // Only advance in-memory cursor after persistence succeeds
        self.last_sequence.store(new_seq, std::sync::atomic::Ordering::SeqCst);
        tracing::debug!(
            "[download_queue] push cursor advanced {} -> {} (mutation={})",
            cursor_before,
            new_seq,
            m.id
        );

        let outcome = if new_seq == cursor_before.wrapping_add(1) {
            PushOutcome::Contiguous
        } else {
            PushOutcome::GapDetected
        };

        let lag = now_ms.saturating_sub(m.timestamp);
        self.metrics.record_sync_lag(lag as f64);
        self.metrics
            .record_download_lag(&self.namespace, lag as f64);

        Ok(outcome)
    }
}
