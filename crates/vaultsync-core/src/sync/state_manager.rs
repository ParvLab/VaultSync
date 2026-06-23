use crate::storage::traits::Storage;
use crate::sync::state::{ConnectionStatus, SyncState};
use crate::VaultSyncError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Centralized manager for sync state (cursor + generation ID).
///
/// Replaces scattered cursor logic across DownloadQueue, client init, and storage.
/// Single source of truth for:
/// - `cursor` — last server sequence applied
/// - `generation_id` — server identity for detecting DB resets
pub struct SyncStateManager {
    namespace: String,
    storage: Arc<dyn Storage>,
    cursor: AtomicU64,
    generation_id: RwLock<String>,
}

impl SyncStateManager {
    /// Create a new SyncStateManager.
    ///
    /// Loads persisted state from storage on construction.
    pub async fn new(namespace: &str, storage: Arc<dyn Storage>) -> Result<Self, VaultSyncError> {
        let cursor;
        let generation_id;

        match storage.read_sync_state(namespace).await? {
            Some(state) => {
                cursor = state.last_synced_sequence;
                generation_id = state.generation_id;
                tracing::info!(
                    "[sync_state] loaded cursor={} generation={}",
                    cursor,
                    if generation_id.is_empty() {
                        "<none>"
                    } else {
                        &generation_id
                    }
                );
            }
            None => {
                cursor = 0;
                generation_id = String::new();
                tracing::info!("[sync_state] no persisted state, cursor=0");
            }
        }

        Ok(Self {
            namespace: namespace.to_string(),
            storage,
            cursor: AtomicU64::new(cursor),
            generation_id: RwLock::new(generation_id),
        })
    }

    /// Current cursor value (last applied server sequence)
    pub fn current_cursor(&self) -> u64 {
        self.cursor.load(Ordering::SeqCst)
    }

    /// Current generation ID (empty = no generation tracking)
    pub fn current_generation(&self) -> String {
        self.generation_id.read().unwrap().clone()
    }

    /// Persist current state to storage
    async fn persist(&self, cursor: u64, generation_id: &str) -> Result<(), VaultSyncError> {
        let state = SyncState {
            namespace: self.namespace.clone(),
            replica_id: String::new(),
            last_synced_sequence: cursor,
            connection_status: ConnectionStatus::Connected,
            leader_status: None,
            last_connected_at: None,
            last_sync_at: Some(crate::time_utils::system_time_now_ms()),
            schema_version: 0,
            generation_id: generation_id.to_string(),
        };
        self.storage.write_sync_state(&state).await?;
        Ok(())
    }

    /// Advance the cursor (call after applying mutations from a pull).
    ///
    /// Does NOT persist — caller must call `persist()`.
    pub fn advance_cursor(&self, new_seq: u64) {
        let old = self.cursor.fetch_max(new_seq, Ordering::SeqCst);
        if new_seq > old {
            tracing::debug!("[sync_state] cursor advanced {} -> {}", old, new_seq);
        }
    }

    /// Reset cursor to a new value and persist immediately.
    pub async fn reset_cursor(&self, to: u64) -> Result<(), VaultSyncError> {
        let old = self.cursor.swap(to, Ordering::SeqCst);
        let gen = self.generation_id.read().unwrap().clone();
        self.persist(to, &gen).await?;
        tracing::info!(
            "[sync_state] cursor reset {} -> {} (generation={})",
            old,
            to,
            if gen.is_empty() { "<none>" } else { &gen }
        );
        Ok(())
    }

    /// Check server generation against local, reset cursor if mismatch.
    ///
    /// Returns `true` if cursor was reset (generation changed).
    pub async fn check_generation(&self, server_gen: &str) -> Result<bool, VaultSyncError> {
        if server_gen.is_empty() {
            // Server doesn't support generation tracking
            return Ok(false);
        }

        let local_gen = self.generation_id.read().unwrap().clone();
        if local_gen == server_gen {
            return Ok(false);
        }

        tracing::info!(
            "[sync_state] generation mismatch: local={} server={} -> resetting cursor",
            if local_gen.is_empty() { "<none>" } else { &local_gen },
            server_gen
        );

        // Update generation and reset cursor
        {
            let mut gen = self.generation_id.write().unwrap();
            *gen = server_gen.to_string();
        }
        self.cursor.store(0, Ordering::SeqCst);
        self.persist(0, server_gen).await?;

        Ok(true)
    }
}
