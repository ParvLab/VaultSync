pub mod compaction;
pub mod download;
pub mod events;
pub mod reconciler;
pub mod retry;
pub mod state;
pub mod state_manager;
pub mod sync_state_store;
pub mod upload;

/// Lifecycle state of the initial synchronization process.
/// Tracks progress from connecting through initial sync completion.
/// Used by `VaultSyncClient.sync_lifecycle_tx` to signal JS bootstrap
/// when OPFS data is ready for populating the DocumentStore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncLifecycle {
    /// Client constructed, not yet connected to coordinator
    Connecting,
    /// Subscribe complete, download worker started, processing initial mutations
    Downloading,
    /// First `process_batch() -> Ok(0)` — OPFS has all available data
    InitialSyncComplete,
}
