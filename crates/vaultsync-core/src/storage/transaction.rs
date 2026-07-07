use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use async_trait::async_trait;

/// A handle returned by `StorageTransaction::prepare()`.
/// Committing or rolling back a prepared transaction is always infallible
/// from the caller's perspective (errors are captured in the handle).
pub struct PrepareHandle {
    inner: Box<dyn StorageTransaction>,
    prepared: bool,
}

impl PrepareHandle {
    pub fn new(tx: Box<dyn StorageTransaction>) -> Self {
        Self {
            inner: tx,
            prepared: true,
        }
    }

    /// Commit the prepared transaction.
    pub async fn commit(self) -> Result<(), VaultSyncError> {
        if self.prepared {
            self.inner.commit().await
        } else {
            Ok(())
        }
    }

    /// Rollback the prepared transaction, discarding all changes.
    pub async fn rollback(self) {
        if self.prepared {
            self.inner.rollback().await;
        }
    }
}

/// A storage transaction that groups multiple mutations into a single atomic commit.
///
/// Lifecycle: `begin()` → operations → `prepare()` → `commit()` | `rollback()`
///
/// The `prepare()` phase ensures all buffered writes are durable but not yet visible,
/// enabling future journaling and two-phase commit without API changes.
#[async_trait]
pub trait StorageTransaction: Send {
    /// Mark an oplog entry as synced with the given server sequence.
    async fn mark_synced(&mut self, id: &str, sequence: u64) -> Result<(), VaultSyncError>;

    /// Mark an oplog entry as failed.
    async fn mark_failed(&mut self, id: &str, error: &str) -> Result<(), VaultSyncError>;

    /// Append a new oplog entry.
    async fn append_oplog(&mut self, entry: &OplogEntry) -> Result<(), VaultSyncError>;

    /// Prepare ensures all buffered writes are durable but not yet visible.
    /// Returns a `PrepareHandle` that can be committed or rolled back.
    async fn prepare(self: Box<Self>) -> Result<PrepareHandle, VaultSyncError>;

    /// Commit all buffered changes, making them visible.
    async fn commit(self: Box<Self>) -> Result<(), VaultSyncError>;

    /// Rollback discards all buffered changes.
    async fn rollback(self: Box<Self>);
}
