use std::sync::Arc;

use async_trait::async_trait;
use vaultsync_core::VaultSyncError;
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::storage::transaction::{PrepareHandle, StorageTransaction};

use crate::page_store::PageStore;
use crate::storage; // for read_all_oplog_entries
use crate::storage_runtime::StorageRuntime;

enum TxOp {
    MarkSynced {
        id: String,
        sequence: u64,
    },
    MarkFailed {
        id: String,
        error: String,
    },
    AppendOplog {
        entry: OplogEntry,
    },
}

/// An OPFS-backed storage transaction.
/// Buffers all mutations in memory, then writes a single delta page on commit.
/// Uses StorageRuntime (when available) for allocation and scheduling.
pub struct OpfsTransaction {
    store: PageStore,
    storage_runtime: Option<Arc<StorageRuntime>>,
    ops: Vec<TxOp>,
    // Read entries once at begin, cache for the transaction lifetime
    pending_entries: Vec<OplogEntry>,
}

impl OpfsTransaction {
    pub async fn new(store: &PageStore) -> Result<Self, VaultSyncError> {
        let pending_entries = storage::read_all_oplog_entries(store).await?;
        Ok(Self {
            store: store.clone(),
            storage_runtime: None,
            ops: Vec::new(),
            pending_entries,
        })
    }

    /// Attach a StorageRuntime for allocation routing and scheduling.
    pub fn with_runtime(mut self, sr: Arc<StorageRuntime>) -> Self {
        self.storage_runtime = Some(sr);
        self
    }
}

#[async_trait]
impl StorageTransaction for OpfsTransaction {
    async fn mark_synced(&mut self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        self.ops.push(TxOp::MarkSynced {
            id: id.to_string(),
            sequence,
        });
        // Apply to cached entries immediately so subsequent ops in the same tx see the change
        for entry in &mut self.pending_entries {
            if entry.id == id {
                entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Synced;
                entry.sequence = Some(sequence);
            }
        }
        Ok(())
    }

    async fn mark_failed(&mut self, id: &str, error: &str) -> Result<(), VaultSyncError> {
        for entry in &mut self.pending_entries {
            if entry.id == id {
                entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Failed;
            }
        }
        self.ops.push(TxOp::MarkFailed {
            id: id.to_string(),
            error: error.to_string(),
        });
        Ok(())
    }

    async fn append_oplog(&mut self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        self.ops.push(TxOp::AppendOplog {
            entry: entry.clone(),
        });
        self.pending_entries.push(entry.clone());
        Ok(())
    }

    async fn prepare(self: Box<Self>) -> Result<PrepareHandle, VaultSyncError> {
        // For OPFS, prepare is equivalent to commit (no journal yet).
        Ok(PrepareHandle::new(self))
    }

    async fn commit(self: Box<Self>) -> Result<(), VaultSyncError> {
        if self.ops.is_empty() {
            return Ok(());
        }

        // Build the delta page from buffered operations
        let mut delta_entries: Vec<OplogEntry> = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            match op {
                TxOp::MarkSynced { id, sequence } => {
                    let matched = self.pending_entries.iter().find(|e| e.id == *id);
                    if let Some(entry) = matched {
                        let mut delta = entry.clone();
                        delta.sync_status = vaultsync_core::oplog::entry::SyncStatus::Synced;
                        delta.sequence = Some(*sequence);
                        delta_entries.push(delta);
                    } else {
                        let short_id = if id.len() >= 8 { &id[..8] } else { id };
                        tracing::warn!(
                            "[tx.commit] MarkSynced id={}.. seq={} matched=false pending_entries={}",
                            short_id, sequence, self.pending_entries.len(),
                        );
                    }
                }
                TxOp::MarkFailed { id, .. } => {
                    if let Some(entry) = self.pending_entries.iter().find(|e| e.id == *id) {
                        let mut delta = entry.clone();
                        delta.sync_status = vaultsync_core::oplog::entry::SyncStatus::Failed;
                        delta_entries.push(delta);
                    }
                }
                TxOp::AppendOplog { entry } => {
                    delta_entries.push(entry.clone());
                }
            }
        }

        if delta_entries.is_empty() {
            tracing::warn!("[tx.commit] delta_entries=0 returning early without write");
            return Ok(());
        }

        // Write all changes as a single delta page
        let encoded = postcard::to_allocvec(&delta_entries)
            .map_err(|e| VaultSyncError::Storage(format!("tx encode: {:?}", e)))?;
        if let Some(ref sr) = self.storage_runtime {
            // Route through StorageRuntime for allocation + scheduling
            let page_id = sr.allocate_page_id("oplog", "tx_commit");
            self.store.commit_allocated_page_id(page_id).await?;
            sr.enqueue_write_page("oplog", page_id, encoded).await?;
            // Re-read from OPFS after write to compute authoritative pending count.
            // Reading before the write would include about-to-be-synced entries
            // as uploadable, causing cache/disk divergence.
            let after_entries = storage::read_all_oplog_entries(&self.store).await?;
            let pending_count = after_entries.iter().filter(|e| e.sync_status.is_uploadable()).count();
            sr.set_pending_count("oplog", pending_count).await?;
            sr.schedule_gc("oplog");
        } else {
            // Fallback: direct allocation + write (no StorageRuntime)
            let page_id = self.store.allocate_page_id("tx_commit").await?;
            self.store.write_page(page_id, &encoded).await?;
            // Re-read from OPFS after write to compute authoritative pending count.
            let after_entries = storage::read_all_oplog_entries(&self.store).await?;
            let pending_count = after_entries.iter().filter(|e| e.sync_status.is_uploadable()).count();
            self.store.set_pending_count(pending_count);
            self.store.schedule_gc();
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) {
        // Simply drop — buffered changes are discarded
    }
}
