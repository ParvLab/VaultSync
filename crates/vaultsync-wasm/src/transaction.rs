use async_trait::async_trait;
use vaultsync_core::VaultSyncError;
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::storage::transaction::{PrepareHandle, StorageTransaction};

use crate::page_store::PageStore;
use crate::storage; // for read_all_oplog_entries

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
pub struct OpfsTransaction {
    store: PageStore,
    ops: Vec<TxOp>,
    // Read entries once at begin, cache for the transaction lifetime
    pending_entries: Vec<OplogEntry>,
}

impl OpfsTransaction {
    pub async fn new(store: &PageStore) -> Result<Self, VaultSyncError> {
        // Read the current resolved state once; all mutations operate on this snapshot.
        let pending_entries = storage::read_all_oplog_entries(store).await?;
        Ok(Self {
            store: store.clone(),
            ops: Vec::new(),
            pending_entries,
        })
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
        let mut found = false;
        let mut before_status = String::new();
        for entry in &mut self.pending_entries {
            if entry.id == id {
                found = true;
                before_status = format!("{:?}", entry.sync_status);
                entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Synced;
                entry.sequence = Some(sequence);
            }
        }
        tracing::info!(
            "[tx.mark_synced] id={}.. seq={} found={} pending_entries={} before_status={}",
            if id.len() >= 8 { &id[..8] } else { id },
            sequence,
            found,
            self.pending_entries.len(),
            before_status,
        );
        Ok(())
    }

    async fn mark_failed(&mut self, id: &str, error: &str) -> Result<(), VaultSyncError> {
        let mut found = false;
        let mut before_status = String::new();
        for entry in &mut self.pending_entries {
            if entry.id == id {
                found = true;
                before_status = format!("{:?}", entry.sync_status);
                entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Failed;
            }
        }
        self.ops.push(TxOp::MarkFailed {
            id: id.to_string(),
            error: error.to_string(),
        });
        tracing::info!(
            "[tx.mark_failed] id={}.. found={} before={} error={} pending_entries={}",
            if id.len() >= 8 { &id[..8] } else { id },
            found, before_status, error, self.pending_entries.len(),
        );
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
            tracing::info!("[tx.commit] ops=0 returning early");
            return Ok(());
        }

        // Build the delta page from buffered operations
        let mut delta_entries: Vec<OplogEntry> = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            match op {
                TxOp::MarkSynced { id, sequence } => {
                    let matched = self.pending_entries.iter().find(|e| e.id == *id);
                    if let Some(entry) = matched {
                        let short_id = if id.len() >= 8 { &id[..8] } else { id };
                        tracing::info!(
                            "[tx.commit] MarkSynced id={}.. seq={} matched=true old_status={:?}",
                            short_id, sequence, entry.sync_status,
                        );
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
        let page_id = self.store.allocate_page_id().await?;
        tracing::info!(
            "[tx.commit] writing delta_entries={} page_id={} first_id={}.. status=Synced seq={}",
            delta_entries.len(),
            page_id,
            if delta_entries[0].id.len() >= 8 { &delta_entries[0].id[..8] } else { &delta_entries[0].id },
            delta_entries[0].sequence.unwrap_or(0),
        );
        self.store.write_page(page_id, &encoded).await?;
        tracing::info!("[tx.commit] write_page done page_id={}", page_id);

        // Update pending_count: count pending entries in the resolved state
        let pending_count = self
            .pending_entries
            .iter()
            .filter(|e| e.sync_status.is_uploadable())
            .count();
        self.store.set_pending_count(pending_count).await?;
        self.store.schedule_gc();

        tracing::info!(
            "[tx.commit] complete page_id={} delta_entries={} pending_count={}",
            page_id, delta_entries.len(), pending_count,
        );
        Ok(())
    }

    async fn rollback(self: Box<Self>) {
        // Simply drop — buffered changes are discarded
    }
}
