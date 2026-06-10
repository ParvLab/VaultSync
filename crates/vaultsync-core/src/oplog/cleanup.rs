use std::time::Duration;
use std::sync::Arc;
use crate::error::VaultSyncError;
use crate::storage::traits::Storage;
use crate::time_utils::system_time_now_ms;

pub struct OplogCleanup {
    storage: Arc<dyn Storage>,
}

impl OplogCleanup {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Remove synced mutations older than `retention` from local storage.
    /// Pending mutations are NEVER removed regardless of age.
    pub async fn compact(
        &self,
        namespace: &str,
        retention: Duration,
    ) -> Result<usize, VaultSyncError> {
        let cutoff_ms = system_time_now_ms().saturating_sub(retention.as_millis() as u64);
        let removed = self.storage.delete_synced_before(namespace, cutoff_ms).await?;
        if removed > 0 {
            tracing::info!(
                namespace = namespace,
                removed = removed,
                "Oplog compaction: removed old synced entries"
            );
        }
        Ok(removed)
    }

    /// Run on a repeating schedule until shutdown.
    pub async fn run_scheduled(
        &self,
        namespace: &str,
        retention: Duration,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                _ = crate::time_utils::sleep(interval) => {
                    if let Err(e) = self.compact(namespace, retention).await {
                        tracing::warn!(error = %e, "Oplog compaction error");
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
    }
}

// Legacy free function — kept for backward compat
pub fn compact_oplog(_max_age: Duration) -> Result<usize, crate::error::VaultSyncError> {
    Ok(0) // Sync compaction not meaningful; use OplogCleanup::compact
}
