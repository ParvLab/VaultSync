use std::sync::Arc;

use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::storage::traits::Storage;

/// Collects pending (Optimistic) oplog entries for materialization.
///
/// This is the only component that reads the oplog.
/// It MUST NOT modify storage or perform network I/O.
#[async_trait::async_trait]
pub trait PendingCollector: Send + Sync {
    /// Return all pending entries for the given namespace.
    async fn collect(&self, namespace: &str) -> Result<Vec<OplogEntry>, VaultSyncError>;
}

/// Default implementation that reads pending entries directly from storage.
pub struct DefaultPendingCollector {
    storage: Arc<dyn Storage>,
}

impl DefaultPendingCollector {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl PendingCollector for DefaultPendingCollector {
    async fn collect(&self, namespace: &str) -> Result<Vec<OplogEntry>, VaultSyncError> {
        self.storage.read_pending_oplog(namespace, 50000).await
    }
}
