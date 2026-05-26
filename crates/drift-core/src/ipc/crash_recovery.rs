use std::sync::Arc;
use crate::error::DriftError;
use crate::storage::traits::Storage;

pub struct CrashRecovery {
    storage: Arc<dyn Storage>,
    namespace: String,
}

impl CrashRecovery {
    pub fn new(storage: Arc<dyn Storage>, namespace: String) -> Self {
        Self { storage, namespace }
    }

    pub async fn recover(&self) -> Result<usize, DriftError> {
        // Reset any stale non-synced entries older than 60 seconds (60,000 ms) back to Pending.
        let recovered = self.storage.reset_stale_pending(&self.namespace, 60_000).await?;
        Ok(recovered)
    }
}
