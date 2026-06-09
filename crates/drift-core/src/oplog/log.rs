use std::sync::Arc;
use crate::error::DriftError;
use crate::oplog::entry::OplogEntry;
use crate::storage::traits::Storage;

pub struct OpLog {
    storage: Arc<dyn Storage>,
    namespace: String,
}

impl OpLog {
    pub fn new(storage: Arc<dyn Storage>, namespace: &str) -> Self {
        Self { storage, namespace: namespace.to_string() }
    }

    pub async fn append(&self, entry: OplogEntry) -> Result<(), DriftError> {
        self.storage.append_oplog(&entry).await
    }

    pub async fn read_pending(&self, limit: usize) -> Result<Vec<OplogEntry>, DriftError> {
        self.storage.read_pending_oplog(&self.namespace, limit).await
    }

    pub async fn pending_entries(&self, namespace: &str) -> Result<Vec<OplogEntry>, DriftError> {
        self.storage.read_pending_oplog(namespace, 50000).await
    }

    pub async fn update_blob(&self, id: &str, new_blob: &[u8]) -> Result<(), DriftError> {
        self.storage.update_oplog_encrypted_blob(id, new_blob).await
    }

    pub async fn read_after_sequence(&self, seq: u64, limit: usize) -> Result<Vec<OplogEntry>, DriftError> {
        let entries = self.storage.read_oplog_after_sequence(&self.namespace, seq).await?;
        Ok(entries.into_iter().take(limit).collect())
    }

    pub async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), DriftError> {
        self.storage.mark_synced(id, sequence).await
    }

    pub async fn mark_failed(&self, id: &str, error_msg: &str) -> Result<(), DriftError> {
        self.storage.mark_failed(id, error_msg).await
    }

    pub async fn count_pending(&self) -> Result<usize, DriftError> {
        Ok(self.storage.read_pending_oplog(&self.namespace, 50000).await?.len())
    }

    pub async fn delete_compacted(&self, _before_sequence: u64) -> Result<usize, DriftError> {
        Ok(0)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
