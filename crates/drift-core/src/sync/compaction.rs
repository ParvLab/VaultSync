use std::sync::Arc;
use crate::error::DriftError;
use crate::storage::traits::Storage;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub max_synced_age_secs: u64,
    pub tombstone_grace_secs: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_synced_age_secs: 7 * 24 * 60 * 60, // 7 days
            tombstone_grace_secs: 24 * 60 * 60,    // 24 hours
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionStats {
    pub oplog_removed: usize,
    pub docs_removed: usize,
}

pub struct CompactionEngine {
    storage: Arc<dyn Storage>,
    config: CompactionConfig,
}

impl CompactionEngine {
    pub fn new(storage: Arc<dyn Storage>, config: CompactionConfig) -> Self {
        Self { storage, config }
    }

    pub async fn run_compaction(&self, namespace: &str) -> Result<CompactionStats, DriftError> {
        let oplog_removed = self.storage.delete_synced_oplog_older_than(namespace, self.config.max_synced_age_secs).await?;

        let candidates = self.storage.list_tombstoned_documents(namespace, self.config.tombstone_grace_secs).await?;
        let mut docs_removed = 0;
        for (doc_id, record_id) in candidates {
            if let Some(bytes) = self.storage.get_document(&doc_id, &record_id).await? {
                if let Ok(doc) = crate::crdt::document::CRDTDocument::from_snapshot(&bytes) {
                    if let Some(crate::crdt::types::CrdtValue::Boolean(true)) = doc.get_field("_deleted") {
                        self.storage.delete_document(&doc_id, &record_id).await?;
                        docs_removed += 1;
                    }
                }
            }
        }

        Ok(CompactionStats {
            oplog_removed,
            docs_removed,
        })
    }
}
