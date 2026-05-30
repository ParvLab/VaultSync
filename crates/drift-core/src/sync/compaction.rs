use std::sync::Arc;
use crate::error::DriftError;
use crate::storage::traits::Storage;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub max_synced_age_secs: u64,
    pub tombstone_grace_secs: u64,
    pub snapshot_entry_threshold: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_synced_age_secs: 7 * 24 * 60 * 60, // 7 days
            tombstone_grace_secs: 24 * 60 * 60,    // 24 hours
            snapshot_entry_threshold: 500,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactionStats {
    pub oplog_removed: usize,
    pub docs_removed: usize,
    pub snapshots_collapsed: usize,
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
        let age_oplog_removed = self.storage.delete_synced_oplog_older_than(namespace, self.config.max_synced_age_secs).await?;

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

        let snapshot_stats = self.run_snapshot_compaction(namespace).await?;

        Ok(CompactionStats {
            oplog_removed: age_oplog_removed + snapshot_stats.oplog_removed,
            docs_removed,
            snapshots_collapsed: snapshot_stats.snapshots_collapsed,
        })
    }

    pub async fn run_snapshot_compaction(&self, namespace: &str) -> Result<CompactionStats, DriftError> {
        let active_docs = self.storage.list_active_documents(namespace).await?;
        let mut oplog_removed = 0;
        let mut snapshots_collapsed = 0;

        for (doc_id, record_id) in active_docs {
            let synced_entries = self.storage.read_synced_oplog_for_document(namespace, &doc_id, &record_id).await?;
            if synced_entries.len() > self.config.snapshot_entry_threshold {
                if let Some(bytes) = self.storage.get_document(&doc_id, &record_id).await? {
                    if let Ok(doc) = crate::crdt::document::CRDTDocument::from_snapshot(&bytes) {
                        let fresh_snapshot = doc.to_snapshot();
                        self.storage.insert_document(&doc_id, &record_id, &fresh_snapshot).await?;
                        
                        let max_timestamp = synced_entries.iter().map(|e| e.created_at).max().unwrap_or(0);
                        if max_timestamp > 0 {
                            let deleted_count = self.storage.delete_synced_oplog_before_timestamp(namespace, &doc_id, &record_id, max_timestamp + 1).await?;
                            oplog_removed += deleted_count;
                            snapshots_collapsed += 1;
                        }
                    }
                }
            }
        }

        Ok(CompactionStats {
            oplog_removed,
            docs_removed: 0,
            snapshots_collapsed,
        })
    }
}
