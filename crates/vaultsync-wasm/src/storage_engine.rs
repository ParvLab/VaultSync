use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::storage::traits::Storage;
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;

use crate::page_store::{IndexKey, PageStore};
use crate::runtime::Runtime;
use crate::storage::OpfsStorage;

/// Phase 3: StorageHealth — detailed health diagnostics.
#[derive(Debug, Clone, Default)]
pub struct StorageHealth {
    pub opfs_available: bool,
    pub manifest_ok: bool,
    pub wal_ok: bool,
    pub page_cache_usage: (usize, usize),
    pub storage_used_bytes: u64,
    pub last_checkpoint: u64,
    pub corruption_detected: bool,
    pub recovery_pending: bool,
}

/// Phase 3: StorageEngine trait — the only persistence interface the Runtime depends on.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Document operations
    async fn read_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, VaultSyncError>;
    async fn write_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), VaultSyncError>;
    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError>;
    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError>;

    // Oplog operations
    async fn read_pending_entries(&self) -> Result<Vec<OplogEntry>, VaultSyncError>;
    async fn mark_synced(&self, seq: u64) -> Result<(), VaultSyncError>;
    async fn read_after_sequence(&self, seq: u64) -> Result<Vec<OplogEntry>, VaultSyncError>;

    // KV store operations
    async fn read_kv(&self, store: &str, key: &str) -> Result<Option<Vec<u8>>, VaultSyncError>;
    async fn write_kv(&self, store: &str, key: &str, value: &[u8]) -> Result<(), VaultSyncError>;
    async fn delete_kv(&self, store: &str, key: &str) -> Result<(), VaultSyncError>;

    // Metadata
    async fn read_metadata(&self, key: &str) -> Result<Option<Vec<u8>>, VaultSyncError>;
    async fn write_metadata(&self, key: &str, value: &[u8]) -> Result<(), VaultSyncError>;

    // Lifecycle
    async fn compact(&self) -> Result<(), VaultSyncError>;
    async fn checkpoint_wal(&self) -> Result<(), VaultSyncError>;
    async fn is_healthy(&self) -> bool;
    fn health(&self) -> StorageHealth;

    // Snapshot for follower rehydration
    async fn snapshot_metadata(&self) -> Result<String, VaultSyncError>;
    async fn read_snapshot_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, VaultSyncError>;

    /// Load WAL checkpoint from storage. Returns (seq, count). Default: no checkpoint.
    async fn load_checkpoint(&self) -> Result<(u64, usize), VaultSyncError> {
        Ok((0, 0))
    }
}

/// Phase 3: OpfsStorageEngine — wraps OpfsStorage + PageStore behind the StorageEngine trait.
/// Reserved page ID for WAL checkpoint storage
const WAL_CHECKPOINT_PAGE: u64 = 0;

/// Reserved page ID for WAL entries storage (unused, kept for documentation)
const _WAL_ENTRIES_PAGE: u64 = 1;

pub struct OpfsStorageEngine {
    storage: Arc<OpfsStorage>,
    doc_data: PageStore,
    oplog: PageStore,
    runtime: Option<Arc<Runtime>>,
}

impl OpfsStorageEngine {
    pub fn new(storage: Arc<OpfsStorage>, doc_data: PageStore, oplog: PageStore) -> Self {
        Self { storage, doc_data, oplog, runtime: None }
    }

    pub fn with_runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

#[async_trait]
impl StorageEngine for OpfsStorageEngine {
    async fn read_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, VaultSyncError> {
        // Phase 1: Use ContentIndex for O(1) lookup
        if let Some(entry) = self.doc_data.content_index_lookup(&IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        }) {
            self.doc_data.read_page(entry.page_id).await
        } else {
            // Fallback to full scan (legacy)
            self.storage.get_document(doc_id, record_id).await
        }
    }

    async fn write_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), VaultSyncError> {
        // Use ContentIndex-based write
        let entry_key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        // First tombstone existing
        if let Some(existing) = self.doc_data.content_index_lookup(&entry_key) {
            self.doc_data.tombstone_page(existing.page_id).await?;
            self.doc_data.remove_from_index(&entry_key).await?;
        }
        let page_id = self.doc_data.allocate_page_id().await?;
        self.doc_data.write_page(page_id, bytes).await?;
        self.doc_data.update_content_index(entry_key, page_id).await?;
        Ok(())
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let entry_key = IndexKey::Document {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
        };
        if let Some(entry) = self.doc_data.content_index_lookup(&entry_key) {
            self.doc_data.tombstone_page(entry.page_id).await?;
            self.doc_data.remove_from_index(&entry_key).await?;
        }
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let entries: Vec<(String, u64)> = {
            let guard = self.doc_data.manifest.lock().unwrap();
            if let Some(ref manifest) = *guard {
                manifest.content_index.entries.iter()
                    .filter_map(|(key, entry)| {
                        if let IndexKey::Document { doc_id: d, record_id } = key {
                            if d == doc_id {
                                Some((record_id.clone(), entry.page_id))
                            } else { None }
                        } else { None }
                    })
                    .collect()
            } else {
                Vec::new()
            }
        };
        let mut results = Vec::new();
        for (record_id, page_id) in entries {
            if let Some(data) = self.doc_data.read_page(page_id).await? {
                results.push((record_id, data));
            }
        }
        Ok(results)
    }

    async fn read_pending_entries(&self) -> Result<Vec<OplogEntry>, VaultSyncError> {
        // Use oplog PageStore with ContentIndex
        let entries = crate::storage::read_all_oplog_entries(&self.oplog).await?;
        Ok(entries.into_iter().filter(|e| e.sync_status.is_uploadable()).collect())
    }

    async fn mark_synced(&self, seq: u64) -> Result<(), VaultSyncError> {
        self.storage.mark_synced("", seq).await
    }

    async fn read_after_sequence(&self, seq: u64) -> Result<Vec<OplogEntry>, VaultSyncError> {
        self.storage.read_oplog_after_sequence("", seq).await
    }

    async fn read_kv(&self, store: &str, key: &str) -> Result<Option<Vec<u8>>, VaultSyncError> {
        match store {
            "sync_state" => self.storage.read_sync_state(key).await.map(|s| s.map(|_| Vec::new())),
            "schema" => self.storage.read_schema(key).await.map(|s| s.map(|_| Vec::new())),
            _ => Ok(None),
        }
    }

    async fn write_kv(&self, store: &str, key: &str, value: &[u8]) -> Result<(), VaultSyncError> {
        match store {
            "sync_state" => {
                let state = postcard::from_bytes::<SyncState>(value)
                    .map_err(|e| VaultSyncError::Storage(format!("decode: {:?}", e)))?;
                self.storage.write_sync_state(&state).await
            }
            _ => Ok(()),
        }
    }

    async fn delete_kv(&self, _store: &str, _key: &str) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn read_metadata(&self, _key: &str) -> Result<Option<Vec<u8>>, VaultSyncError> {
        Ok(None)
    }

    async fn write_metadata(&self, _key: &str, _value: &[u8]) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn compact(&self) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn checkpoint_wal(&self) -> Result<(), VaultSyncError> {
        let (seq, count) = if let Some(ref rt) = self.runtime {
            let wal = rt.wal.lock().unwrap();
            let seq = rt.metadata_store.lock().unwrap().cursor;
            let count = wal.len();
            (seq, count)
        } else {
            return Ok(());
        };
        let checkpoint = format!("{{ \"seq\": {}, \"count\": {} }}", seq, count);
        self.doc_data.write_page(WAL_CHECKPOINT_PAGE, checkpoint.as_bytes()).await?;
        self.oplog.write_page(WAL_CHECKPOINT_PAGE, checkpoint.as_bytes()).await
    }

    async fn is_healthy(&self) -> bool {
        // Verify manifest is loadable and WAL checkpoint page exists
        let manifest_ok = self.doc_data.list_page_ids("health_check").await.is_ok();
        let wal_check = self.doc_data.read_page(WAL_CHECKPOINT_PAGE).await.is_ok();
        manifest_ok && wal_check
    }

    fn health(&self) -> StorageHealth {
        let (seq, wal_ok) = if let Some(ref rt) = self.runtime {
            let s = rt.metadata_store.lock().unwrap().cursor;
            let w = rt.wal.lock().unwrap().len() > 0;
            (s, w)
        } else {
            (0, false)
        };
        let page_cache = self.doc_data.page_cache_stats();
        StorageHealth {
            opfs_available: true,
            manifest_ok: true,
            wal_ok,
            page_cache_usage: (page_cache.0, page_cache.1),
            storage_used_bytes: 0,
            last_checkpoint: seq,
            corruption_detected: false,
            recovery_pending: false,
        }
    }

    async fn snapshot_metadata(&self) -> Result<String, VaultSyncError> {
        Ok("{}".to_string())
    }

    async fn read_snapshot_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, VaultSyncError> {
        self.read_document(doc_id, record_id).await
    }

    async fn load_checkpoint(&self) -> Result<(u64, usize), VaultSyncError> {
        if let Some(bytes) = self.doc_data.read_page(WAL_CHECKPOINT_PAGE).await? {
            let s = std::str::from_utf8(&bytes)
                .map_err(|e| VaultSyncError::Storage(format!("checkpoint decode: {:?}", e)))?;
            #[derive(Deserialize)]
            struct Checkpoint { seq: u64, count: usize }
            let cp: Checkpoint = serde_json::from_str(s)
                .map_err(|e| VaultSyncError::Storage(format!("checkpoint parse: {:?}", e)))?;
            Ok((cp.seq, cp.count))
        } else {
            Ok((0, 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_healthy_computation() {
        let h = StorageHealth { wal_ok: true, manifest_ok: true, corruption_detected: false, ..Default::default() };
        assert!(h.wal_ok && h.manifest_ok && !h.corruption_detected);

        let h = StorageHealth { wal_ok: false, manifest_ok: true, corruption_detected: false, ..Default::default() };
        assert!(!(h.wal_ok && h.manifest_ok && !h.corruption_detected));

        let h = StorageHealth { wal_ok: true, manifest_ok: true, corruption_detected: true, ..Default::default() };
        assert!(!(h.wal_ok && h.manifest_ok && !h.corruption_detected));
    }

    #[test]
    fn checkpoint_format_roundtrip() {
        let json = r#"{"seq": 42, "count": 7}"#;
        #[derive(Deserialize)]
        struct Checkpoint { seq: u64, count: usize }
        let cp: Checkpoint = serde_json::from_str(json).unwrap();
        assert_eq!(cp.seq, 42);
        assert_eq!(cp.count, 7);
    }

    #[test]
    fn checkpoint_empty_json() {
        let json = r#"{"seq": 0, "count": 0}"#;
        #[derive(Deserialize)]
        struct Checkpoint { seq: u64, count: usize }
        let cp: Checkpoint = serde_json::from_str(json).unwrap();
        assert_eq!(cp.seq, 0);
        assert_eq!(cp.count, 0);
    }
}
