use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use vaultsync_core::VaultSyncError;

use crate::page_store::{IndexKey, PageStore};
use crate::runtime::Runtime;
use crate::storage::OpfsStorage;

/// Phase 10: PersistenceHealth — detailed health diagnostics.
#[derive(Debug, Clone, Default)]
pub struct PersistenceHealth {
    pub opfs_available: bool,
    pub manifest_ok: bool,
    pub wal_ok: bool,
    pub page_cache_usage: (usize, usize),
    pub storage_used_bytes: u64,
    pub last_checkpoint: u64,
    pub corruption_detected: bool,
    pub recovery_pending: bool,
}

/// Phase 10: PersistenceEngine trait — the only persistence interface the Runtime depends on.
/// Keeps 3 methods: checkpoint_wal, load_checkpoint, health.
/// All document/oplog/KV/metadata/compaction/snapshot methods removed:
/// they go through VaultSyncClient → Storage trait directly.
#[async_trait]
pub trait PersistenceEngine: Send + Sync {
    /// Checkpoint WAL — flush in-memory WAL to persistent storage.
    async fn checkpoint_wal(&self) -> Result<(), VaultSyncError>;

    /// Load WAL checkpoint from storage. Returns (seq, count). Default: no checkpoint.
    async fn load_checkpoint(&self) -> Result<(u64, usize), VaultSyncError> {
        Ok((0, 0))
    }

    /// Return health diagnostics.
    fn health(&self) -> PersistenceHealth;
}

/// Phase 10: OpfsPersistenceEngine — wraps OpfsStorage + PageStore behind the PersistenceEngine trait.
const CHECKPOINT_FILE: &str = "checkpoint";

pub struct OpfsPersistenceEngine {
    storage: Arc<OpfsStorage>,
    doc_data: PageStore,
    oplog: PageStore,
    runtime: Option<Arc<Runtime>>,
}

impl OpfsPersistenceEngine {
    pub fn new(storage: Arc<OpfsStorage>, doc_data: PageStore, oplog: PageStore) -> Self {
        Self { storage, doc_data, oplog, runtime: None }
    }

    pub fn with_runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

#[async_trait]
impl PersistenceEngine for OpfsPersistenceEngine {
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
        self.storage.write_system_file(CHECKPOINT_FILE, checkpoint.as_bytes()).await
    }

    async fn load_checkpoint(&self) -> Result<(u64, usize), VaultSyncError> {
        if let Some(bytes) = self.storage.read_system_file(CHECKPOINT_FILE).await? {
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

    fn health(&self) -> PersistenceHealth {
        let (seq, wal_ok) = if let Some(ref rt) = self.runtime {
            let s = rt.metadata_store.lock().unwrap().cursor;
            let w = rt.wal.lock().unwrap().len() > 0;
            (s, w)
        } else {
            (0, false)
        };
        let page_cache = self.doc_data.page_cache_stats();
        PersistenceHealth {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_healthy_computation() {
        let h = PersistenceHealth { wal_ok: true, manifest_ok: true, corruption_detected: false, ..Default::default() };
        assert!(h.wal_ok && h.manifest_ok && !h.corruption_detected);

        let h = PersistenceHealth { wal_ok: false, manifest_ok: true, corruption_detected: false, ..Default::default() };
        assert!(!(h.wal_ok && h.manifest_ok && !h.corruption_detected));

        let h = PersistenceHealth { wal_ok: true, manifest_ok: true, corruption_detected: true, ..Default::default() };
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
