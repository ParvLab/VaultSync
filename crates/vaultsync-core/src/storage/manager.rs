use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use async_trait::async_trait;

use crate::oplog::entry::OplogEntry;
use crate::storage::resource_manager::{ResourceManager, ResourceManagerStats};
use crate::storage::traits::Storage;
use crate::workspace::WorkspaceManager;
use crate::VaultSyncError;

pub use crate::storage::lifecycle::{
    LifecycleEngine, LifecycleEvent, LifecycleStats, SegmentState,
};
pub use crate::storage::compaction::{
    CompactionEngine, CompactionPolicy, CompactionStats, SnapshotMetadata,
};

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub sync_oplog: bool,
    pub oplog_entry: Option<OplogEntry>,
    pub priority: WritePriority,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            sync_oplog: true,
            oplog_entry: None,
            priority: WritePriority::Normal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WritePriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_pages: u64,
    pub active_segments: u64,
    pub sealed_segments: u64,
    pub tombstone_pages: u64,
    pub total_bytes: u64,
    pub last_compaction_ms: u64,
    pub last_lifecycle_ms: u64,
}

#[async_trait]
pub trait StorageManager: Send + Sync + std::fmt::Debug {
    async fn read_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError>;

    async fn write_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError>;

    async fn delete_document(
        &self,
        doc_id: &str,
        record_id: &str,
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError>;

    async fn list_documents(
        &self,
        doc_id: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError>;

    async fn flush(&self) -> Result<(), VaultSyncError>;

    async fn compact_namespace(
        &self,
        namespace: &str,
    ) -> Result<CompactionStats, VaultSyncError>;

    async fn run_lifecycle(&self, namespace: &str) -> Result<LifecycleStats, VaultSyncError>;

    async fn stats(&self) -> Result<StorageStats, VaultSyncError>;

    fn compaction_engine(&self) -> &CompactionEngine;
    fn lifecycle_engine(&self) -> &LifecycleEngine;
    fn resource_manager(&self) -> &ResourceManager;
    async fn run_resource_sweep(&self) -> Result<ResourceManagerStats, VaultSyncError>;
}

#[derive(Debug)]
pub struct DefaultStorageManager {
    storage: Arc<dyn Storage>,
    compaction: CompactionEngine,
    lifecycle: LifecycleEngine,
    resource_manager: ResourceManager,
    workspace_manager: Option<Arc<WorkspaceManager>>,
    stats: std::sync::Mutex<StorageStats>,
}

impl DefaultStorageManager {
    pub fn new(
        storage: Arc<dyn Storage>,
        compaction_policy: CompactionPolicy,
    ) -> Self {
        Self {
            storage,
            compaction: CompactionEngine::new(compaction_policy),
            lifecycle: LifecycleEngine::new(),
            resource_manager: ResourceManager::default(),
            workspace_manager: None,
            stats: std::sync::Mutex::new(StorageStats::default()),
        }
    }

    pub fn with_workspace_manager(
        storage: Arc<dyn Storage>,
        compaction_policy: CompactionPolicy,
        workspace_manager: Arc<WorkspaceManager>,
    ) -> Self {
        Self {
            storage,
            compaction: CompactionEngine::new(compaction_policy),
            lifecycle: LifecycleEngine::new(),
            resource_manager: ResourceManager::default(),
            workspace_manager: Some(workspace_manager),
            stats: std::sync::Mutex::new(StorageStats::default()),
        }
    }

    pub fn with_resource_manager(
        storage: Arc<dyn Storage>,
        compaction_policy: CompactionPolicy,
        resource_manager: ResourceManager,
    ) -> Self {
        Self {
            storage,
            compaction: CompactionEngine::new(compaction_policy),
            lifecycle: LifecycleEngine::new(),
            resource_manager,
            workspace_manager: None,
            stats: std::sync::Mutex::new(StorageStats::default()),
        }
    }

    pub fn from_parts(
        storage: Arc<dyn Storage>,
        compaction: CompactionEngine,
        lifecycle: LifecycleEngine,
        resource_manager: ResourceManager,
    ) -> Self {
        Self {
            storage,
            compaction,
            lifecycle,
            resource_manager,
            workspace_manager: None,
            stats: std::sync::Mutex::new(StorageStats::default()),
        }
    }

    pub fn set_workspace_manager(&mut self, wm: Arc<WorkspaceManager>) {
        self.workspace_manager = Some(wm);
    }
}

#[async_trait]
impl StorageManager for DefaultStorageManager {
    async fn read_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let result = self.storage.get_document(doc_id, record_id).await?;
        if let Some(ref bytes) = result {
            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            self.resource_manager.record_access(doc_id, record_id, bytes.len() as u64, now_ms);
        }
        Ok(result)
    }

    async fn write_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.resource_manager.record_access(doc_id, record_id, bytes.len() as u64, now_ms);
        match opts.oplog_entry {
            Some(entry) => {
                self.storage
                    .write_document_and_oplog(doc_id, record_id, bytes, &entry)
                    .await
            }
            None => {
                self.storage
                    .insert_document(doc_id, record_id, bytes)
                    .await
            }
        }
    }

    async fn delete_document(
        &self,
        doc_id: &str,
        record_id: &str,
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError> {
        self.resource_manager.remove_page(doc_id, record_id);
        match opts.oplog_entry {
            Some(entry) => {
                self.storage
                    .delete_document_and_oplog(doc_id, record_id, &entry)
                    .await
            }
            None => {
                self.storage.delete_document(doc_id, record_id).await
            }
        }
    }

    async fn list_documents(
        &self,
        doc_id: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        self.storage.list_documents(doc_id).await
    }

    async fn flush(&self) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn compact_namespace(
        &self,
        namespace: &str,
    ) -> Result<CompactionStats, VaultSyncError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;

        let active_docs = self.storage.list_active_documents(namespace).await?;
        let tombstones = self
            .storage
            .list_tombstoned_documents(namespace, 0)
            .await?;

        let decision = self.compaction.should_compact(
            active_docs.len() as u64,
            tombstones.len() as u64,
        );

        if !decision.should_run {
            return Ok(CompactionStats {
                documents_compacted: 0,
                tombstones_removed: 0,
                bytes_saved: 0,
                duration_ms: 0,
                snapshots_created: Vec::new(),
            });
        }

        let start = std::time::Instant::now();

        let mut compacted = 0u64;
        let mut snapshots = Vec::new();

        for (doc_id, record_id) in &active_docs {
            if let Ok(Some(bytes)) = self.storage.get_document(doc_id, record_id).await {
                let checksum = crc32fast::hash(&bytes);
                snapshots.push(SnapshotMetadata {
                    seq: compacted + 1,
                    doc_id: doc_id.clone(),
                    record_id: record_id.clone(),
                    checksum,
                    byte_len: bytes.len() as u64,
                    created_at: now_ms,
                });
                compacted += 1;
            }
        }

        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.lock().unwrap();
            stats.last_compaction_ms = now_ms;
        }

        Ok(CompactionStats {
            documents_compacted: compacted,
            tombstones_removed: tombstones.len() as u64,
            bytes_saved: 0,
            duration_ms: elapsed.as_millis() as u64,
            snapshots_created: snapshots,
        })
    }

    async fn run_lifecycle(&self, namespace: &str) -> Result<LifecycleStats, VaultSyncError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;

        let tombstone_age_secs = if let Some(ref wm) = self.workspace_manager {
            if let Some(workspace) = wm.get_workspace_for_namespace(namespace) {
                workspace.retention_policy.tombstone_retention_ms / 1000
            } else {
                86400
            }
        } else {
            86400
        };

        let tombstones = self
            .storage
            .list_tombstoned_documents(namespace, tombstone_age_secs)
            .await?;

        let mut events = Vec::new();
        for (doc_id, record_id) in &tombstones {
            let _ = self.storage.delete_document(doc_id, record_id).await;
            events.push(LifecycleEvent {
                doc_id: doc_id.clone(),
                record_id: record_id.clone(),
                new_state: SegmentState::Deleted,
                timestamp: now,
            });
        }

        {
            let mut stats = self.stats.lock().unwrap();
            stats.last_lifecycle_ms = now;
        }

        Ok(LifecycleStats {
            segments_cleaned: tombstones.len() as u64,
            events,
        })
    }

    async fn stats(&self) -> Result<StorageStats, VaultSyncError> {
        Ok(self.stats.lock().unwrap().clone())
    }

    fn compaction_engine(&self) -> &CompactionEngine {
        &self.compaction
    }

    fn lifecycle_engine(&self) -> &LifecycleEngine {
        &self.lifecycle
    }

    fn resource_manager(&self) -> &ResourceManager {
        &self.resource_manager
    }

    async fn run_resource_sweep(&self) -> Result<ResourceManagerStats, VaultSyncError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        Ok(self.resource_manager.sweep(now_ms))
    }
}


