use std::sync::Arc;
use vaultsync_core::storage::compaction::CompactionPolicy;
use vaultsync_core::storage::lifecycle::LifecycleStats;
use vaultsync_core::storage::manager::{
    CompactionStats, DefaultStorageManager, StorageManager, StorageStats, WriteOptions,
};
use vaultsync_core::storage::resource_manager::ResourceManagerStats;
use vaultsync_core::storage::traits::Storage;
use vaultsync_core::VaultSyncError;

#[derive(Debug, Clone)]
pub struct WasmStorageManager {
    inner: Arc<dyn StorageManager>,
}

impl WasmStorageManager {
    pub async fn new(
        storage: Arc<dyn Storage>,
        policy: Option<CompactionPolicy>,
    ) -> Result<Self, VaultSyncError> {
        let compaction_policy = policy.unwrap_or_default();
        let manager = DefaultStorageManager::new(storage, compaction_policy);
        Ok(Self {
            inner: Arc::new(manager),
        })
    }

    pub fn from_manager(manager: Arc<dyn StorageManager>) -> Self {
        Self { inner: manager }
    }
}

#[async_trait::async_trait]
impl StorageManager for WasmStorageManager {
    async fn read_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        self.inner.read_document(doc_id, record_id).await
    }

    async fn write_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError> {
        self.inner.write_document(doc_id, record_id, bytes, opts).await
    }

    async fn delete_document(
        &self,
        doc_id: &str,
        record_id: &str,
        opts: WriteOptions,
    ) -> Result<(), VaultSyncError> {
        self.inner.delete_document(doc_id, record_id, opts).await
    }

    async fn list_documents(
        &self,
        doc_id: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        self.inner.list_documents(doc_id).await
    }

    async fn flush(&self) -> Result<(), VaultSyncError> {
        self.inner.flush().await
    }

    async fn compact_namespace(
        &self,
        namespace: &str,
    ) -> Result<CompactionStats, VaultSyncError> {
        self.inner.compact_namespace(namespace).await
    }

    async fn run_lifecycle(
        &self,
        namespace: &str,
    ) -> Result<LifecycleStats, VaultSyncError> {
        self.inner.run_lifecycle(namespace).await
    }

    async fn stats(&self) -> Result<StorageStats, VaultSyncError> {
        self.inner.stats().await
    }

    fn compaction_engine(&self) -> &vaultsync_core::storage::compaction::CompactionEngine {
        self.inner.compaction_engine()
    }

    fn lifecycle_engine(&self) -> &vaultsync_core::storage::lifecycle::LifecycleEngine {
        self.inner.lifecycle_engine()
    }

    fn resource_manager(&self) -> &vaultsync_core::storage::resource_manager::ResourceManager {
        self.inner.resource_manager()
    }

    async fn run_resource_sweep(&self) -> Result<ResourceManagerStats, VaultSyncError> {
        self.inner.run_resource_sweep().await
    }
}
