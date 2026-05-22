use crate::storage::traits::StorageConfig;
use crate::sync::upload::UploadConfig;
use crate::sync::download::DownloadConfig;
use crate::sync::retry::RetryConfig;

#[derive(Debug, Clone)]
pub struct DriftConfig {
    pub namespace: String,
    pub replica_id: String,
    pub storage: StorageConfig,
    pub coordinator_endpoint: String,
    pub upload: UploadConfig,
    pub download: DownloadConfig,
    pub retry: RetryConfig,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            replica_id: uuid::Uuid::new_v4().to_string(),
            storage: StorageConfig::InMemory,
            coordinator_endpoint: "http://localhost:9876".to_string(),
            upload: UploadConfig::default(),
            download: DownloadConfig::default(),
            retry: RetryConfig::default(),
        }
    }
}
