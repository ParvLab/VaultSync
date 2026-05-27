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
    pub auth_token: Option<String>,
    pub upload: UploadConfig,
    pub download: DownloadConfig,
    pub retry: RetryConfig,
    pub sync_interval: std::time::Duration,
    pub debug_port: Option<u16>,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            replica_id: uuid::Uuid::new_v4().to_string(),
            storage: StorageConfig::InMemory,
            coordinator_endpoint: "http://localhost:9876".to_string(),
            auth_token: None,
            upload: UploadConfig::default(),
            download: DownloadConfig::default(),
            retry: RetryConfig::default(),
            sync_interval: std::time::Duration::from_secs(5),
            debug_port: None,
        }
    }
}
