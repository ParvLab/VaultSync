use crate::storage::traits::StorageConfig;
use crate::sync::download::DownloadConfig;
use crate::sync::retry::RetryConfig;
use crate::sync::upload::UploadConfig;

#[derive(Debug, Clone)]
pub struct VaultSyncConfig {
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
    pub fallback_coordinator_urls: Vec<String>,
    pub enable_p2p: bool,
    pub p2p_listen_addr: Option<String>,
    pub max_clock_skew: std::time::Duration,
    pub max_document_size: usize,
}

impl Default for VaultSyncConfig {
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
            fallback_coordinator_urls: Vec::new(),
            enable_p2p: false,
            p2p_listen_addr: Some("/ip4/0.0.0.0/udp/0/quic-v1".to_string()),
            max_clock_skew: std::time::Duration::from_secs(24 * 3600), // 24 hours
            max_document_size: 100 * 1024 * 1024,                      // 100 MB
        }
    }
}
