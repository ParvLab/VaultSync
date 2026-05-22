use std::sync::Arc;
use crate::error::DriftError;
use crate::coordinator::traits::{Coordinator, CoordinatorError};

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub batch_size: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self { Self { batch_size: 50 } }
}

pub struct DownloadQueue {
    coordinator: Arc<dyn Coordinator>,
    namespace: String,
    last_sequence: std::sync::atomic::AtomicU64,
    config: DownloadConfig,
}

impl DownloadQueue {
    pub fn new(coordinator: Arc<dyn Coordinator>, namespace: &str, last_sequence: u64, config: DownloadConfig) -> Self {
        Self {
            coordinator,
            namespace: namespace.to_string(),
            last_sequence: std::sync::atomic::AtomicU64::new(last_sequence),
            config,
        }
    }

    pub async fn process_batch(&self) -> Result<usize, DriftError> {
        let after = self.last_sequence.load(std::sync::atomic::Ordering::SeqCst);
        match self.coordinator.pull(&self.namespace, after, self.config.batch_size).await {
            Ok(mutations) => {
                let count = mutations.len();
                if let Some(last) = mutations.last() {
                    self.last_sequence.store(last.sequence, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(count)
            }
            Err(CoordinatorError::NotAvailable) => Ok(0),
            Err(e) => Err(DriftError::Coordinator(format!("download failed: {e:?}"))),
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(std::sync::atomic::Ordering::SeqCst)
    }
}
