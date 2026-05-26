use std::sync::atomic::{AtomicU64, Ordering};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub mutations_uploaded: u64,
    pub mutations_downloaded: u64,
    pub sync_errors: u64,
    pub last_sync_lag_ms: u64,
}

pub struct DriftMetrics {
    pub mutations_uploaded: AtomicU64,
    pub mutations_downloaded: AtomicU64,
    pub sync_errors: AtomicU64,
    pub last_sync_lag_ms: AtomicU64,
}

impl DriftMetrics {
    pub fn new() -> Self {
        Self {
            mutations_uploaded: AtomicU64::new(0),
            mutations_downloaded: AtomicU64::new(0),
            sync_errors: AtomicU64::new(0),
            last_sync_lag_ms: AtomicU64::new(0),
        }
    }

    pub fn record_upload(&self, count: usize) {
        self.mutations_uploaded.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_download(&self, count: usize) {
        self.mutations_downloaded.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_sync_lag(&self, ms: f64) {
        self.last_sync_lag_ms.store(ms as u64, Ordering::Relaxed);
    }

    pub fn record_sync_error(&self) {
        self.sync_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            mutations_uploaded: self.mutations_uploaded.load(Ordering::Relaxed),
            mutations_downloaded: self.mutations_downloaded.load(Ordering::Relaxed),
            sync_errors: self.sync_errors.load(Ordering::Relaxed),
            last_sync_lag_ms: self.last_sync_lag_ms.load(Ordering::Relaxed),
        }
    }
}
