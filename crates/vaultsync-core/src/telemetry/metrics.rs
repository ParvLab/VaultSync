use serde::Serialize;
use std::sync::OnceLock;

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub mutations_uploaded: u64,
    pub mutations_downloaded: u64,
    pub sync_errors: u64,
    pub last_sync_lag_ms: u64,
    pub pending_mutations: u64,
    pub replica_count: u64,
    pub doc_count: u64,
}

#[cfg(feature = "telemetry")]
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

#[cfg(feature = "telemetry")]
pub struct VaultSyncMetrics {
    pub mutations_total: IntCounterVec,
    pub mutations_failed: IntCounterVec,
    pub conflicts_total: IntCounterVec,
    pub mutations_pending: IntGaugeVec,
    pub connection_status: IntGaugeVec,
    pub leader_status: IntGaugeVec,
    pub oplog_size: IntGaugeVec,
    pub replica_count: IntGaugeVec,
    pub doc_count: IntGaugeVec,
    pub sync_lag_ms: HistogramVec,
    pub download_lag_ms: HistogramVec,
    pub encryption_time_us: HistogramVec,
    pub crdt_merge_time_us: HistogramVec,
}

#[cfg(feature = "telemetry")]
impl VaultSyncMetrics {
    pub fn new() -> Self {
        let registry = get_registry();

        let mutations_total = IntCounterVec::new(
            Opts::new(
                "vaultsync_mutations_total",
                "Total number of mutations attempted",
            ),
            &["status", "namespace"],
        )
        .unwrap();
        let mutations_failed = IntCounterVec::new(
            Opts::new(
                "vaultsync_mutations_failed",
                "Total number of failed mutations",
            ),
            &["namespace", "reason"],
        )
        .unwrap();
        let conflicts_total = IntCounterVec::new(
            Opts::new(
                "vaultsync_conflicts_total",
                "Total number of sync conflicts encountered",
            ),
            &[],
        )
        .unwrap();
        let mutations_pending = IntGaugeVec::new(
            Opts::new(
                "vaultsync_mutations_pending",
                "Number of mutations pending sync",
            ),
            &["namespace"],
        )
        .unwrap();
        let connection_status = IntGaugeVec::new(
            Opts::new(
                "vaultsync_connection_status",
                "Connection status (1=connected, 0=disconnected)",
            ),
            &["namespace"],
        )
        .unwrap();
        let leader_status = IntGaugeVec::new(
            Opts::new(
                "vaultsync_leader_status",
                "Leader status (1=leader, 0=reader)",
            ),
            &[],
        )
        .unwrap();
        let oplog_size = IntGaugeVec::new(
            Opts::new("vaultsync_oplog_size", "Size of the oplog in entries"),
            &["namespace"],
        )
        .unwrap();
        let replica_count = IntGaugeVec::new(
            Opts::new("vaultsync_replica_count", "Number of known replicas"),
            &["namespace"],
        )
        .unwrap();
        let doc_count = IntGaugeVec::new(
            Opts::new("vaultsync_doc_count", "Number of documents"),
            &["doc_id"],
        )
        .unwrap();
        let sync_lag_ms = HistogramVec::new(
            HistogramOpts::new("vaultsync_sync_lag_ms", "Sync lag in milliseconds")
                .buckets(vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]),
            &["namespace"],
        )
        .unwrap();
        let download_lag_ms = HistogramVec::new(
            HistogramOpts::new("vaultsync_download_lag_ms", "Download lag in milliseconds")
                .buckets(vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]),
            &["namespace"],
        )
        .unwrap();
        let encryption_time_us = HistogramVec::new(
            HistogramOpts::new(
                "vaultsync_encryption_time_us",
                "Encryption/decryption time in microseconds",
            )
            .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]),
            &["operation"],
        )
        .unwrap();
        let crdt_merge_time_us = HistogramVec::new(
            HistogramOpts::new(
                "vaultsync_crdt_merge_time_us",
                "CRDT merge time in microseconds",
            )
            .buckets(vec![5.0, 25.0, 100.0, 500.0, 1000.0, 5000.0]),
            &["doc_id"],
        )
        .unwrap();

        let _ = registry.register(Box::new(mutations_total.clone()));
        let _ = registry.register(Box::new(mutations_failed.clone()));
        let _ = registry.register(Box::new(conflicts_total.clone()));
        let _ = registry.register(Box::new(mutations_pending.clone()));
        let _ = registry.register(Box::new(connection_status.clone()));
        let _ = registry.register(Box::new(leader_status.clone()));
        let _ = registry.register(Box::new(oplog_size.clone()));
        let _ = registry.register(Box::new(replica_count.clone()));
        let _ = registry.register(Box::new(doc_count.clone()));
        let _ = registry.register(Box::new(sync_lag_ms.clone()));
        let _ = registry.register(Box::new(download_lag_ms.clone()));
        let _ = registry.register(Box::new(encryption_time_us.clone()));
        let _ = registry.register(Box::new(crdt_merge_time_us.clone()));

        Self {
            mutations_total,
            mutations_failed,
            conflicts_total,
            mutations_pending,
            connection_status,
            leader_status,
            oplog_size,
            replica_count,
            doc_count,
            sync_lag_ms,
            download_lag_ms,
            encryption_time_us,
            crdt_merge_time_us,
        }
    }

    pub fn record_upload(&self, count: usize) {
        self.mutations_total
            .with_label_values(&["success", "default"])
            .inc_by(count as u64);
    }

    pub fn record_download(&self, count: usize) {
        self.mutations_total
            .with_label_values(&["success", "default"])
            .inc_by(count as u64);
    }

    pub fn record_sync_lag(&self, ms: f64) {
        self.sync_lag_ms.with_label_values(&["default"]).observe(ms);
    }

    pub fn record_sync_error(&self) {
        self.mutations_failed
            .with_label_values(&["default", "sync_error"])
            .inc();
    }

    pub fn record_mutation_attempt(&self, namespace: &str, status: &str) {
        self.mutations_total
            .with_label_values(&[status, namespace])
            .inc();
    }

    pub fn record_mutation_failed(&self, namespace: &str, reason: &str) {
        self.mutations_failed
            .with_label_values(&[namespace, reason])
            .inc();
    }

    pub fn record_conflict(&self) {
        self.conflicts_total.with_label_values(&[]).inc();
    }

    pub fn set_mutations_pending(&self, namespace: &str, count: i64) {
        self.mutations_pending
            .with_label_values(&[namespace])
            .set(count);
    }

    pub fn set_connection_status(&self, namespace: &str, connected: bool) {
        self.connection_status
            .with_label_values(&[namespace])
            .set(if connected { 1 } else { 0 });
    }

    pub fn set_leader_status(&self, leader: bool) {
        self.leader_status
            .with_label_values(&[])
            .set(if leader { 1 } else { 0 });
    }

    pub fn set_oplog_size(&self, namespace: &str, size: i64) {
        self.oplog_size.with_label_values(&[namespace]).set(size);
    }

    pub fn set_replica_count(&self, namespace: &str, count: i64) {
        self.replica_count
            .with_label_values(&[namespace])
            .set(count);
    }

    pub fn set_doc_count(&self, doc_id: &str, count: i64) {
        self.doc_count.with_label_values(&[doc_id]).set(count);
    }

    pub fn record_download_lag(&self, namespace: &str, ms: f64) {
        self.download_lag_ms
            .with_label_values(&[namespace])
            .observe(ms);
    }

    pub fn record_encryption_time(&self, operation: &str, us: f64) {
        self.encryption_time_us
            .with_label_values(&[operation])
            .observe(us);
    }

    pub fn record_crdt_merge_time(&self, doc_id: &str, us: f64) {
        self.crdt_merge_time_us
            .with_label_values(&[doc_id])
            .observe(us);
    }

    pub fn record_key_rotation(&self, _namespace: &str) {
        // No-op or we can increment key rotation counters if registry exists.
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            mutations_uploaded: self
                .mutations_total
                .with_label_values(&["success", "default"])
                .get(),
            mutations_downloaded: self
                .mutations_total
                .with_label_values(&["success", "default"])
                .get(),
            sync_errors: self
                .mutations_failed
                .with_label_values(&["default", "sync_error"])
                .get(),
            last_sync_lag_ms: self
                .sync_lag_ms
                .with_label_values(&["default"])
                .get_sample_sum() as u64,
            pending_mutations: self.mutations_pending.with_label_values(&["default"]).get() as u64,
            replica_count: self.replica_count.with_label_values(&["default"]).get() as u64,
            doc_count: 0,
        }
    }
}

#[cfg(feature = "telemetry")]
static REGISTRY: OnceLock<Registry> = OnceLock::new();

#[cfg(feature = "telemetry")]
static METRICS: OnceLock<VaultSyncMetrics> = OnceLock::new();

#[cfg(feature = "telemetry")]
pub fn get_registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

#[cfg(feature = "telemetry")]
pub fn get_metrics() -> &'static VaultSyncMetrics {
    METRICS.get_or_init(VaultSyncMetrics::new)
}

// Fallback when telemetry feature is disabled
#[cfg(not(feature = "telemetry"))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(feature = "telemetry"))]
pub struct VaultSyncMetrics {
    pub mutations_uploaded: AtomicU64,
    pub mutations_downloaded: AtomicU64,
    pub sync_errors: AtomicU64,
    pub last_sync_lag_ms: AtomicU64,
}

#[cfg(not(feature = "telemetry"))]
impl VaultSyncMetrics {
    pub fn new() -> Self {
        Self {
            mutations_uploaded: AtomicU64::new(0),
            mutations_downloaded: AtomicU64::new(0),
            sync_errors: AtomicU64::new(0),
            last_sync_lag_ms: AtomicU64::new(0),
        }
    }

    pub fn record_upload(&self, count: usize) {
        self.mutations_uploaded
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_download(&self, count: usize) {
        self.mutations_downloaded
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_sync_lag(&self, ms: f64) {
        self.last_sync_lag_ms.store(ms as u64, Ordering::Relaxed);
    }

    pub fn record_sync_error(&self) {
        self.sync_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mutation_attempt(&self, _namespace: &str, _status: &str) {}
    pub fn record_mutation_failed(&self, _namespace: &str, _reason: &str) {}
    pub fn record_conflict(&self) {}
    pub fn set_mutations_pending(&self, _namespace: &str, _count: i64) {}
    pub fn set_connection_status(&self, _namespace: &str, _connected: bool) {}
    pub fn set_leader_status(&self, _leader: bool) {}
    pub fn set_oplog_size(&self, _namespace: &str, _size: i64) {}
    pub fn set_replica_count(&self, _namespace: &str, _count: i64) {}
    pub fn set_doc_count(&self, _doc_id: &str, _count: i64) {}
    pub fn record_download_lag(&self, _namespace: &str, _ms: f64) {}
    pub fn record_encryption_time(&self, _operation: &str, _us: f64) {}
    pub fn record_crdt_merge_time(&self, _doc_id: &str, _us: f64) {}
    pub fn record_key_rotation(&self, _namespace: &str) {}

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            mutations_uploaded: self.mutations_uploaded.load(Ordering::Relaxed),
            mutations_downloaded: self.mutations_downloaded.load(Ordering::Relaxed),
            sync_errors: self.sync_errors.load(Ordering::Relaxed),
            last_sync_lag_ms: self.last_sync_lag_ms.load(Ordering::Relaxed),
            pending_mutations: 0,
            replica_count: 0,
            doc_count: 0,
        }
    }
}

#[cfg(not(feature = "telemetry"))]
static METRICS: OnceLock<VaultSyncMetrics> = OnceLock::new();

#[cfg(not(feature = "telemetry"))]
pub fn get_metrics() -> &'static VaultSyncMetrics {
    METRICS.get_or_init(VaultSyncMetrics::new)
}
