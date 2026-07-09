use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct RunningStats {
    count: AtomicU64,
    sum: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl Default for RunningStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }
}

impl RunningStats {
    pub fn record(&self, value: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.min.fetch_min(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    pub fn avg(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.sum.load(Ordering::Relaxed) as f64 / count as f64
    }
}

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    // Cache (SegmentedLRU — per-segment breakdown)
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub slru_probation_hits: AtomicU64,
    pub slru_protected_hits: AtomicU64,
    pub slru_promotions: AtomicU64,
    pub slru_demotions: AtomicU64,

    // Storage
    pub page_reads: AtomicU64,
    pub page_writes: AtomicU64,
    pub page_deletes: AtomicU64,
    pub page_enumerations: AtomicU64,

    // Bus
    pub mutations_sent: AtomicU64,
    pub mutations_received: AtomicU64,
    pub mutations_rejected_gen: AtomicU64,
    pub mutations_gap_recovered: AtomicU64,
    pub snapshots_sent: AtomicU64,
    pub snapshots_received: AtomicU64,
    pub hot_docs_pushed: AtomicU64,
    pub hot_docs_hit: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub heartbeats_missed: AtomicU64,
    pub heartbeats_received: AtomicU64,
    // HELLO/DISCOVER protocol (Phase 4 v2)
    pub hello_sent: AtomicU64,
    pub hello_received: AtomicU64,
    pub discover_sent: AtomicU64,
    pub discover_received: AtomicU64,
    pub snapshot_requests_sent: AtomicU64,
    pub snapshot_requests_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bus_messages_dropped: AtomicU64,

    // Recovery
    pub wal_entries_replayed: AtomicU64,
    pub manifest_checksum_failures: AtomicU64,
    pub manifest_rebuilds: AtomicU64,
    pub content_index_repairs: AtomicU64,
    pub recovery_duration_ms: AtomicU64,

    // Lifecycle
    pub leader_promotions: AtomicU64,
    pub leader_demotions: AtomicU64,
    pub follower_attaches: AtomicU64,
    pub follower_detaches: AtomicU64,
    pub promotion_candidate_attempts: AtomicU64,
    pub hydration_time_ms: AtomicU64,
    pub failover_time_ms: AtomicU64,

    // Phase 8 — Runtime performance metrics
    pub startup_ms: AtomicU64,           // total startup duration
    pub snapshot_size: AtomicU64,        // bytes in snapshot payloads
    pub pending_count: AtomicU64,        // current pending mutations
    pub page_count: AtomicU64,           // total pages across all stores
    pub tombstone_count: AtomicU64,      // tombstone pages
    pub live_pages: AtomicU64,           // live page count
    pub dirty_pages: AtomicU64,          // dirty (un-uploaded) page count

    // Latency histograms
    pub mutation_latency_us: RunningStats,
    pub bc_latency_us: RunningStats,
    pub page_read_latency_us: RunningStats,
    pub page_write_latency_us: RunningStats,
    pub recovery_latency_us: RunningStats,
    pub failover_latency_us: RunningStats,
    pub prefetch_latency_us: RunningStats,
    /// Phase 8: Upload/download/compaction latency
    pub upload_latency_us: RunningStats,
    pub download_latency_us: RunningStats,
    pub compaction_latency_us: RunningStats,
}

impl RuntimeMetrics {
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 1.0;
        }
        hits as f64 / total as f64
    }

    pub fn snapshot_json(&self) -> String {
        let hit_ratio = self.cache_hit_ratio();
        let protected = self.slru_protected_hits.load(Ordering::Relaxed);
        let probation = self.slru_probation_hits.load(Ordering::Relaxed);
        let protected_ratio = if protected + probation > 0 {
            protected as f64 / (protected + probation) as f64
        } else {
            0.0
        };

        serde_json::json!({
            "cacheHitRatio": hit_ratio,
            "slruProtectedRatio": protected_ratio,
            "pageReads": self.page_reads.load(Ordering::Relaxed),
            "pageWrites": self.page_writes.load(Ordering::Relaxed),
            "pageEnumerations": self.page_enumerations.load(Ordering::Relaxed),
            "mutationsSent": self.mutations_sent.load(Ordering::Relaxed),
            "mutationsReceived": self.mutations_received.load(Ordering::Relaxed),
            "hotDocsPushed": self.hot_docs_pushed.load(Ordering::Relaxed),
            "hotDocsHit": self.hot_docs_hit.load(Ordering::Relaxed),
            "busMessagesDropped": self.bus_messages_dropped.load(Ordering::Relaxed),
            "leaderPromotions": self.leader_promotions.load(Ordering::Relaxed),
            "followerAttaches": self.follower_attaches.load(Ordering::Relaxed),
            "hydrationTimeMs": self.hydration_time_ms.load(Ordering::Relaxed),
            "failoverTimeMs": self.failover_time_ms.load(Ordering::Relaxed),
            "recoveryDurationMs": self.recovery_duration_ms.load(Ordering::Relaxed),
            "manifestChecksumFailures": self.manifest_checksum_failures.load(Ordering::Relaxed),
            "startupMs": self.startup_ms.load(Ordering::Relaxed),
            "snapshotSize": self.snapshot_size.load(Ordering::Relaxed),
            "pendingCount": self.pending_count.load(Ordering::Relaxed),
            "pageCount": self.page_count.load(Ordering::Relaxed),
            "tombstoneCount": self.tombstone_count.load(Ordering::Relaxed),
            "livePages": self.live_pages.load(Ordering::Relaxed),
            "dirtyPages": self.dirty_pages.load(Ordering::Relaxed),
            "mutationLatencyAvgUs": self.mutation_latency_us.avg(),
            "recoveryLatencyAvgUs": self.recovery_latency_us.avg(),
            "failoverLatencyAvgUs": self.failover_latency_us.avg(),
            "uploadLatencyAvgUs": self.upload_latency_us.avg(),
            "downloadLatencyAvgUs": self.download_latency_us.avg(),
            "compactionLatencyAvgUs": self.compaction_latency_us.avg(),
        }).to_string()
    }
}
