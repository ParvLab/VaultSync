use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPolicy {
    pub max_segment_size: u64,
    pub max_tombstone_ratio: f64,
    pub min_compaction_interval: Duration,
    pub max_documents_per_batch: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_segment_size: 4 * 1024 * 1024,
            max_tombstone_ratio: 0.25,
            min_compaction_interval: Duration::from_secs(300),
            max_documents_per_batch: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub seq: u64,
    pub doc_id: String,
    pub record_id: String,
    pub checksum: u32,
    pub byte_len: u64,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionStats {
    pub documents_compacted: u64,
    pub tombstones_removed: u64,
    pub bytes_saved: u64,
    pub duration_ms: u64,
    pub snapshots_created: Vec<SnapshotMetadata>,
}

#[derive(Debug, Clone)]
pub struct CompactionDecision {
    pub should_run: bool,
    pub reason: CompactionReason,
}

#[derive(Debug, Clone)]
pub enum CompactionReason {
    NotNeeded,
    TombstoneRatioExceeded { ratio: f64, threshold: f64 },
    SegmentFull { size: u64, max: u64 },
    ManualTrigger,
}

#[derive(Debug)]
pub struct CompactionEngine {
    policy: CompactionPolicy,
    last_run: std::sync::Mutex<Option<std::time::Instant>>,
}

impl CompactionEngine {
    pub fn new(policy: CompactionPolicy) -> Self {
        Self {
            policy,
            last_run: std::sync::Mutex::new(None),
        }
    }

    pub fn policy(&self) -> &CompactionPolicy {
        &self.policy
    }

    pub fn should_compact(
        &self,
        total_documents: u64,
        tombstone_count: u64,
    ) -> CompactionDecision {
        if total_documents == 0 {
            return CompactionDecision {
                should_run: false,
                reason: CompactionReason::NotNeeded,
            };
        }

        let last = self.last_run.lock().unwrap();
        if let Some(last_time) = *last {
            if last_time.elapsed() < self.policy.min_compaction_interval {
                return CompactionDecision {
                    should_run: false,
                    reason: CompactionReason::NotNeeded,
                };
            }
        }

        if tombstone_count > 0 {
            let ratio = tombstone_count as f64 / total_documents as f64;
            if ratio >= self.policy.max_tombstone_ratio {
                return CompactionDecision {
                    should_run: true,
                    reason: CompactionReason::TombstoneRatioExceeded {
                        ratio,
                        threshold: self.policy.max_tombstone_ratio,
                    },
                };
            }
        }

        CompactionDecision {
            should_run: false,
            reason: CompactionReason::NotNeeded,
        }
    }

    pub fn mark_run(&self) {
        let mut last = self.last_run.lock().unwrap();
        *last = Some(std::time::Instant::now());
    }
}


