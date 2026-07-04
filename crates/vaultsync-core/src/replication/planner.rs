use serde::{Deserialize, Serialize};
use std::sync::RwLock;

use super::priority::Priority;
use super::network::{NetworkMonitor, BandwidthEstimator, NetworkTier};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPlan {
    pub priority_mutations: Vec<MutationRef>,
    pub background_pull: PullRange,
    pub snapshot_opportunities: Vec<SnapshotRef>,
    pub estimated_duration_ms: u64,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationRef {
    pub doc_id: String,
    pub record_id: String,
    pub sequence: u64,
    pub priority: Priority,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRange {
    pub from_sequence: u64,
    pub to_sequence: u64,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub doc_id: String,
    pub record_id: String,
    pub segment_id: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Urgency {
    Low,
    Medium,
    High,
    Critical,
}

pub struct ReplicationPlanner {
    network: RwLock<NetworkMonitor>,
    bandwidth: RwLock<BandwidthEstimator>,
    pending_mutations: RwLock<Vec<MutationRef>>,
    observed_patterns: RwLock<ObservationBuffer>,
}

#[derive(Debug, Clone, Default)]
struct ObservationBuffer {
    recent_queries: Vec<QueryObservation>,
    recent_documents: Vec<DocumentObservation>,
    max_observations: usize,
}

#[derive(Debug, Clone)]
struct QueryObservation {
    workspace_id: Option<u64>,
    filter_hash: u64,
    timestamp_ms: u64,
}

#[derive(Debug, Clone)]
struct DocumentObservation {
    doc_id: String,
    record_id: String,
    access_time_ms: u64,
    workspace_id: Option<u64>,
}

impl ReplicationPlanner {
    pub fn new() -> Self {
        Self {
            network: RwLock::new(NetworkMonitor::new()),
            bandwidth: RwLock::new(BandwidthEstimator::new()),
            pending_mutations: RwLock::new(Vec::new()),
            observed_patterns: RwLock::new(ObservationBuffer {
                max_observations: 1000,
                ..Default::default()
            }),
        }
    }

    pub fn observe_query(&self, workspace_id: Option<u64>, filter_hash: u64, now_ms: u64) {
        if let Ok(mut obs) = self.observed_patterns.write() {
            obs.recent_queries.push(QueryObservation {
                workspace_id,
                filter_hash,
                timestamp_ms: now_ms,
            });
            if obs.recent_queries.len() > obs.max_observations {
                obs.recent_queries.remove(0);
            }
        }
    }

    pub fn observe_document_access(
        &self,
        doc_id: &str,
        record_id: &str,
        workspace_id: Option<u64>,
        now_ms: u64,
    ) {
        if let Ok(mut obs) = self.observed_patterns.write() {
            obs.recent_documents.push(DocumentObservation {
                doc_id: doc_id.to_string(),
                record_id: record_id.to_string(),
                access_time_ms: now_ms,
                workspace_id,
            });
            if obs.recent_documents.len() > obs.max_observations {
                obs.recent_documents.remove(0);
            }
        }
    }

    pub fn predict_next_documents(&self, limit: usize) -> Vec<(String, String)> {
        let obs = match self.observed_patterns.read() {
            Ok(obs) => obs,
            Err(_) => return Vec::new(),
        };

        let mut predictions: Vec<(String, String)> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for doc_obs in obs.recent_documents.iter().rev() {
            let key = (doc_obs.doc_id.clone(), doc_obs.record_id.clone());
            if seen.insert(key.clone()) {
                predictions.push(key);
                if predictions.len() >= limit {
                    break;
                }
            }
        }

        predictions
    }

    pub fn add_pending_mutation(&self, mutation: MutationRef) {
        if let Ok(mut pending) = self.pending_mutations.write() {
            pending.push(mutation);
        }
    }

    pub fn compute_plan(
        &self,
        local_cursor: u64,
        server_max_sequence: u64,
        budget_bytes: u64,
    ) -> ReplicationPlan {
        let _network_tier = self.network.read().map(|n| n.current_tier()).unwrap_or(NetworkTier::WiFi);
        let bps = self.bandwidth.read().map(|b| b.estimated_bps()).unwrap_or(1_000_000);

        let mut pending = self.pending_mutations.write().unwrap_or_else(|e| e.into_inner());
        pending.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut priority_mutations = Vec::new();
        let mut total_bytes = 0u64;

        for mutation in pending.iter() {
            if total_bytes + mutation.size_bytes > budget_bytes {
                break;
            }
            total_bytes += mutation.size_bytes;
            priority_mutations.push(mutation.clone());
        }

        let remaining_budget = budget_bytes.saturating_sub(total_bytes);
        let bytes_per_mutation = if !pending.is_empty() {
            total_bytes / pending.len() as u64
        } else {
            1024
        };

        let _background_mutations = if bytes_per_mutation > 0 {
            (remaining_budget / bytes_per_mutation) as usize
        } else {
            0
        };

        let background_pull = if local_cursor < server_max_sequence {
            let from = local_cursor + 1;
            let to = server_max_sequence;
            let estimated_mutation_bytes = 512;
            let estimated_pull_bytes = (to - from) * estimated_mutation_bytes;
            PullRange {
                from_sequence: from,
                to_sequence: to,
                estimated_bytes: estimated_pull_bytes.min(remaining_budget),
            }
        } else {
            PullRange {
                from_sequence: local_cursor,
                to_sequence: local_cursor,
                estimated_bytes: 0,
            }
        };

        let estimated_duration_ms = if bps > 0 {
            (total_bytes * 1000) / bps
        } else {
            0
        };

        ReplicationPlan {
            priority_mutations,
            background_pull,
            snapshot_opportunities: Vec::new(),
            estimated_duration_ms,
            estimated_bytes: total_bytes,
        }
    }

    pub fn on_network_change(&self, tier: NetworkTier) {
        if let Ok(mut network) = self.network.write() {
            network.set_tier(tier);
        }
    }

    pub fn on_bandwidth_change(&self, bps: u64) {
        if let Ok(mut bandwidth) = self.bandwidth.write() {
            bandwidth.record_sample(bps, 1000);
        }
    }

    pub fn hint_prefetch(
        &self,
        doc_id: &str,
        record_id: &str,
        urgency: Urgency,
        size_bytes: u64,
    ) {
        let priority = match urgency {
            Urgency::Low => Priority::Low,
            Urgency::Medium => Priority::Normal,
            Urgency::High => Priority::High,
            Urgency::Critical => Priority::Critical,
        };

        self.add_pending_mutation(MutationRef {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            sequence: 0,
            priority,
            size_bytes,
        });
    }

    pub fn clear_pending(&self) {
        if let Ok(mut pending) = self.pending_mutations.write() {
            pending.clear();
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending_mutations.read().map(|p| p.len()).unwrap_or(0)
    }
}

impl Default for ReplicationPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_planner() {
        let planner = ReplicationPlanner::new();
        assert_eq!(planner.pending_count(), 0);
    }

    #[test]
    fn test_add_pending_mutation() {
        let planner = ReplicationPlanner::new();
        planner.add_pending_mutation(MutationRef {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            sequence: 1,
            priority: Priority::High,
            size_bytes: 1024,
        });
        assert_eq!(planner.pending_count(), 1);
    }

    #[test]
    fn test_compute_plan_empty() {
        let planner = ReplicationPlanner::new();
        let plan = planner.compute_plan(100, 100, 10_000);
        assert!(plan.priority_mutations.is_empty());
        assert_eq!(plan.background_pull.from_sequence, 100);
        assert_eq!(plan.background_pull.to_sequence, 100);
    }

    #[test]
    fn test_compute_plan_with_pending() {
        let planner = ReplicationPlanner::new();
        planner.add_pending_mutation(MutationRef {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            sequence: 1,
            priority: Priority::High,
            size_bytes: 1024,
        });
        planner.add_pending_mutation(MutationRef {
            doc_id: "d2".into(),
            record_id: "r2".into(),
            sequence: 2,
            priority: Priority::Normal,
            size_bytes: 2048,
        });

        let plan = planner.compute_plan(0, 10, 5000);
        assert!(!plan.priority_mutations.is_empty());
        assert!(plan.estimated_bytes > 0);
    }

    #[test]
    fn test_compute_plan_background_pull() {
        let planner = ReplicationPlanner::new();
        let plan = planner.compute_plan(50, 100, 10_000);
        assert_eq!(plan.background_pull.from_sequence, 51);
        assert_eq!(plan.background_pull.to_sequence, 100);
    }

    #[test]
    fn test_observe_document_access() {
        let planner = ReplicationPlanner::new();
        planner.observe_document_access("d1", "r1", Some(1), 1000);
        planner.observe_document_access("d2", "r2", Some(1), 2000);

        let predictions = planner.predict_next_documents(5);
        assert_eq!(predictions.len(), 2);
        assert_eq!(predictions[0].0, "d2");
        assert_eq!(predictions[1].0, "d1");
    }

    #[test]
    fn test_predict_deduplicates() {
        let planner = ReplicationPlanner::new();
        planner.observe_document_access("d1", "r1", Some(1), 1000);
        planner.observe_document_access("d1", "r1", Some(1), 2000);
        planner.observe_document_access("d2", "r2", Some(1), 3000);

        let predictions = planner.predict_next_documents(5);
        assert_eq!(predictions.len(), 2);
    }

    #[test]
    fn test_hint_prefetch() {
        let planner = ReplicationPlanner::new();
        planner.hint_prefetch("d1", "r1", Urgency::Critical, 1024);
        assert_eq!(planner.pending_count(), 1);
    }

    #[test]
    fn test_clear_pending() {
        let planner = ReplicationPlanner::new();
        planner.add_pending_mutation(MutationRef {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            sequence: 1,
            priority: Priority::High,
            size_bytes: 1024,
        });
        assert_eq!(planner.pending_count(), 1);

        planner.clear_pending();
        assert_eq!(planner.pending_count(), 0);
    }

    #[test]
    fn test_network_change() {
        let planner = ReplicationPlanner::new();
        planner.on_network_change(NetworkTier::Cellular);
        assert_eq!(planner.network.read().unwrap().current_tier(), NetworkTier::Cellular);
    }

    #[test]
    fn test_bandwidth_change() {
        let planner = ReplicationPlanner::new();
        planner.on_bandwidth_change(5_000_000);
        assert!(planner.bandwidth.read().unwrap().estimated_bps() > 0);
    }
}
