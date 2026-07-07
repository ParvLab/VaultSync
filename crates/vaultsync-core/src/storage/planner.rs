/// Actions the planner can recommend.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanAction {
    SplitPage { page_id: u64 },
    MergeDeltas { base_id: u64, delta_count: usize },
    GarbageCollect { threshold_bytes: u64 },
    CreateSnapshot { namespace: String },
    FlushManifest,
    EvictPages { count: usize },
}

/// Metrics describing a single page's state.
#[derive(Debug, Clone)]
pub struct PageMetrics {
    pub page_id: u64,
    pub byte_size: usize,
    pub entry_count: usize,
    pub is_base: bool,
}

/// Overall storage metrics for the planner.
#[derive(Debug, Clone, Default)]
pub struct StorageMetrics {
    pub pages: Vec<PageMetrics>,
    pub total_bytes: u64,
    pub delta_page_count: usize,
    pub base_page_count: usize,
    pub pending_mutation_count: usize,
    pub last_compaction_at: u64,
    pub idle_time_ms: u64,
}

const SOFT_SPLIT_BYTES: usize = 8_000_000;
const MAX_ENTRIES_PER_PAGE: usize = 4096;
const MAX_DELTA_PAGES_BEFORE_MERGE: usize = 50;

/// Evaluates storage metrics and recommends actions.
pub struct StoragePlanner;

impl StoragePlanner {
    pub fn evaluate(&self, metrics: &StorageMetrics) -> Vec<PlanAction> {
        let mut actions = Vec::new();

        // 1. Check for oversized or overpopulated pages
        for page in &metrics.pages {
            if page.is_base {
                let needs_split = page.byte_size > SOFT_SPLIT_BYTES
                    || page.entry_count > MAX_ENTRIES_PER_PAGE;

                if needs_split {
                    actions.push(PlanAction::SplitPage {
                        page_id: page.page_id,
                    });
                }
            }
        }

        // 2. Check if too many delta pages need merging
        if metrics.delta_page_count > MAX_DELTA_PAGES_BEFORE_MERGE {
            // Find the base page with the most deltas
            if let Some(base) = metrics.pages.iter().find(|p| p.is_base) {
                actions.push(PlanAction::MergeDeltas {
                    base_id: base.page_id,
                    delta_count: metrics.delta_page_count,
                });
            }
        }

        // 3. Suggest GC if many pending entries
        if metrics.pending_mutation_count > 5000 {
            actions.push(PlanAction::GarbageCollect {
                threshold_bytes: 1024 * 1024, // 1MB
            });
        }

        // 4. Suggest manifest flush if idle
        if metrics.idle_time_ms > 30_000 {
            actions.push(PlanAction::FlushManifest);
        }

        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_metrics_no_actions() {
        let planner = StoragePlanner;
        let metrics = StorageMetrics::default();
        let actions = planner.evaluate(&metrics);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_oversized_page_triggers_split() {
        let planner = StoragePlanner;
        let metrics = StorageMetrics {
            pages: vec![PageMetrics {
                page_id: 1,
                byte_size: SOFT_SPLIT_BYTES + 1,
                entry_count: 100,
                is_base: true,
            }],
            ..Default::default()
        };
        let actions = planner.evaluate(&metrics);
        assert!(actions.iter().any(|a| matches!(a, PlanAction::SplitPage { .. })));
    }

    #[test]
    fn test_too_many_entries_triggers_split() {
        let planner = StoragePlanner;
        let metrics = StorageMetrics {
            pages: vec![PageMetrics {
                page_id: 1,
                byte_size: 1000,
                entry_count: MAX_ENTRIES_PER_PAGE + 1,
                is_base: true,
            }],
            ..Default::default()
        };
        let actions = planner.evaluate(&metrics);
        assert!(actions.iter().any(|a| matches!(a, PlanAction::SplitPage { .. })));
    }

    #[test]
    fn test_too_many_deltas_triggers_merge() {
        let planner = StoragePlanner;
        let metrics = StorageMetrics {
            pages: vec![PageMetrics {
                page_id: 1,
                byte_size: 1000,
                entry_count: 10,
                is_base: true,
            }],
            delta_page_count: MAX_DELTA_PAGES_BEFORE_MERGE + 1,
            ..Default::default()
        };
        let actions = planner.evaluate(&metrics);
        assert!(actions.iter().any(|a| matches!(a, PlanAction::MergeDeltas { .. })));
    }
}
