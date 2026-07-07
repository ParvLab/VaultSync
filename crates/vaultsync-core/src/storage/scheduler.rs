use super::planner::{PlanAction, StorageMetrics};
use std::collections::VecDeque;

/// Priority for scheduling an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchedulePriority {
    Critical,   // Do now (page >16MB, entries >8192)
    High,       // Do soon (page >8MB, entries >4096)
    Normal,     // Do eventually (delta merge, GC)
    Low,        // Do when idle (flush manifest)
}

/// A scheduled action with priority.
#[derive(Debug, Clone)]
pub struct ScheduledAction {
    pub action: PlanAction,
    pub priority: SchedulePriority,
}

/// Determines priority for a given action.
fn priority_for(action: &PlanAction, metrics: &StorageMetrics) -> SchedulePriority {
    match action {
        PlanAction::SplitPage { page_id } => {
            if let Some(page) = metrics.pages.iter().find(|p| p.page_id == *page_id) {
                if page.byte_size > 16_000_000 || page.entry_count > 8192 {
                    return SchedulePriority::Critical;
                }
                if page.byte_size > 8_000_000 || page.entry_count > 4096 {
                    return SchedulePriority::High;
                }
            }
            SchedulePriority::Normal
        }
        PlanAction::MergeDeltas { .. } => SchedulePriority::Normal,
        PlanAction::GarbageCollect { .. } => SchedulePriority::Low,
        PlanAction::CreateSnapshot { .. } => SchedulePriority::Normal,
        PlanAction::FlushManifest => SchedulePriority::Low,
        PlanAction::EvictPages { .. } => SchedulePriority::Normal,
    }
}

/// Tracks when each action was last executed to throttle.
#[derive(Debug, Default)]
pub struct SchedulerState {
    last_executed: VecDeque<(String, u64)>, // (action_descriptor, timestamp_ms)
}

impl SchedulerState {
    /// Returns the minimum interval between executions of the same class of action.
    fn cooldown_ms(&self, action: &PlanAction) -> u64 {
        match action {
            PlanAction::SplitPage { .. } => 5000,
            PlanAction::MergeDeltas { .. } => 2000,
            PlanAction::GarbageCollect { .. } => 10_000,
            PlanAction::CreateSnapshot { .. } => 30_000,
            PlanAction::FlushManifest => 500,
            PlanAction::EvictPages { .. } => 5000,
        }
    }

    fn descriptor(action: &PlanAction) -> String {
        match action {
            PlanAction::SplitPage { page_id } => format!("split_{}", page_id),
            PlanAction::MergeDeltas { base_id, .. } => format!("merge_{}", base_id),
            PlanAction::GarbageCollect { .. } => "gc".to_string(),
            PlanAction::CreateSnapshot { namespace } => format!("snap_{}", namespace),
            PlanAction::FlushManifest => "flush".to_string(),
            PlanAction::EvictPages { .. } => "evict".to_string(),
        }
    }

    /// Check if this action was executed too recently.
    fn is_on_cooldown(&self, action: &PlanAction, now_ms: u64) -> bool {
        let desc = Self::descriptor(action);
        let cooldown = self.cooldown_ms(action);
        self.last_executed
            .iter()
            .any(|(d, t)| *d == desc && now_ms - *t < cooldown)
    }

    /// Record that an action was executed.
    fn record_execution(&mut self, action: &PlanAction, now_ms: u64) {
        let desc = Self::descriptor(action);
        self.last_executed.push_back((desc, now_ms));
        while self.last_executed.len() > 100 {
            self.last_executed.pop_front();
        }
    }
}

/// Decides when to execute planned actions.
#[derive(Debug)]
pub struct CompactionScheduler {
    state: SchedulerState,
}

impl CompactionScheduler {
    pub fn new() -> Self {
        Self {
            state: SchedulerState::default(),
        }
    }

    /// Given planned actions and current metrics, return the subset to execute now
    /// with their priority ordering (highest first).
    pub fn schedule(
        &mut self,
        actions: &[PlanAction],
        metrics: &StorageMetrics,
        now_ms: u64,
    ) -> Vec<ScheduledAction> {
        let mut scheduled: Vec<ScheduledAction> = actions
            .iter()
            .filter(|a| !self.state.is_on_cooldown(a, now_ms))
            .map(|a| {
                let priority = priority_for(a, metrics);
                ScheduledAction {
                    action: a.clone(),
                    priority,
                }
            })
            .collect();

        scheduled.sort_by(|a, b| a.priority.cmp(&b.priority));
        scheduled
    }

    /// Record that an action was executed (updates cooldown tracking).
    pub fn record_execution(&mut self, action: &PlanAction, now_ms: u64) {
        self.state.record_execution(action, now_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metrics() -> StorageMetrics {
        StorageMetrics {
            pages: vec![
                super::super::planner::PageMetrics {
                    page_id: 1,
                    byte_size: 10_000_000,
                    entry_count: 5000,
                    is_base: true,
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn test_critical_split_gets_highest_priority() {
        let metrics = StorageMetrics {
            pages: vec![
                super::super::planner::PageMetrics {
                    page_id: 1,
                    byte_size: 17_000_000,
                    entry_count: 9000,
                    is_base: true,
                },
            ],
            ..Default::default()
        };
        let actions = vec![PlanAction::FlushManifest, PlanAction::SplitPage { page_id: 1 }];
        let mut scheduler = CompactionScheduler::new();
        let scheduled = scheduler.schedule(&actions, &metrics, 0);
        assert_eq!(scheduled.len(), 2);
        assert!(matches!(scheduled[0].priority, SchedulePriority::Critical));
        assert_eq!(scheduled[0].action, PlanAction::SplitPage { page_id: 1 });
    }

    #[test]
    fn test_cooldown_suppresses_repeated_actions() {
        let metrics = sample_metrics();
        let mut scheduler = CompactionScheduler::new();
        let split = PlanAction::SplitPage { page_id: 1 };

        // First run — action is scheduled
        let s1 = scheduler.schedule(&[split.clone()], &metrics, 1000);
        assert_eq!(s1.len(), 1);

        // Record execution
        scheduler.record_execution(&split, 1000);

        // Second run at same time — cooldown active
        let s2 = scheduler.schedule(&[split.clone()], &metrics, 1000);
        assert_eq!(s2.len(), 0);

        // After cooldown expires — action is allowed again
        let s3 = scheduler.schedule(&[split.clone()], &metrics, 7000);
        assert_eq!(s3.len(), 1);
    }

    #[test]
    fn test_low_priority_action_is_last() {
        let metrics = sample_metrics();
        let actions = vec![
            PlanAction::FlushManifest,
            PlanAction::GarbageCollect { threshold_bytes: 1024 * 1024 },
            PlanAction::MergeDeltas { base_id: 1, delta_count: 10 },
        ];
        let mut scheduler = CompactionScheduler::new();
        let scheduled = scheduler.schedule(&actions, &metrics, 0);

        assert_eq!(scheduled.len(), 3);
        // Normal before Low
        assert!(scheduled[2].priority == SchedulePriority::Low);
    }
}
