use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vaultsync_core::VaultSyncError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
    Broadcast,   // P0
    Upload,      // P1
    Heartbeat,   // P2
    Download,    // P2
    Prefetch,    // P3
    Snapshot,    // P3
    GarbageCollect, // P4
    Compaction,  // P5
    IdlePrefetch, // P6
}

impl TaskType {
    fn priority(&self) -> u8 {
        match self {
            TaskType::Broadcast => 0,
            TaskType::Upload => 1,
            TaskType::Heartbeat | TaskType::Download => 2,
            TaskType::Prefetch | TaskType::Snapshot => 3,
            TaskType::GarbageCollect => 4,
            TaskType::Compaction => 5,
            TaskType::IdlePrefetch => 6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub task_type: TaskType,
    pub name: &'static str,
    pub deadline_ms: Option<u64>,
    pub budget_ms: Option<u64>,
}

impl Task {
    pub fn new(task_type: TaskType) -> Self {
        Self {
            task_type,
            name: match task_type {
                TaskType::Broadcast => "broadcast",
                TaskType::Upload => "upload",
                TaskType::Heartbeat => "heartbeat",
                TaskType::Download => "download",
                TaskType::Prefetch => "prefetch",
                TaskType::Snapshot => "snapshot",
                TaskType::GarbageCollect => "gc",
                TaskType::Compaction => "compaction",
                TaskType::IdlePrefetch => "idle_prefetch",
            },
            deadline_ms: None,
            budget_ms: None,
        }
    }

    pub fn with_deadline(mut self, ms: u64) -> Self {
        self.deadline_ms = Some(ms);
        self
    }

    pub fn with_budget(mut self, ms: u64) -> Self {
        self.budget_ms = Some(ms);
        self
    }
}

#[derive(Debug, Clone)]
struct ScheduledItem {
    task: Task,
    scheduled_at: u64,
    // Lower priority value = higher priority
    effective_priority: u8,
}

impl Eq for ScheduledItem {}
impl PartialEq for ScheduledItem {
    fn eq(&self, other: &Self) -> bool {
        self.effective_priority == other.effective_priority
    }
}

impl PartialOrd for ScheduledItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is max-heap, so reverse for min-priority
        other
            .effective_priority
            .cmp(&self.effective_priority)
            .then_with(|| self.scheduled_at.cmp(&other.scheduled_at))
    }
}

pub struct RuntimeScheduler {
    queue: Arc<Mutex<BinaryHeap<ScheduledItem>>>,
    running: Arc<AtomicBool>,
    current_task: Arc<Mutex<Option<TaskType>>>,
    start_time: js_sys::Date,
}

impl Default for RuntimeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeScheduler {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
            running: Arc::new(AtomicBool::new(true)),
            current_task: Arc::new(Mutex::new(None)),
            start_time: js_sys::Date::new_0(),
        }
    }

    pub fn schedule(&self, task: Task) {
        let now = js_sys::Date::now() as u64;
        let item = ScheduledItem {
            effective_priority: task.task_type.priority(),
            scheduled_at: now,
            task,
        };
        let mut queue = self.queue.lock().unwrap();
        queue.push(item);
    }

    pub fn schedule_at(&self, task: Task, delay_ms: u64) {
        let now = js_sys::Date::now() as u64;
        let item = ScheduledItem {
            effective_priority: task.task_type.priority(),
            scheduled_at: now + delay_ms,
            task,
        };
        let mut queue = self.queue.lock().unwrap();
        queue.push(item);
    }

    pub fn dequeue(&self) -> Option<Task> {
        let now = js_sys::Date::now() as u64;
        let mut queue = self.queue.lock().unwrap();
        // Peek at top item
        loop {
            let item = queue.peek()?;
            if item.scheduled_at > now {
                return None;
            }
            let item = queue.pop()?;
            *self.current_task.lock().unwrap() = Some(item.task.task_type);
            return Some(item.task);
        }
    }

    pub fn task_done(&self) {
        *self.current_task.lock().unwrap() = None;
    }

    pub fn current_task_type(&self) -> Option<TaskType> {
        *self.current_task.lock().unwrap()
    }

    pub fn has_pending(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        !queue.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.queue.lock().unwrap().clear();
    }

    pub fn stop(&self) {
        self.running.store(false, AtomicOrdering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(AtomicOrdering::Acquire)
    }

    pub fn uptime_ms(&self) -> u64 {
        (js_sys::Date::now() - self.start_time.get_time()) as u64
    }
}
