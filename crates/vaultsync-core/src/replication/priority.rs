use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

impl Priority {
    pub fn numeric(&self) -> u8 {
        match self {
            Priority::Low => 0,
            Priority::Normal => 1,
            Priority::High => 2,
            Priority::Critical => 3,
        }
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.numeric().cmp(&other.numeric())
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct PriorityItem {
    pub doc_id: String,
    pub record_id: String,
    pub priority: Priority,
    pub size_bytes: u64,
}

impl Ord for PriorityItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for PriorityItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PriorityItem {}

impl PartialEq for PriorityItem {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id && self.record_id == other.record_id
    }
}

pub struct PriorityMutationQueue {
    heap: BinaryHeap<PriorityItem>,
}

impl PriorityMutationQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, item: PriorityItem) {
        self.heap.push(item);
    }

    pub fn pop(&mut self) -> Option<PriorityItem> {
        self.heap.pop()
    }

    pub fn peek(&self) -> Option<&PriorityItem> {
        self.heap.peek()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn drain(&mut self) -> Vec<PriorityItem> {
        let mut items = Vec::new();
        while let Some(item) = self.heap.pop() {
            items.push(item);
        }
        items
    }
}

impl Default for PriorityMutationQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn test_priority_queue_max_heap() {
        let mut queue = PriorityMutationQueue::new();
        queue.push(PriorityItem {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            priority: Priority::Low,
            size_bytes: 100,
        });
        queue.push(PriorityItem {
            doc_id: "d2".into(),
            record_id: "r2".into(),
            priority: Priority::Critical,
            size_bytes: 200,
        });
        queue.push(PriorityItem {
            doc_id: "d3".into(),
            record_id: "r3".into(),
            priority: Priority::High,
            size_bytes: 300,
        });

        assert_eq!(queue.len(), 3);
        let item = queue.pop().unwrap();
        assert_eq!(item.priority, Priority::Critical);
        assert_eq!(item.doc_id, "d2");
    }

    #[test]
    fn test_priority_queue_drain() {
        let mut queue = PriorityMutationQueue::new();
        queue.push(PriorityItem {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            priority: Priority::Low,
            size_bytes: 100,
        });
        queue.push(PriorityItem {
            doc_id: "d2".into(),
            record_id: "r2".into(),
            priority: Priority::High,
            size_bytes: 200,
        });

        let items = queue.drain();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].priority, Priority::High);
        assert_eq!(items[1].priority, Priority::Low);
    }

    #[test]
    fn test_priority_queue_empty() {
        let mut queue = PriorityMutationQueue::new();
        assert!(queue.is_empty());
        assert!(queue.pop().is_none());
    }
}
