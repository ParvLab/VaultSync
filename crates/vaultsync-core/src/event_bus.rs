use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEvent {
    DocumentRead {
        doc_id: String,
        record_id: String,
        workspace_id: Option<u64>,
        size_bytes: u64,
    },
    DocumentWritten {
        doc_id: String,
        record_id: String,
        workspace_id: Option<u64>,
        size_bytes: u64,
    },
    DocumentUpdated {
        doc_id: String,
        record_id: String,
        workspace_id: Option<u64>,
        size_bytes: u64,
    },
    DocumentDeleted {
        doc_id: String,
        record_id: String,
        workspace_id: Option<u64>,
    },
    QueryExecuted {
        workspace_id: Option<u64>,
        filter_hash: u64,
        result_count: usize,
    },
    WorkspaceActivated {
        workspace_id: u64,
        namespace: String,
    },
    WorkspaceClosed {
        workspace_id: u64,
    },
    SnapshotCreated {
        namespace: String,
        doc_id: String,
        record_id: String,
        sequence: u64,
    },
    SegmentCompacted {
        namespace: String,
        segment_id: String,
        document_count: usize,
        bytes: u64,
    },
    DownloadCompleted {
        namespace: String,
        mutations_processed: u64,
    },
    UploadCompleted {
        namespace: String,
        mutations_pushed: u64,
    },
    LeaderChanged {
        is_leader: bool,
        tab_id: String,
    },
    NetworkChanged {
        tier: String,
        latency_ms: u64,
    },
}

pub type SubscriberId = u64;

pub struct EventBus {
    subscribers: RwLock<HashMap<SubscriberId, Box<dyn Fn(&EngineEvent) + Send + Sync>>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn subscribe<F>(&self, handler: F) -> SubscriberId
    where
        F: Fn(&EngineEvent) + Send + Sync + 'static,
    {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Ok(mut subs) = self.subscribers.write() {
            subs.insert(id, Box::new(handler));
        }
        id
    }

    pub fn unsubscribe(&self, id: SubscriberId) {
        if let Ok(mut subs) = self.subscribers.write() {
            subs.remove(&id);
        }
    }

    pub fn publish(&self, event: &EngineEvent) {
        if let Ok(subs) = self.subscribers.read() {
            for handler in subs.values() {
                handler(event);
            }
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().map(|s| s.len()).unwrap_or(0)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_create_event_bus() {
        let bus = EventBus::new();
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[test]
    fn test_subscribe_and_publish() {
        let bus = EventBus::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        bus.subscribe(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(bus.subscriber_count(), 1);

        bus.publish(&EngineEvent::DocumentRead {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            workspace_id: None,
            size_bytes: 100,
        });

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let count = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let c = count.clone();
            bus.subscribe(move |_event| {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        assert_eq!(bus.subscriber_count(), 3);

        bus.publish(&EngineEvent::QueryExecuted {
            workspace_id: None,
            filter_hash: 0,
            result_count: 5,
        });

        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_unsubscribe() {
        let bus = EventBus::new();
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        let id = bus.subscribe(move |_event| {
            c.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(bus.subscriber_count(), 1);

        bus.publish(&EngineEvent::LeaderChanged {
            is_leader: true,
            tab_id: "tab1".into(),
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);

        bus.unsubscribe(id);
        assert_eq!(bus.subscriber_count(), 0);

        bus.publish(&EngineEvent::LeaderChanged {
            is_leader: false,
            tab_id: "tab1".into(),
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_event_types() {
        let bus = EventBus::new();
        let received = Arc::new(RwLock::new(Vec::new()));
        let r = received.clone();

        bus.subscribe(move |event| {
            if let Ok(mut v) = r.write() {
                v.push(format!("{:?}", event));
            }
        });

        bus.publish(&EngineEvent::DocumentRead {
            doc_id: "d1".into(),
            record_id: "r1".into(),
            workspace_id: Some(1),
            size_bytes: 100,
        });
        bus.publish(&EngineEvent::SnapshotCreated {
            namespace: "ns".into(),
            doc_id: "d2".into(),
            record_id: "r2".into(),
            sequence: 42,
        });

        let events = received.read().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].contains("DocumentRead"));
        assert!(events[1].contains("SnapshotCreated"));
    }
}
