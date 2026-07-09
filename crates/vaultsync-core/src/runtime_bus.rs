use std::sync::Mutex;

/// Runtime event — all subsystem communication flows through this bus.
///
/// No subsystem talks to another directly. Every interaction is an event
/// published on RuntimeBus and dispatched to all subscribers.
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// A document changed (source: Storage, target: UI/Mirror)
    DocumentInvalidated {
        doc_id: String,
        record_id: String,
    },
    /// Runtime reached a new state (source: Runtime, target: all)
    StateChanged {
        from: super::runtime_state::RuntimeState,
        to: super::runtime_state::RuntimeState,
    },
    /// A follower attached (source: BroadcastAdapter, target: RuntimeCoordinator)
    FollowerAttached {
        tab_id: String,
        generation: u64,
    },
    /// Leadership was acquired (source: WebLock, target: Runtime)
    LeadershipAcquired {
        tab_id: String,
    },
    /// Leadership was lost (source: WebLock, target: Runtime)
    LeadershipLost {
        tab_id: String,
    },
    /// A push mutation arrived (source: Coordinator, target: Reconciler)
    MutationReceived {
        doc_id: String,
        record_id: String,
        fields_json: String,
    },
    /// Runtime snapshot requested (source: MirrorRuntime, target: RuntimeCoordinator)
    SnapshotRequested {
        tab_id: String,
        reason: String,
    },
    /// Shutdown signal (source: beforeunload, target: all)
    Shutdown,
}

/// Subscriber trait — any subsystem can subscribe to RuntimeBus events.
pub trait RuntimeSubscriber: Send + 'static {
    fn on_event(&mut self, event: &RuntimeEvent);
}

/// Simple pub/sub event bus.
///
/// Not mpsc — multiple subscribers, synchronous dispatch.
/// Async queues are the caller's responsibility (schedule work, don't do it inline).
pub struct RuntimeBus {
    subscribers: Mutex<Vec<Box<dyn RuntimeSubscriber>>>,
}

impl RuntimeBus {
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn subscribe(&self, subscriber: Box<dyn RuntimeSubscriber>) {
        self.subscribers.lock().unwrap().push(subscriber);
    }

    /// Dispatch event to all subscribers. Runs synchronously — subscribers
    /// should schedule heavy work, not execute it inline.
    pub fn dispatch(&self, event: &RuntimeEvent) {
        let mut subs = self.subscribers.lock().unwrap();
        for sub in subs.iter_mut() {
            sub.on_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestSubscriber {
        count: Arc<AtomicUsize>,
        name: &'static str,
    }

    impl RuntimeSubscriber for TestSubscriber {
        fn on_event(&mut self, _event: &RuntimeEvent) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_dispatch_to_multiple_subscribers() {
        let bus = RuntimeBus::new();
        let c1 = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::new(AtomicUsize::new(0));

        bus.subscribe(Box::new(TestSubscriber { count: c1.clone(), name: "a" }));
        bus.subscribe(Box::new(TestSubscriber { count: c2.clone(), name: "b" }));

        bus.dispatch(&RuntimeEvent::Shutdown);

        assert_eq!(c1.load(Ordering::Relaxed), 1);
        assert_eq!(c2.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_multiple_events() {
        let bus = RuntimeBus::new();
        let c = Arc::new(AtomicUsize::new(0));

        bus.subscribe(Box::new(TestSubscriber { count: c.clone(), name: "a" }));

        for _ in 0..5 {
            bus.dispatch(&RuntimeEvent::Shutdown);
        }

        assert_eq!(c.load(Ordering::Relaxed), 5);
    }
}
