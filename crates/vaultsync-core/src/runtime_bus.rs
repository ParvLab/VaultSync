use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::runtime_state::RuntimeState;

pub type SubscriberId = u64;

/// LeaderEvent — emitted by LeaderElection when leadership state changes.
///
/// All leadership transitions flow through this event type.
/// VaultRuntime subscribes to drive RuntimeState transitions.
#[derive(Debug, Clone)]
pub enum LeaderEvent {
    /// Lock acquired — this tab is now the leader.
    Acquired { timestamp_ms: f64 },
    /// Acquire(Immediate) returned false — another tab holds the lock.
    Waiting,
    /// This tab released the lock (graceful demotion).
    Released,
    /// Lock unexpectedly lost (browser API error, tab crash, etc.).
    Lost,
    /// Acquisition failed with an error.
    Failed(String),
}

/// High-level runtime event — backward compatible with existing users.
///
/// This is the event type used by subsystems that currently reference
/// RuntimeBus. As new typed buses are introduced (LeaderEvent,
/// SessionEvent, etc.), subsystems will migrate to their specific bus.
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    DocumentInvalidated {
        doc_id: String,
        record_id: String,
    },
    StateChanged {
        from: RuntimeState,
        to: RuntimeState,
    },
    LeadershipAcquired {
        tab_id: String,
    },
    LeadershipLost {
        tab_id: String,
    },
    MutationReceived {
        doc_id: String,
        record_id: String,
        fields_json: String,
    },
    Shutdown,
}

/// Generic typed event bus.
///
/// All runtime events flow through this same abstraction:
///   RuntimeBus<LeaderEvent>
///   RuntimeBus<SessionEvent>
///   RuntimeBus<StorageEvent>
///   RuntimeBus<MaintenanceEvent>
///
/// Subsystems subscribe to specific bus instances rather than
/// communicating directly with each other.
pub struct RuntimeBus<E> {
    subscribers: RwLock<HashMap<SubscriberId, Box<dyn Fn(&E) + Send + Sync>>>,
    next_id: AtomicU64,
}

impl<E: Send + 'static> RuntimeBus<E> {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn subscribe<F>(&self, handler: F) -> SubscriberId
    where
        F: Fn(&E) + Send + Sync + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
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

    pub fn publish(&self, event: &E) {
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

impl<E: Send + 'static> Default for RuntimeBus<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_generic_bus_leader_event() {
        let bus = RuntimeBus::<LeaderEvent>::new();
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        bus.subscribe(move |_event: &LeaderEvent| {
            c.fetch_add(1, Ordering::SeqCst);
        });

        bus.publish(&LeaderEvent::Waiting);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_generic_bus_runtime_event() {
        let bus = RuntimeBus::<RuntimeEvent>::new();
        // No subscribers — no crash
        bus.publish(&RuntimeEvent::Shutdown);
        bus.publish(&RuntimeEvent::Shutdown);
    }

    #[test]
    fn test_multiple_subscribers() {
        let bus = RuntimeBus::<RuntimeEvent>::new();
        let count = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let c = count.clone();
            bus.subscribe(move |_event: &RuntimeEvent| {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        assert_eq!(bus.subscriber_count(), 3);
        bus.publish(&RuntimeEvent::Shutdown);
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_unsubscribe() {
        let bus = RuntimeBus::<RuntimeEvent>::new();
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        let id = bus.subscribe(move |_event: &RuntimeEvent| {
            c.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(bus.subscriber_count(), 1);

        bus.publish(&RuntimeEvent::Shutdown);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        bus.unsubscribe(id);
        assert_eq!(bus.subscriber_count(), 0);

        bus.publish(&RuntimeEvent::Shutdown);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
