use std::collections::HashMap;
use crate::error::DriftError;
use crate::crdt::types::CrdtValue;

pub type Callback = Box<dyn Fn(&str, &str, &HashMap<String, CrdtValue>) + Send>;

#[derive(Debug, Clone)]
pub struct SubscriptionHandle(u64);

pub struct Subscription {
    pub doc_id: String,
    pub handle: SubscriptionHandle,
    pub callback: Callback,
}

pub struct SubscriptionEngine {
    subscriptions: HashMap<u64, Subscription>,
    next_handle: u64,
}

impl SubscriptionEngine {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_handle: 1,
        }
    }

    pub fn register(&mut self, doc_id: &str, callback: Callback) -> SubscriptionHandle {
        let handle = SubscriptionHandle(self.next_handle);
        self.subscriptions.insert(self.next_handle, Subscription {
            doc_id: doc_id.to_string(),
            handle: handle.clone(),
            callback,
        });
        self.next_handle += 1;
        handle
    }

    pub fn unregister(&mut self, handle: SubscriptionHandle) -> Result<(), DriftError> {
        self.subscriptions.remove(&handle.0)
            .ok_or_else(|| DriftError::Storage("subscription not found".into()))?;
        Ok(())
    }

    pub fn fire(&self, doc_id: &str, record_id: &str, state: &HashMap<String, CrdtValue>) {
        for sub in self.subscriptions.values() {
            if sub.doc_id == doc_id {
                (sub.callback)(doc_id, record_id, state);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }
}
