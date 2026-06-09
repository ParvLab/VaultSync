use std::collections::HashMap;
use std::sync::Arc;
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
    by_doc_id: HashMap<String, Vec<u64>>,
    next_handle: u64,
    global_listener: Option<Arc<dyn Fn(&str, &str) + Send + Sync>>,
}

impl SubscriptionEngine {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            by_doc_id: HashMap::new(),
            next_handle: 1,
            global_listener: None,
        }
    }

    pub fn set_global_listener(&mut self, listener: Arc<dyn Fn(&str, &str) + Send + Sync>) {
        self.global_listener = Some(listener);
    }

    pub fn register(&mut self, doc_id: &str, callback: Callback) -> SubscriptionHandle {
        let handle_id = self.next_handle;
        let handle = SubscriptionHandle(handle_id);
        
        self.subscriptions.insert(handle_id, Subscription {
            doc_id: doc_id.to_string(),
            handle: handle.clone(),
            callback,
        });
        
        self.by_doc_id.entry(doc_id.to_string())
            .or_default()
            .push(handle_id);

        self.next_handle += 1;
        handle
    }

    pub fn unregister(&mut self, handle: SubscriptionHandle) -> Result<(), DriftError> {
        let sub = self.subscriptions.remove(&handle.0)
            .ok_or_else(|| DriftError::Storage("subscription not found".into()))?;
        
        if let Some(handles) = self.by_doc_id.get_mut(&sub.doc_id) {
            handles.retain(|&h| h != handle.0);
            if handles.is_empty() {
                self.by_doc_id.remove(&sub.doc_id);
            }
        }
        
        Ok(())
    }

    pub fn fire(&self, doc_id: &str, record_id: &str, state: &HashMap<String, CrdtValue>) {
        if let Some(ref listener) = self.global_listener {
            listener(doc_id, record_id);
        }
        self.fire_local(doc_id, record_id, state);
    }

    pub fn fire_local(&self, doc_id: &str, record_id: &str, state: &HashMap<String, CrdtValue>) {
        if let Some(handles) = self.by_doc_id.get(doc_id) {
            for &handle_id in handles {
                if let Some(sub) = self.subscriptions.get(&handle_id) {
                    (sub.callback)(doc_id, record_id, state);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }
}

