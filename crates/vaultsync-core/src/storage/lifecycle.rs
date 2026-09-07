use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentState {
    Active,
    Sealed,
    Compacting,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub doc_id: String,
    pub record_id: String,
    pub new_state: SegmentState,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleStats {
    pub segments_cleaned: u64,
    pub events: Vec<LifecycleEvent>,
}

#[derive(Debug)]
pub struct LifecycleEngine {
    events: std::sync::Mutex<Vec<LifecycleEvent>>,
}

impl LifecycleEngine {
    pub fn new() -> Self {
        Self {
            events: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn record_event(&self, doc_id: &str, record_id: &str, new_state: SegmentState) {
        let mut events = self.events.lock().unwrap();
        events.push(LifecycleEvent {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            new_state,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
    }

    pub fn recent_events(&self, limit: usize) -> Vec<LifecycleEvent> {
        let events = self.events.lock().unwrap();
        let len = events.len();
        events
            .iter()
            .skip(len.saturating_sub(limit))
            .cloned()
            .collect()
    }

    pub fn should_seal(
        &self,
        segment_size: u64,
        max_size: u64,
    ) -> bool {
        segment_size >= max_size
    }
}

impl Default for LifecycleEngine {
    fn default() -> Self {
        Self::new()
    }
}
