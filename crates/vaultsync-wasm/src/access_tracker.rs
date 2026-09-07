use std::collections::HashMap;

/// Phase 4: LFU access tracker with periodic decay.
/// Tracks document access frequency with exponential decay over time.
pub struct AccessTracker {
    counts: HashMap<String, (f64, u64)>, // doc_id → (frequency, last_decay_tick)
    decay_interval_ms: u64,
    decay_factor: f64,
}

impl AccessTracker {
    pub fn new(decay_interval_ms: u64, decay_factor: f64) -> Self {
        Self {
            counts: HashMap::new(),
            decay_interval_ms,
            decay_factor,
        }
    }

    pub fn record_access(&mut self, doc_id: &str) {
        let now = js_sys::Date::now() as u64;
        let (count, last_decay) = self
            .counts
            .entry(doc_id.to_string())
            .or_insert((0.0, now));

        if now - *last_decay >= self.decay_interval_ms {
            *count *= self.decay_factor;
            *last_decay = now;
        }
        *count += 1.0;
    }

    pub fn frequency(&self, doc_id: &str) -> f64 {
        self.counts.get(doc_id).map(|(c, _)| *c).unwrap_or(0.0)
    }

    pub fn top_n(&self, n: usize) -> Vec<String> {
        let mut sorted: Vec<(String, f64)> = self
            .counts
            .iter()
            .map(|(k, (v, _))| (k.clone(), *v))
            .collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted.into_iter().take(n).map(|(k, _)| k).collect()
    }

    pub fn boost(&mut self, doc_id: &str, amount: f64) {
        let now = js_sys::Date::now() as u64;
        let (count, last_decay) = self
            .counts
            .entry(doc_id.to_string())
            .or_insert((0.0, now));
        *count += amount;
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    pub fn clear(&mut self) {
        self.counts.clear();
    }
}
