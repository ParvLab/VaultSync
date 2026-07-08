use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub struct LruCache<K, V> {
    max_entries: usize,
    entries: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.entries.contains_key(key) {
            // Promote to most recently used
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
                self.order.push_back(key.clone());
            }
            self.entries.get(key)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.entries.contains_key(&key) {
            if let Some(pos) = self.order.iter().position(|k| *k == key) {
                self.order.remove(pos);
            }
        } else if self.entries.len() >= self.max_entries {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, value);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.entries.remove(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

/// Phase 2: SegmentedLRU — probation (20%) + protected (80%) segments.
/// Probation entries are evicted first; frequently accessed entries promote to protected.
#[derive(Debug)]
pub struct SegmentedLruCache<K, V> {
    probation: LruCache<K, V>,
    protected: LruCache<K, V>,
    probation_ratio: f64,
    max_total: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> SegmentedLruCache<K, V> {
    pub fn new(max_total: usize, probation_ratio: f64) -> Self {
        let probation_max = (max_total as f64 * probation_ratio) as usize;
        let protected_max = max_total.saturating_sub(probation_max);
        Self {
            probation: LruCache::new(std::cmp::max(1, probation_max)),
            protected: LruCache::new(std::cmp::max(1, protected_max)),
            probation_ratio,
            max_total,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        // Check protected first
        if let Some(val) = self.protected.get(key) {
            return Some(val.clone());
        }
        // Check probation; if found, promote to protected
        if self.probation.contains(key) {
            let val_clone = self.probation.remove(key);
            if let Some(v) = val_clone {
                self.protected.insert(key.clone(), v.clone());
                return Some(v);
            }
        }
        None
    }

    /// Insert new entry into probation segment.
    pub fn insert(&mut self, key: K, value: V) {
        if self.protected.contains(&key) {
            self.protected.insert(key, value);
            return;
        }
        self.probation.insert(key, value);
        // Rebalance if probation exceeds its ratio
        self.rebalance();
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.probation
            .remove(key)
            .or_else(|| self.protected.remove(key))
    }

    pub fn contains(&self, key: &K) -> bool {
        self.probation.contains(key) || self.protected.contains(key)
    }

    pub fn len(&self) -> usize {
        self.probation.len() + self.protected.len()
    }

    pub fn clear(&mut self) {
        self.probation.clear();
        self.protected.clear();
    }

    /// Demote oldest protected entries to probation until ratio is restored.
    fn rebalance(&mut self) {
        let total = self.probation.len() + self.protected.len();
        if total <= self.max_total {
            return;
        }
        let target_probation = (self.max_total as f64 * self.probation_ratio) as usize;
        while self.probation.len() > target_probation && self.probation.len() > 1 {
            // Evict from probation (oldest entries)
            // LruCache's order: front = oldest, back = newest
            // We can't directly evict from the front of LruCache, so we use its eviction behavior
            // by exceeding max_entries. Instead, let's just force-evict the front.
            let key = self.probation.order.front().cloned();
            if let Some(k) = key {
                self.probation.remove(&k);
            } else {
                break;
            }
        }
    }
}
