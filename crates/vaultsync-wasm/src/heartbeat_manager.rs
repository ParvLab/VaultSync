use crate::broadcast_manager::BroadcastManager;
use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Phase 4: HeartbeatManager — sends periodic heartbeats and detects follower timeouts.
pub struct HeartbeatManager {
    runtime: Arc<Runtime>,
    broadcast: Arc<BroadcastManager>,
    metrics: Arc<RuntimeMetrics>,
    interval_ms: u64,
    timeout_ms: u64,
}

impl HeartbeatManager {
    pub fn new(
        runtime: Arc<Runtime>,
        broadcast: Arc<BroadcastManager>,
        metrics: Arc<RuntimeMetrics>,
        interval_ms: u64,
        timeout_ms: u64,
    ) -> Self {
        Self {
            runtime,
            broadcast,
            metrics,
            interval_ms,
            timeout_ms,
        }
    }

    pub fn send_heartbeat(&self) {
        self.broadcast.broadcast_heartbeat();
    }

    pub fn check_timeouts(&self) -> Vec<String> {
        let timed_out = self.runtime.presence_store.lock().unwrap()
            .timed_out_followers(self.timeout_ms);
        let count = timed_out.len();
        if count > 0 {
            self.metrics.heartbeats_missed.fetch_add(count as u64, Ordering::Relaxed);
        }
        timed_out
    }

    pub fn remove_timed_out(&self) -> usize {
        let timed_out = self.check_timeouts();
        let count = timed_out.len();
        for tab_id in &timed_out {
            self.runtime.presence_store.lock().unwrap().remove_follower(tab_id);
        }
        count
    }

    pub fn interval_ms(&self) -> u64 { self.interval_ms }
    pub fn timeout_ms(&self) -> u64 { self.timeout_ms }
}
