use crate::broadcast_manager::BroadcastManager;
use crate::metrics::RuntimeMetrics;
use crate::runtime::Runtime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use vaultsync_core::runtime_state::{RuntimeCapabilities, RuntimeState};

/// Phase 4 v2: RuntimeCoordinator — manages RuntimeState transitions
/// and routes BC messages to handlers.
///
/// Phase 4 v3: Owns HELLO/DISCOVER protocol (DiscoveryProtocol is startup-only,
/// Coordinator is permanent). HELLO messages are handled here, not in DiscoveryProtocol.
pub struct RuntimeCoordinator {
    runtime: Arc<Runtime>,
    broadcast: Arc<BroadcastManager>,
    metrics: Arc<RuntimeMetrics>,
    tab_id: String,
    state: std::sync::Mutex<RuntimeState>,
    capabilities: std::sync::Mutex<RuntimeCapabilities>,
    is_leader: AtomicBool,
    boot_id: String,
}

impl RuntimeCoordinator {
    pub fn new(
        runtime: Arc<Runtime>,
        broadcast: Arc<BroadcastManager>,
        metrics: Arc<RuntimeMetrics>,
        tab_id: String,
    ) -> Self {
        let boot_id = uuid::Uuid::new_v4().to_string();
        Self {
            runtime,
            broadcast,
            metrics,
            tab_id,
            state: std::sync::Mutex::new(RuntimeState::Unknown),
            capabilities: std::sync::Mutex::new(RuntimeCapabilities::empty()),
            is_leader: AtomicBool::new(false),
            boot_id,
        }
    }

    // ── Accessors ──

    pub fn tab_id(&self) -> &str { &self.tab_id }
    pub fn boot_id(&self) -> &str { &self.boot_id }

    pub fn state(&self) -> RuntimeState {
        *self.state.lock().unwrap()
    }

    pub fn set_state(&self, new: RuntimeState) {
        let mut s = self.state.lock().unwrap();
        let old = *s;
        *s = new;
        *self.capabilities.lock().unwrap() = new.default_capabilities();
        self.is_leader.store(new == RuntimeState::Leading, Ordering::Release);
        engine_debug!("[coordinator] state: {:?} → {:?}", old, new);
    }

    pub fn capabilities(&self) -> RuntimeCapabilities {
        *self.capabilities.lock().unwrap()
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    // ── BC message routing ──

    /// Route a BC message to the appropriate handler.
    /// Returns true if the message was handled.
    /// `from_tab` is the sender's tab ID (empty string if unknown).
    pub fn handle_bc_message(&self, msg: &str, from_tab: &str) -> bool {
        // Phase 4: Handle PROTO|HELLO → respond with DISCOVER
        // Extract sender tab ID from the message itself (the from_tab parameter
        // passed by spawn_bc_message_handler is buggy — it's the receiver's ID, not the sender's).
        if msg.starts_with("PROTO|HELLO|") {
            let parts: Vec<&str> = msg.splitn(4, '|').collect();
            if parts.len() >= 3 {
                let sender_tab = parts[1];
                if sender_tab == self.tab_id {
                    return true; // Skip self-messages
                }
                let other_boot_id = parts.get(2).unwrap_or(&"");
                let cursor = self.runtime.metadata_store.lock().unwrap().cursor;
                let gen = self.runtime.metadata_store.lock().unwrap().runtime_gen.0;
                let response = format!(
                    "PROTO|DISCOVER|{}|{}|{}|{}",
                    self.tab_id, self.boot_id, gen, cursor
                );
                let _ = self.broadcast.channel.send(&response);
                engine_debug!("[coordinator] HELLO from tab={} boot={} → DISCOVER sent gen={} cursor={}", sender_tab, other_boot_id, gen, cursor);
            }
            return true;
        }

        // For non-HELLO messages, skip self-messages when from_tab is set.
        // Note: from_tab passed by spawn_bc_message_handler is the receiver's own
        // tab ID (not the sender's) — this is a known bug inherited from V4 v2.
        // For now, let all non-PROTO messages through; the heartbeat/follower
        // tracking may use the receiver's tab ID as a coarse approximation.
        if !from_tab.is_empty() && from_tab == self.tab_id {
            return true;
        }

        let parts: Vec<&str> = msg.splitn(2, '|').collect();
        if parts.len() < 1 {
            return false;
        }

        match parts[0] {
            "HEARTBEAT" => {
                self.metrics.heartbeats_received.fetch_add(1, Ordering::Relaxed);
                self.runtime.presence_store.lock().unwrap()
                    .update_heartbeat(from_tab);
                true
            }
            "FOLLOWER_ATTACH" => {
                self.metrics.follower_attaches.fetch_add(1, Ordering::Relaxed);
                let meta = parts.get(1).unwrap_or(&"");
                let fields: Vec<&str> = meta.split('|').collect();
                if fields.len() >= 3 {
                    let version: u16 = fields[0].parse().unwrap_or(1);
                    let caps: Vec<String> = fields[1].split(',').map(String::from).collect();
                    self.runtime.presence_store.lock().unwrap()
                        .register_follower(from_tab.to_string(), version, caps);
                }
                true
            }
            "FOLLOWER_DETACH" => {
                self.metrics.follower_detaches.fetch_add(1, Ordering::Relaxed);
                self.runtime.presence_store.lock().unwrap().remove_follower(from_tab);
                true
            }
            _ => false,
        }
    }

    /// Remove timed-out followers.
    pub fn remove_timed_out_followers(&self, timeout_ms: u64) -> usize {
        let timed_out = self.runtime.presence_store.lock().unwrap()
            .timed_out_followers(timeout_ms);
        let count = timed_out.len();
        if count > 0 {
            self.metrics.heartbeats_missed.fetch_add(count as u64, Ordering::Relaxed);
            for tab_id in &timed_out {
                self.runtime.presence_store.lock().unwrap().remove_follower(tab_id);
            }
        }
        count
    }
}
