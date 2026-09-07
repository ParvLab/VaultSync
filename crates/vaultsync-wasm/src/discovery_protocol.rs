use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::time_utils;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// Result of leader discovery.
#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub tab_id: String,
    pub boot_id: String,
    pub generation: u64,
    pub cursor: u64,
}

/// Boot state shared between the lightweight BC handler and the builder.
///
/// The lightweight handler is registered immediately after BC creation and
/// stays active until the full RuntimeCoordinator handler replaces it.
///
/// State transitions:
///   UNKNOWN(0) → BUILDING(1) → LEADER(2)
#[derive(Debug, Clone)]
pub struct SharedBootState {
    /// 0=unknown, 1=building(engine init in progress), 2=leader(ready)
    pub state: Arc<AtomicU8>,
    /// Set to true when DISCOVER or READY is received from another tab
    pub found: Arc<AtomicBool>,
    /// Set to true when BUILDING is received from another tab (extends wait)
    pub building_seen: Arc<AtomicBool>,
    /// The last-discovered leader info
    pub result: Arc<Mutex<Option<LeaderInfo>>>,
    /// Deadline extension time (in ms from start) when building_seen triggers
    pub deadline_ext: Arc<AtomicU64>,
    pub my_tab_id: String,
}

impl SharedBootState {
    pub const UNKNOWN: u8 = 0;
    pub const BUILDING: u8 = 1;
    pub const LEADER: u8 = 2;

    pub fn new(my_tab_id: String) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(Self::BUILDING)),
            found: Arc::new(AtomicBool::new(false)),
            building_seen: Arc::new(AtomicBool::new(false)),
            result: Arc::new(Mutex::new(None)),
            deadline_ext: Arc::new(AtomicU64::new(3000)), // 3s extension when building seen
            my_tab_id,
        }
    }
}

/// HELLO/DISCOVER protocol for leader election bootstrap.
///
/// Phase 4 v3: Protocol messages use the PROTO| namespace.
/// The lightweight BC handler is created separately (in client.rs) and
/// uses SharedBootState for coordination with wait_for_leader.
pub struct DiscoveryProtocol {
    bc: web_sys::BroadcastChannel,
    tab_id: String,
    boot_id: String,
}

impl DiscoveryProtocol {
    pub fn new(bc: web_sys::BroadcastChannel, tab_id: String) -> Self {
        let boot_id = uuid::Uuid::new_v4().to_string();
        Self { bc, tab_id, boot_id }
    }

    pub fn tab_id(&self) -> &str { &self.tab_id }
    pub fn boot_id(&self) -> &str { &self.boot_id }

    /// Send PROTO|HELLO on BC.
    pub fn send_hello(&self) {
        let msg = format!("PROTO|HELLO|{}|{}", self.tab_id, self.boot_id);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Respond to HELLO with DISCOVER — provides full leader state.
    pub fn send_discover(&self, generation: u64, cursor: u64) {
        let msg = format!("PROTO|DISCOVER|{}|{}|{}|{}", self.tab_id, self.boot_id, generation, cursor);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Announce that THIS tab is building the runtime (becoming leader).
    pub fn announce_building(&self) {
        let msg = format!("PROTO|BUILDING|{}|{}", self.tab_id, self.boot_id);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Announce that THIS tab's runtime is fully READY (leader).
    pub fn announce_ready(&self, cursor: u64) {
        let msg = format!("PROTO|READY|{}|{}|{}", self.tab_id, self.boot_id, cursor);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Announce that THIS tab is leaving (beforeunload or demotion).
    pub fn announce_left(&self) {
        let msg = format!("PROTO|LEFT|{}|{}", self.tab_id, self.boot_id);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Create a lightweight BC handler closure that:
    /// 1. Responds to HELLO from other tabs (if building or leader)
    /// 2. Records DISCOVER/READY from other tabs (triggers MirrorRuntime)
    /// 3. Records BUILDING from other tabs (extends wait timeout)
    ///
    /// This handler is registered before send_hello() and stays active
    /// until the full engine handler replaces it.
    ///
    /// Returns (Closure, state) — the Closure must be.forget()'d.
    pub fn create_lightweight_handler(&self, state: &SharedBootState) -> wasm_bindgen::closure::Closure<dyn FnMut(web_sys::MessageEvent)> {
        let my_tid = self.tab_id.clone();
        let my_bid = self.boot_id.clone();
        let s = state.state.clone();
        let f = state.found.clone();
        let bs = state.building_seen.clone();
        let r = state.result.clone();
        let state_my_tid = state.my_tab_id.clone();

        let bc_for_responding = self.bc.clone();

        Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
            if let Some(msg) = e.data().as_string() {
                // Ignore self-messages
                if msg.starts_with("PROTO|HELLO|") {
                    let parts: Vec<&str> = msg.splitn(4, '|').collect();
                    if parts.len() >= 3 && parts[1] != state_my_tid {
                        let cur_state = s.load(Ordering::Relaxed);
                        if cur_state == SharedBootState::LEADER {
                            let response = format!("PROTO|DISCOVER|{}|{}|0|0", my_tid, my_bid);
                            let _ = bc_for_responding.post_message(&JsValue::from_str(&response));
                        } else if cur_state == SharedBootState::BUILDING {
                            let response = format!("PROTO|BUILDING|{}|{}", my_tid, my_bid);
                            let _ = bc_for_responding.post_message(&JsValue::from_str(&response));
                        }
                    }
                    return;
                }

                // Record leader announcements from other tabs
                let is_ready = msg.starts_with("PROTO|DISCOVER|") || msg.starts_with("PROTO|READY|");
                let is_building = msg.starts_with("PROTO|BUILDING|");

                if is_ready || is_building {
                    let parts: Vec<&str> = msg.splitn(5, '|').collect();
                    if parts.len() >= 3 && parts[1] != state_my_tid {
                        if is_ready {
                            // DISCOVER or READY: leader is ready → take MirrorRuntime
                            let info = LeaderInfo {
                                tab_id: parts[1].to_string(),
                                boot_id: parts.get(2).unwrap_or(&"").to_string(),
                                generation: parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0),
                                cursor: parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0),
                            };
                            *r.lock().unwrap() = Some(info);
                            f.store(true, Ordering::Relaxed);
                        } else if is_building {
                            // BUILDING: another tab is becoming leader → extend our wait
                            bs.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }) as Box<dyn FnMut(web_sys::MessageEvent)>)
    }

    /// Poll for leader discovery (lightweight handler must be registered first).
    ///
    /// Behavior:
    /// - DISCOVER/READY received → returns immediately with LeaderInfo
    /// - BUILDING received → extends deadline so we wait for the building tab to finish
    /// - Nothing received before timeout → returns None (self-elect as leader)
    pub async fn wait_for_leader(&self, state: &SharedBootState, initial_timeout_ms: u64) -> Option<LeaderInfo> {
        let started = js_sys::Date::now();
        let max_timeout = initial_timeout_ms.max(200);

        // Dynamic deadline: starts at initial_timeout, extends when building_seen
        let deadline = Arc::new(std::sync::Mutex::new(started + max_timeout as f64));
        let d = deadline.clone();
        // Check building_seen every iteration and extend deadline
        loop {
            let now = js_sys::Date::now();
            let current_deadline = *d.lock().unwrap();

            if state.found.load(Ordering::Relaxed) {
                return state.result.lock().unwrap().take();
            }

            if state.building_seen.load(Ordering::Relaxed) {
                // Extend deadline: wait up to 5s total for the building tab
                let extended = started + state.deadline_ext.load(Ordering::Relaxed) as f64;
                if extended > current_deadline {
                    *d.lock().unwrap() = extended;
                    engine_debug!("[Discovery] building_seen — extended deadline to {:.0}ms", extended - started);
                }
            }

            if now >= current_deadline {
                return None;
            }

            time_utils::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
