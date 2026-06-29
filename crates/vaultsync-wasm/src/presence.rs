use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use vaultsync_core::telemetry::metrics::VaultSyncMetrics;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent};

/// Message exchanged via BroadcastChannel for presence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PresenceMessage {
    #[serde(rename = "join")]
    Join {
        replica_id: String,
        namespace: String,
        timestamp: u64,
    },
    #[serde(rename = "leave")]
    Leave {
        replica_id: String,
        namespace: String,
    },
    #[serde(rename = "heartbeat")]
    Heartbeat {
        replica_id: String,
        namespace: String,
        timestamp: u64,
    },
}

/// Manages multi-tab presence awareness via BroadcastChannel.
///
/// Each tab announces join/leave/heartbeat so peers know who is active.
/// Exported to JavaScript via `#[wasm_bindgen]`.
#[wasm_bindgen]
#[derive(Clone)]
pub struct PresenceManager {
    bc: BroadcastChannel,
    replica_id: String,
    namespace: String,
    peers: Arc<Mutex<HashMap<String, u64>>>, // replica_id -> last_seen ms
    metrics: &'static VaultSyncMetrics,
}

#[wasm_bindgen]
impl PresenceManager {
    /// Create and start announcing presence on a dedicated BroadcastChannel.
    /// The channel name is `"vaultsync-presence-{namespace}"`.
    #[wasm_bindgen(constructor)]
    pub fn new(
        namespace: &str,
        replica_id: &str,
    ) -> Result<PresenceManager, JsValue> {
        let channel_name = format!("vaultsync-presence-{}", namespace);
        let bc = BroadcastChannel::new(&channel_name)?;
        let metrics = vaultsync_core::telemetry::metrics::get_metrics();

        let peers = Arc::new(Mutex::new(HashMap::new()));
        let peers_clone = peers.clone();
        let metrics_clone = metrics;
        let _bc_clone = bc.clone();
        let ns = namespace.to_string();

        // Listen for presence messages
        let callback = Closure::wrap(Box::new(move |event: MessageEvent| {
            if let Ok(ab) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&ab);
                let bytes = array.to_vec();
                if let Ok(msg) = serde_json::from_slice::<PresenceMessage>(&bytes) {
                    let is_leave = matches!(&msg, PresenceMessage::Leave { .. });
                    let replica_id = match &msg {
                        PresenceMessage::Join { replica_id, .. }
                        | PresenceMessage::Leave { replica_id, .. }
                        | PresenceMessage::Heartbeat { replica_id, .. } => replica_id.clone(),
                    };
                    let mut p = peers_clone.lock().unwrap();
                    let prev_len = p.len();
                    if is_leave {
                        p.remove(&replica_id);
                    } else {
                        p.insert(replica_id, js_sys::Date::now() as u64);
                    }
                    if p.len() != prev_len {
                        metrics_clone.set_active_peers(p.len() as u64);
                    }
                }
            }
        }) as Box<dyn FnMut(_)>);

        bc.set_onmessage(Some(callback.as_ref().unchecked_ref()));
        callback.forget(); // Leak intentionally — lives for program lifetime

        let mgr = Self {
            bc: bc.clone(),
            replica_id: replica_id.to_string(),
            namespace: ns.clone(),
            peers,
            metrics,
        };

        // Announce join
        mgr.send_presence(PresenceMessage::Join {
            replica_id: replica_id.to_string(),
            namespace: ns.clone(),
            timestamp: js_sys::Date::now() as u64,
        })?;

        // Register beforeunload to announce leave
        let bc_leave = bc.clone();
        let rid = replica_id.to_string();
        let nid = ns.clone();
        let leave_callback = Closure::wrap(Box::new(move || {
            let leave = PresenceMessage::Leave {
                replica_id: rid.clone(),
                namespace: nid.clone(),
            };
            if let Ok(json) = serde_json::to_vec(&leave) {
                let array = js_sys::Uint8Array::from(&json[..]);
                let _ = bc_leave.post_message(&array);
            }
        }) as Box<dyn FnMut()>);
        let window = web_sys::window().ok_or("No window")?;
        window.set_onbeforeunload(Some(leave_callback.as_ref().unchecked_ref()));
        leave_callback.forget();

        Ok(mgr)
    }

    fn send_presence(&self, msg: PresenceMessage) -> Result<(), JsValue> {
        let json = serde_json::to_vec(&msg).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let array = js_sys::Uint8Array::from(&json[..]);
        self.bc.post_message(&array)
    }

    /// Number of visible peers (excluding self).
    #[wasm_bindgen]
    pub fn peer_count(&self) -> u32 {
        let peers = self.peers.lock().unwrap();
        peers.len() as u32
    }

    /// Returns a JSON string of active peers: `{"replica_id": last_seen_ms, ...}`.
    #[wasm_bindgen(js_name = activePeers)]
    pub fn active_peers_json(&self) -> String {
        let peers = self.peers.lock().unwrap();
        serde_json::to_string(&*peers).unwrap_or_else(|_| "{}".to_string())
    }
}

impl Drop for PresenceManager {
    fn drop(&mut self) {
        // Announce leave on drop
        if let Ok(json) = serde_json::to_vec(&PresenceMessage::Leave {
            replica_id: self.replica_id.clone(),
            namespace: self.namespace.clone(),
        }) {
            let array = js_sys::Uint8Array::from(&json[..]);
            let _ = self.bc.post_message(&array);
        }
    }
}
