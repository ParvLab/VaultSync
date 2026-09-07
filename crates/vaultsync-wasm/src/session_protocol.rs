use std::sync::atomic::AtomicU64;
use wasm_bindgen::prelude::*;

/// Phase 5c: Monotonic counter for ordering BC messages.
pub(crate) static BC_MSG_SEQ: AtomicU64 = AtomicU64::new(1);

/// SessionProtocol — versioned synchronization protocol over BroadcastChannel.
///
/// Replaces the old DiscoveryProtocol (HELLO/DISCOVER/BUILDING/READY) with
/// a clean session establishment protocol:
///
///   SYNC|ver=1|caps=0|ns={ns}|cursor={cursor}|gen={gen}
///     → SYNC_BEGIN|ver=1|caps=0|ns={ns}|gen={gen}|cursor={cursor}
///     → MUTATION|{tab_id}|{doc_id}|{record_id}|{fields_json}
///     → MUTATION|...
///     → SYNC_DONE|ver=1|cursor={cursor}
///
/// Protocol version 1 uses positional pipe-delimited fields.
///
/// The protocol does NOT handle leader election — that is owned by
/// LeaderElection (Web Locks). SessionProtocol only handles the
/// synchronization session between leader and follower.
pub struct SessionProtocol {
    bc: web_sys::BroadcastChannel,
    tab_id: String,
    boot_id: String,
    version: u8,
}

impl SessionProtocol {
    pub fn new(bc: web_sys::BroadcastChannel, tab_id: String) -> Self {
        let boot_id = uuid::Uuid::new_v4().to_string();
        Self {
            bc,
            tab_id,
            boot_id,
            version: 1,
        }
    }

    pub fn tab_id(&self) -> &str {
        &self.tab_id
    }
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Send SYNC request from follower to leader.
    /// Format: SYNC|{ver}|{caps}|{ns}|{cursor}|{gen}
    pub fn send_sync(&self, namespace: &str, cursor: u64, generation: &str) {
        let msg = format!("SYNC|{}|0|{}|{}|{}", self.version, namespace, cursor, generation);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Respond to SYNC: begin streaming mutations.
    /// Format: SYNC_BEGIN|{ver}|{ns}|{gen}|{cursor}
    pub fn send_sync_begin(&self, namespace: &str, generation: u64, cursor: u64) {
        let msg = format!("SYNC_BEGIN|{}|{}|{}|{}", self.version, namespace, generation, cursor);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Send a MUTATION frame (leader → follower during sync or live).
    /// Format: MUTATION|{bc_seq}|{tab_id}|{doc_id}|{record_id}|{fields_json}
    pub fn send_mutation(&self, doc_id: &str, record_id: &str, fields_json: &str) {
        let bc_seq = BC_MSG_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let msg = format!("MUTATION|{}|{}|{}|{}|{}", bc_seq, self.tab_id, doc_id, record_id, fields_json);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Mark sync complete.
    /// Format: SYNC_DONE|{ver}|{cursor}
    pub fn send_sync_done(&self, cursor: u64) {
        let msg = format!("SYNC_DONE|{}|{}", self.version, cursor);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Announce tab departure (beforeunload).
    pub fn send_left(&self) {
        let msg = format!("PROTO|LEFT|{}|{}", self.tab_id, self.boot_id);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Send heartbeat from leader.
    /// Format: HEARTBEAT|{tab_id}|{cursor}|{pending}
    pub fn send_heartbeat(&self, cursor: u64, pending: u64) {
        let msg = format!("HEARTBEAT|{}|{}|{}", self.tab_id, cursor, pending);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
    }

    /// Parse an incoming SYNC_BEGIN message.
    /// Returns (version, namespace, generation, cursor) or None.
    /// Supports optional trailing |replay=N field (ignored).
    pub fn parse_sync_begin(msg: &str) -> Option<(u8, String, u64, u64)> {
        let parts: Vec<&str> = msg.splitn(6, '|').collect();
        if parts.len() >= 5 {
            let ver: u8 = parts[1].parse().ok()?;
            let ns = parts[2].to_string();
            let gen: u64 = parts[3].parse().ok()?;
            let cursor: u64 = parts[4].parse().ok()?;
            Some((ver, ns, gen, cursor))
        } else {
            None
        }
    }

    /// Parse an incoming SYNC request from follower.
    /// Returns (version, caps, namespace, cursor, generation) or None.
    pub fn parse_sync(msg: &str) -> Option<(u8, u16, String, u64, String)> {
        let parts: Vec<&str> = msg.splitn(6, '|').collect();
        if parts.len() >= 6 {
            let ver: u8 = parts[1].parse().ok()?;
            let caps: u16 = parts[2].parse().ok()?;
            let ns = parts[3].to_string();
            let cursor: u64 = parts[4].parse().ok()?;
            let gen = parts[5].to_string();
            Some((ver, caps, ns, cursor, gen))
        } else {
            None
        }
    }

    /// Parse an incoming MUTATION frame.
    /// Returns (tab_id, doc_id, record_id, fields_json) or None.
    /// Format (v2): MUTATION|{bc_seq}|{tab_id}|{doc_id}|{record_id}|{fields_json}
    /// Format (v1, backward compat): MUTATION|{tab_id}|{doc_id}|{record_id}|{fields_json}
    pub fn parse_mutation(msg: &str) -> Option<(String, String, String, String)> {
        let parts: Vec<&str> = msg.splitn(6, '|').collect();
        let has_bc_seq = parts.len() >= 6;
        let tab_idx = if has_bc_seq { 2 } else { 1 };
        if parts.len() >= if has_bc_seq { 6 } else { 5 } {
            Some((
                parts[tab_idx].to_string(),
                parts[tab_idx + 1].to_string(),
                parts[tab_idx + 2].to_string(),
                parts[tab_idx + 3].to_string(),
            ))
        } else {
            None
        }
    }

    /// Parse SYNC_DONE.
    /// Returns (version, cursor) or None.
    /// Supports optional trailing |replay=N field (ignored).
    pub fn parse_sync_done(msg: &str) -> Option<(u8, u64)> {
        let parts: Vec<&str> = msg.splitn(4, '|').collect();
        if parts.len() >= 3 {
            let ver: u8 = parts[1].parse().ok()?;
            let cursor: u64 = parts[2].parse().ok()?;
            Some((ver, cursor))
        } else {
            None
        }
    }
}
