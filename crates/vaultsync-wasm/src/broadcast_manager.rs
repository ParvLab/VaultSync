use crate::ipc::WasmIPC;
use crate::runtime::{BusGeneration, Runtime, RuntimeGeneration};
use crate::metrics::RuntimeMetrics;
use std::sync::Arc;

/// Phase 5: BroadcastManager — handles mutation dispatch, gap recovery, and flow control.
pub struct BroadcastManager {
    runtime: Arc<Runtime>,
    pub(crate) channel: Arc<WasmIPC>,
    metrics: Arc<RuntimeMetrics>,
}

impl BroadcastManager {
    pub fn new(runtime: Arc<Runtime>, channel: Arc<WasmIPC>, metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            runtime,
            channel,
            metrics,
        }
    }

    /// Broadcast a structured mutation: MUTATION|<bus_gen>|<doc_id>|<record_id>|<fields_json>
    pub fn broadcast_mutation(&self, doc_id: &str, record_id: &str, fields_json: &str) {
        let mut bus_gen = self.runtime.metadata_store.lock().unwrap().bus_gen;
        bus_gen = BusGeneration(bus_gen.0 + 1);
        self.runtime.metadata_store.lock().unwrap().bus_gen = bus_gen;

        let msg = format!("MUTATION|{}|{}|{}|{}", bus_gen.0, doc_id, record_id, fields_json);
        if self.channel.send(&msg).is_ok() {
            self.metrics.mutations_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.metrics.bytes_sent.fetch_add(msg.len() as u64, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.metrics.bus_messages_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn broadcast_heartbeat(&self) {
        let meta = self.runtime.metadata_store.lock().unwrap();
        let msg = format!(
            "HEARTBEAT|{}|{}|{}|{}",
            meta.runtime_gen.0,
            meta.bus_gen.0,
            meta.cursor,
            meta.pending_count
        );
        drop(meta);
        let _ = self.channel.send(&msg);
        self.metrics.heartbeats_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn request_gap_recovery(&self, from_seq: u64, expected_bus_gen: u64) {
        let msg = format!("REQUEST_SYNC|{}|{}", from_seq, expected_bus_gen);
        let _ = self.channel.send(&msg);
    }

    pub fn broadcast_leader_transfer(&self, new_gen: RuntimeGeneration, bus_gen: BusGeneration, cursor: u64) {
        let msg = format!(
            "LEADER_TRANSFER|{}|{}|{}",
            new_gen.0, bus_gen.0, cursor
        );
        let _ = self.channel.send(&msg);
    }

    /// Announce leadership after Web Locks election
    pub fn broadcast_leader_announcement(&self, tab_id: &str) {
        let meta = self.runtime.metadata_store.lock().unwrap();
        let msg = format!(
            "LEADER_ELECTED|{}|{}|{}|{}",
            tab_id, meta.runtime_gen.0, meta.bus_gen.0, meta.cursor
        );
        drop(meta);
        let _ = self.channel.send(&msg);
        engine_info!("[leader] announced tab={}", tab_id);
    }

    pub fn handle_message(&self, msg: &str, from_tab: &str) {
        let parts: Vec<&str> = msg.splitn(2, '|').collect();
        if parts.len() < 1 { return; }

        match parts[0] {
            "MUTATION" => {
                self.metrics.mutations_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            "HEARTBEAT" => {
                self.runtime.presence_store.lock().unwrap().update_heartbeat(from_tab);
            }
            "FOLLOWER_ATTACH" => {
                let meta = parts.get(1).unwrap_or(&"");
                let fields: Vec<&str> = meta.split('|').collect();
                if fields.len() >= 3 {
                    let version: u16 = fields[0].parse().unwrap_or(1);
                    let caps: Vec<String> = fields[1].split(',').map(String::from).collect();
                    self.runtime.presence_store.lock().unwrap()
                        .register_follower(from_tab.to_string(), version, caps);
                    self.metrics.follower_attaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            "FOLLOWER_DETACH" => {
                self.runtime.presence_store.lock().unwrap().remove_follower(from_tab);
                self.metrics.follower_detaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            "REQUEST_SYNC" => {
                self.metrics.mutations_gap_recovered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            "LEADER_TRANSFER" => {
                if let Some(meta) = parts.get(1) {
                    let fields: Vec<&str> = meta.split('|').collect();
                    if fields.len() >= 3 {
                        let gen: u64 = fields[0].parse().unwrap_or(0);
                        let bus: u64 = fields[1].parse().unwrap_or(0);
                        let cursor: u64 = fields[2].parse().unwrap_or(0);
                        let mut m = self.runtime.metadata_store.lock().unwrap();
                        m.runtime_gen = RuntimeGeneration(gen);
                        m.bus_gen = BusGeneration(bus);
                        m.cursor = cursor;
                    }
                }
            }
            "LEADER_ELECTED" => {
                // Another tab became leader — demote self if currently leader
                let mut meta = self.runtime.metadata_store.lock().unwrap();
                if meta.status == crate::runtime::RuntimeStatus::Leader {
                    meta.status = crate::runtime::RuntimeStatus::Follower;
                    self.metrics.leader_demotions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    engine_info!("[leader] demoted by LEADER_ELECTED from tab={}", parts.get(1).unwrap_or(&""));
                }
            }
            _ => {}
        }
    }
}
