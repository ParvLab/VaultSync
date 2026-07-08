use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent};

/// Phase 4/5: BusMessage protocol enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BusMessage {
    FOLLOWER_ATTACH {
        tab_id: String,
        protocol_version: u16,
        capabilities: Vec<String>,
    },
    FOLLOWER_DETACH {
        tab_id: String,
    },
    SNAPSHOT_METADATA {
        runtime_gen: u64,
        bus_gen: u64,
        document_ids: Vec<(String, u64)>,
        cursor: u64,
        pending_count: usize,
    },
    SNAPSHOT_DOCUMENT {
        doc_id: String,
        record_id: String,
        fields_json: String,
    },
    MUTATION {
        doc_id: String,
        record_id: String,
        field: String,
        value_json: String,
        seq: u64,
        runtime_gen: u64,
        bus_gen: u64,
    },
    MUTATION_BATCH {
        mutations: Vec<String>,
        from_seq: u64,
        to_seq: u64,
    },
    HEARTBEAT {
        runtime_gen: u64,
        bus_gen: u64,
        cursor: u64,
        pending_count: usize,
    },
    LEADER_TRANSFER {
        runtime_gen: u64,
        bus_gen: u64,
        cursor: u64,
        pending_count: usize,
    },
    ACK {
        seq: u64,
        level: String, // "Persisted", "Uploaded", "Replicated"
        error: Option<String>,
    },
    HOT_DOCUMENTS {
        documents: Vec<String>, // doc_id list
        runtime_gen: u64,
        bus_gen: u64,
    },
    REQUEST_SYNC {
        from_seq: u64,
        expected_bus_gen: Option<u64>,
    },
    REQUEST_DOCUMENT {
        doc_id: String,
        record_id: String,
    },
}

#[wasm_bindgen]
pub struct WasmIPC {
    channel: BroadcastChannel,
    last_received: Arc<Mutex<String>>,
    on_message_callback: Arc<Mutex<Option<js_sys::Function>>>,
}

#[wasm_bindgen]
impl WasmIPC {
    /// Create WasmIPC from an existing BroadcastChannel (used by client integration)
    pub fn new_with_channel(channel: web_sys::BroadcastChannel) -> WasmIPC {
        let last_received = Arc::new(Mutex::new(String::new()));
        let on_message_callback: Arc<Mutex<Option<js_sys::Function>>> = Arc::new(Mutex::new(None));
        WasmIPC {
            channel,
            last_received,
            on_message_callback,
        }
    }

    pub fn new(channel_name: &str) -> Result<WasmIPC, JsValue> {
        let channel = BroadcastChannel::new(channel_name)?;
        let last_received = Arc::new(Mutex::new(String::new()));
        let last_received_clone = last_received.clone();
        let on_message_callback: Arc<Mutex<Option<js_sys::Function>>> = Arc::new(Mutex::new(None));

        let onmsg = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Some(msg_str) = e.data().as_string() {
                let mut lock = last_received_clone.lock().unwrap();
                *lock = msg_str;
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        channel.set_onmessage(Some(onmsg.as_ref().unchecked_ref()));
        onmsg.forget();

        Ok(Self {
            channel,
            last_received,
            on_message_callback,
        })
    }

    pub fn send(&self, msg: &str) -> Result<(), JsValue> {
        self.channel.post_message(&JsValue::from_str(msg))?;
        Ok(())
    }

    pub fn receive(&self) -> String {
        let mut lock = self.last_received.lock().unwrap();
        let val = lock.clone();
        *lock = String::new();
        val
    }

    pub fn on_message(&self, callback: js_sys::Function) {
        let last_received_clone = self.last_received.clone();
        let closure = Closure::wrap(Box::new(move |e: MessageEvent| {
            let data = e.data();
            if let Some(msg_str) = data.as_string() {
                let mut lock = last_received_clone.lock().unwrap();
                *lock = msg_str;
            }
            let _ = callback.call1(&JsValue::NULL, &data);
        }) as Box<dyn FnMut(MessageEvent)>);
        self.channel
            .set_onmessage(Some(closure.as_ref().unchecked_ref()));
        closure.forget();
    }

}

/// Encode a BusMessage to string format for BC transport.
pub fn encode_message(msg: &BusMessage) -> String {
        match msg {
            BusMessage::MUTATION { doc_id, record_id, field, value_json, seq, runtime_gen, bus_gen } => {
                format!("MUTATION|{}|{}|{}|{}|{}|{}|{}", doc_id, record_id, field, value_json, seq, runtime_gen, bus_gen)
            }
            BusMessage::HEARTBEAT { runtime_gen, bus_gen, cursor, pending_count } => {
                format!("HEARTBEAT|{}|{}|{}|{}", runtime_gen, bus_gen, cursor, pending_count)
            }
            BusMessage::FOLLOWER_ATTACH { tab_id, protocol_version, capabilities } => {
                format!("FOLLOWER_ATTACH|{}|{}|{}", tab_id, protocol_version, capabilities.join(","))
            }
            BusMessage::FOLLOWER_DETACH { tab_id } => {
                format!("FOLLOWER_DETACH|{}", tab_id)
            }
            BusMessage::SNAPSHOT_METADATA { runtime_gen, bus_gen, document_ids, cursor, pending_count } => {
                let ids: Vec<String> = document_ids.iter().map(|(id, hash)| format!("{}:{}", id, hash)).collect();
                format!("SNAPSHOT_METADATA|{}|{}|{}|{}|{}", runtime_gen, bus_gen, ids.join(","), cursor, pending_count)
            }
            BusMessage::SNAPSHOT_DOCUMENT { doc_id, record_id, fields_json } => {
                format!("SNAPSHOT_DOCUMENT|{}|{}|{}", doc_id, record_id, fields_json)
            }
            BusMessage::LEADER_TRANSFER { runtime_gen, bus_gen, cursor, pending_count } => {
                format!("LEADER_TRANSFER|{}|{}|{}|{}", runtime_gen, bus_gen, cursor, pending_count)
            }
            BusMessage::ACK { seq, level, error } => {
                format!("ACK|{}|{}|{}", seq, level, error.as_deref().unwrap_or(""))
            }
            BusMessage::HOT_DOCUMENTS { documents, runtime_gen, bus_gen } => {
                format!("HOT_DOCUMENTS|{}|{}|{}", runtime_gen, bus_gen, documents.join(","))
            }
            BusMessage::REQUEST_SYNC { from_seq, expected_bus_gen } => {
                format!("REQUEST_SYNC|{}|{}", from_seq, expected_bus_gen.unwrap_or(0))
            }
            BusMessage::REQUEST_DOCUMENT { doc_id, record_id } => {
                format!("REQUEST_DOCUMENT|{}|{}", doc_id, record_id)
            }
            BusMessage::MUTATION_BATCH { mutations, from_seq, to_seq } => {
                format!("MUTATION_BATCH|{}|{}|{}", from_seq, to_seq, mutations.join(";"))
            }
        }
    }

/// Parse a string message into a BusMessage.
pub fn parse_message(msg: &str) -> Option<BusMessage> {
        let parts: Vec<&str> = msg.splitn(2, '|').collect();
        if parts.is_empty() { return None; }
        let rest = parts.get(1).unwrap_or(&"");
        let fields: Vec<&str> = rest.split('|').collect();

        match parts[0] {
            "MUTATION" if fields.len() >= 7 => {
                Some(BusMessage::MUTATION {
                    doc_id: fields[0].to_string(),
                    record_id: fields[1].to_string(),
                    field: fields[2].to_string(),
                    value_json: fields[3].to_string(),
                    seq: fields[4].parse().unwrap_or(0),
                    runtime_gen: fields[5].parse().unwrap_or(0),
                    bus_gen: fields[6].parse().unwrap_or(0),
                })
            }
            "HEARTBEAT" if fields.len() >= 4 => {
                Some(BusMessage::HEARTBEAT {
                    runtime_gen: fields[0].parse().unwrap_or(0),
                    bus_gen: fields[1].parse().unwrap_or(0),
                    cursor: fields[2].parse().unwrap_or(0),
                    pending_count: fields[3].parse().unwrap_or(0),
                })
            }
            "FOLLOWER_ATTACH" if fields.len() >= 3 => {
                Some(BusMessage::FOLLOWER_ATTACH {
                    tab_id: fields[0].to_string(),
                    protocol_version: fields[1].parse().unwrap_or(1),
                    capabilities: fields[2].split(',').map(String::from).collect(),
                })
            }
            "FOLLOWER_DETACH" if fields.len() >= 1 => {
                Some(BusMessage::FOLLOWER_DETACH {
                    tab_id: fields[0].to_string(),
                })
            }
            "SNAPSHOT_METADATA" if fields.len() >= 5 => {
                let ids: Vec<(String, u64)> = fields[2].split(',')
                    .filter_map(|s| {
                        let p: Vec<&str> = s.split(':').collect();
                        if p.len() >= 2 {
                            Some((p[0].to_string(), p[1].parse().unwrap_or(0)))
                        } else { None }
                    })
                    .collect();
                Some(BusMessage::SNAPSHOT_METADATA {
                    runtime_gen: fields[0].parse().unwrap_or(0),
                    bus_gen: fields[1].parse().unwrap_or(0),
                    document_ids: ids,
                    cursor: fields[3].parse().unwrap_or(0),
                    pending_count: fields[4].parse().unwrap_or(0),
                })
            }
            "SNAPSHOT_DOCUMENT" if fields.len() >= 3 => {
                Some(BusMessage::SNAPSHOT_DOCUMENT {
                    doc_id: fields[0].to_string(),
                    record_id: fields[1].to_string(),
                    fields_json: fields[2].to_string(),
                })
            }
            "LEADER_TRANSFER" if fields.len() >= 4 => {
                Some(BusMessage::LEADER_TRANSFER {
                    runtime_gen: fields[0].parse().unwrap_or(0),
                    bus_gen: fields[1].parse().unwrap_or(0),
                    cursor: fields[2].parse().unwrap_or(0),
                    pending_count: fields[3].parse().unwrap_or(0),
                })
            }
            "ACK" if fields.len() >= 3 => {
                Some(BusMessage::ACK {
                    seq: fields[0].parse().unwrap_or(0),
                    level: fields[1].to_string(),
                    error: if fields[2].is_empty() { None } else { Some(fields[2].to_string()) },
                })
            }
            "HOT_DOCUMENTS" if fields.len() >= 3 => {
                Some(BusMessage::HOT_DOCUMENTS {
                    documents: fields[2].split(',').map(String::from).collect(),
                    runtime_gen: fields[0].parse().unwrap_or(0),
                    bus_gen: fields[1].parse().unwrap_or(0),
                })
            }
            "REQUEST_SYNC" if fields.len() >= 2 => {
                Some(BusMessage::REQUEST_SYNC {
                    from_seq: fields[0].parse().unwrap_or(0),
                    expected_bus_gen: Some(fields[1].parse().unwrap_or(0)),
                })
            }
            "REQUEST_DOCUMENT" if fields.len() >= 2 => {
                Some(BusMessage::REQUEST_DOCUMENT {
                    doc_id: fields[0].to_string(),
                    record_id: fields[1].to_string(),
                })
            }
            "MUTATION_BATCH" if fields.len() >= 3 => {
                Some(BusMessage::MUTATION_BATCH {
                    mutations: fields[2].split(';').map(String::from).collect(),
                    from_seq: fields[0].parse().unwrap_or(0),
                    to_seq: fields[1].parse().unwrap_or(0),
                })
            }
            _ => None,
        }
    }

