use crate::coordinator::traits::{EncryptedMutation, PendingMutation};
use serde::{Deserialize, Serialize};

// Message Type Enum Values
pub const MSG_AUTH: u8 = 0x01;
pub const MSG_AUTH_ACK: u8 = 0x02;
pub const MSG_REGISTER: u8 = 0x03;
pub const MSG_REGISTER_ACK: u8 = 0x04;
pub const MSG_PUSH: u8 = 0x05;
pub const MSG_PUSH_ACK: u8 = 0x06;
pub const MSG_PULL: u8 = 0x07;
pub const MSG_PULL_RESPONSE: u8 = 0x08;
pub const MSG_SUBSCRIBE: u8 = 0x09;
pub const MSG_MUTATION_PUSH: u8 = 0x0A;
pub const MSG_HEARTBEAT: u8 = 0x0B;
pub const MSG_HEARTBEAT_ACK: u8 = 0x0C;
pub const MSG_ERROR: u8 = 0x0F;
pub const MSG_SCHEMA_SYNC: u8 = 0x10;
pub const MSG_SCHEMA_MIGRATION: u8 = 0x11;
pub const MSG_SNAPSHOT: u8 = 0x12;
pub const MSG_P2P_SIGNAL: u8 = 0x13;
pub const MSG_P2P_SIGNAL_ACK: u8 = 0x14;
pub const MSG_NAMESPACE_ADD: u8 = 0x22;
pub const MSG_NAMESPACE_ACK: u8 = 0x23;
pub const MSG_NAMESPACE_DROP: u8 = 0x24;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthPayload {
    pub token: String,
    pub protocol_version: u32,
    pub replica_id: String,
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAckPayload {
    pub status: String, // "ok" | "error"
    pub error: Option<String>,
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPayload {
    pub replica_id: String,
    pub namespace: String,
    pub public_key: Vec<u8>,
    pub schema_version: u64,
    pub last_sequence: u64,
    #[serde(default)]
    pub key_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterAckPayload {
    pub status: String, // "ok" | "error"
    pub coordinator_sequence: u64,
    pub snapshot_available: bool,
    pub snapshot_sequence: u64,
    pub snapshot_url: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub generation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushPayload {
    pub request_id: String,
    pub mutations: Vec<EncryptedMutation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushAckPayload {
    pub request_id: String,
    pub sequences: Vec<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullPayload {
    pub request_id: String,
    pub namespace: String,
    pub after: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullResponsePayload {
    pub request_id: String,
    pub mutations: Vec<PendingMutation>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribePayload {
    pub request_id: String,
    pub namespace: String,
    pub after: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub replica_id: String,
    #[serde(default)]
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceAddPayload {
    pub namespace: String,
    pub replica_id: String,
    pub public_key: Vec<u8>,
    pub schema_version: u64,
    pub last_sequence: u64,
    #[serde(default)]
    pub key_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceAckPayload {
    pub namespace: String,
    pub status: String, // "ok" | "error"
    pub coordinator_sequence: u64,
    pub snapshot_available: bool,
    pub snapshot_sequence: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceDropPayload {
    pub namespace: String,
    pub replica_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatAckPayload {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub request_id: Option<String>,
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

/// Helper to serialize a typed payload into a binary frame
pub fn encode_frame<T: Serialize>(msg_type: u8, payload: &T) -> Result<Vec<u8>, serde_json::Error> {
    let payload_bytes = serde_json::to_vec(payload)?;
    let length = payload_bytes.len() as u32;

    let mut frame = Vec::with_capacity(5 + payload_bytes.len());
    frame.push(msg_type);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload_bytes);

    Ok(frame)
}

/// Helper to decode a frame header and extract type + payload
pub fn decode_frame(data: &[u8]) -> Result<(u8, &[u8]), String> {
    if data.len() < 5 {
        return Err("Frame too short".to_string());
    }

    let msg_type = data[0];
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&data[1..5]);
    let length = u32::from_be_bytes(len_bytes) as usize;

    if data.len() < 5 + length {
        return Err(format!(
            "Frame incomplete: expected {} bytes payload, got {} bytes",
            length,
            data.len() - 5
        ));
    }

    Ok((msg_type, &data[5..5 + length]))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSyncPayload {
    pub namespace: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMigrationPayload {
    pub status: String, // "ok" | "mismatch"
    pub current_version: u64,
    pub error: Option<String>,
    #[serde(default)]
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PSignalPayload {
    pub target_replica_id: String,
    pub signal_type: String, // "offer" | "answer" | "candidate"
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PSignalAckPayload {
    pub sender_replica_id: String,
    pub signal_type: String, // "offer" | "answer" | "candidate"
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPayload {
    pub doc_id: String,
    pub record_id: String,
    pub sequence: u64,
    pub bytes: Vec<u8>,
    pub checksum: u32,
    #[serde(default)]
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateReplicaKeyPayload {
    pub public_key: Vec<u8>,
    pub key_version: u64,
}
