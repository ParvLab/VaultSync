// Stub implementation for target_arch = "wasm32" to ensure drift builds on WebAssembly.
pub struct LibP2pTransport;
pub struct LibP2pTransportHandle;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub sequence: u64,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub key_version: u64,
}
