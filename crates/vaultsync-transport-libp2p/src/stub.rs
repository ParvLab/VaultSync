// Stub implementation for target_arch = "wasm32" to ensure vaultsync builds on WebAssembly.

#[derive(Debug, Clone)]
pub struct LibP2pTransport;

#[derive(Debug, Clone)]
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
    #[serde(default)]
    pub replica_id: String,
}

impl LibP2pTransportHandle {
    pub async fn broadcast_mutation(&self, _: PendingMutation) -> Result<(), String> {
        Err("libp2p is not supported in WASM. Use WebRTC transport for browser P2P.".into())
    }

    pub async fn dial(&self, _addr: String) -> Result<(), String> {
        Err("libp2p is not supported in WASM.".into())
    }

    pub fn listen_addresses(&self) -> Vec<String> {
        vec![]
    }
}
