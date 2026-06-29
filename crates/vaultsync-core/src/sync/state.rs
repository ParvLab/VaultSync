use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncState {
    pub namespace: String,
    pub replica_id: String,
    pub last_synced_sequence: u64,
    pub connection_status: ConnectionStatus,
    pub leader_status: Option<bool>,
    pub last_connected_at: Option<u64>,
    pub last_sync_at: Option<u64>,
    pub schema_version: u64,
    #[serde(default)]
    pub generation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Syncing,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Disconnected
    }
}

impl SyncState {
    pub fn new(namespace: String, generation_id: String) -> Self {
        Self {
            namespace,
            replica_id: String::new(),
            last_synced_sequence: 0,
            connection_status: ConnectionStatus::Connected,
            leader_status: Some(true),
            last_connected_at: None,
            last_sync_at: None,
            schema_version: 0,
            generation_id,
        }
    }
}

impl FromStr for ConnectionStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Connected" => Ok(ConnectionStatus::Connected),
            "Syncing" => Ok(ConnectionStatus::Syncing),
            _ => Ok(ConnectionStatus::Disconnected),
        }
    }
}
