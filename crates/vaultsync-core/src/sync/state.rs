use serde::{Serialize, Deserialize};
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Syncing,
}

impl Default for ConnectionStatus {
    fn default() -> Self { ConnectionStatus::Disconnected }
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
