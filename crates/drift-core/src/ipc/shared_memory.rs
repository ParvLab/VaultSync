use std::sync::{Arc, RwLock};
use crate::error::DriftError;
use crate::sync::state::SyncState;

#[derive(Clone)]
pub struct SharedMemory {
    data: Arc<RwLock<Vec<u8>>>,
    capacity: usize,
}

impl SharedMemory {
    pub fn create(size: usize) -> Result<Self, DriftError> {
        Ok(Self {
            data: Arc::new(RwLock::new(vec![0; size])),
            capacity: size,
        })
    }

    pub fn write(&self, data: &[u8]) -> Result<(), DriftError> {
        let mut buffer = self.data.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        let len = data.len().min(self.capacity);
        buffer[..len].copy_from_slice(&data[..len]);
        if len < self.capacity {
            buffer[len..].fill(0);
        }
        Ok(())
    }

    pub fn read(&self) -> Result<Vec<u8>, DriftError> {
        let buffer = self.data.read().map_err(|e| DriftError::Storage(e.to_string()))?;
        Ok(buffer.clone())
    }
}

pub fn encode_sync_state(state: &SyncState) -> Result<Vec<u8>, DriftError> {
    bincode::serialize(state).map_err(|e| DriftError::Storage(format!("serialization failed: {e}")))
}

pub fn decode_sync_state(bytes: &[u8]) -> Option<SyncState> {
    bincode::deserialize(bytes).ok()
}
