use crate::error::DriftError;

pub struct SharedMemory;

impl SharedMemory {
    pub fn create(_size: usize) -> Result<Self, DriftError> { Ok(Self) }
    pub fn write(&self, _data: &[u8]) -> Result<(), DriftError> { Ok(()) }
    pub fn read(&self) -> Result<Vec<u8>, DriftError> { Ok(Vec::new()) }
}
