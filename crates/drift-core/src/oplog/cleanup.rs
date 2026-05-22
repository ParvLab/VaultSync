use crate::error::DriftError;
pub fn compact_oplog(_max_age: std::time::Duration) -> Result<usize, DriftError> { Ok(0) }
