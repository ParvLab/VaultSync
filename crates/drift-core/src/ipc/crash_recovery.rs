use crate::error::DriftError;

pub struct CrashRecovery;

impl CrashRecovery {
    pub fn recover() -> Result<(), DriftError> { Ok(()) }
    pub fn replay_pending() -> Result<usize, DriftError> { Ok(0) }
}
