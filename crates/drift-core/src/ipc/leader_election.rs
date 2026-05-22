use crate::error::DriftError;

pub struct LeaderElection;

impl LeaderElection {
    pub fn try_acquire() -> Result<bool, DriftError> { Ok(true) }
    pub fn release() {}
    pub fn is_leader() -> bool { true }
}
