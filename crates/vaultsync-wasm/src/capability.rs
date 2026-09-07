use serde::{Deserialize, Serialize};

/// Phase 4: Runtime capability enum for protocol negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeCapability {
    /// Full engine: persistence, network, compaction, scheduler
    Leader,
    /// Cache + BC + RPC client. No persistence, no network.
    Follower,
    /// Like follower but no writes. Broadcast-only receive.
    ReadonlyWarm,
    /// Leader with no coordinator. Local-first only.
    Offline,
}

impl RuntimeCapability {
    pub fn as_str(&self) -> &'static str {
        match self {
            RuntimeCapability::Leader => "leader",
            RuntimeCapability::Follower => "follower",
            RuntimeCapability::ReadonlyWarm => "readonly_warm",
            RuntimeCapability::Offline => "offline",
        }
    }

    /// Returns the JS-compatible mode string (matches old RuntimeStatus::as_str() format).
    /// Used by runtime_status() to maintain backward compatibility with UI badge checks.
    pub fn mode_str(&self) -> &'static str {
        match self {
            RuntimeCapability::Leader => "Leader",
            RuntimeCapability::Follower => "Mirror",
            RuntimeCapability::ReadonlyWarm => "Connecting",
            RuntimeCapability::Offline => "Offline",
        }
    }

    pub fn can_write(&self) -> bool {
        matches!(self, RuntimeCapability::Leader | RuntimeCapability::Offline | RuntimeCapability::Follower)
    }

    pub fn can_persist(&self) -> bool {
        matches!(self, RuntimeCapability::Leader | RuntimeCapability::Offline)
    }

    pub fn has_network(&self) -> bool {
        matches!(self, RuntimeCapability::Leader)
    }
}

/// Phase 4: CapabilityManager — tracks current runtime role
pub struct CapabilityManager {
    current: std::sync::Mutex<RuntimeCapability>,
}

impl CapabilityManager {
    pub fn new() -> Self {
        Self {
            current: std::sync::Mutex::new(RuntimeCapability::Follower),
        }
    }

    pub fn set(&self, capability: RuntimeCapability) {
        *self.current.lock().unwrap() = capability;
    }

    pub fn get(&self) -> RuntimeCapability {
        *self.current.lock().unwrap()
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.get(), RuntimeCapability::Leader)
    }

    pub fn is_follower(&self) -> bool {
        matches!(self.get(), RuntimeCapability::Follower)
    }

    pub fn can_write(&self) -> bool {
        self.get().can_write()
    }

    pub fn can_persist(&self) -> bool {
        self.get().can_persist()
    }

    pub fn has_network(&self) -> bool {
        self.get().has_network()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_default_is_follower() {
        let cm = CapabilityManager::new();
        assert!(cm.is_follower());
        assert!(!cm.is_leader());
    }

    #[test]
    fn capability_set_leader() {
        let cm = CapabilityManager::new();
        cm.set(RuntimeCapability::Leader);
        assert!(cm.is_leader());
        assert!(!cm.is_follower());
    }

    #[test]
    fn capability_can_write() {
        assert!(RuntimeCapability::Leader.can_write());
        assert!(RuntimeCapability::Follower.can_write());
        assert!(RuntimeCapability::Offline.can_write());
        assert!(!RuntimeCapability::ReadonlyWarm.can_write());
    }

    #[test]
    fn capability_can_persist() {
        assert!(RuntimeCapability::Leader.can_persist());
        assert!(!RuntimeCapability::Follower.can_persist());
    }

    #[test]
    fn capability_has_network() {
        assert!(RuntimeCapability::Leader.has_network());
        assert!(!RuntimeCapability::Follower.has_network());
        assert!(!RuntimeCapability::Offline.has_network());
    }

    #[test]
    fn capability_as_str() {
        assert_eq!(RuntimeCapability::Leader.as_str(), "leader");
        assert_eq!(RuntimeCapability::Follower.as_str(), "follower");
    }

    #[test]
    fn capability_transition_leader_to_follower() {
        let cm = CapabilityManager::new();
        cm.set(RuntimeCapability::Leader);
        assert!(cm.is_leader());
        cm.set(RuntimeCapability::Follower);
        assert!(cm.is_follower());
        assert!(!cm.is_leader());
    }

    #[test]
    fn capability_manager_get() {
        let cm = CapabilityManager::new();
        assert_eq!(cm.get(), RuntimeCapability::Follower);
        cm.set(RuntimeCapability::Offline);
        assert_eq!(cm.get(), RuntimeCapability::Offline);
    }
}
