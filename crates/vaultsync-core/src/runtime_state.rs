use bitflags::bitflags;

/// Runtime lifecycle state.
///
/// Every tab passes through these states. Transitions are deterministic:
/// UNKNOWN → DISCOVERING → BUILDING or MIRRORING
/// BUILDING → RECOVERING → CONNECTED → READY (as LEADER)
/// MIRRORING → FOLLOWER → RECOVERING → PROMOTING → BUILDING (if leader dies)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeState {
    #[default]
    Unknown,
    Discovering,
    Building,
    Recovering,
    Connecting,
    Ready,
    Leading,
    Mirroring,
    Follower,
    Promoting,
}

impl RuntimeState {
    pub fn can_read(&self) -> bool {
        matches!(self, Self::Ready | Self::Leading | Self::Follower | Self::Connecting)
    }

    pub fn can_write(&self) -> bool {
        matches!(self, Self::Ready | Self::Leading)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leading)
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, Self::Follower)
    }

    pub fn default_capabilities(&self) -> RuntimeCapabilities {
        match self {
            Self::Leading | Self::Ready => RuntimeCapabilities::all(),
            Self::Follower => RuntimeCapabilities::STORAGE,
            Self::Promoting => RuntimeCapabilities::STORAGE,
            Self::Building => RuntimeCapabilities::empty(),
            Self::Recovering => RuntimeCapabilities::STORAGE,
            Self::Connecting => RuntimeCapabilities::STORAGE,
            Self::Mirroring => RuntimeCapabilities::STORAGE,
            Self::Discovering => RuntimeCapabilities::empty(),
            Self::Unknown => RuntimeCapabilities::empty(),
        }
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RuntimeCapabilities: u8 {
        const STORAGE        = 0b0001;
        const COORDINATOR    = 0b0010;
        const UPLOADS        = 0b0100;
        const RECONCILIATION = 0b1000;
    }
}

impl RuntimeCapabilities {
    pub fn has_storage(&self) -> bool { self.contains(Self::STORAGE) }
    pub fn has_coordinator(&self) -> bool { self.contains(Self::COORDINATOR) }
    pub fn has_uploads(&self) -> bool { self.contains(Self::UPLOADS) }
    pub fn has_reconciliation(&self) -> bool { self.contains(Self::RECONCILIATION) }
}
