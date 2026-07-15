use serde::{Deserialize, Serialize};
use bitflags::bitflags;
use async_trait::async_trait;
use std::sync::Arc;

/// RuntimeState — lifecycle state of VaultRuntime.
///
/// Transitions are enforced by `transition()`. Every legal transition
/// is explicitly listed. Any other transition panics in debug or logs
/// a critical error in release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RuntimeState {
    /// Initial state — runtime created, no leadership decision yet.
    #[default]
    Starting,
    /// Acquire(Immediate) returned Waiting — another tab holds the lock.
    /// This tab is a follower, running MirrorSession.
    Mirroring,
    /// LeaderEvent::Acquired received — swapping MirrorSession → LeaderSession.
    Promoting,
    /// Lock held, engine built, coordinator connected — full leader.
    Leading,
    /// Leader with a failing subsystem — still serving requests, but degraded.
    Degraded {
        reason: DegradedReason,
    },
    /// beforeunload received — terminal state. No outgoing transitions.
    ShuttingDown,
}

/// Specific subsystem that triggered Degraded state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DegradedReason {
    Storage,
    Coordinator,
    WebSocket,
    Broadcast,
    Maintenance,
}

impl RuntimeState {
    /// Attempt a state transition. Returns Ok(new_state) if legal,
    /// Err(description) if illegal.
    ///
    /// In debug builds, panics on illegal transitions. In release,
    /// returns Err for the caller to log and handle.
    pub fn transition(&self, target: RuntimeState) -> Result<RuntimeState, String> {
        let result = match (*self, &target) {
            // Starting → Leading | Mirroring | ShuttingDown
            (RuntimeState::Starting, RuntimeState::Leading) => Ok(target),
            (RuntimeState::Starting, RuntimeState::Mirroring) => Ok(target),
            (RuntimeState::Starting, RuntimeState::ShuttingDown) => Ok(target),

            // Mirroring → Promoting | ShuttingDown
            (RuntimeState::Mirroring, RuntimeState::Promoting) => Ok(target),
            (RuntimeState::Mirroring, RuntimeState::ShuttingDown) => Ok(target),

            // Promoting → Leading | Mirroring (session swap failed) | ShuttingDown
            (RuntimeState::Promoting, RuntimeState::Leading) => Ok(target),
            (RuntimeState::Promoting, RuntimeState::Mirroring) => Ok(target),
            (RuntimeState::Promoting, RuntimeState::ShuttingDown) => Ok(target),

            // Leading → Degraded | ShuttingDown
            (RuntimeState::Leading, RuntimeState::Degraded { .. }) => Ok(target),
            (RuntimeState::Leading, RuntimeState::ShuttingDown) => Ok(target),

            // Degraded → Leading (recovered) | ShuttingDown
            (RuntimeState::Degraded { .. }, RuntimeState::Leading) => Ok(target),
            (RuntimeState::Degraded { .. }, RuntimeState::ShuttingDown) => Ok(target),

            // ShuttingDown is terminal — no outgoing transitions
            (RuntimeState::ShuttingDown, _) => {
                Err("ShuttingDown is terminal — no outgoing transitions".into())
            }

            // All other transitions are illegal
            _ => Err(format!("Illegal transition: {:?} → {:?}", self, target)),
        };

        #[cfg(debug_assertions)]
        if let Err(ref msg) = result {
            panic!("{}", msg);
        }

        result
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, RuntimeState::Leading)
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, RuntimeState::Mirroring)
    }

    pub fn can_read(&self) -> bool {
        matches!(
            self,
            RuntimeState::Leading | RuntimeState::Mirroring | RuntimeState::Degraded { .. }
        )
    }

    pub fn can_write(&self) -> bool {
        matches!(self, RuntimeState::Leading)
    }

    pub fn default_capabilities(&self) -> RuntimeCapabilities {
        match self {
            RuntimeState::Leading => RuntimeCapabilities::all(),
            RuntimeState::Degraded { .. } => RuntimeCapabilities::all(),
            RuntimeState::Mirroring => RuntimeCapabilities::STORAGE,
            RuntimeState::Promoting => RuntimeCapabilities::STORAGE,
            RuntimeState::Starting | RuntimeState::ShuttingDown => RuntimeCapabilities::empty(),
        }
    }
}

// ── 6-variant connection state exposed to JS ──

/// Connection state reported to the UI layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    Leader,
    Mirror,
    Recovering,
    Connecting,
    Promoting,
    Offline,
}

impl RuntimeState {
    pub fn to_connection_state(&self) -> ConnectionState {
        match self {
            Self::Leading => ConnectionState::Leader,
            Self::Mirroring => ConnectionState::Mirror,
            Self::Promoting => ConnectionState::Promoting,
            Self::Degraded { .. } => ConnectionState::Leader,
            Self::Starting => ConnectionState::Connecting,
            Self::ShuttingDown => ConnectionState::Offline,
        }
    }
}

// ── Startup phase tracking ──

/// Boot→Ready→Warm→Idle lifecycle for startup timing.
/// Independent of RuntimeState — a tab can be in Warm phase while
/// showing ConnectionState::Leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StartupPhase {
    #[default]
    Boot,
    Ready,
    Warm,
    Idle,
}

// ── Runtime capabilities ──

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

// ── Runtime lifecycle trait ──

/// Every subsystem implements these phases:
/// - `boot()`: blocking init (manifests, content index, metadata). Target: ~100ms.
/// - `ready()`: blocking init (leader election, coordinator connect). Target: ~300ms.
/// - `warm()`: non-blocking (replay, snapshot catch-up, pending queue).
/// - `idle()`: non-blocking (compaction, GC, health probes, metrics).
/// - `shutdown()`: cleanup (scheduler clear, cache flush, metrics).
#[async_trait]
pub trait RuntimeLifecycle: Send + Sync {
    async fn boot(&self) -> Result<(), String>;
    async fn ready(&self) -> Result<(), String>;
    async fn warm(&self) -> Result<(), String>;
    async fn idle(&self) -> Result<(), String>;
    async fn shutdown(&self) -> Result<(), String>;
}

#[async_trait]
impl RuntimeLifecycle for Arc<()> {
    async fn boot(&self) -> Result<(), String> { Ok(()) }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> { Ok(()) }
    async fn idle(&self) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legal_transitions_from_starting() {
        let s = RuntimeState::Starting;
        assert!(s.transition(RuntimeState::Leading).is_ok());
        assert!(s.transition(RuntimeState::Mirroring).is_ok());
        assert!(s.transition(RuntimeState::ShuttingDown).is_ok());
    }

    #[test]
    fn test_illegal_transitions_panic_in_debug() {
        let s = RuntimeState::Starting;
        // Starting → Promoting is illegal
        #[cfg(debug_assertions)]
        {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s.transition(RuntimeState::Promoting);
            }));
            assert!(result.is_err());
        }
        #[cfg(not(debug_assertions))]
        {
            assert!(s.transition(RuntimeState::Promoting).is_err());
        }
    }

    #[test]
    fn test_mirroring_transitions() {
        let s = RuntimeState::Mirroring;
        assert!(s.transition(RuntimeState::Promoting).is_ok());
        assert!(s.transition(RuntimeState::ShuttingDown).is_ok());
    }

    #[test]
    fn test_promoting_transitions() {
        let s = RuntimeState::Promoting;
        assert!(s.transition(RuntimeState::Leading).is_ok());
        assert!(s.transition(RuntimeState::Mirroring).is_ok()); // promotion failed
        assert!(s.transition(RuntimeState::ShuttingDown).is_ok());
    }

    #[test]
    fn test_leading_transitions() {
        let s = RuntimeState::Leading;
        assert!(s.transition(RuntimeState::Degraded { reason: DegradedReason::Storage }).is_ok());
        assert!(s.transition(RuntimeState::ShuttingDown).is_ok());
    }

    #[test]
    fn test_degraded_transitions() {
        let s = RuntimeState::Degraded { reason: DegradedReason::Coordinator };
        assert!(s.transition(RuntimeState::Leading).is_ok());
        assert!(s.transition(RuntimeState::ShuttingDown).is_ok());
    }

    #[test]
    fn test_shutting_down_terminal() {
        let s = RuntimeState::ShuttingDown;
        #[cfg(debug_assertions)]
        {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s.transition(RuntimeState::Starting);
            })).is_err());
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s.transition(RuntimeState::Leading);
            })).is_err());
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s.transition(RuntimeState::Mirroring);
            })).is_err());
        }
        #[cfg(not(debug_assertions))]
        {
            assert!(s.transition(RuntimeState::Starting).is_err());
            assert!(s.transition(RuntimeState::Leading).is_err());
            assert!(s.transition(RuntimeState::Mirroring).is_err());
        }
    }

    #[test]
    fn test_connection_state_mapping() {
        assert_eq!(RuntimeState::Starting.to_connection_state(), ConnectionState::Connecting);
        assert_eq!(RuntimeState::Mirroring.to_connection_state(), ConnectionState::Mirror);
        assert_eq!(RuntimeState::Promoting.to_connection_state(), ConnectionState::Promoting);
        assert_eq!(RuntimeState::Leading.to_connection_state(), ConnectionState::Leader);
        assert_eq!(RuntimeState::ShuttingDown.to_connection_state(), ConnectionState::Offline);
    }

    #[test]
    fn test_capabilities() {
        assert!(RuntimeState::Leading.default_capabilities().has_storage());
        assert!(RuntimeState::Leading.default_capabilities().has_coordinator());
        assert!(RuntimeState::Mirroring.default_capabilities().has_storage());
        assert!(!RuntimeState::Mirroring.default_capabilities().has_coordinator());
        assert!(RuntimeState::Starting.default_capabilities().is_empty());
        assert!(RuntimeState::ShuttingDown.default_capabilities().is_empty());
    }
}
