use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use bitflags::bitflags;

/// Runtime lifecycle state.
///
/// Every tab passes through these states. Transitions are deterministic:
/// UNKNOWN → DISCOVERING → BUILDING or MIRRORING
/// BUILDING → RECOVERING → CONNECTED → READY (as LEADER)
/// MIRRORING → FOLLOWER → RECOVERING → PROMOTING → BUILDING (if leader dies)
/// 6-variant connection state exposed to JS.
/// Replaces the old boolean Connected/Offline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// This tab is the leader — full engine running.
    Leader,
    /// This tab is a follower (mirror) — lightweight, no storage/recovery.
    Mirror,
    /// Engine is recovering (replay, snapshot catch-up).
    Recovering,
    /// Connecting to coordinator.
    Connecting,
    /// Follower promoting to leader (leader died).
    Promoting,
    /// No coordinator connection available.
    Offline,
}

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
    Offline,
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

    pub fn to_connection_state(&self) -> ConnectionState {
        match self {
            Self::Leading | Self::Ready => ConnectionState::Leader,
            Self::Mirroring | Self::Follower => ConnectionState::Mirror,
            Self::Recovering => ConnectionState::Recovering,
            Self::Connecting | Self::Discovering => ConnectionState::Connecting,
            Self::Promoting | Self::Building => ConnectionState::Promoting,
            Self::Unknown | Self::Offline => ConnectionState::Offline,
        }
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
            Self::Offline => RuntimeCapabilities::empty(),
        }
    }
}

/// RuntimeLifecycle — consistent lifecycle interface for every runtime.
///
/// Every subsystem implements these phases:
/// - `boot()`: blocking init (manifests, content index, metadata). Target: ~100ms.
/// - `ready()`: blocking init (leader election, coordinator connect). Target: ~300ms.
/// - `warm()`: non-blocking (replay, snapshot catch-up, pending queue).
/// - `idle()`: non-blocking (compaction, GC, health probes, metrics).
/// - `shutdown()`: cleanup (scheduler clear, cache flush, metrics).
#[async_trait]
pub trait RuntimeLifecycle: Send + Sync {
    /// Boot phase: read manifests, CRC validate, load ContentIndex.
    async fn boot(&self) -> Result<(), String>;
    /// Ready phase: UI interactive. Leader election done, coordinator connected.
    async fn ready(&self) -> Result<(), String>;
    /// Warm phase: background recovery (replay, snapshot catch-up).
    async fn warm(&self) -> Result<(), String>;
    /// Idle phase: compaction, GC, health probes, metrics.
    async fn idle(&self) -> Result<(), String>;
    /// Shutdown: clear scheduler, flush caches.
    async fn shutdown(&self) -> Result<(), String>;
}

/// Default no-op implementation for runtimes that don't need a phase.
/// All methods return Ok(()).
#[async_trait]
impl RuntimeLifecycle for Arc<()> {
    async fn boot(&self) -> Result<(), String> { Ok(()) }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> { Ok(()) }
    async fn idle(&self) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> { Ok(()) }
}

/// Startup phase tracking for Boot→Ready→Warm→Idle lifecycle.
///
/// These are timing/UI-interactive markers, not connection states.
/// A tab can be in Warm phase (background recovery) while showing
/// ConnectionState::Leader (if the leader was already established).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StartupPhase {
    /// Reading manifests, CRC validate, load ContentIndex. UI not yet interactive.
    #[default]
    Boot,
    /// UI interactive. Leader election done, coordinator connected.
    Ready,
    /// Background recovery: download replay, snapshot catch-up.
    Warm,
    /// Steady state: compaction, GC, health probes, metrics aggregation.
    Idle,
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
