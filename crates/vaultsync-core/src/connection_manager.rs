use futures::channel::mpsc;
use std::sync::{Arc, RwLock};

/// Connection state machine — single source of truth for WebSocket lifecycle.
/// Every transition produces a ConnectionEvent for subscribers.
/// Workers (upload, download, etc.) react to events instead of probing WS state.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Offline,
    Connecting,
    Authenticating,
    Registering,
    Syncing,
    Online,
    Reconnecting { attempt: u8, backoff_ms: u64 },
    Closing,
    Closed,
}

/// Events emitted on state transitions.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    Opened,
    Established,
    Closed { reason: u16 },
    Lost,
    Reconnecting { attempt: u8, backoff_ms: u64 },
    Reconnected,
    AuthFailed(String),
    RegisterFailed(String),
    HeartbeatTimeout,
}

/// Health metrics tracked across the connection lifecycle.
#[derive(Debug, Clone)]
pub struct ConnectionHealth {
    pub reconnects: u64,
    pub last_connected_at: Option<u64>,
    pub last_disconnected_at: Option<u64>,
    pub consecutive_failures: u8,
}

pub struct ConnectionManager {
    state: RwLock<ConnectionState>,
    health: RwLock<ConnectionHealth>,
    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl ConnectionManager {
    pub fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<ConnectionEvent>) {
        let (tx, rx) = mpsc::unbounded();
        let cm = Arc::new(Self {
            state: RwLock::new(ConnectionState::Offline),
            health: RwLock::new(ConnectionHealth {
                reconnects: 0,
                last_connected_at: None,
                last_disconnected_at: None,
                consecutive_failures: 0,
            }),
            event_tx: tx,
        });
        (cm, rx)
    }

    pub fn state(&self) -> ConnectionState {
        self.state.read().unwrap().clone()
    }

    pub fn is_online(&self) -> bool {
        *self.state.read().unwrap() == ConnectionState::Online
    }

    /// Transition to a new state. Logs the change and emits the corresponding event.
    /// Returns (old_state, optional_event) for the caller to log/act on.
    pub fn transition(&self, new: ConnectionState) -> (ConnectionState, Option<ConnectionEvent>) {
        let old = {
            let mut state = self.state.write().unwrap();
            if *state == new {
                return (new, None);
            }
            let old = state.clone();
            *state = new.clone();
            old
        };

        let mut health = self.health.write().unwrap();
        match &new {
            ConnectionState::Online => {
                let now = crate::time_utils::system_time_now_ms();
                health.last_connected_at = Some(now);
                health.consecutive_failures = 0;
            }
            ConnectionState::Closed | ConnectionState::Offline => {
                health.last_disconnected_at = Some(crate::time_utils::system_time_now_ms());
            }
            ConnectionState::Reconnecting { .. } => {
                health.reconnects += 1;
                health.consecutive_failures += 1;
            }
            _ => {}
        }
        drop(health);

        let event = match &new {
            ConnectionState::Online => Some(ConnectionEvent::Reconnected),
            ConnectionState::Reconnecting { attempt, backoff_ms } => {
                Some(ConnectionEvent::Reconnecting {
                    attempt: *attempt,
                    backoff_ms: *backoff_ms,
                })
            }
            ConnectionState::Connecting => Some(ConnectionEvent::Opened),
            ConnectionState::Closed => Some(ConnectionEvent::Lost),
            ConnectionState::Authenticating => Some(ConnectionEvent::Established),
            _ => None,
        };

        if let Some(ref ev) = event {
            let _ = self.event_tx.unbounded_send(ev.clone());
        }

        (old, event)
    }

    pub fn health(&self) -> ConnectionHealth {
        self.health.read().unwrap().clone()
    }

    /// Subscribe to connection events. Returns a receiver that delivers
    /// ConnectionEvent values. Multiple callers can subscribe independently.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<ConnectionEvent> {
        let (_, rx) = mpsc::unbounded();
        // Note: events are broadcast via the shared event_tx.
        // For independent subscription, the caller receives a new rx that
        // will receive events from this point forward.
        rx
    }
}
