use crate::coordinator::traits::{EncryptedMutation, PendingMutation};
use async_trait::async_trait;
use std::fmt::Debug;

/// Source of an inbound mutation — used for metrics and debugging
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportSource {
    BroadcastChannel,
    WebSocket,
    WebRtc,
}

/// A mutation received from a peer via any transport
#[derive(Debug, Clone)]
pub struct InboundMutation {
    pub mutation: PendingMutation,
    pub source: TransportSource,
    /// Server-assigned sequence number, if available (WS only)
    pub server_seq: Option<u64>,
}

/// Errors that can occur during transport operations
#[derive(Debug)]
pub enum TransportError {
    NotAvailable,
    SendFailed(String),
    ReceiveFailed(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAvailable => write!(f, "transport not available"),
            Self::SendFailed(msg) => write!(f, "send failed: {msg}"),
            Self::ReceiveFailed(msg) => write!(f, "receive failed: {msg}"),
        }
    }
}

/// A transport layer for sending and receiving mutations
///
/// This is the core abstraction for all communication channels:
/// - `BroadcastChannelTransport` — same-origin tab-to-tab (plaintext)
/// - `WasmWsTransport` — cross-device via WebSocket (encrypted)
/// - Future: WebRTC, libp2p, etc.
#[async_trait]
pub trait Transport: Send + Sync + Debug {
    /// Send a mutation through this transport
    async fn send(
        &self,
        namespace: &str,
        mutation: &EncryptedMutation,
    ) -> Result<(), TransportError>;

    /// Subscribe to incoming mutations from this transport
    async fn incoming(
        &self,
        namespace: &str,
    ) -> Result<Box<dyn futures::Stream<Item = InboundMutation> + Send + Unpin>, TransportError>;

    /// Whether this transport is currently connected and available
    fn is_available(&self) -> bool;

    /// Human-readable name for logging and metrics
    fn name(&self) -> &'static str;
}
