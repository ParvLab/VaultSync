#[cfg(not(target_arch = "wasm32"))]
pub mod transport;

#[cfg(target_arch = "wasm32")]
pub mod stub;

#[cfg(not(target_arch = "wasm32"))]
pub use transport::{LibP2pTransport, LibP2pTransportHandle, PendingMutation};

#[cfg(target_arch = "wasm32")]
pub use stub::{LibP2pTransport, LibP2pTransportHandle, PendingMutation};
