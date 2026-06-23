pub mod clock;
pub mod coordinator;
pub mod crdt;
pub mod e2ee;
pub mod ipc;
pub mod oplog;
pub mod transport;
pub mod schema;
pub mod storage;
pub mod subscription;
pub mod sync;
pub mod telemetry;
pub mod time_utils;
#[cfg(feature = "coordinator-http")]
pub mod vaultsync_server;

#[cfg(feature = "async-runtime")]
mod client;
mod config;
mod error;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(feature = "async-runtime")]
pub use client::VaultSyncClient;
pub use config::VaultSyncConfig;
pub use error::VaultSyncError;
