pub mod crdt;
pub mod e2ee;
pub mod storage;
pub mod oplog;
pub mod schema;
pub mod subscription;
pub mod sync;
pub mod coordinator;
#[cfg(feature = "coordinator-http")]
pub mod drift_server;
pub mod ipc;
pub mod telemetry;
pub mod time_utils;

#[cfg(feature = "async-runtime")]
mod client;
mod config;
mod error;

#[cfg(feature = "async-runtime")]
pub use client::DriftClient;
pub use config::DriftConfig;
pub use error::DriftError;
