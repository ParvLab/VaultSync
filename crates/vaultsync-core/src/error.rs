use thiserror::Error;

#[derive(Error, Debug)]
pub enum VaultSyncError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Coordinator error: {0}")]
    Coordinator(String),

    #[error("CRDT error: {0}")]
    Crdt(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SQLite error: {0}")]
    Sqlite(String),

    #[error("Document too large: {0} bytes, limit is {1} bytes")]
    DocumentTooLarge(usize, usize),

    #[error("Clock skew error: mutation timestamp {0} is skewed compared to system time {1}")]
    ClockSkew(u64, u64),

    #[error("Version mismatch: document schema version {0} does not match registered version {1} for doc '{2}'")]
    VersionMismatch(u64, u64, String),
}

#[cfg(feature = "storage-sqlite")]
impl From<rusqlite::Error> for VaultSyncError {
    fn from(e: rusqlite::Error) -> Self {
        VaultSyncError::Sqlite(e.to_string())
    }
}
