use thiserror::Error;

#[derive(Error, Debug)]
pub enum DriftError {
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
}

#[cfg(feature = "storage-sqlite")]
impl From<rusqlite::Error> for DriftError {
    fn from(e: rusqlite::Error) -> Self {
        DriftError::Sqlite(e.to_string())
    }
}
