use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use crate::VaultSyncError;

/// Abstraction over cursor + generation state.
/// DownloadQueue reads cursor/generation from this trait instead of
/// going through Storage (OPFS). Implementors:
///   - SyncRuntime (WASM): in-memory AtomicU64 + RwLock
///   - InMemorySyncStateStore: simple all-in-memory impl (used by WASM client)
///   - MockCursorStore (tests): inspectable in-memory store
#[async_trait]
pub trait SyncStateStore: Send + Sync + std::fmt::Debug {
    async fn cursor(&self) -> Result<u64, VaultSyncError>;
    async fn set_cursor(&self, seq: u64, caller: &'static str) -> Result<(), VaultSyncError>;
    async fn generation(&self) -> Result<String, VaultSyncError>;
    async fn set_generation(&self, gen: &str) -> Result<(), VaultSyncError>;
}

/// Simple in-memory SyncStateStore backed by AtomicU64 + RwLock.
/// Used by WASM client for O(1) cursor/generation reads, bypassing OPFS.
#[derive(Debug)]
pub struct InMemorySyncStateStore {
    cursor: AtomicU64,
    generation: RwLock<String>,
}

impl InMemorySyncStateStore {
    pub fn new() -> Self {
        Self {
            cursor: AtomicU64::new(0),
            generation: RwLock::new(String::new()),
        }
    }

    pub fn with_cursor(cursor: u64) -> Self {
        Self {
            cursor: AtomicU64::new(cursor),
            generation: RwLock::new(String::new()),
        }
    }

    pub fn with_cursor_and_generation(cursor: u64, generation: &str) -> Self {
        Self {
            cursor: AtomicU64::new(cursor),
            generation: RwLock::new(generation.to_string()),
        }
    }
}

impl Default for InMemorySyncStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SyncStateStore for InMemorySyncStateStore {
    async fn cursor(&self) -> Result<u64, VaultSyncError> {
        Ok(self.cursor.load(Ordering::SeqCst))
    }

    async fn set_cursor(&self, seq: u64, _caller: &'static str) -> Result<(), VaultSyncError> {
        let old = self.cursor.swap(seq, Ordering::SeqCst);
        tracing::trace!(
            "[InMemorySyncStateStore] SET_CURSOR caller={} old={} new={}",
            _caller, old, seq,
        );
        Ok(())
    }

    async fn generation(&self) -> Result<String, VaultSyncError> {
        Ok(self.generation.read().unwrap().clone())
    }

    async fn set_generation(&self, gen: &str) -> Result<(), VaultSyncError> {
        *self.generation.write().unwrap() = gen.to_string();
        Ok(())
    }
}
