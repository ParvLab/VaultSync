use std::sync::{Arc, Mutex, RwLock};

use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;

use crate::metadata_store::MetadataStore;

/// MetadataRuntime — in-memory metadata database over MetadataStore<T>.
///
/// All reads served from in-memory cache. Writes update cache + mark dirty.
/// `checkpoint()` persists dirty values through MetadataStore (OPFS).
/// This eliminates OPFS reads for SyncState, Schema, Key, Migration records
/// during normal operation — they only touch OPFS at boot (load) and on checkpoint.
#[derive(Debug)]
pub struct MetadataRuntime {
    inner: RwLock<MetadataCache>,
    dirty: std::sync::atomic::AtomicBool,
    // MetadataStore persistence backends (OPFS-backed)
    sync_state_store: MetadataStore<SyncState>,
    schema_store: MetadataStore<Vec<(String, SchemaMeta)>>,
    migration_store: MetadataStore<Vec<MigrationRecord>>,
    key_store: MetadataStore<Vec<KeyRecord>>,
}

#[derive(Debug, Default)]
struct MetadataCache {
    sync_state: Option<SyncState>,
    schemas: Vec<(String, SchemaMeta)>,
    migrations: Vec<MigrationRecord>,
    keys: Vec<KeyRecord>,
}

struct MetadataCacheStats {
    has_sync_state: bool,
    schemas: usize,
    migrations: usize,
    keys: usize,
}

impl MetadataRuntime {
    /// Load all metadata from OPFS-backed MetadataStores into memory.
    pub async fn load(
        sync_state_store: MetadataStore<SyncState>,
        schema_store: MetadataStore<Vec<(String, SchemaMeta)>>,
        migration_store: MetadataStore<Vec<MigrationRecord>>,
        key_store: MetadataStore<Vec<KeyRecord>>,
    ) -> Result<Arc<Self>, VaultSyncError> {
        let sync_state = sync_state_store.read().await?;
        let schemas = schema_store.read().await?.unwrap_or_default();
        let migrations = migration_store.read().await?.unwrap_or_default();
        let keys = key_store.read().await?.unwrap_or_default();

        // Log loaded state (before constructing Arc to avoid borrow issues)
        let info = Arc::new(MetadataCacheStats {
            has_sync_state: sync_state.is_some(),
            schemas: schemas.len(),
            migrations: migrations.len(),
            keys: keys.len(),
        });

        let rt = Arc::new(Self {
            inner: RwLock::new(MetadataCache {
                sync_state,
                schemas,
                migrations,
                keys,
            }),
            dirty: std::sync::atomic::AtomicBool::new(false),
            sync_state_store,
            schema_store,
            migration_store,
            key_store,
        });

        engine_debug!(
            "[MetadataRuntime] loaded: sync_state={}, schemas={}, migrations={}, keys={}",
            info.has_sync_state,
            info.schemas,
            info.migrations,
            info.keys,
        );

        Ok(rt)
    }

    /// Read SyncState from memory.
    pub fn read_sync_state(&self) -> Option<SyncState> {
        self.inner.read().unwrap().sync_state.clone()
    }

    /// Write SyncState to memory + mark dirty.
    pub fn write_sync_state(&self, state: SyncState) {
        self.inner.write().unwrap().sync_state = Some(state);
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Read SchemaMeta for a doc_id from memory.
    pub fn read_schema(&self, doc_id: &str) -> Option<SchemaMeta> {
        self.inner
            .read()
            .unwrap()
            .schemas
            .iter()
            .find(|(d, _)| d == doc_id)
            .map(|(_, s)| s.clone())
    }

    /// Write SchemaMeta to memory + mark dirty.
    pub fn write_schema(&self, meta: SchemaMeta) {
        let mut cache = self.inner.write().unwrap();
        if let Some(pos) = cache.schemas.iter().position(|(d, _)| d == &meta.doc_id) {
            cache.schemas[pos] = (meta.doc_id.clone(), meta);
        } else {
            cache.schemas.push((meta.doc_id.clone(), meta));
        }
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// List all schemas from memory.
    pub fn list_schemas(&self) -> Vec<SchemaMeta> {
        self.inner
            .read()
            .unwrap()
            .schemas
            .iter()
            .map(|(_, s)| s.clone())
            .collect()
    }

    /// Read migrations from memory.
    pub fn read_migrations(&self) -> Vec<MigrationRecord> {
        self.inner.read().unwrap().migrations.clone()
    }

    /// Write a migration record to memory + mark dirty.
    pub fn write_migration(&self, record: MigrationRecord) {
        self.inner.write().unwrap().migrations.push(record);
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Read keys for a namespace from memory.
    pub fn read_keys(&self, namespace: &str) -> Vec<KeyRecord> {
        self.inner
            .read()
            .unwrap()
            .keys
            .iter()
            .filter(|k| k.namespace == namespace)
            .cloned()
            .collect()
    }

    /// Write a key record to memory + mark dirty.
    pub fn write_key(&self, key: KeyRecord) {
        self.inner.write().unwrap().keys.push(key);
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Persist all dirty metadata to OPFS through MetadataStore.
    /// Clones data under lock, then awaits outside it (avoids Send issues).
    pub async fn checkpoint(&self) -> Result<(), VaultSyncError> {
        if !self.dirty.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Clone data under lock, then persist outside it
        let (sync_state, schemas, migrations, keys) = {
            let cache = self.inner.read().unwrap();
            (cache.sync_state.clone(), cache.schemas.clone(), cache.migrations.clone(), cache.keys.clone())
        };

        if let Some(ref state) = sync_state {
            self.sync_state_store.write(state).await?;
        }
        if !schemas.is_empty() {
            self.schema_store.write(&schemas).await?;
        }
        if !migrations.is_empty() {
            self.migration_store.write(&migrations).await?;
        }
        if !keys.is_empty() {
            self.key_store.write(&keys).await?;
        }

        self.dirty.store(false, std::sync::atomic::Ordering::SeqCst);
        engine_trace!("[MetadataRuntime] checkpoint complete");
        Ok(())
    }

    /// Returns true if there are unpresisted changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::SeqCst)
    }
}
