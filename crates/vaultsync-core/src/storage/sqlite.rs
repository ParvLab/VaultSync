use super::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use async_trait::async_trait;
use rusqlite::params;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SQLiteStorage {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SQLiteStorage {
    pub fn new(path: &str) -> Result<Self, VaultSyncError> {
        let conn = rusqlite::Connection::open(path)?;
        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.init_tables()?;
        Ok(storage)
    }

    pub fn in_memory() -> Result<Self, VaultSyncError> {
        let conn = rusqlite::Connection::open_in_memory()?;
        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.init_tables()?;
        Ok(storage)
    }

    fn init_tables(&self) -> Result<(), VaultSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS documents (
                doc_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                bytes BLOB NOT NULL,
                PRIMARY KEY (doc_id, record_id)
            );
            CREATE TABLE IF NOT EXISTS oplog (
                id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                replica_id TEXT NOT NULL,
                mutation_type TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                yrs_update BLOB,
                encrypted_blob BLOB,
                timestamp INTEGER NOT NULL,
                sequence INTEGER,
                sync_status TEXT NOT NULL DEFAULT 'Pending',
                synced_at INTEGER,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sync_state (
                namespace TEXT PRIMARY KEY,
                replica_id TEXT NOT NULL,
                last_synced_sequence INTEGER NOT NULL DEFAULT 0,
                connection_status TEXT NOT NULL DEFAULT 'Disconnected',
                leader_status TEXT,
                last_connected_at INTEGER,
                last_sync_at INTEGER,
                schema_version INTEGER NOT NULL DEFAULT 0,
                generation_id TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS schemas (
                doc_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 0,
                schema_bytes BLOB
            );
            CREATE TABLE IF NOT EXISTS migrations (
                version TEXT PRIMARY KEY,
                applied_at INTEGER NOT NULL,
                checksum TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS keys (
                namespace TEXT NOT NULL,
                key_bytes BLOB NOT NULL,
                version INTEGER NOT NULL,
                PRIMARY KEY (namespace, version)
            );
        ",
        )?;
        Ok(())
    }
}

#[async_trait]
impl Storage for SQLiteStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let bytes = bytes.to_vec();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO documents (doc_id, record_id, bytes) VALUES (?1, ?2, ?3)",
                params![doc_id, record_id, bytes],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt =
                conn.prepare("SELECT bytes FROM documents WHERE doc_id = ?1 AND record_id = ?2")?;
            let mut rows = stmt.query(params![doc_id, record_id])?;
            match rows.next()? {
                Some(row) => Ok(Some(row.get(0)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "DELETE FROM documents WHERE doc_id = ?1 AND record_id = ?2",
                params![doc_id, record_id],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let doc_id = doc_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt =
                conn.prepare("SELECT record_id, bytes FROM documents WHERE doc_id = ?1")?;
            let rows = stmt.query_map(params![doc_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let bytes = bytes.to_vec();
        let entry = entry.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction()?;

            tx.execute(
                "INSERT OR REPLACE INTO documents (doc_id, record_id, bytes) VALUES (?1, ?2, ?3)",
                params![doc_id, record_id, bytes],
            )?;

            tx.execute(
                "INSERT INTO oplog (id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    entry.id, entry.namespace, entry.replica_id,
                    format!("{:?}", entry.mutation_type),
                    entry.doc_id, entry.record_id, entry.yrs_update,
                    entry.encrypted_blob, entry.timestamp, entry.sequence,
                    format!("{:?}", entry.sync_status), entry.created_at,
                ],
            )?;

            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let entry = entry.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction()?;

            tx.execute(
                "DELETE FROM documents WHERE doc_id = ?1 AND record_id = ?2",
                params![doc_id, record_id],
            )?;

            tx.execute(
                "INSERT INTO oplog (id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    entry.id, entry.namespace, entry.replica_id,
                    format!("{:?}", entry.mutation_type),
                    entry.doc_id, entry.record_id, entry.yrs_update,
                    entry.encrypted_blob, entry.timestamp, entry.sequence,
                    format!("{:?}", entry.sync_status), entry.created_at,
                ],
            )?;

            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let entry = entry.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT INTO oplog (id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    entry.id, entry.namespace, entry.replica_id,
                    format!("{:?}", entry.mutation_type),
                    entry.doc_id, entry.record_id, entry.yrs_update,
                    entry.encrypted_blob, entry.timestamp, entry.sequence,
                    format!("{:?}", entry.sync_status), entry.created_at,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, synced_at, created_at
                 FROM oplog WHERE namespace = ?1 AND sync_status IN ('Pending', 'Optimistic') ORDER BY created_at ASC LIMIT ?2"
            )?;
            let rows = stmt.query_map(params![namespace, limit as i64], Self::map_oplog_entry)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        let id = id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE oplog SET sync_status = 'Synced', sequence = ?2, synced_at = unixepoch() WHERE id = ?1",
                params![id, sequence as i64],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn mark_failed(&self, id: &str, _error_msg: &str) -> Result<(), VaultSyncError> {
        let id = id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE oplog SET sync_status = 'Failed' WHERE id = ?1",
                params![id],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, synced_at, created_at
                 FROM oplog WHERE namespace = ?1 AND sequence > ?2 ORDER BY sequence ASC"
            )?;
            let rows = stmt.query_map(params![namespace, seq as i64], Self::map_oplog_entry)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT namespace, replica_id, last_synced_sequence, connection_status, leader_status, last_connected_at, last_sync_at, schema_version, COALESCE(generation_id, '')
                 FROM sync_state WHERE namespace = ?1"
            )?;
            let mut rows = stmt.query(params![namespace])?;
            match rows.next()? {
                Some(row) => {
                    let leader_status = row.get::<_, Option<String>>(4)?;
                    Ok(Some(SyncState {
                        namespace: row.get(0)?,
                        replica_id: row.get(1)?,
                        last_synced_sequence: row.get::<_, i64>(2)? as u64,
                        connection_status: row.get::<_, String>(3)?.parse().unwrap_or_default(),
                        leader_status: leader_status.map(|s| s == "Leader"),
                        last_connected_at: row.get(5)?,
                        last_sync_at: row.get(6)?,
                        schema_version: row.get::<_, i64>(7)? as u64,
                        generation_id: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
                    }))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        let state = state.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO sync_state (namespace, replica_id, last_synced_sequence, connection_status, leader_status, last_connected_at, last_sync_at, schema_version, generation_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    state.namespace, state.replica_id, state.last_synced_sequence as i64,
                    format!("{:?}", state.connection_status),
                    state.leader_status.map(|b| if b { "Leader" } else { "Reader" }),
                    state.last_connected_at, state.last_sync_at, state.schema_version as i64,
                    state.generation_id,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        let doc_id = doc_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT doc_id, version, schema_bytes FROM schemas WHERE doc_id = ?1")?;
            let mut rows = stmt.query(params![doc_id])?;
            match rows.next()? {
                Some(row) => Ok(Some(SchemaMeta {
                    doc_id: row.get(0)?,
                    version: row.get::<_, i64>(1)? as u64,
                    schema_bytes: row.get(2)?,
                })),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        let meta = meta.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO schemas (doc_id, version, schema_bytes) VALUES (?1, ?2, ?3)",
                params![meta.doc_id, meta.version as i64, meta.schema_bytes],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT version, applied_at, checksum FROM migrations ORDER BY applied_at ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(MigrationRecord {
                    version: row.get(0)?,
                    applied_at: row.get(1)?,
                    checksum: row.get(2)?,
                })
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        let record = record.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO migrations (version, applied_at, checksum) VALUES (?1, ?2, ?3)",
                params![record.version, record.applied_at as i64, record.checksum],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare("SELECT namespace, key_bytes, version FROM keys WHERE namespace = ?1 ORDER BY version ASC")?;
            let rows = stmt.query_map(params![namespace], |row| {
                Ok(KeyRecord {
                    namespace: row.get(0)?,
                    key_bytes: row.get(1)?,
                    version: row.get::<_, i64>(2)? as u64,
                })
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        let key = key.clone();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO keys (namespace, key_bytes, version) VALUES (?1, ?2, ?3)",
                params![key.namespace, key.key_bytes, key.version as i64],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let now = crate::time_utils::system_time_now_ms();
            let threshold = now.saturating_sub(older_than_ms) as i64;
            let changes = conn.execute(
                "UPDATE oplog SET sync_status = 'Pending'
                 WHERE namespace = ?1 AND sync_status != 'Synced' AND created_at < ?2",
                params![namespace, threshold],
            )?;
            Ok(changes)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let now_secs = crate::time_utils::system_time_now_secs();
            let threshold = now_secs.saturating_sub(older_than_secs) as i64;
            let changes = conn.execute(
                "DELETE FROM oplog WHERE namespace = ?1 AND sync_status = 'Synced' AND synced_at < ?2",
                params![namespace, threshold],
            )?;
            Ok(changes)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let now_ms = crate::time_utils::system_time_now_ms();
            let threshold = now_ms.saturating_sub(older_than_secs * 1000) as i64;
            let mut stmt = conn.prepare(
                "SELECT DISTINCT doc_id, record_id FROM oplog
                 WHERE namespace = ?1 AND mutation_type = 'CrdtDelete' AND sync_status = 'Synced' AND created_at < ?2"
            )?;
            let rows = stmt.query_map(params![namespace, threshold], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        let id = id.to_string();
        let new_blob = new_blob.to_vec();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE oplog SET encrypted_blob = ?2 WHERE id = ?1",
                params![id, new_blob],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt =
                conn.prepare("SELECT DISTINCT doc_id, record_id FROM oplog WHERE namespace = ?1")?;
            let rows = stmt.query_map(params![namespace], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let namespace = namespace.to_string();
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT id, namespace, replica_id, mutation_type, doc_id, record_id, yrs_update, encrypted_blob, timestamp, sequence, sync_status, synced_at, created_at
                 FROM oplog WHERE namespace = ?1 AND doc_id = ?2 AND record_id = ?3 AND sync_status = 'Synced' ORDER BY sequence ASC"
            )?;
            let rows = stmt.query_map(params![namespace, doc_id, record_id], Self::map_oplog_entry)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        let namespace = namespace.to_string();
        let doc_id = doc_id.to_string();
        let record_id = record_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let changes = conn.execute(
                "DELETE FROM oplog WHERE namespace = ?1 AND doc_id = ?2 AND record_id = ?3 AND sync_status = 'Synced' AND created_at < ?4",
                params![namespace, doc_id, record_id, timestamp as i64],
            )?;
            Ok(changes)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let namespace = namespace.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let count = conn.execute(
                "DELETE FROM oplog WHERE namespace = ?1 AND sync_status = 'Synced' AND created_at < ?2",
                params![namespace, cutoff_ms as i64],
            )?;
            Ok(count)
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }

    async fn write_batch_reconciliation(
        &self,
        documents: Vec<(String, String, Vec<u8>)>,
    ) -> Result<(), VaultSyncError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction()?;

            for (doc_id, record_id, bytes) in documents {
                tx.execute(
                    "INSERT OR REPLACE INTO documents (doc_id, record_id, bytes) VALUES (?1, ?2, ?3)",
                    params![doc_id, record_id, bytes],
                )?;
            }

            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| VaultSyncError::Storage(format!("spawn_blocking error: {e}")))?
    }
}

impl SQLiteStorage {
    fn map_oplog_entry(row: &rusqlite::Row) -> rusqlite::Result<OplogEntry> {
        use crate::oplog::entry::{MutationType, SyncStatus};
        let sync_status_str: String = row.get(10)?;
        let sync_status = match sync_status_str.as_str() {
            "Synced" => SyncStatus::Synced,
            "Failed" => SyncStatus::Failed,
            _ => SyncStatus::Pending,
        };
        let mutation_type_str: String = row.get(3)?;
        let mutation_type = match mutation_type_str.as_str() {
            "CrdtInsert" => MutationType::CrdtInsert,
            "CrdtDelete" => MutationType::CrdtDelete,
            "CrdtBatch" => MutationType::CrdtBatch,
            _ => MutationType::CrdtUpdate,
        };
        Ok(OplogEntry {
            id: row.get(0)?,
            namespace: row.get(1)?,
            replica_id: row.get(2)?,
            mutation_type,
            doc_id: row.get(4)?,
            record_id: row.get(5)?,
            yrs_update: row.get(6)?,
            encrypted_blob: row.get(7)?,
            timestamp: row.get::<_, i64>(8)? as u64,
            sequence: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
            sync_status,
            synced_at: row.get::<_, Option<i64>>(11)?.map(|v| v as u64),
            created_at: row.get::<_, i64>(12)? as u64,
        })
    }
}
