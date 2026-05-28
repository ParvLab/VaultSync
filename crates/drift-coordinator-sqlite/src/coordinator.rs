use async_trait::async_trait;
use drift_core::coordinator::traits::*;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use futures::Stream;
use crate::migrations;
use rusqlite::params;

#[derive(Debug)]
pub struct SQLiteCoordinator {
    conn: Arc<Mutex<rusqlite::Connection>>,
    tx: broadcast::Sender<PendingMutation>,
}

impl SQLiteCoordinator {
    pub fn new(path: &str) -> Self {
        tracing::info!("Opening SQLite coordinator: {}", path);
        let conn = if path == ":memory:" {
            rusqlite::Connection::open_in_memory().expect("failed to open memory sqlite database")
        } else {
            rusqlite::Connection::open(path).expect("failed to open sqlite database")
        };
        migrations::initialize(&conn).expect("failed to initialize sqlite migrations");
        let (tx, _) = broadcast::channel(1024);
        Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
        }
    }
}

#[async_trait]
impl Coordinator for SQLiteCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        let tx_sender = self.tx.clone();
        
        let push_res = tokio::task::spawn_blocking(move || {
            let mut conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let tx = conn_guard.transaction().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let mut seq_ids = Vec::new();
            let mut pending_to_broadcast = Vec::new();
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO mutations (id, namespace, replica_id, doc_id, record_id, encrypted_blob, timestamp)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                
                for m in mutations {
                    stmt.execute(params![
                        m.id,
                        namespace_str,
                        m.replica_id,
                        m.doc_id,
                        m.record_id,
                        m.encrypted_blob,
                        m.timestamp as i64
                    ]).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                    let seq = tx.last_insert_rowid() as u64;
                    seq_ids.push(seq);
                    pending_to_broadcast.push(PendingMutation {
                        id: m.id,
                        namespace: namespace_str.clone(),
                        sequence: seq,
                        doc_id: m.doc_id,
                        record_id: m.record_id,
                        encrypted_blob: m.encrypted_blob,
                        timestamp: m.timestamp,
                    });
                }
            }
            tx.commit().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok::<(Vec<SequenceId>, Vec<PendingMutation>), CoordinatorError>((seq_ids, pending_to_broadcast))
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))??;
        
        let (seq_ids, pending_to_broadcast) = push_res;
        for pm in pending_to_broadcast {
            let _ = tx_sender.send(pm);
        }
        Ok(seq_ids)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let mut stmt = conn_guard.prepare(
                "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp
                 FROM mutations
                 WHERE namespace = ?1 AND sequence > ?2
                 ORDER BY sequence ASC
                 LIMIT ?3"
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let rows = stmt.query_map(params![namespace_str, after, limit], |row| {
                Ok(PendingMutation {
                    id: row.get(0)?,
                    namespace: row.get(1)?,
                    sequence: row.get(2)?,
                    doc_id: row.get(3)?,
                    record_id: row.get(4)?,
                    encrypted_blob: row.get(5)?,
                    timestamp: row.get(6)?,
                })
            }).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let mut res = Vec::new();
            for row in rows {
                res.push(row.map_err(|e| CoordinatorError::Internal(e.to_string()))?);
            }
            Ok(res)
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx_mpsc, rx_mpsc) = tokio::sync::mpsc::channel(1024);
        let mut rx_broadcast = self.tx.subscribe();
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        
        tokio::spawn(async move {
            let mut last_sent = from_sequence;
            
            // 1. Query existing mutations from DB that occurred after `from_sequence`
            let ns_db = namespace_str.clone();
            let db_res = tokio::task::spawn_blocking(move || {
                let conn_guard = conn.lock().ok()?;
                let mut stmt = conn_guard.prepare(
                    "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp
                     FROM mutations
                     WHERE namespace = ?1 AND sequence > ?2
                     ORDER BY sequence ASC"
                ).ok()?;
                let rows = stmt.query_map(params![ns_db, from_sequence], |row| {
                    Ok(PendingMutation {
                        id: row.get(0)?,
                        namespace: row.get(1)?,
                        sequence: row.get(2)?,
                        doc_id: row.get(3)?,
                        record_id: row.get(4)?,
                        encrypted_blob: row.get(5)?,
                        timestamp: row.get(6)?,
                    })
                }).ok()?;
                let mut res = Vec::new();
                for r in rows {
                    if let Ok(m) = r {
                        res.push(m);
                    }
                }
                Some(res)
            }).await;
            
            if let Ok(Some(mutations)) = db_res {
                for m in mutations {
                    last_sent = last_sent.max(m.sequence);
                    if tx_mpsc.send(m).await.is_err() {
                        return;
                    }
                }
            }
            
            // 2. Stream new mutations from the broadcast channel
            while let Ok(m) = rx_broadcast.recv().await {
                if m.namespace == namespace_str && m.sequence > last_sent {
                    last_sent = m.sequence;
                    if tx_mpsc.send(m).await.is_err() {
                        return;
                    }
                }
            }
        });
        
        Ok(Box::new(SqliteSubscription { rx: rx_mpsc }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        
        tokio::task::spawn_blocking(move || {
            let mut conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let tx = conn_guard.transaction().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            tx.execute(
                "INSERT OR REPLACE INTO replicas (replica_id, namespace, public_key, schema_version, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    info.replica_id,
                    namespace_str,
                    info.public_key,
                    info.schema_version as i64,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64
                ]
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            tx.execute(
                "INSERT INTO schema_versions (namespace, version)
                 VALUES (?1, ?2)
                 ON CONFLICT(namespace) DO UPDATE SET version = MAX(version, excluded.version)",
                params![namespace_str, info.schema_version as i64]
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            tx.commit().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok(())
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        let replica_id_str = replica_id.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            
            conn_guard.execute(
                "UPDATE replicas SET last_seen = ?1 WHERE namespace = ?2 AND replica_id = ?3",
                params![now, namespace_str, replica_id_str]
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok(())
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let mut stmt = conn_guard.prepare(
                "SELECT version FROM schema_versions WHERE namespace = ?1"
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let mut rows = stmt.query(params![namespace_str]).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            if let Some(row) = rows.next().map_err(|e| CoordinatorError::Internal(e.to_string()))? {
                let v: i64 = row.get(0).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                Ok(v as u64)
            } else {
                Ok(0)
            }
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?
    }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        let conn = self.conn.clone();
        let namespace_str = namespace.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let mut stmt = conn_guard.prepare(
                "SELECT replica_id, namespace, public_key, schema_version
                 FROM replicas
                 WHERE namespace = ?1"
            ).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let rows = stmt.query_map(params![namespace_str], |row| {
                let schema_version: i64 = row.get(3)?;
                Ok(ReplicaInfo {
                    replica_id: row.get(0)?,
                    namespace: row.get(1)?,
                    public_key: row.get(2)?,
                    schema_version: schema_version as u64,
                })
            }).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let mut replicas = Vec::new();
            for r in rows {
                replicas.push(r.map_err(|e| CoordinatorError::Internal(e.to_string()))?);
            }
            Ok(replicas)
        }).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?
    }
}

struct SqliteSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for SqliteSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_sqlite_coordinator_flow() {
        let coord = SQLiteCoordinator::new(":memory:");
        let ns = "test-namespace";
        
        // Check schema version is 0 initially
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 0);
        
        // Register a replica
        let replica_id = "replica-1";
        coord.register(ns, ReplicaInfo {
            replica_id: replica_id.to_string(),
            namespace: ns.to_string(),
            public_key: vec![1, 2, 3],
            schema_version: 5,
        }).await.unwrap();
        
        // Schema version should now be 5
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 5);
        
        // Heartbeat should work
        coord.heartbeat(ns, replica_id).await.unwrap();
        
        // Subscribe to changes
        let sub = coord.subscribe(ns, 0).await.unwrap();
        let mut sub = std::pin::Pin::from(sub);
        
        // Push a mutation
        let mutations = vec![
            EncryptedMutation {
                id: "m-1".to_string(),
                namespace: ns.to_string(),
                replica_id: replica_id.to_string(),
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                encrypted_blob: vec![4, 5, 6],
                timestamp: 1000,
                schema_version: 5,
            }
        ];
        let seqs = coord.push(ns, mutations).await.unwrap();
        assert_eq!(seqs, vec![1]);
        
        // Pull mutations
        let pulled = coord.pull(ns, 0, 10).await.unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].id, "m-1");
        assert_eq!(pulled[0].sequence, 1);
        
        // Subscribe should yield the mutation
        let next_m = sub.next().await;
        assert!(next_m.is_some());
        let next_m = next_m.unwrap();
        assert_eq!(next_m.id, "m-1");
        assert_eq!(next_m.sequence, 1);
    }
}
