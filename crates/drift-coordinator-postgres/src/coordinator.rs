use async_trait::async_trait;
use drift_core::coordinator::traits::*;
use std::sync::Arc;
use tokio::sync::broadcast;
use futures::Stream;
use crate::migrations;
use tokio_postgres::NoTls;

#[derive(Debug, Clone)]
pub struct PostgresCoordinator {
    client: Arc<tokio::sync::Mutex<tokio_postgres::Client>>,
    tx: broadcast::Sender<String>,
}

impl PostgresCoordinator {
    pub async fn new(connection_string: &str) -> Result<Self, CoordinatorError> {
        tracing::info!("Connecting to Postgres: {}", connection_string);
        let (client, mut connection) = tokio_postgres::connect(connection_string, NoTls).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        let (tx, _) = broadcast::channel(1024);
        let tx_clone = tx.clone();
        
        tokio::spawn(async move {
            let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
            use futures::StreamExt;
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(tokio_postgres::AsyncMessage::Notification(notif)) => {
                        let payload = notif.payload().to_string();
                        let _ = tx_clone.send(payload);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("Postgres connection error: {}", e);
                        break;
                    }
                }
            }
        });
        
        migrations::initialize(&client).await?;
        
        client.execute("LISTEN drift_mutations", &[]).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            tx,
        })
    }
}

#[async_trait]
impl Coordinator for PostgresCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let mut seqs = Vec::new();
        let mut client_guard = self.client.lock().await;
        let namespace_str = namespace.to_string();
        
        let tx = client_guard.transaction().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        for m in mutations {
            let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
                &m.id,
                &namespace_str,
                &m.replica_id,
                &m.doc_id,
                &m.record_id,
                &m.encrypted_blob,
                &(m.timestamp as i64),
            ];
            
            let row = tx.query_one(
                "INSERT INTO mutations (id, namespace, replica_id, doc_id, record_id, encrypted_blob, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 RETURNING sequence",
                params,
            ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
            let seq: i64 = row.get(0);
            seqs.push(seq as u64);
        }
        
        tx.commit().await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace_str];
        client_guard.execute("SELECT pg_notify('drift_mutations', $1)", params).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            
        Ok(seqs)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let after_i64 = after as i64;
        let limit_i64 = limit as i64;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &namespace,
            &after_i64,
            &limit_i64,
        ];
        
        let rows = client_guard.query(
            "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp
             FROM mutations
             WHERE namespace = $1 AND sequence > $2
             ORDER BY sequence ASC
             LIMIT $3",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        let mut res = Vec::new();
        for row in rows {
            let seq: i64 = row.get(2);
            let ts: i64 = row.get(6);
            res.push(PendingMutation {
                id: row.get(0),
                namespace: row.get(1),
                sequence: seq as u64,
                doc_id: row.get(3),
                record_id: row.get(4),
                encrypted_blob: row.get(5),
                timestamp: ts as u64,
            });
        }
        Ok(res)
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx_mpsc, rx_mpsc) = tokio::sync::mpsc::channel(1024);
        let mut rx_broadcast = self.tx.subscribe();
        let client = self.client.clone();
        let namespace_str = namespace.to_string();
        
        tokio::spawn(async move {
            let mut last_sent = from_sequence;
            let pull_limit = 1000;
            
            // 1. Fetch initial historical data
            loop {
                let client_guard = client.lock().await;
                let last_sent_i64 = last_sent as i64;
                let pull_limit_i64 = pull_limit as i64;
                let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
                    &namespace_str,
                    &last_sent_i64,
                    &pull_limit_i64,
                ];
                let rows_res = client_guard.query(
                    "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp
                     FROM mutations
                     WHERE namespace = $1 AND sequence > $2
                     ORDER BY sequence ASC
                     LIMIT $3",
                    params,
                ).await;
                
                drop(client_guard);
                
                match rows_res {
                    Ok(rows) => {
                        if rows.is_empty() {
                            break;
                        }
                        for row in &rows {
                            let seq: i64 = row.get(2);
                            let ts: i64 = row.get(6);
                            let pm = PendingMutation {
                                id: row.get(0),
                                namespace: row.get(1),
                                sequence: seq as u64,
                                doc_id: row.get(3),
                                record_id: row.get(4),
                                encrypted_blob: row.get(5),
                                timestamp: ts as u64,
                            };
                            last_sent = last_sent.max(pm.sequence);
                            if tx_mpsc.send(pm).await.is_err() {
                                return;
                            }
                        }
                        if rows.len() < pull_limit {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            
            // 2. Poll/listen for new notifications
            while let Ok(notif_ns) = rx_broadcast.recv().await {
                if notif_ns == namespace_str {
                    loop {
                        let client_guard = client.lock().await;
                        let last_sent_i64 = last_sent as i64;
                        let pull_limit_i64 = pull_limit as i64;
                        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
                            &namespace_str,
                            &last_sent_i64,
                            &pull_limit_i64,
                        ];
                        let rows_res = client_guard.query(
                            "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp
                             FROM mutations
                             WHERE namespace = $1 AND sequence > $2
                             ORDER BY sequence ASC
                             LIMIT $3",
                            params,
                        ).await;
                        
                        drop(client_guard);
                        
                        match rows_res {
                            Ok(rows) => {
                                if rows.is_empty() {
                                    break;
                                }
                                for row in &rows {
                                    let seq: i64 = row.get(2);
                                    let ts: i64 = row.get(6);
                                    let pm = PendingMutation {
                                        id: row.get(0),
                                        namespace: row.get(1),
                                        sequence: seq as u64,
                                        doc_id: row.get(3),
                                        record_id: row.get(4),
                                        encrypted_blob: row.get(5),
                                        timestamp: ts as u64,
                                    };
                                    last_sent = last_sent.max(pm.sequence);
                                    if tx_mpsc.send(pm).await.is_err() {
                                        return;
                                    }
                                }
                                if rows.len() < pull_limit {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        
        Ok(Box::new(PostgresSubscription { rx: rx_mpsc }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let mut client_guard = self.client.lock().await;
        let namespace_str = namespace.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        
        let tx = client_guard.transaction().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        let schema_version_i64 = info.schema_version as i64;
        let params1: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &info.replica_id,
            &namespace_str,
            &info.public_key,
            &schema_version_i64,
            &now,
        ];
        tx.execute(
            "INSERT INTO replicas (replica_id, namespace, public_key, schema_version, last_seen)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (replica_id) DO UPDATE
             SET namespace = EXCLUDED.namespace,
                 public_key = EXCLUDED.public_key,
                 schema_version = EXCLUDED.schema_version,
                 last_seen = EXCLUDED.last_seen",
            params1,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        let params2: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &namespace_str,
            &schema_version_i64,
        ];
        tx.execute(
            "INSERT INTO schema_versions (namespace, version)
             VALUES ($1, $2)
             ON CONFLICT (namespace) DO UPDATE
             SET version = GREATEST(schema_versions.version, EXCLUDED.version)",
            params2,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        tx.commit().await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &now,
            &namespace,
            &replica_id,
        ];
        client_guard.execute(
            "UPDATE replicas SET last_seen = $1 WHERE namespace = $2 AND replica_id = $3",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &namespace,
        ];
        let row_opt = client_guard.query_opt(
            "SELECT version FROM schema_versions WHERE namespace = $1",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        
        if let Some(row) = row_opt {
            let v: i64 = row.get(0);
            Ok(v as u64)
        } else {
            Ok(0)
        }
    }
}

struct PostgresSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for PostgresSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
