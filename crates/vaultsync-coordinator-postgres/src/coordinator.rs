use crate::migrations;
use async_trait::async_trait;
use futures::Stream;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_postgres::NoTls;
use vaultsync_core::coordinator::traits::*;

#[derive(Debug, Clone)]
pub struct PostgresCoordinator {
    client: Arc<tokio::sync::Mutex<tokio_postgres::Client>>,
    tx: broadcast::Sender<String>,
}

impl PostgresCoordinator {
    pub async fn new(connection_string: &str) -> Result<Self, CoordinatorError> {
        tracing::info!("Connecting to Postgres: {}", connection_string);
        let (client, mut connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
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

        client
            .execute("LISTEN vaultsync_mutations", &[])
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            tx,
        })
    }
}

#[async_trait]
impl Coordinator for PostgresCoordinator {
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        let mut seqs = Vec::new();
        let mut client_guard = self.client.lock().await;
        let namespace_str = namespace.to_string();

        let tx = client_guard
            .transaction()
            .await
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
                &(m.key_version as i64),
            ];

            let row = tx.query_one(
                "INSERT INTO mutations (id, namespace, replica_id, doc_id, record_id, encrypted_blob, timestamp, key_version)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 RETURNING sequence",
                params,
            ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

            let seq: i64 = row.get(0);
            seqs.push(seq as u64);
        }

        tx.commit()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace_str];
        client_guard
            .execute("SELECT pg_notify('vaultsync_mutations', $1)", params)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(seqs)
    }

    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let after_i64 = after as i64;
        let limit_i64 = limit as i64;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
            &[&namespace, &after_i64, &limit_i64];

        let rows = client_guard.query(
            "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp, key_version
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
            let kv: i64 = row.get(7);
            res.push(PendingMutation {
                id: row.get(0),
                namespace: row.get(1),
                sequence: seq as u64,
                doc_id: row.get(3),
                record_id: row.get(4),
                encrypted_blob: row.get(5),
                timestamp: ts as u64,
                key_version: kv as u64,
            });
        }
        Ok(res)
    }

    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
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
                let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
                    &[&namespace_str, &last_sent_i64, &pull_limit_i64];
                let rows_res = client_guard.query(
                    "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp, key_version
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
                            let kv: i64 = row.get(7);
                            let pm = PendingMutation {
                                id: row.get(0),
                                namespace: row.get(1),
                                sequence: seq as u64,
                                doc_id: row.get(3),
                                record_id: row.get(4),
                                encrypted_blob: row.get(5),
                                timestamp: ts as u64,
                                key_version: kv as u64,
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
                        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
                            &[&namespace_str, &last_sent_i64, &pull_limit_i64];
                        let rows_res = client_guard.query(
                            "SELECT id, namespace, sequence, doc_id, record_id, encrypted_blob, timestamp, key_version
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
                                    let kv: i64 = row.get(7);
                                    let pm = PendingMutation {
                                        id: row.get(0),
                                        namespace: row.get(1),
                                        sequence: seq as u64,
                                        doc_id: row.get(3),
                                        record_id: row.get(4),
                                        encrypted_blob: row.get(5),
                                        timestamp: ts as u64,
                                        key_version: kv as u64,
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

        let tx = client_guard
            .transaction()
            .await
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
        )
        .await
        .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let params2: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
            &[&namespace_str, &schema_version_i64];
        tx.execute(
            "INSERT INTO schema_versions (namespace, version)
             VALUES ($1, $2)
             ON CONFLICT (namespace) DO UPDATE
             SET version = GREATEST(schema_versions.version, EXCLUDED.version)",
            params2,
        )
        .await
        .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
            &[&now, &namespace, &replica_id];
        client_guard
            .execute(
                "UPDATE replicas SET last_seen = $1 WHERE namespace = $2 AND replica_id = $3",
                params,
            )
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace];
        let row_opt = client_guard
            .query_opt(
                "SELECT version FROM schema_versions WHERE namespace = $1",
                params,
            )
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if let Some(row) = row_opt {
            let v: i64 = row.get(0);
            Ok(v as u64)
        } else {
            Ok(0)
        }
    }

    async fn update_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        key_version: u64,
    ) -> Result<(), CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
            &[&public_key, &(key_version as i64), &namespace, &replica_id];
        client_guard.execute(
            "UPDATE replicas SET public_key = $1, key_version = $2 WHERE namespace = $3 AND replica_id = $4",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace, &replica_id];
        let row_opt = client_guard.query_opt(
            "SELECT public_key, key_version FROM replicas WHERE namespace = $1 AND replica_id = $2",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if let Some(row) = row_opt {
            let pk: Vec<u8> = row.get(0);
            let kv: i64 = row.get(1);
            Ok(Some((pk, kv as u64)))
        } else {
            Ok(None)
        }
    }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace];
        let rows = client_guard.query(
            "SELECT replica_id, namespace, public_key, schema_version
             FROM replicas
             WHERE namespace = $1",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut replicas = Vec::new();
        for row in rows {
            let schema_version: i64 = row.get(3);
            replicas.push(ReplicaInfo {
                replica_id: row.get(0),
                namespace: row.get(1),
                public_key: row.get(2),
                schema_version: schema_version as u64,
            });
        }
        Ok(replicas)
    }

    async fn get_snapshot(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<vaultsync_core::crdt::snapshot::Snapshot>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace, &doc_id, &record_id];
        let row_opt = client_guard.query_opt(
            "SELECT bytes FROM snapshots WHERE namespace = $1 AND doc_id = $2 AND record_id = $3",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if let Some(row) = row_opt {
            let bytes: Vec<u8> = row.get(0);
            let snap = vaultsync_core::crdt::snapshot::Snapshot::decode(&bytes)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok(Some(snap))
        } else {
            Ok(None)
        }
    }

    async fn store_snapshot(
        &self,
        namespace: &str,
        snapshot: &vaultsync_core::crdt::snapshot::Snapshot,
    ) -> Result<(), CoordinatorError> {
        let client_guard = self.client.lock().await;
        let bytes = snapshot.encode().map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        let seq_i64 = snapshot.sequence as i64;
        let created_at_i64 = snapshot.created_at as i64;
        let checksum_i64 = snapshot.checksum as i64;

        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
            &namespace,
            &snapshot.doc_id,
            &snapshot.record_id,
            &seq_i64,
            &created_at_i64,
            &bytes,
            &checksum_i64,
        ];

        client_guard.execute(
            "INSERT INTO snapshots (namespace, doc_id, record_id, sequence, created_at, bytes, checksum)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (namespace, doc_id, record_id) DO UPDATE
             SET sequence = EXCLUDED.sequence,
                 created_at = EXCLUDED.created_at,
                 bytes = EXCLUDED.bytes,
                 checksum = EXCLUDED.checksum",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn list_snapshots(
        &self,
        namespace: &str,
    ) -> Result<Vec<vaultsync_core::crdt::snapshot::Snapshot>, CoordinatorError> {
        let client_guard = self.client.lock().await;
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&namespace];
        let rows = client_guard.query(
            "SELECT bytes FROM snapshots WHERE namespace = $1",
            params,
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut snaps = Vec::new();
        for row in rows {
            let bytes: Vec<u8> = row.get(0);
            let snap = vaultsync_core::crdt::snapshot::Snapshot::decode(&bytes)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            snaps.push(snap);
        }
        Ok(snaps)
    }

    async fn compact_oplog(
        &self,
        namespace: &str,
    ) -> Result<vaultsync_core::sync::compaction::CompactionStats, CoordinatorError> {
        let mut client_guard = self.client.lock().await;
        let namespace_str = namespace.to_string();

        let tx = client_guard
            .transaction()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        // 1. Get all snapshots for the namespace
        let rows = tx.query(
            "SELECT doc_id, record_id, sequence FROM snapshots WHERE namespace = $1",
            &[&namespace_str],
        ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut snaps = Vec::new();
        for row in rows {
            let seq: i64 = row.get(2);
            snaps.push((
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                seq,
            ));
        }

        let mut oplog_removed = 0;
        let mut snapshots_collapsed = 0;

        // 2. Delete mutations with sequence <= snap.sequence
        for (doc_id, record_id, sequence) in snaps {
            let deleted = tx.execute(
                "DELETE FROM mutations
                 WHERE namespace = $1 AND doc_id = $2 AND record_id = $3 AND sequence <= $4",
                &[&namespace_str, &doc_id, &record_id, &sequence],
            ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            if deleted > 0 {
                oplog_removed += deleted as usize;
                snapshots_collapsed += 1;
            }
        }

        tx.commit()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(vaultsync_core::sync::compaction::CompactionStats {
            oplog_removed,
            docs_removed: 0,
            snapshots_collapsed,
        })
    }
}

struct PostgresSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for PostgresSubscription {
    type Item = PendingMutation;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
