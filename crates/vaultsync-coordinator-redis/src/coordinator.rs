use async_trait::async_trait;
use futures::Stream;
use vaultsync_core::coordinator::traits::*;

#[derive(Debug, Clone)]
pub struct RedisCoordinator {
    conn: redis::aio::MultiplexedConnection,
}

impl RedisCoordinator {
    pub async fn new(url: &str) -> Result<Self, CoordinatorError> {
        tracing::info!("Connecting to Redis: {}", url);
        let client =
            redis::Client::open(url).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        let conn = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(Self { conn })
    }
}

fn stream_id_to_seq(id: &str) -> SequenceId {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    let ms: u64 = ms.parse().unwrap_or(0);
    let seq: u64 = seq.parse().unwrap_or(0);
    ms * 1_000_000 + seq
}

fn seq_to_stream_id(seq: SequenceId) -> String {
    let ms = seq / 1_000_000;
    let s = seq % 1_000_000;
    format!("{}-{}", ms, s)
}

fn parse_stream_entry(entry_val: &redis::Value, namespace: &str) -> Option<PendingMutation> {
    let bulk = match entry_val {
        redis::Value::Bulk(b) if b.len() == 2 => b,
        _ => return None,
    };

    let id_str = match &bulk[0] {
        redis::Value::Data(d) => std::str::from_utf8(d).ok()?,
        _ => return None,
    };
    let sequence = stream_id_to_seq(id_str);

    let fields = match &bulk[1] {
        redis::Value::Bulk(f) => f,
        _ => return None,
    };

    let mut id = None;
    let mut doc_id = None;
    let mut record_id = None;
    let mut encrypted_blob = None;
    let mut timestamp = None;
    let mut key_version = None;
    let mut replica_id = None;

    for chunk in fields.chunks_exact(2) {
        let key = match &chunk[0] {
            redis::Value::Data(d) => std::str::from_utf8(d).ok()?,
            _ => continue,
        };
        match key {
            "id" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    id = Some(std::str::from_utf8(d).ok()?.to_string());
                }
            }
            "doc_id" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    doc_id = Some(std::str::from_utf8(d).ok()?.to_string());
                }
            }
            "record_id" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    record_id = Some(std::str::from_utf8(d).ok()?.to_string());
                }
            }
            "blob" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    encrypted_blob = Some(d.clone());
                }
            }
            "ts" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    let ts_str = std::str::from_utf8(d).ok()?;
                    timestamp = Some(ts_str.parse::<u64>().ok()?);
                } else if let redis::Value::Int(i) = &chunk[1] {
                    timestamp = Some(*i as u64);
                }
            }
            "key_version" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    let kv_str = std::str::from_utf8(d).ok()?;
                    key_version = Some(kv_str.parse::<u64>().ok()?);
                } else if let redis::Value::Int(i) = &chunk[1] {
                    key_version = Some(*i as u64);
                }
            }
            "replica_id" => {
                if let redis::Value::Data(d) = &chunk[1] {
                    replica_id = Some(std::str::from_utf8(d).ok()?.to_string());
                }
            }
            _ => {}
        }
    }

    Some(PendingMutation {
        id: id?,
        namespace: namespace.to_string(),
        sequence,
        doc_id: doc_id?,
        record_id: record_id?,
        encrypted_blob: encrypted_blob?,
        timestamp: timestamp.unwrap_or(0),
        key_version: key_version.unwrap_or(1),
        replica_id: replica_id.unwrap_or_default(),
    })
}

#[async_trait]
impl Coordinator for RedisCoordinator {
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let stream_key = format!("vaultsync:{}:mutations", namespace);
        let pushed_key = format!("vaultsync:{}:pushed", namespace);
        let mut seqs = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            // Deduplicate mutation ID using SADD
            let is_new: u8 = redis::cmd("SADD")
                .arg(&pushed_key)
                .arg(&mutation.id)
                .query_async(&mut conn)
                .await
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

            if is_new == 0 {
                // Duplicate! We return a dummy sequence id (0)
                seqs.push(0);
                continue;
            }

            let res: String = redis::cmd("XADD")
                .arg(&stream_key)
                .arg("*")
                .arg("id")
                .arg(&mutation.id)
                .arg("replica_id")
                .arg(&mutation.replica_id)
                .arg("doc_id")
                .arg(&mutation.doc_id)
                .arg("record_id")
                .arg(&mutation.record_id)
                .arg("blob")
                .arg(&mutation.encrypted_blob)
                .arg("ts")
                .arg(mutation.timestamp.to_string())
                .arg("schema_version")
                .arg(mutation.schema_version.to_string())
                .arg("key_version")
                .arg(mutation.key_version.to_string())
                .query_async(&mut conn)
                .await
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

            seqs.push(stream_id_to_seq(&res));
        }
        Ok(seqs)
    }

    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let stream_key = format!("vaultsync:{}:mutations", namespace);
        let start_id = seq_to_stream_id(after.saturating_add(1));

        let res: redis::Value = redis::cmd("XRANGE")
            .arg(&stream_key)
            .arg(&start_id)
            .arg("+")
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut mutations = Vec::new();
        if let redis::Value::Bulk(entries) = res {
            for entry in entries {
                if let Some(m) = parse_stream_entry(&entry, namespace) {
                    mutations.push(m);
                }
            }
        }

        Ok(mutations)
    }

    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
        _last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        let mut conn = self.conn.clone();
        let replicas_key = format!("vaultsync:{}:replicas", namespace);
        let schema_key = format!("vaultsync:{}:schema_version", namespace);

        let info_str =
            serde_json::to_string(&info).map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        redis::cmd("HSET")
            .arg(&replicas_key)
            .arg(&info.replica_id)
            .arg(&info_str)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let current_version: Option<String> = redis::cmd("GET")
            .arg(&schema_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let current_v = match current_version {
            Some(s) => s.parse::<u64>().unwrap_or(0),
            None => 0,
        };

        if info.schema_version > current_v {
            redis::cmd("SET")
                .arg(&schema_key)
                .arg(info.schema_version.to_string())
                .query_async::<_, ()>(&mut conn)
                .await
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let mut conn = self.conn.clone();
        let heartbeats_key = format!("vaultsync:{}:replicas:heartbeats", namespace);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        redis::cmd("HSET")
            .arg(&heartbeats_key)
            .arg(replica_id)
            .arg(now.to_string())
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx_mpsc, rx_mpsc) = tokio::sync::mpsc::channel(1024);
        let mut conn = self.conn.clone();
        let stream_key = format!("vaultsync:{}:mutations", namespace);
        let namespace = namespace.to_string();

        tokio::spawn(async move {
            let mut last_id_str = if from_sequence == 0 {
                "0-0".to_string()
            } else {
                seq_to_stream_id(from_sequence)
            };

            loop {
                let res: redis::Value = match redis::cmd("XREAD")
                    .arg("BLOCK")
                    .arg(500)
                    .arg("COUNT")
                    .arg(100)
                    .arg("STREAMS")
                    .arg(&stream_key)
                    .arg(&last_id_str)
                    .query_async(&mut conn)
                    .await
                {
                    Ok(val) => val,
                    Err(e) => {
                        tracing::error!("Redis XREAD error: {}; retrying in 1s...", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let mut entries = Vec::new();
                if let redis::Value::Bulk(streams) = res {
                    for stream in streams {
                        if let redis::Value::Bulk(si) = stream {
                            if si.len() == 2 {
                                if let redis::Value::Bulk(e) = &si[1] {
                                    entries.extend(e.clone());
                                }
                            }
                        }
                    }
                }

                for entry in entries {
                    let entry_id = match &entry {
                        redis::Value::Bulk(b) if b.len() == 2 => match &b[0] {
                            redis::Value::Data(d) => {
                                std::str::from_utf8(d).ok().map(|s| s.to_string())
                            }
                            _ => None,
                        },
                        _ => None,
                    };

                    if let Some(id_str) = entry_id {
                        if let Some(m) = parse_stream_entry(&entry, &namespace) {
                            last_id_str = id_str;
                            if tx_mpsc.send(m).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Box::new(RedisSubscription { rx: rx_mpsc }))
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let mut conn = self.conn.clone();
        let schema_key = format!("vaultsync:{}:schema_version", namespace);

        let res: Option<String> = redis::cmd("GET")
            .arg(&schema_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        match res {
            Some(s) => Ok(s.parse::<u64>().unwrap_or(0)),
            None => Ok(0),
        }
    }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let replicas_key = format!("vaultsync:{}:replicas", namespace);

        let res: Vec<String> = redis::cmd("HVALS")
            .arg(&replicas_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut replicas = Vec::new();
        for s in res {
            if let Ok(info) = serde_json::from_str(&s) {
                replicas.push(info);
            }
        }
        Ok(replicas)
    }

    async fn update_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        key_version: u64,
    ) -> Result<(), CoordinatorError> {
        let mut conn = self.conn.clone();
        let key = format!("vaultsync:{}:replica_keys", namespace);
        let val = serde_json::to_string(&(public_key, key_version))
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        redis::cmd("HSET")
            .arg(&key)
            .arg(replica_id)
            .arg(&val)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let key = format!("vaultsync:{}:replica_keys", namespace);
        let res: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg(replica_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if let Some(s) = res {
            let parsed: (Vec<u8>, u64) =
                serde_json::from_str(&s).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok(Some(parsed))
        } else {
            Ok(None)
        }
    }

    async fn get_snapshot(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<vaultsync_core::crdt::snapshot::Snapshot>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let key = format!("vaultsync:{}:snapshot:{}:{}", namespace, doc_id, record_id);

        let res: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if let Some(bytes) = res {
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
        let mut conn = self.conn.clone();
        let key = format!(
            "vaultsync:{}:snapshot:{}:{}",
            namespace, snapshot.doc_id, snapshot.record_id
        );
        let set_key = format!("vaultsync:{}:snapshot_keys", namespace);
        let bytes = snapshot
            .encode()
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        // Store bytes in GET/SET key
        redis::cmd("SET")
            .arg(&key)
            .arg(&bytes)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        // Add key to set of snapshot keys
        redis::cmd("SADD")
            .arg(&set_key)
            .arg(&key)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn list_snapshots(
        &self,
        namespace: &str,
    ) -> Result<Vec<vaultsync_core::crdt::snapshot::Snapshot>, CoordinatorError> {
        let mut conn = self.conn.clone();
        let set_key = format!("vaultsync:{}:snapshot_keys", namespace);

        let keys: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&set_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut cmd = redis::cmd("MGET");
        for key in &keys {
            cmd.arg(key);
        }

        let res: Vec<Option<Vec<u8>>> = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mut snaps = Vec::new();
        for bytes_opt in res {
            if let Some(bytes) = bytes_opt {
                if let Ok(snap) = vaultsync_core::crdt::snapshot::Snapshot::decode(&bytes) {
                    snaps.push(snap);
                }
            }
        }
        Ok(snaps)
    }

    async fn compact_oplog(
        &self,
        namespace: &str,
    ) -> Result<vaultsync_core::sync::compaction::CompactionStats, CoordinatorError> {
        let mut conn = self.conn.clone();
        let stream_key = format!("vaultsync:{}:mutations", namespace);
        let snaps = self.list_snapshots(namespace).await?;
        let mut oplog_removed = 0;
        let mut snapshots_collapsed = 0;

        for snap in snaps {
            let start_id = "0-0".to_string();
            let end_id = seq_to_stream_id(snap.sequence);

            let res: redis::Value = redis::cmd("XRANGE")
                .arg(&stream_key)
                .arg(&start_id)
                .arg(&end_id)
                .query_async(&mut conn)
                .await
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

            if let redis::Value::Bulk(entries) = res {
                for entry in entries {
                    if let redis::Value::Bulk(bulk) = entry {
                        if bulk.len() == 2 {
                            if let redis::Value::Data(id_bytes) = &bulk[0] {
                                if let Ok(stream_id) = std::str::from_utf8(id_bytes) {
                                    let deleted: usize = redis::cmd("XDEL")
                                        .arg(&stream_key)
                                        .arg(stream_id)
                                        .query_async(&mut conn)
                                        .await
                                        .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                                    oplog_removed += deleted;
                                }
                            }
                        }
                    }
                }
                snapshots_collapsed += 1;
            }
        }

        Ok(vaultsync_core::sync::compaction::CompactionStats {
            oplog_removed,
            docs_removed: 0,
            snapshots_collapsed,
        })
    }
}

struct RedisSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for RedisSubscription {
    type Item = PendingMutation;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
