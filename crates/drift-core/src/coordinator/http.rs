use async_trait::async_trait;
use crate::coordinator::traits::*;
use crate::coordinator::ws_proto::*;
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
use futures::SinkExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpCoordinatorConfig {
    pub url: String,
    pub auth_token: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
struct WsClientHandle {
    tx: tokio::sync::mpsc::Sender<tokio_tungstenite::tungstenite::Message>,
    pending_requests: Arc<tokio::sync::Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<Vec<u8>>>>>,
    /// Snapshots received during the REGISTER handshake, keyed by (doc_id, record_id)
    received_snapshots: Arc<tokio::sync::Mutex<Vec<crate::crdt::snapshot::Snapshot>>>,
}

#[derive(Debug, Clone)]
pub struct HttpCoordinator {
    client: reqwest::Client,
    config: HttpCoordinatorConfig,
    #[cfg(not(target_arch = "wasm32"))]
    ws_client: Arc<tokio::sync::Mutex<Option<WsClientHandle>>>,
    schema_version: Arc<std::sync::atomic::AtomicU64>,
}

impl HttpCoordinator {
    pub fn new(config: HttpCoordinatorConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
            #[cfg(not(target_arch = "wasm32"))]
            ws_client: Arc::new(tokio::sync::Mutex::new(None)),
            schema_version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn try_connect_ws(&self, namespace: &str, after: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        use tokio_tungstenite::tungstenite::Message;

        let ws_url = get_ws_url(&self.config.url, namespace);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let (mut ws_sink, mut ws_stream) = ws_stream.split();

        // 1. AUTH handshake
        let token = self.config.auth_token.clone().unwrap_or_default();
        let auth = AuthPayload {
            token,
            protocol_version: 1,
            replica_id: format!("client-{}", uuid::Uuid::new_v4()),
            namespace: namespace.to_string(),
        };

        let auth_frame = encode_frame(MSG_AUTH, &auth)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws_sink.send(Message::Binary(auth_frame)).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let resp = ws_stream.next().await
            .ok_or_else(|| CoordinatorError::Internal("Connection closed".to_string()))?
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let bin = match resp {
            Message::Binary(b) => b,
            _ => return Err(CoordinatorError::Internal("Expected binary frame during AUTH".to_string())),
        };

        let (msg_type, payload) = decode_frame(&bin)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_AUTH_ACK {
            return Err(CoordinatorError::Internal(format!("Expected MSG_AUTH_ACK, got {:02X}", msg_type)));
        }

        let auth_ack: AuthAckPayload = serde_json::from_slice(payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if auth_ack.status != "ok" {
            return Err(CoordinatorError::AuthFailed);
        }

        // 2. REGISTER handshake
        let current_version = self.schema_version.load(std::sync::atomic::Ordering::Relaxed);
        let reg = RegisterPayload {
            replica_id: auth.replica_id.clone(),
            namespace: namespace.to_string(),
            public_key: vec![],
            schema_version: current_version,
            last_sequence: after,
            key_version: 1,
        };

        let reg_frame = encode_frame(MSG_REGISTER, &reg)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws_sink.send(Message::Binary(reg_frame)).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let resp = ws_stream.next().await
            .ok_or_else(|| CoordinatorError::Internal("Connection closed during REGISTER".to_string()))?
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let bin = match resp {
            Message::Binary(b) => b,
            _ => return Err(CoordinatorError::Internal("Expected binary frame during REGISTER".to_string())),
        };

        let (msg_type, _payload) = decode_frame(&bin)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_REGISTER_ACK {
            return Err(CoordinatorError::Internal(format!("Expected MSG_REGISTER_ACK, got {:02X}", msg_type)));
        }

        // 2.5 Drain any MSG_SNAPSHOT frames the server pushes immediately after REGISTER_ACK.
        // The server sends N snapshot frames (one per document behind), then waits for the
        // client to send SCHEMA_SYNC.  We collect them here so callers can apply them.
        let mut bootstrap_snapshots: Vec<crate::crdt::snapshot::Snapshot> = Vec::new();
        loop {
            // Peek the next frame with a short timeout; if nothing arrives quickly
            // the server has finished sending snapshots.
            let next = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                ws_stream.next(),
            ).await;
            match next {
                Ok(Some(Ok(Message::Binary(peeked)))) => {
                    if let Ok((peeked_type, peeked_payload)) = decode_frame(&peeked) {
                        if peeked_type == MSG_SNAPSHOT {
                            if let Ok(snap_payload) = serde_json::from_slice::<SnapshotPayload>(peeked_payload) {
                                let snap = crate::crdt::snapshot::Snapshot {
                                    doc_id: snap_payload.doc_id,
                                    record_id: snap_payload.record_id,
                                    schema_version: 0,
                                    sequence: snap_payload.sequence,
                                    created_at: 0,
                                    bytes: snap_payload.bytes,
                                    checksum: snap_payload.checksum,
                                };
                                if snap.verify_checksum() {
                                    bootstrap_snapshots.push(snap);
                                }
                            }
                            continue; // read more snapshot frames
                        }
                        // Non-snapshot frame — put it back... we can't unread, so just warn and drop.
                        // In practice this should not happen because the server only sends snapshots
                        // between REGISTER_ACK and waiting for SCHEMA_SYNC.
                        warn!("Unexpected frame type 0x{:02X} while draining post-REGISTER snapshots — ignoring", peeked_type);
                    }
                    break;
                }
                _ => break, // timeout or close — done draining
            }
        }

        // 2.5. SCHEMA_SYNC handshake
        let sync = SchemaSyncPayload {
            namespace: namespace.to_string(),
            version: current_version,
        };
        let sync_frame = encode_frame(MSG_SCHEMA_SYNC, &sync)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws_sink.send(Message::Binary(sync_frame)).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let resp = ws_stream.next().await
            .ok_or_else(|| CoordinatorError::Internal("Connection closed during SCHEMA_SYNC".to_string()))?
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let bin = match resp {
            Message::Binary(b) => b,
            _ => return Err(CoordinatorError::Internal("Expected binary frame during SCHEMA_SYNC".to_string())),
        };

        let (msg_type, payload) = decode_frame(&bin)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_SCHEMA_MIGRATION {
            return Err(CoordinatorError::Internal(format!("Expected MSG_SCHEMA_MIGRATION, got {:02X}", msg_type)));
        }

        let migration_payload: SchemaMigrationPayload = serde_json::from_slice(payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if migration_payload.status == "mismatch" {
            return Err(CoordinatorError::SchemaMismatch);
        }

        // 3. SUBSCRIBE handshake
        let sub = SubscribePayload {
            request_id: uuid::Uuid::new_v4().to_string(),
            namespace: namespace.to_string(),
            after,
        };

        let sub_frame = encode_frame(MSG_SUBSCRIBE, &sub)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws_sink.send(Message::Binary(sub_frame)).await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        // Setup writer/reader tasks
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Message>(128);
        let pending_requests = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let pending_requests_clone = pending_requests.clone();

        // Writer task
        crate::time_utils::spawn(async move {
            while let Some(msg) = write_rx.recv().await {
                if ws_sink.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Heartbeat task
        let write_tx_heartbeat = write_tx.clone();
        let replica_id_hb = auth.replica_id.clone();
        crate::time_utils::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let hb = HeartbeatPayload {
                    replica_id: replica_id_hb.clone(),
                    namespace: "".to_string(),
                };
                if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                    if write_tx_heartbeat.send(Message::Binary(frame)).await.is_err() {
                        break;
                    }
                }
            }
        });

        let (mut_tx, mut_rx) = tokio::sync::mpsc::channel::<PendingMutation>(1024);

        // Reader task
        let ws_client_to_clear = self.ws_client.clone();
        // Build the shared snapshot cache from bootstrapped snapshots before locking ws_client
        let received_snapshots_arc = Arc::new(tokio::sync::Mutex::new(bootstrap_snapshots));
        let mut client_lock = self.ws_client.lock().await;
        *client_lock = Some(WsClientHandle {
            tx: write_tx,
            pending_requests: pending_requests_clone,
            received_snapshots: received_snapshots_arc.clone(),
        });
        drop(client_lock);

        let _namespace_str = namespace.to_string();
        crate::time_utils::spawn(async move {
            while let Some(msg_res) = ws_stream.next().await {
                let msg = match msg_res {
                    Ok(m) => m,
                    Err(_) => break,
                };

                if let Message::Binary(bin) = msg {
                    if let Ok((msg_type, payload)) = decode_frame(&bin) {
                        if msg_type == MSG_MUTATION_PUSH {
                            if let Ok(mutat) = serde_json::from_slice::<PendingMutation>(payload) {
                                if mut_tx.send(mutat).await.is_err() {
                                    break;
                                }
                            }
                        } else if msg_type == MSG_SNAPSHOT {
                            // Server occasionally pushes a fresh snapshot (e.g. after compaction).
                            // Store it in the cache; the sync engine will apply it via get_snapshot.
                            if let Ok(snap_payload) = serde_json::from_slice::<SnapshotPayload>(payload) {
                                if snap_payload.checksum != 0 {
                                    let snap = crate::crdt::snapshot::Snapshot {
                                        doc_id: snap_payload.doc_id,
                                        record_id: snap_payload.record_id,
                                        schema_version: 0,
                                        sequence: snap_payload.sequence,
                                        created_at: 0,
                                        bytes: snap_payload.bytes,
                                        checksum: snap_payload.checksum,
                                    };
                                    if snap.verify_checksum() {
                                        let mut snaps = received_snapshots_arc.lock().await;
                                        // Update or insert keyed by (doc_id, record_id)
                                        if let Some(existing) = snaps.iter_mut().find(|s| s.doc_id == snap.doc_id && s.record_id == snap.record_id) {
                                            if snap.sequence > existing.sequence {
                                                *existing = snap;
                                            }
                                        } else {
                                            snaps.push(snap);
                                        }
                                    }
                                }
                            }
                        } else {
                            if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(req_id) = json_val.get("request_id").and_then(|v| v.as_str()) {
                                    let mut reqs = pending_requests.lock().await;
                                    if let Some(tx) = reqs.remove(req_id) {
                                        let _ = tx.send(bin);
                                    }
                                }
                            }
                        }
                    }
                } else if msg.is_close() {
                    break;
                }
            }

            let mut client_lock = ws_client_to_clear.lock().await;
            *client_lock = None;
        });

        Ok(Box::new(HttpSubscription { rx: mut_rx }))
    }
}

fn get_ws_url(http_url: &str, namespace: &str) -> String {
    let trimmed = http_url.trim_end_matches('/');
    let base = if trimmed.starts_with("https://") {
        trimmed.replace("https://", "wss://")
    } else if trimmed.starts_with("http://") {
        trimmed.replace("http://", "ws://")
    } else {
        format!("ws://{}", trimmed)
    };
    format!("{}/namespace/{}/ws", base, namespace)
}

#[async_trait]
impl Coordinator for HttpCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let req_id = uuid::Uuid::new_v4().to_string();
                let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
                {
                    let mut reqs = handle.pending_requests.lock().await;
                    reqs.insert(req_id.clone(), tx);
                }

                let push = PushPayload {
                    request_id: req_id,
                    mutations: mutations.clone(),
                };

                if let Ok(frame) = encode_frame(MSG_PUSH, &push) {
                    if handle.tx.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await.is_ok() {
                        if let Ok(resp_bin) = rx.await {
                            if let Ok((msg_type, payload)) = decode_frame(&resp_bin) {
                                if msg_type == MSG_PUSH_ACK {
                                    if let Ok(ack) = serde_json::from_slice::<PushAckPayload>(payload) {
                                        if let Some(err) = ack.error {
                                            return Err(CoordinatorError::Internal(err));
                                        }
                                        return Ok(ack.sequences);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fallback to HTTP POST
        let url = format!("{}/namespace/{}/push", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&mutations);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let seq_ids = resp.json::<Vec<SequenceId>>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(seq_ids)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let req_id = uuid::Uuid::new_v4().to_string();
                let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
                {
                    let mut reqs = handle.pending_requests.lock().await;
                    reqs.insert(req_id.clone(), tx);
                }

                let pull = PullPayload {
                    request_id: req_id,
                    namespace: namespace.to_string(),
                    after,
                    limit,
                };

                if let Ok(frame) = encode_frame(MSG_PULL, &pull) {
                    if handle.tx.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await.is_ok() {
                        if let Ok(resp_bin) = rx.await {
                            if let Ok((msg_type, payload)) = decode_frame(&resp_bin) {
                                if msg_type == MSG_PULL_RESPONSE {
                                    if let Ok(resp) = serde_json::from_slice::<PullResponsePayload>(payload) {
                                        return Ok(resp.mutations);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fallback to HTTP GET
        let url = format!(
            "{}/namespace/{}/pull?after={}&limit={}",
            self.config.url.trim_end_matches('/'),
            namespace,
            after,
            limit
        );
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let mutations = resp.json::<Vec<PendingMutation>>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(mutations)
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            match self.try_connect_ws(namespace, from_sequence).await {
                Ok(stream) => return Ok(stream),
                Err(CoordinatorError::AuthFailed) => return Err(CoordinatorError::AuthFailed),
                Err(e) => {
                    warn!("WebSocket subscription failed, falling back to SSE: {:?}", e);
                }   
            }
        }

        // Fallback to SSE stream
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let client = self.client.clone();
        let config = self.config.clone();
        let ns = namespace.to_string();
        
        crate::time_utils::spawn(async move {
            let mut after = from_sequence;
            let url_base = config.url.clone();
            
            loop {
                let url = format!(
                    "{}/namespace/{}/events?after={}",
                    url_base.trim_end_matches('/'),
                    ns,
                    after
                );
                
                let mut builder = client.get(&url).header("Accept", "text/event-stream");
                if let Some(ref token) = config.auth_token {
                    builder = builder.bearer_auth(token);
                }
                
                let resp = match builder.send().await {
                    Ok(r) if r.status().is_success() => r,
                    _ => {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                
                let mut stream = resp.bytes_stream();
                let mut buffer = Vec::new();
                
                while let Some(chunk_res) = stream.next().await {
                    let chunk = match chunk_res {
                        Ok(bytes) => bytes,
                        Err(_) => break,
                    };
                    
                    buffer.extend_from_slice(&chunk);
                    
                    while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                        let line_bytes = buffer.drain(..=pos).collect::<Vec<u8>>();
                        let line = match std::str::from_utf8(&line_bytes) {
                            Ok(s) => s.trim(),
                            Err(_) => continue,
                        };
                        
                        if line.starts_with("data:") {
                            let data_json = line["data:".len()..].trim();
                            if let Ok(mutation) = serde_json::from_str::<PendingMutation>(data_json) {
                                after = after.max(mutation.sequence);
                                if tx.send(mutation).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
                
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        
        Ok(Box::new(HttpSubscription { rx }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        self.schema_version.store(info.schema_version, std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let reg = RegisterPayload {
                    replica_id: info.replica_id.clone(),
                    namespace: namespace.to_string(),
                    public_key: info.public_key.clone(),
                    schema_version: info.schema_version,
                    last_sequence: 0,
                    key_version: 1,
                };
                if let Ok(frame) = encode_frame(MSG_REGISTER, &reg) {
                    if handle.tx.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await.is_ok() {
                        return Ok(());
                    }
                }
            }
        }

        // Fallback to HTTP POST
        let url = format!("{}/namespace/{}/register", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&info);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        Ok(())
    }

    async fn update_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        key_version: u64,
    ) -> Result<(), CoordinatorError> {
        let url = format!("{}/namespace/{}/replicas/{}/key", self.config.url.trim_end_matches('/'), namespace, replica_id);
        let payload = crate::coordinator::ws_proto::UpdateReplicaKeyPayload {
            public_key,
            key_version,
        };
        let mut builder = self.client.put(&url).json(&payload);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        Ok(())
    }

    async fn get_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        let url = format!("{}/namespace/{}/replicas/{}/key", self.config.url.trim_end_matches('/'), namespace, replica_id);
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let payload: Option<crate::coordinator::ws_proto::UpdateReplicaKeyPayload> = resp.json().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(payload.map(|p| (p.public_key, p.key_version)))
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let hb = HeartbeatPayload {
                    replica_id: replica_id.to_string(),
                    namespace: namespace.to_string(),
                };
                if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                    if handle.tx.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await.is_ok() {
                        return Ok(());
                    }
                }
            }
        }

        // Fallback to HTTP POST
        let url = format!(
            "{}/namespace/{}/heartbeat?replica_id={}",
            self.config.url.trim_end_matches('/'),
            namespace,
            replica_id
        );
        let mut builder = self.client.post(&url);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        Ok(())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let url = format!("{}/namespace/{}/schema_version", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let version = resp.json::<u64>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(version)
    }

    async fn store_snapshot(&self, _namespace: &str, snapshot: &crate::crdt::snapshot::Snapshot) -> Result<(), CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let frame = encode_frame(MSG_SNAPSHOT, snapshot)
                    .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                if handle.tx.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await.is_ok() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn get_snapshot(
        &self,
        _namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let snaps = handle.received_snapshots.lock().await;
                let found = snaps
                    .iter()
                    .find(|s| s.doc_id == doc_id && s.record_id == record_id)
                    .cloned();
                return Ok(found);
            }
        }
        Ok(None)
    }

    async fn list_snapshots(
        &self,
        _namespace: &str,
    ) -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let snaps = handle.received_snapshots.lock().await;
                return Ok(snaps.clone());
            }
        }
        Ok(vec![])
    }
}

struct HttpSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for HttpSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
