use crate::coordinator::http::HttpCoordinatorConfig;
use crate::coordinator::traits::*;
use crate::coordinator::ws_proto::*;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
use futures::SinkExt;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
struct MuxWsHandle {
    tx: tokio::sync::mpsc::Sender<tokio_tungstenite::tungstenite::Message>,
    pending_requests: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<Vec<u8>>>,
        >,
    >,
    pending_registers: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<NamespaceAckPayload>>,
        >,
    >,
    pending_schema_syncs: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::oneshot::Sender<SchemaMigrationPayload>>,
        >,
    >,
    namespace_txs: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::mpsc::Sender<PendingMutation>>,
        >,
    >,
    snapshots: Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, Vec<crate::crdt::snapshot::Snapshot>>>,
    >,
}

#[derive(Clone, Debug)]
struct ActiveNamespaceConfig {
    replica_id: String,
    public_key: Vec<u8>,
    schema_version: u64,
    last_sequence: u64,
}

#[derive(Debug, Clone)]
pub struct MuxCoordinator {
    client: reqwest::Client,
    config: HttpCoordinatorConfig,

    #[cfg(not(target_arch = "wasm32"))]
    ws_client: Arc<tokio::sync::Mutex<Option<MuxWsHandle>>>,

    active_namespaces:
        Arc<tokio::sync::Mutex<std::collections::HashMap<String, ActiveNamespaceConfig>>>,
}

impl MuxCoordinator {
    pub fn new(config: HttpCoordinatorConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
            #[cfg(not(target_arch = "wasm32"))]
            ws_client: Arc::new(tokio::sync::Mutex::new(None)),
            active_namespaces: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn ensure_connected(
        self_arc: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MuxWsHandle, CoordinatorError>> + Send>,
    > {
        Box::pin(async move {
            {
                let ws_client_lock = self_arc.ws_client.lock().await;
                if let Some(ref handle) = *ws_client_lock {
                    return Ok(handle.clone());
                }
            }

            let ws_url = get_ws_mux_url(&self_arc.config.url);
            let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
                .await
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

            let (mut ws_sink, mut ws_stream) = ws_stream.split();
            let (write_tx, mut write_rx) =
                tokio::sync::mpsc::channel::<tokio_tungstenite::tungstenite::Message>(128);

            // Writer task
            crate::time_utils::spawn(async move {
                while let Some(msg) = write_rx.recv().await {
                    if ws_sink.send(msg).await.is_err() {
                        break;
                    }
                }
            });

            let pending_requests =
                Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
            let pending_registers =
                Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
            let pending_schema_syncs =
                Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
            let namespace_txs = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
            let snapshots = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

            let handle = MuxWsHandle {
                tx: write_tx.clone(),
                pending_requests: pending_requests.clone(),
                pending_registers: pending_registers.clone(),
                pending_schema_syncs: pending_schema_syncs.clone(),
                namespace_txs: namespace_txs.clone(),
                snapshots: snapshots.clone(),
            };

            // Heartbeat task
            let write_tx_heartbeat = write_tx.clone();
            let active_namespaces_hb = self_arc.active_namespaces.clone();
            crate::time_utils::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    let namespaces = {
                        let guard = active_namespaces_hb.lock().await;
                        guard
                            .iter()
                            .map(|(ns, cfg)| (ns.clone(), cfg.replica_id.clone()))
                            .collect::<Vec<_>>()
                    };

                    if namespaces.is_empty() {
                        let hb = HeartbeatPayload {
                            replica_id: "client-mux".to_string(),
                            namespace: "".to_string(),
                        };
                        if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                            if write_tx_heartbeat
                                .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    } else {
                        for (ns, replica_id) in namespaces {
                            let hb = HeartbeatPayload {
                                replica_id,
                                namespace: ns,
                            };
                            if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                                if write_tx_heartbeat
                                    .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            // Reader task
            let ws_client_to_clear = self_arc.ws_client.clone();
            let active_namespaces_clone = self_arc.active_namespaces.clone();
            let pending_requests_clone = pending_requests.clone();
            let pending_registers_clone = pending_registers.clone();
            let pending_schema_syncs_clone = pending_schema_syncs.clone();
            let namespace_txs_clone = namespace_txs.clone();
            let snapshots_clone = snapshots.clone();

            let self_weak = Arc::downgrade(&self_arc);

            crate::time_utils::spawn(async move {
                use tokio_tungstenite::tungstenite::Message;
                while let Some(msg_res) = ws_stream.next().await {
                    let msg = match msg_res {
                        Ok(m) => m,
                        Err(_) => break,
                    };

                    if let Message::Binary(bin) = msg {
                        if let Ok((msg_type, payload)) = decode_frame(&bin) {
                            match msg_type {
                                MSG_MUTATION_PUSH => {
                                    if let Ok(mutat) =
                                        serde_json::from_slice::<PendingMutation>(payload)
                                    {
                                        {
                                            let mut active_guard =
                                                active_namespaces_clone.lock().await;
                                            if let Some(cfg) =
                                                active_guard.get_mut(&mutat.namespace)
                                            {
                                                cfg.last_sequence =
                                                    cfg.last_sequence.max(mutat.sequence);
                                            }
                                        }
                                        let tx_opt = {
                                            let guard = namespace_txs_clone.lock().await;
                                            guard.get(&mutat.namespace).cloned()
                                        };
                                        if let Some(tx) = tx_opt {
                                            let _ = tx.send(mutat).await;
                                        }
                                    }
                                }
                                MSG_SNAPSHOT => {
                                    if let Ok(snap_payload) =
                                        serde_json::from_slice::<SnapshotPayload>(payload)
                                    {
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
                                            let mut snaps_guard = snapshots_clone.lock().await;
                                            let ns_snaps = snaps_guard
                                                .entry(snap_payload.namespace.clone())
                                                .or_default();
                                            if let Some(existing) = ns_snaps.iter_mut().find(|s| {
                                                s.doc_id == snap.doc_id
                                                    && s.record_id == snap.record_id
                                            }) {
                                                if snap.sequence > existing.sequence {
                                                    *existing = snap;
                                                }
                                            } else {
                                                ns_snaps.push(snap);
                                            }
                                        }
                                    }
                                }
                                MSG_NAMESPACE_ACK => {
                                    if let Ok(ack) =
                                        serde_json::from_slice::<NamespaceAckPayload>(payload)
                                    {
                                        let mut reqs = pending_registers_clone.lock().await;
                                        if let Some(tx) = reqs.remove(&ack.namespace) {
                                            let _ = tx.send(ack);
                                        }
                                    }
                                }
                                MSG_SCHEMA_MIGRATION => {
                                    if let Ok(mig) =
                                        serde_json::from_slice::<SchemaMigrationPayload>(payload)
                                    {
                                        let mut reqs = pending_schema_syncs_clone.lock().await;
                                        if let Some(tx) = reqs.remove(&mig.namespace) {
                                            let _ = tx.send(mig);
                                        }
                                    }
                                }
                                _ => {
                                    if let Ok(json_val) =
                                        serde_json::from_slice::<serde_json::Value>(payload)
                                    {
                                        if let Some(req_id) =
                                            json_val.get("request_id").and_then(|v| v.as_str())
                                        {
                                            let mut reqs = pending_requests_clone.lock().await;
                                            if let Some(tx) = reqs.remove(req_id) {
                                                let _ = tx.send(bin);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if msg.is_close() {
                        break;
                    }
                }

                {
                    let mut client_lock = ws_client_to_clear.lock().await;
                    *client_lock = None;
                }

                let active = {
                    let guard = active_namespaces_clone.lock().await;
                    guard.clone()
                };
                if !active.is_empty() {
                    warn!("Mux WebSocket connection lost. Reconnecting in 2 seconds...");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    if let Some(self_arc_reconnect) = self_weak.upgrade() {
                        if let Ok(new_handle) = Self::ensure_connected(self_arc_reconnect).await {
                            // Copy old channels and snapshots to the new handle
                            {
                                let old_chans = namespace_txs_clone.lock().await;
                                let mut new_chans = new_handle.namespace_txs.lock().await;
                                for (ns, tx) in old_chans.iter() {
                                    new_chans.insert(ns.clone(), tx.clone());
                                }
                            }
                            {
                                let old_snaps = snapshots_clone.lock().await;
                                let mut new_snaps = new_handle.snapshots.lock().await;
                                for (ns, snaps) in old_snaps.iter() {
                                    new_snaps.insert(ns.clone(), snaps.clone());
                                }
                            }

                            let tx = new_handle.tx.clone();
                            for (ns, cfg) in active {
                                let add_payload = NamespaceAddPayload {
                                    namespace: ns.clone(),
                                    replica_id: cfg.replica_id.clone(),
                                    public_key: cfg.public_key,
                                    schema_version: cfg.schema_version,
                                    last_sequence: cfg.last_sequence,
                                    key_version: 1,
                                };
                                if let Ok(frame) = encode_frame(MSG_NAMESPACE_ADD, &add_payload) {
                                    let _ = tx.send(Message::Binary(frame)).await;
                                }

                                // Send MSG_SUBSCRIBE as well
                                let sub = SubscribePayload {
                                    request_id: uuid::Uuid::new_v4().to_string(),
                                    namespace: ns.clone(),
                                    after: cfg.last_sequence,
                                };
                                if let Ok(frame) = encode_frame(MSG_SUBSCRIBE, &sub) {
                                    let _ = tx.send(Message::Binary(frame)).await;
                                }
                            }
                        }
                    }
                }
            });

            let mut ws_client_lock = self_arc.ws_client.lock().await;
            *ws_client_lock = Some(handle.clone());
            Ok(handle)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn add_namespace(
        self_arc: Arc<Self>,
        namespace: &str,
        replica_id: &str,
        public_key: Vec<u8>,
        schema_version: u64,
        last_sequence: u64,
    ) -> Result<(), CoordinatorError> {
        let handle = Self::ensure_connected(self_arc.clone()).await?;

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<NamespaceAckPayload>();
        {
            let mut reqs = handle.pending_registers.lock().await;
            reqs.insert(namespace.to_string(), ack_tx);
        }

        let add_payload = NamespaceAddPayload {
            namespace: namespace.to_string(),
            replica_id: replica_id.to_string(),
            public_key: public_key.clone(),
            schema_version,
            last_sequence,
            key_version: 1,
        };

        let add_frame = encode_frame(MSG_NAMESPACE_ADD, &add_payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        handle
            .tx
            .send(tokio_tungstenite::tungstenite::Message::Binary(add_frame))
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let ack = tokio::time::timeout(std::time::Duration::from_secs(10), ack_rx)
            .await
            .map_err(|_| CoordinatorError::Timeout)?
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if ack.status != "ok" {
            return Err(CoordinatorError::AuthFailed);
        }

        let (mig_tx, mig_rx) = tokio::sync::oneshot::channel::<SchemaMigrationPayload>();
        {
            let mut reqs = handle.pending_schema_syncs.lock().await;
            reqs.insert(namespace.to_string(), mig_tx);
        }

        let sync = SchemaSyncPayload {
            namespace: namespace.to_string(),
            version: schema_version,
        };

        let sync_frame = encode_frame(MSG_SCHEMA_SYNC, &sync)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        handle
            .tx
            .send(tokio_tungstenite::tungstenite::Message::Binary(sync_frame))
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let mig = tokio::time::timeout(std::time::Duration::from_secs(10), mig_rx)
            .await
            .map_err(|_| CoordinatorError::Timeout)?
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if mig.status == "mismatch" {
            return Err(CoordinatorError::SchemaMismatch);
        }

        let mut active = self_arc.active_namespaces.lock().await;
        active.insert(
            namespace.to_string(),
            ActiveNamespaceConfig {
                replica_id: replica_id.to_string(),
                public_key,
                schema_version,
                last_sequence,
            },
        );

        Ok(())
    }

    pub fn join_namespace(self: &Arc<Self>, namespace: String) -> Arc<NamespacedCoordinator> {
        Arc::new(NamespacedCoordinator {
            namespace,
            replica_id: format!("client-{}", uuid::Uuid::new_v4()),
            mux: self.clone(),
        })
    }

    pub async fn drop_namespace(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<(), CoordinatorError> {
        {
            let mut active = self.active_namespaces.lock().await;
            active.remove(namespace);
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                {
                    let mut txs = handle.namespace_txs.lock().await;
                    txs.remove(namespace);
                }

                let drop_payload = NamespaceDropPayload {
                    namespace: namespace.to_string(),
                    replica_id: replica_id.to_string(),
                };

                if let Ok(frame) = encode_frame(MSG_NAMESPACE_DROP, &drop_payload) {
                    let _ = handle
                        .tx
                        .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
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
                    if handle
                        .tx
                        .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                        .await
                        .is_ok()
                    {
                        if let Ok(resp_bin) = rx.await {
                            if let Ok((msg_type, payload)) = decode_frame(&resp_bin) {
                                if msg_type == MSG_PUSH_ACK {
                                    if let Ok(ack) =
                                        serde_json::from_slice::<PushAckPayload>(payload)
                                    {
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

        let url = format!(
            "{}/namespace/{}/push",
            self.config.url.trim_end_matches('/'),
            namespace
        );
        let mut builder = self.client.post(&url).json(&mutations);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!(
                "HTTP error status: {}",
                resp.status()
            )));
        }
        let seq_ids = resp
            .json::<Vec<SequenceId>>()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(seq_ids)
    }

    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
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
                    if handle
                        .tx
                        .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                        .await
                        .is_ok()
                    {
                        if let Ok(resp_bin) = rx.await {
                            if let Ok((msg_type, payload)) = decode_frame(&resp_bin) {
                                if msg_type == MSG_PULL_RESPONSE {
                                    if let Ok(resp) =
                                        serde_json::from_slice::<PullResponsePayload>(payload)
                                    {
                                        return Ok(resp.mutations);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

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
        let resp = builder
            .send()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!(
                "HTTP error status: {}",
                resp.status()
            )));
        }
        let mutations = resp
            .json::<Vec<PendingMutation>>()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(mutations)
    }

    async fn subscribe(
        self_arc: Arc<Self>,
        namespace: &str,
        replica_id: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            match Self::add_namespace(
                self_arc.clone(),
                namespace,
                replica_id,
                vec![],
                0,
                from_sequence,
            )
            .await
            {
                Ok(_) => {
                    let (mut_tx, mut_rx) = tokio::sync::mpsc::channel::<PendingMutation>(1024);
                    let handle = Self::ensure_connected(self_arc.clone()).await?;
                    {
                        let mut txs = handle.namespace_txs.lock().await;
                        txs.insert(namespace.to_string(), mut_tx);
                    }

                    let sub = SubscribePayload {
                        request_id: uuid::Uuid::new_v4().to_string(),
                        namespace: namespace.to_string(),
                        after: from_sequence,
                    };
                    if let Ok(frame) = encode_frame(MSG_SUBSCRIBE, &sub) {
                        let _ = handle
                            .tx
                            .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                            .await;
                    }

                    return Ok(Box::new(MuxSubscription { rx: mut_rx }));
                }
                Err(e) => {
                    warn!(
                        "Multiplexed WebSocket subscription failed, falling back to SSE: {:?}",
                        e
                    );
                }
            }
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let client = self_arc.client.clone();
        let config = self_arc.config.clone();
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
                            if let Ok(mutation) = serde_json::from_str::<PendingMutation>(data_json)
                            {
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

        Ok(Box::new(MuxSubscription { rx: rx }))
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
                    if handle
                        .tx
                        .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                        .await
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
            }
        }

        let url = format!(
            "{}/namespace/{}/heartbeat",
            self.config.url.trim_end_matches('/'),
            namespace
        );
        let mut builder = self.client.post(&url).json(&replica_id);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let _ = builder.send().await;
        Ok(())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let url = format!(
            "{}/namespace/{}/schema_version",
            self.config.url.trim_end_matches('/'),
            namespace
        );
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!(
                "HTTP error status: {}",
                resp.status()
            )));
        }
        let version = resp
            .json::<u64>()
            .await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(version)
    }

    async fn get_snapshot(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let snaps_guard = handle.snapshots.lock().await;
                if let Some(ns_snaps) = snaps_guard.get(namespace) {
                    let found = ns_snaps
                        .iter()
                        .find(|s| s.doc_id == doc_id && s.record_id == record_id)
                        .cloned();
                    return Ok(found);
                }
            }
        }
        Ok(None)
    }

    async fn store_snapshot(
        &self,
        namespace: &str,
        snapshot: &crate::crdt::snapshot::Snapshot,
    ) -> Result<(), CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let snap_payload = SnapshotPayload {
                    doc_id: snapshot.doc_id.clone(),
                    record_id: snapshot.record_id.clone(),
                    sequence: snapshot.sequence,
                    bytes: snapshot.bytes.clone(),
                    checksum: snapshot.checksum,
                    namespace: namespace.to_string(),
                };
                let frame = encode_frame(MSG_SNAPSHOT, &snap_payload)
                    .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
                if handle
                    .tx
                    .send(tokio_tungstenite::tungstenite::Message::Binary(frame))
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn list_snapshots(
        &self,
        namespace: &str,
    ) -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let handle_opt = self.ws_client.lock().await.clone();
            if let Some(handle) = handle_opt {
                let snaps_guard = handle.snapshots.lock().await;
                if let Some(ns_snaps) = snaps_guard.get(namespace) {
                    return Ok(ns_snaps.clone());
                }
            }
        }
        Ok(vec![])
    }
}

#[derive(Debug, Clone)]
pub struct NamespacedCoordinator {
    pub namespace: String,
    pub replica_id: String,
    pub mux: Arc<MuxCoordinator>,
}

impl NamespacedCoordinator {
    pub async fn leave(&self) -> Result<(), CoordinatorError> {
        self.mux
            .drop_namespace(&self.namespace, &self.replica_id)
            .await
    }
}

#[async_trait]
impl Coordinator for NamespacedCoordinator {
    async fn push(
        &self,
        _namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        self.mux.push(&self.namespace, mutations).await
    }

    async fn pull(
        &self,
        _namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        self.mux.pull(&self.namespace, after, limit).await
    }

    async fn subscribe(
        &self,
        _namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        MuxCoordinator::subscribe(
            self.mux.clone(),
            &self.namespace,
            &self.replica_id,
            from_sequence,
        )
        .await
    }

    async fn register(
        &self,
        _namespace: &str,
        info: ReplicaInfo,
        _last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            MuxCoordinator::add_namespace(
                self.mux.clone(),
                &self.namespace,
                &info.replica_id,
                info.public_key,
                info.schema_version,
                0,
            )
            .await
        }
        #[cfg(target_arch = "wasm32")]
        {
            Ok(())
        }
    }

    async fn heartbeat(&self, _namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        self.mux.heartbeat(&self.namespace, replica_id).await
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        self.mux.schema_version(&self.namespace).await
    }

    async fn get_snapshot(
        &self,
        _namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        self.mux
            .get_snapshot(&self.namespace, doc_id, record_id)
            .await
    }

    async fn store_snapshot(
        &self,
        _namespace: &str,
        snapshot: &crate::crdt::snapshot::Snapshot,
    ) -> Result<(), CoordinatorError> {
        self.mux.store_snapshot(&self.namespace, snapshot).await
    }

    async fn list_snapshots(
        &self,
        _namespace: &str,
    ) -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        self.mux.list_snapshots(&self.namespace).await
    }
}

struct MuxSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for MuxSubscription {
    type Item = PendingMutation;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

fn get_ws_mux_url(http_url: &str) -> String {
    let trimmed = http_url.trim_end_matches('/');
    let base = if trimmed.starts_with("https://") {
        trimmed.replace("https://", "wss://")
    } else if trimmed.starts_with("http://") {
        trimmed.replace("http://", "ws://")
    } else {
        trimmed.to_string()
    };
    format!("{}/ws", base)
}

#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn assert_send<T: Send>() {}
#[allow(dead_code)]
fn _test_send_sync() {
    assert_send_sync::<MuxCoordinator>();
    assert_send::<MuxWsHandle>();
}
