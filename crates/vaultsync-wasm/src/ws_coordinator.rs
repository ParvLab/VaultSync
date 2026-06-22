use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::future::Either;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use vaultsync_core::coordinator::traits::{
    Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId,
};
use vaultsync_core::coordinator::ws_proto::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, WebSocket};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (console_log_str(&format!($($t)*)));
}

#[derive(Debug, Clone)]
pub struct WasmWsCoordinator {
    inner: Arc<WasmWsCoordinatorInner>,
}

struct WasmWsCoordinatorInner {
    url: String,
    auth_token: Option<String>,
    replica_id: Mutex<String>,
    ws: Mutex<Option<WebSocket>>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    subscribers: Mutex<Vec<mpsc::Sender<PendingMutation>>>,
    peer_coord: Mutex<Option<vaultsync_transport_webrtc::PeerCoordinator>>,
    download_notify: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

impl std::fmt::Debug for WasmWsCoordinatorInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmWsCoordinatorInner")
            .field("url", &self.url)
            .finish()
    }
}

impl WasmWsCoordinator {
    pub fn new(url: &str, auth_token: Option<String>) -> Self {
        let ice_servers = vec!["stun:stun.l.google.com:19302".to_string()];
        let peer_coord = vaultsync_transport_webrtc::PeerCoordinator::new(
            vaultsync_transport_webrtc::WebRtcConfig { ice_servers },
        );
        Self {
            inner: Arc::new(WasmWsCoordinatorInner {
                url: url.to_string(),
                auth_token,
                replica_id: Mutex::new(String::new()),
                ws: Mutex::new(None),
                pending_requests: Mutex::new(HashMap::new()),
                subscribers: Mutex::new(Vec::new()),
                peer_coord: Mutex::new(Some(peer_coord)),
                download_notify: Mutex::new(None),
            }),
        }
    }

    pub fn set_download_notify(&self, tx: mpsc::UnboundedSender<()>) {
        *self.inner.download_notify.lock().unwrap() = Some(tx);
        console_log!("[WasmWs] download_notify registered");
    }

    async fn connect_and_handshake(
        &self,
        namespace: &str,
        after: SequenceId,
        info: Option<&ReplicaInfo>,
    ) -> Result<(), CoordinatorError> {
        console_log!("[5/9] coordinator connect");

        let ws_url = get_ws_url(&self.inner.url, namespace);
        console_log!("[WasmWs] connecting to url={}", ws_url);
        let ws = WebSocket::new(&ws_url).map_err(|e| {
            CoordinatorError::Internal(format!("Failed to create WebSocket: {:?}", e))
        })?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        // 1. Wait for connection to open
        let (open_rx, close_rx) = {
            let (open_tx, open_rx) = oneshot::channel::<()>();
            let open_tx_cell = std::cell::RefCell::new(Some(open_tx));
            let open_callback = Closure::wrap(Box::new(move |_e: web_sys::Event| {
                console_log!("[WasmWs] WebSocket opened!");
                if let Some(tx) = open_tx_cell.borrow_mut().take() {
                    let _ = tx.send(());
                }
            }) as Box<dyn Fn(web_sys::Event)>);
            ws.set_onopen(Some(open_callback.as_ref().unchecked_ref()));
            open_callback.forget();

            // Also setup onclose/onerror to abort if open fails
            let (close_tx, close_rx) = oneshot::channel::<()>();
            let close_tx_cell = std::cell::RefCell::new(Some(close_tx));
            let inner_close_clone = self.inner.clone();
            let close_callback = Closure::wrap(Box::new(move |e: web_sys::Event| {
                console_log!("[WasmWs] WebSocket closed/failed: {:?}", e);
                if let Some(tx) = close_tx_cell.borrow_mut().take() {
                    let _ = tx.send(());
                }
                *inner_close_clone.ws.lock().unwrap() = None;
            }) as Box<dyn Fn(web_sys::Event)>);
            ws.set_onclose(Some(close_callback.as_ref().unchecked_ref()));
            close_callback.forget();

            (open_rx, close_rx)
        };

        futures::select! {
            _ = open_rx.fuse() => {
                console_log!("[WasmWs] open_rx resolved, connected!");
            }
            _ = close_rx.fuse() => {
                console_log!("[WasmWs] close_rx resolved, connection failed!");
                return Err(CoordinatorError::NotAvailable);
            }
        }

        // Setup message handler
        let mut msg_rx = {
            let (msg_tx, msg_rx) = mpsc::channel::<Vec<u8>>(128);
            let msg_tx_cell = std::cell::RefCell::new(msg_tx);
            let msg_callback = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Ok(ab) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let array = js_sys::Uint8Array::new(&ab);
                    let vec = array.to_vec();
                    let _ = msg_tx_cell.borrow_mut().try_send(vec);
                }
            })
                as Box<dyn Fn(web_sys::MessageEvent)>);
            ws.set_onmessage(Some(msg_callback.as_ref().unchecked_ref()));
            msg_callback.forget();

            msg_rx
        };

        // 2. Perform AUTH
        console_log!("[6/9] auth");
        let token = self.inner.auth_token.clone().unwrap_or_default();
        let replica_id = info
            .map(|i| i.replica_id.clone())
            .unwrap_or_else(|| format!("client-wasm-{}", uuid::Uuid::new_v4()));
        let auth = AuthPayload {
            token,
            protocol_version: 1,
            replica_id: replica_id.clone(),
            namespace: namespace.to_string(),
        };
        let auth_frame =
            encode_frame(MSG_AUTH, &auth).map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&auth_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Wait for AUTH_ACK with 5s timeout
        let auth_resp = recv_with_timeout(&mut msg_rx, 5).await?;

        let (msg_type, payload) =
            decode_frame(&auth_resp).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_AUTH_ACK {
            console_log!(
                "[WasmWs] auth failed: expected MSG_AUTH_ACK, got {:02X}",
                msg_type
            );
            return Err(CoordinatorError::Internal(format!(
                "Expected MSG_AUTH_ACK, got {:02X}",
                msg_type
            )));
        }

        let auth_ack: AuthAckPayload = serde_json::from_slice(payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if auth_ack.status != "ok" {
            console_log!("[WasmWs] auth ack status is not ok: {:?}", auth_ack.error);
            return Err(CoordinatorError::AuthFailed);
        }
        console_log!("[WasmWs] auth successful!");

        // 3. Perform REGISTER
        console_log!("[7/9] register");
        let reg = RegisterPayload {
            replica_id: replica_id.clone(),
            namespace: namespace.to_string(),
            public_key: info.map(|i| i.public_key.clone()).unwrap_or_default(),
            schema_version: info.map(|i| i.schema_version).unwrap_or(0),
            last_sequence: after,
            key_version: 1,
        };
        let reg_frame = encode_frame(MSG_REGISTER, &reg)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&reg_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Wait for REGISTER_ACK with 5s timeout
        let reg_resp = recv_with_timeout(&mut msg_rx, 5).await?;

        let (msg_type, _) = decode_frame(&reg_resp).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_REGISTER_ACK {
            console_log!(
                "[WasmWs] register failed: expected MSG_REGISTER_ACK, got {:02X}",
                msg_type
            );
            return Err(CoordinatorError::Internal(format!(
                "Expected MSG_REGISTER_ACK, got {:02X}",
                msg_type
            )));
        }
        console_log!("[WasmWs] register successful!");

        // 4. Perform SUBSCRIBE
        console_log!("[8/9] subscribe");
        let sub = SubscribePayload {
            request_id: uuid::Uuid::new_v4().to_string(),
            namespace: namespace.to_string(),
            after,
        };
        let sub_frame = encode_frame(MSG_SUBSCRIBE, &sub)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&sub_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Spawn background message processor
        let inner_clone = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bin) = msg_rx.next().await {
                if let Ok((msg_type, payload)) = decode_frame(&bin) {
                    if msg_type == MSG_MUTATION_PUSH {
                        if let Ok(mutat) = serde_json::from_slice::<PendingMutation>(payload) {
                            // Self-filter: skip mutations from our own replica
                            let self_rid = inner_clone.replica_id.lock().unwrap();
                            if mutat.replica_id == *self_rid {
                                console_log!(
                                    "[WasmWs] skipping own mutation: seq={}",
                                    mutat.sequence
                                );
                                continue;
                            }
                            drop(self_rid);
                            console_log!(
                                "[WasmWs] received MSG_MUTATION_PUSH: seq={}",
                                mutat.sequence
                            );
                            console_log!(
                                "[ws] forwarding mutation seq={}",
                                mutat.sequence
                            );
                            let mut subs = inner_clone.subscribers.lock().unwrap();
                            subs.retain_mut(|sub| sub.try_send(mutat.clone()).is_ok());
                            drop(subs);
                            if cfg!(debug_assertions) {
                                console_log!(
                                    "[WasmWs] notify download seq={}",
                                    mutat.sequence
                                );
                            }
                            if let Some(ref notify) = *inner_clone.download_notify.lock().unwrap() {
                                match notify.unbounded_send(()) {
                                    Ok(_) => {
                                        console_log!("[ws] download notify sent");
                                    }
                                    Err(e) => {
                                        console_log!("[ws] download notify FAILED {:?}", e);
                                    }
                                }
                            }
                        }
                    } else if msg_type == MSG_P2P_SIGNAL_ACK {
                        if let Ok(ack) = serde_json::from_slice::<P2PSignalAckPayload>(payload) {
                            if let Some(ref peer_coord) = *inner_clone.peer_coord.lock().unwrap() {
                                let _ = peer_coord
                                    .handle_signaling_message(
                                        &ack.sender_replica_id,
                                        &ack.signal_type,
                                        &ack.data,
                                    )
                                    .await;
                            }
                        }
                    } else {
                        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                            if let Some(req_id) =
                                json_val.get("request_id").and_then(|v| v.as_str())
                            {
                                let mut reqs = inner_clone.pending_requests.lock().unwrap();
                                if let Some(tx) = reqs.remove(req_id) {
                                    let _ = tx.send(bin);
                                }
                            }
                        }
                    }
                }
            }
        });

        // Spawn heartbeat loop
        let ws_clone = ws.clone();
        let replica_id_hb = auth.replica_id.clone();
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(30)).await;
                let hb = HeartbeatPayload {
                    replica_id: replica_id_hb.clone(),
                    namespace: "".to_string(),
                };
                if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                    if ws_clone.send_with_u8_array(&frame).is_err() {
                        break;
                    }
                }
            }
        });

        // 5. Initialize WebRTC peer coordinator signaling and registration
        if let Some(ref peer_coord) = *self.inner.peer_coord.lock().unwrap() {
            let ws_sender = ws.clone();
            let peer_coord_clone = peer_coord.clone();
            let ns_str = namespace.to_string();
            let my_replica_id = auth.replica_id.clone();

            // Set signaling handler
            wasm_bindgen_futures::spawn_local(async move {
                peer_coord_clone
                    .set_signal_handler(move |target_id, signal_type, data| {
                        let ws_send = ws_sender.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            if ws_send.ready_state() != 1 {
                                return;
                            }
                            let sig = P2PSignalPayload {
                                target_replica_id: target_id,
                                signal_type,
                                data,
                            };
                            if let Ok(frame) = encode_frame(MSG_P2P_SIGNAL, &sig) {
                                let _ = ws_send.send_with_u8_array(&frame);
                            }
                        });
                    })
                    .await;
            });

            // Register with PeerCoordinator
            let peer_info = ReplicaInfo {
                replica_id: auth.replica_id.clone(),
                namespace: namespace.to_string(),
                public_key: info.map(|i| i.public_key.clone()).unwrap_or_default(),
                schema_version: info.map(|i| i.schema_version).unwrap_or(0),
            };

            let peer_coord_clone = peer_coord.clone();
            let ns_str_clone = ns_str.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = peer_coord_clone.register(&ns_str_clone, peer_info).await;
            });

            // Spawn discovery loop
            let peer_coord_clone = peer_coord.clone();
            let http_url = self.inner.url.clone();
            let ws_check = ws.clone();
            let inner_loop = self.inner.clone();
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(30)).await;
                    {
                        let ws_lock = inner_loop.ws.lock().unwrap();
                        if let Some(ref current_ws) = *ws_lock {
                            if current_ws != &ws_check {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    if let Ok(active_replicas) = fetch_replicas(&http_url, &ns_str).await {
                        for peer in active_replicas {
                            if peer.replica_id > my_replica_id {
                                let _ =
                                    peer_coord_clone.initiate_connection(&peer.replica_id).await;
                            }
                        }
                    }
                }
            });
        }

        {
            let mut ws_lock = self.inner.ws.lock().unwrap();
            if let Some(old_ws) = ws_lock.take() {
                old_ws.set_onopen(None);
                old_ws.set_onclose(None);
                old_ws.set_onmessage(None);
                let _ = old_ws.close();
            }
            *ws_lock = Some(ws);
        }
        Ok(())
    }

    async fn send_request<T: serde::Serialize>(
        &self,
        msg_type: u8,
        request_id: &str,
        payload: &T,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        {
            let mut reqs = self.inner.pending_requests.lock().unwrap();
            reqs.insert(request_id.to_string(), tx);
        }

        let frame = encode_frame(msg_type, payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        {
            let ws_lock = self.inner.ws.lock().unwrap();
            if let Some(ref ws) = *ws_lock {
                if ws.ready_state() == 1 {
                    ws.send_with_u8_array(&frame)
                        .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                } else {
                    return Err(CoordinatorError::NotAvailable);
                }
            } else {
                return Err(CoordinatorError::NotAvailable);
            }
        }

        let timeout =
            vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(10));

        futures::pin_mut!(timeout);
        futures::pin_mut!(rx);

        match futures::future::select(rx, timeout).await {
            Either::Left((Ok(resp), _)) => Ok(resp),

            Either::Left((Err(_), _)) => {
                Err(CoordinatorError::Internal("request cancelled".into()))
            }

            Either::Right(_) => {
                tracing::warn!("[ws] request timed out after 10s");
                Err(CoordinatorError::Timeout)
            }
        }
    }
}

async fn recv_with_timeout(
    rx: &mut mpsc::Receiver<Vec<u8>>,
    timeout_secs: u64,
) -> Result<Vec<u8>, CoordinatorError> {
    futures::select! {
        bin = rx.next().fuse() => bin.ok_or_else(|| {
            CoordinatorError::Internal("Connection closed during handshake".to_string())
        }),
        _ = vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(timeout_secs)).fuse() => {
            Err(CoordinatorError::Internal(format!(
                "Handshake timeout after {}s",
                timeout_secs
            )))
        }
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
impl Coordinator for WasmWsCoordinator {
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        let is_connected = {
            let ws_lock = self.inner.ws.lock().unwrap();
            ws_lock.is_some()
        };
        if !is_connected {
            if let Err(e) = self.connect_and_handshake(namespace, 0, None).await {
                console_log!("[WasmWs] Reconnect failed during push: {:?}", e);
                return Err(e);
            }
        }
        if !mutations.is_empty() {
            console_log!(
                "[WasmWs] push called for namespace={}, mutations count={}",
                namespace,
                mutations.len()
            );
        }
        let req_id = uuid::Uuid::new_v4().to_string();
        let payload = PushPayload {
            request_id: req_id.clone(),
            mutations,
        };

        let resp_bin = self.send_request(MSG_PUSH, &req_id, &payload).await?;
        let (msg_type, payload) =
            decode_frame(&resp_bin).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type == MSG_PUSH_ACK {
            let ack: PushAckPayload = serde_json::from_slice(payload)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            if let Some(err) = ack.error {
                console_log!("[WasmWs] push failed with error: {}", err);
                return Err(CoordinatorError::Internal(err));
            }
            if !ack.sequences.is_empty() {
                console_log!(
                    "[WasmWs] push ack success, returned sequences={:?}",
                    ack.sequences
                );
            }
            Ok(ack.sequences)
        } else {
            console_log!("[WasmWs] push got invalid response type: {:02X}", msg_type);
            Err(CoordinatorError::Internal(
                "Invalid response type".to_string(),
            ))
        }
    }

    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let is_connected = {
            let ws_lock = self.inner.ws.lock().unwrap();
            ws_lock.is_some()
        };
        if !is_connected {
            if let Err(e) = self.connect_and_handshake(namespace, after, None).await {
                console_log!("[WasmWs] Reconnect failed during pull: {:?}", e);
                return Err(e);
            }
        }
        // Suppress idle pull logs
        let req_id = uuid::Uuid::new_v4().to_string();
        let payload = PullPayload {
            request_id: req_id.clone(),
            namespace: namespace.to_string(),
            after,
            limit,
        };

        let resp_bin = self.send_request(MSG_PULL, &req_id, &payload).await?;
        let (msg_type, payload) =
            decode_frame(&resp_bin).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type == MSG_PULL_RESPONSE {
            let resp: PullResponsePayload = serde_json::from_slice(payload)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            if !resp.mutations.is_empty() {
                console_log!(
                    "[WasmWs] pull returned {} mutations, has_more={}",
                    resp.mutations.len(),
                    resp.has_more
                );
            }
            Ok(resp.mutations)
        } else {
            console_log!("[WasmWs] pull got invalid response type: {:02X}", msg_type);
            Err(CoordinatorError::Internal(
                "Invalid response type".to_string(),
            ))
        }
    }

    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let is_connected = {
            let ws_lock = self.inner.ws.lock().unwrap();
            ws_lock.is_some()
        };

        if !is_connected {
            self.connect_and_handshake(namespace, from_sequence, None)
                .await?;
        } else {
            let sub = SubscribePayload {
                request_id: uuid::Uuid::new_v4().to_string(),
                namespace: namespace.to_string(),
                after: from_sequence,
            };
            let sub_frame = encode_frame(MSG_SUBSCRIBE, &sub)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let ws_lock = self.inner.ws.lock().unwrap();
            if let Some(ref ws) = *ws_lock {
                if ws.ready_state() == 1 {
                    ws.send_with_u8_array(&sub_frame)
                        .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                }
            }
        }

        let (tx, rx) = mpsc::channel::<PendingMutation>(100);
        {
            let mut subs = self.inner.subscribers.lock().unwrap();
            subs.push(tx);
        }

        Ok(Box::new(rx))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        // Store our replica_id for self-filtering
        {
            let mut rid = self.inner.replica_id.lock().unwrap();
            *rid = info.replica_id.clone();
        }

        let is_connected = {
            let ws_lock = self.inner.ws.lock().unwrap();
            ws_lock.is_some()
        };

        if !is_connected {
            self.connect_and_handshake(namespace, 0, Some(&info))
                .await?;
        } else {
            let reg = RegisterPayload {
                replica_id: info.replica_id.clone(),
                namespace: namespace.to_string(),
                public_key: info.public_key.clone(),
                schema_version: info.schema_version,
                last_sequence: 0,
                key_version: 1,
            };
            let frame = encode_frame(MSG_REGISTER, &reg)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let ws_lock = self.inner.ws.lock().unwrap();
            if let Some(ref ws) = *ws_lock {
                if ws.ready_state() == 1 {
                    ws.send_with_u8_array(&frame)
                        .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                }
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, _namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let hb = HeartbeatPayload {
            replica_id: replica_id.to_string(),
            namespace: "".to_string(),
        };
        let frame = encode_frame(MSG_HEARTBEAT, &hb)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let ws_lock = self.inner.ws.lock().unwrap();
        if let Some(ref ws) = *ws_lock {
            if ws.ready_state() == 1 {
                ws.send_with_u8_array(&frame)
                    .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                Ok(())
            } else {
                Err(CoordinatorError::NotAvailable)
            }
        } else {
            Err(CoordinatorError::NotAvailable)
        }
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(0)
    }
}

async fn fetch_replicas(url: &str, namespace: &str) -> Result<Vec<ReplicaInfo>, String> {
    use vaultsync_core::time_utils::SendJsFuture;
    let window = web_sys::window().ok_or("No window object found".to_string())?;
    let req_url = format!(
        "{}/namespace/{}/replicas",
        url.trim_end_matches('/'),
        namespace
    );

    let opts = web_sys::RequestInit::new();
    opts.set_method("GET");

    let request = web_sys::Request::new_with_str_and_init(&req_url, &opts)
        .map_err(|e| format!("Failed to create request: {:?}", e))?;

    let resp_value = SendJsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|e| format!("Fetch failed: {:?}", e))?;

    let resp = resp_value
        .dyn_into::<web_sys::Response>()
        .map_err(|_| "Failed to cast to Response".to_string())?;

    if !resp.ok() {
        return Err(format!("HTTP error: {}", resp.status()));
    }

    let text_value = SendJsFuture::from(resp.text().map_err(|e| format!("{:?}", e))?)
        .await
        .map_err(|e| format!("Failed to read text: {:?}", e))?;

    let text_str: String = text_value.as_string().unwrap_or_default();
    let replicas: Vec<ReplicaInfo> =
        serde_json::from_str(&text_str).map_err(|e| format!("Failed to parse JSON: {:?}", e))?;

    Ok(replicas)
}
