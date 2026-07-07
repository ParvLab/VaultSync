use crate::{engine_debug, engine_info, engine_trace, engine_warn};
use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::future::Either;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vaultsync_core::coordinator::traits::{
    Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId,
};
use vaultsync_core::coordinator::ws_proto::*;
use vaultsync_core::crdt::snapshot::Snapshot;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, WebSocket};

#[derive(Debug, Clone)]
pub struct WasmWsCoordinator {
    inner: Arc<WasmWsCoordinatorInner>,
}

struct ConnectionState {
    ws: Option<WebSocket>,
    /// true while a connect_and_handshake is in progress
    connecting: bool,
    /// One-shot senders for callers waiting on an in-flight connection.
    waiters: Vec<oneshot::Sender<Result<(), CoordinatorError>>>,
    /// Generation ID of the current connection — used for generation-aware cleanup.
    generation: u64,
}

struct WasmWsCoordinatorInner {
    url: String,
    auth_token: Option<String>,
    replica_id: Mutex<String>,
    connection: Mutex<ConnectionState>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    subscribers: Mutex<Vec<mpsc::Sender<PendingMutation>>>,
    peer_coord: Mutex<Option<vaultsync_transport_webrtc::PeerCoordinator>>,
    download_notify: Mutex<Option<mpsc::UnboundedSender<Option<PendingMutation>>>>,
    upload_notify: Mutex<Option<mpsc::UnboundedSender<()>>>,
    generation_id: Mutex<String>,
    history_preserved: AtomicBool,
    max_sequence: AtomicU64,
    conn_gen: AtomicU64,
    /// Snapshots received from server pushes (MSG_SNAPSHOT) during REGISTER or later.
    received_snapshots: Mutex<Vec<Snapshot>>,
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
                connection: Mutex::new(ConnectionState {
                    ws: None,
                    connecting: false,
                    waiters: Vec::new(),
                    generation: 0,
                }),
                pending_requests: Mutex::new(HashMap::new()),
                subscribers: Mutex::new(Vec::new()),
                peer_coord: Mutex::new(Some(peer_coord)),
                download_notify: Mutex::new(None),
                upload_notify: Mutex::new(None),
                generation_id: Mutex::new(String::new()),
                history_preserved: AtomicBool::new(false),
                max_sequence: AtomicU64::new(0),
                conn_gen: AtomicU64::new(0),
                received_snapshots: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn set_download_notify(&self, tx: mpsc::UnboundedSender<Option<PendingMutation>>) {
        *self.inner.download_notify.lock().unwrap() = Some(tx);
        engine_debug!("[WasmWs] download_notify registered");
    }

    pub fn set_upload_notify(&self, tx: mpsc::UnboundedSender<()>) {
        *self.inner.upload_notify.lock().unwrap() = Some(tx);
        engine_debug!("[WasmWs] upload_notify registered");
    }

    async fn connect_and_handshake(
        &self,
        namespace: &str,
        after: SequenceId,
        info: Option<&ReplicaInfo>,
    ) -> Result<(), CoordinatorError> {
        self.connect_and_handshake_impl(namespace, after, info, false).await
    }

    async fn connect_and_handshake_impl(
        &self,
        namespace: &str,
        after: SequenceId,
        info: Option<&ReplicaInfo>,
        skip_subscribe: bool,
    ) -> Result<(), CoordinatorError> {
        engine_debug!("[VaultSync] coordinator connect");

        // ── Single-flight guard: only one caller connects, others wait ──
        let wait_for_connection = {
            let mut state = self.inner.connection.lock().unwrap();
            if state.ws.is_some() {
                engine_debug!("[WasmWs] already connected");
                return Ok(());
            }
            if state.connecting {
                engine_debug!("[WasmWs] connection in progress, waiting...");
                let (tx, rx) = oneshot::channel();
                state.waiters.push(tx);
                Some(rx)
            } else {
                state.connecting = true;
                None
            }
        };
        if let Some(rx) = wait_for_connection {
            return rx.await.unwrap_or(Err(CoordinatorError::NotAvailable));
        }

        // Increment generation BEFORE spawning tasks so reader/heartbeat/signal
        // capture the correct generation and survive.
        let generation = self.inner.conn_gen.fetch_add(1, Ordering::SeqCst) + 1;

        // Perform connection handshake
        let reconn_t0 = js_sys::Date::now();
        let result = self.do_connect(namespace, after, info, generation, skip_subscribe).await;
        let reconn_elapsed = (js_sys::Date::now() - reconn_t0) as u64;

        // Notify waiters and finalize state
        let _had_ws = {
            let mut state = self.inner.connection.lock().unwrap();
            let had = state.ws.is_some();
            state.connecting = false;
            if result.is_ok() {
                state.generation = generation;
            } else if state.generation == generation {
                // Only clean up our own generation's socket to avoid killing a
                // newer connection that was established concurrently.
                if let Some(old_ws) = state.ws.take() {
                    old_ws.set_onopen(None);
                    old_ws.set_onclose(None);
                    old_ws.set_onmessage(None);
                    let _ = old_ws.close();
                }
            }
            for tx in state.waiters.drain(..) {
                let _ = tx.send(result.clone());
            }
            had
        };

        // On successful reconnect, wake workers to resume syncing
        if result.is_ok() {
            engine_info!(
                "[reconnect] SUCCESS generation={} elapsed={}ms",
                generation, reconn_elapsed,
            );
            if let Some(ref notify) = *self.inner.upload_notify.lock().unwrap() {
                let _ = notify.unbounded_send(());
            }
            if let Some(ref notify) = *self.inner.download_notify.lock().unwrap() {
                let _ = notify.unbounded_send(None);
            }
        } else {
            engine_warn!(
                "[reconnect] FAIL generation={} elapsed={}ms",
                generation, reconn_elapsed,
            );
        }

        result
    }

    /// Performs the actual WebSocket connection and handshake.
    /// When `skip_subscribe` is true, SUBSCRIBE is not sent — the caller
    /// must call `subscribe()` separately after the generation check.
    async fn do_connect(
        &self,
        namespace: &str,
        after: SequenceId,
        info: Option<&ReplicaInfo>,
        generation: u64,
        skip_subscribe: bool,
    ) -> Result<(), CoordinatorError> {
        let dc_t0 = js_sys::Date::now();
        let ws_url = get_ws_url(&self.inner.url, namespace);
        engine_debug!("[reconnect] state=Connecting url={}", ws_url);
        let ws = WebSocket::new(&ws_url).map_err(|e| {
            CoordinatorError::Internal(format!("Failed to create WebSocket: {:?}", e))
        })?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        // 1. Wait for connection to open
        let (open_rx, close_rx) = {
            let (open_tx, open_rx) = oneshot::channel::<()>();
            let open_tx_cell = std::cell::RefCell::new(Some(open_tx));
            let open_callback = Closure::wrap(Box::new(move |_e: web_sys::Event| {
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
                if let Some(tx) = close_tx_cell.borrow_mut().take() {
                    let _ = tx.send(());
                }
                inner_close_clone.connection.lock().unwrap().ws = None;
            }) as Box<dyn Fn(web_sys::Event)>);
            ws.set_onclose(Some(close_callback.as_ref().unchecked_ref()));
            close_callback.forget();

            (open_rx, close_rx)
        };

        futures::select! {
            _ = open_rx.fuse() => {
                let ws_open_elapsed = (js_sys::Date::now() - dc_t0) as u64;
                engine_info!("[ws] ws_open elapsed={}ms", ws_open_elapsed);
            }
            _ = close_rx.fuse() => {
                engine_info!("[WasmWs] WebSocket closed/failed!");
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
        let auth_t0 = js_sys::Date::now();
        engine_debug!("[reconnect] state=Authenticating");
        engine_debug!("[VaultSync] auth");
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
            engine_warn!(
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
            engine_warn!("[WasmWs] auth ack status is not ok: {:?}", auth_ack.error);
            return Err(CoordinatorError::AuthFailed);
        }
        let auth_elapsed = (js_sys::Date::now() - auth_t0) as u64;
        engine_info!("[ws] auth elapsed={}ms", auth_elapsed);

        // 3. Perform REGISTER
        let reg_t0 = js_sys::Date::now();
        engine_debug!("[VaultSync] register");
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

        let (msg_type, payload) =
            decode_frame(&reg_resp).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_REGISTER_ACK {
            engine_warn!(
                "[WasmWs] register failed: expected MSG_REGISTER_ACK, got {:02X}",
                msg_type
            );
            return Err(CoordinatorError::Internal(format!(
                "Expected MSG_REGISTER_ACK, got {:02X}",
                msg_type
            )));
        }

        // Extract generation_id, history_preserved, and max_sequence from RegisterAck
        if let Ok(ack) = serde_json::from_slice::<RegisterAckPayload>(payload) {
            if !ack.generation_id.is_empty() {
                let mut gen = self.inner.generation_id.lock().unwrap();
                *gen = ack.generation_id.clone();
                engine_debug!(
                    "[WasmWs] server generation_id={} max_sequence={}",
                    ack.generation_id,
                    ack.max_sequence
                );
            }
            self.inner.history_preserved.store(ack.history_preserved, Ordering::SeqCst);
            self.inner.max_sequence.store(ack.max_sequence, Ordering::SeqCst);
        }

        let reg_elapsed = (js_sys::Date::now() - reg_t0) as u64;
        engine_info!("[ws] register elapsed={}ms", reg_elapsed);

        // 3.5 Drain MSG_SNAPSHOT frames pushed by server after REGISTER_ACK
        let snap_t0 = js_sys::Date::now();
        engine_debug!("[WasmWs] draining snapshots after register");
        loop {
            let timeout = vaultsync_core::time_utils::sleep(
                std::time::Duration::from_millis(300),
            );
            futures::pin_mut!(timeout);
            match futures::future::select(msg_rx.next(), timeout).await {
                Either::Left((Some(bin), _)) => {
                    if let Ok((mt, pl)) = decode_frame(&bin) {
                        if mt == MSG_SNAPSHOT {
                            if let Ok(sp) = serde_json::from_slice::<SnapshotPayload>(pl) {
                                engine_trace!("[WasmWs] drained snapshot doc={} seq={}", sp.doc_id, sp.sequence);
                                let snapshot = Snapshot {
                                    doc_id: sp.doc_id,
                                    record_id: sp.record_id,
                                    schema_version: sp.schema_version,
                                    sequence: sp.sequence,
                                    created_at: sp.created_at,
                                    bytes: sp.bytes,
                                    checksum: sp.checksum,
                                };
                                self.inner.received_snapshots.lock().unwrap().push(snapshot);
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        let snap_elapsed = (js_sys::Date::now() - snap_t0) as u64;
        engine_info!("[ws] snapshot_drain elapsed={}ms", snap_elapsed);

        // Compute effective subscribe cursor: prefer max snapshot seq from drain over raw `after`
        let max_snap_seq = self.inner.received_snapshots.lock().unwrap()
            .iter()
            .map(|s| s.sequence)
            .max()
            .unwrap_or(0);
        let effective_after = std::cmp::max(after, max_snap_seq);

        // 4. Perform SUBSCRIBE (skipped when caller will subscribe separately after gen check)
        let sub_t0 = js_sys::Date::now();
        if !skip_subscribe {
            engine_debug!("[VaultSync] subscribe after={} (after={} max_snapshot={})", effective_after, after, max_snap_seq);
            let sub = SubscribePayload {
                request_id: uuid::Uuid::new_v4().to_string(),
                namespace: namespace.to_string(),
                after: effective_after,
            };
            let sub_frame = encode_frame(MSG_SUBSCRIBE, &sub)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            ws.send_with_u8_array(&sub_frame)
                .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
            let sub_elapsed = (js_sys::Date::now() - sub_t0) as u64;
            engine_info!("[ws] subscribe_send elapsed={}ms", sub_elapsed);
        } else {
            engine_debug!("[VaultSync] subscribe deferred (will subscribe after gen check)");
        }

        // Spawn background message processor with generation guard
        let inner_clone = self.inner.clone();
        let reader_gen = generation;
        engine_debug!("[WasmWs] reader started gen={}", reader_gen);
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bin) = msg_rx.next().await {
                if inner_clone.conn_gen.load(Ordering::SeqCst) != reader_gen {
                    engine_debug!(
                        "[WasmWs] reader exiting gen={} current={}",
                        reader_gen,
                        inner_clone.conn_gen.load(Ordering::SeqCst)
                    );
                    break; // connection was replaced, zombie reader exits
                }
                if let Ok((msg_type, payload)) = decode_frame(&bin) {
                    if msg_type == MSG_MUTATION_PUSH {
                        if let Ok(mutat) = serde_json::from_slice::<PendingMutation>(payload) {
                            // Self-filter: skip mutations from our own replica
                            let self_rid = inner_clone.replica_id.lock().unwrap();
                            if mutat.replica_id == *self_rid {
                                engine_trace!(
                                    "[WasmWs] skipping own mutation: seq={}",
                                    mutat.sequence
                                );
                                continue;
                            }
                            drop(self_rid);
                            engine_trace!(
                                "[WasmWs] push seq={}",
                                mutat.sequence
                            );
                            let mut subs = inner_clone.subscribers.lock().unwrap();
                            subs.retain_mut(|sub| sub.try_send(mutat.clone()).is_ok());
                            drop(subs);
                            if let Some(ref notify) = *inner_clone.download_notify.lock().unwrap() {
                                let _ = notify.unbounded_send(Some(mutat.clone()));
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
                    } else if msg_type == MSG_SNAPSHOT {
                        if let Ok(sp) = serde_json::from_slice::<SnapshotPayload>(payload) {
                            let snapshot = Snapshot {
                                doc_id: sp.doc_id,
                                record_id: sp.record_id,
                                schema_version: sp.schema_version,
                                sequence: sp.sequence,
                                created_at: sp.created_at,
                                bytes: sp.bytes,
                                checksum: sp.checksum,
                            };
                            inner_clone.received_snapshots.lock().unwrap().push(snapshot);
                            engine_trace!("[reader] cached snapshot seq={}", sp.sequence);
                        }
                    } else if matches!(msg_type, MSG_HEARTBEAT | MSG_HEARTBEAT_ACK | MSG_ERROR) {
                        // System messages — no response routing needed
                    } else {
                        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                            if let Some(req_id) =
                                json_val.get("request_id").and_then(|v| v.as_str())
                            {
                                let mut reqs = inner_clone.pending_requests.lock().unwrap();
                                engine_trace!(
                                    "[reader] routing req={} pending_requests={}",
                                    req_id,
                                    reqs.len()
                                );
                                if let Some(tx) = reqs.remove(req_id) {
                                    let _ = tx.send(bin);
                                } else {
                                    engine_trace!("[reader] NO WAITER for req={}", req_id);
                                }
                            }
                        }
                    }
                }
            }
        });

        // Spawn heartbeat loop with generation guard
        let hb_inner = self.inner.clone();
        let hb_gen = generation;
        engine_debug!("[WasmWs] heartbeat started gen={}", hb_gen);
        let ws_clone = ws.clone();
        let replica_id_hb = auth.replica_id.clone();
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(30)).await;
                if hb_inner.conn_gen.load(Ordering::SeqCst) != hb_gen {
                    break; // connection was replaced, zombie heartbeat exits
                }
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

            // Set signaling handler with generation guard
            let signal_inner = self.inner.clone();
            let signal_gen = generation;
            engine_debug!("[WasmWs] signal loop started gen={}", signal_gen);
            wasm_bindgen_futures::spawn_local(async move {
                peer_coord_clone
                    .set_signal_handler(move |target_id, signal_type, data| {
                        if signal_inner.conn_gen.load(Ordering::SeqCst) != signal_gen {
                            return;
                        }
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
                let _ = peer_coord_clone.register(&ns_str_clone, peer_info, 0).await;
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
                        let state = inner_loop.connection.lock().unwrap();
                        if let Some(ref current_ws) = state.ws {
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
            let mut state = self.inner.connection.lock().unwrap();
            if let Some(old_ws) = state.ws.take() {
                old_ws.set_onopen(None);
                old_ws.set_onclose(None);
                old_ws.set_onmessage(None);
                let _ = old_ws.close();
            }
            state.ws = Some(ws);
            state.generation = generation;
        }

        Ok(())
    }

    async fn send_request<T: serde::Serialize>(
        &self,
        msg_type: u8,
        request_id: &str,
        payload: &T,
    ) -> Result<Vec<u8>, CoordinatorError> {
        tracing::trace!("[send_request] create oneshot channel req={}", request_id);
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        {
            let mut reqs = self.inner.pending_requests.lock().unwrap();
            reqs.insert(request_id.to_string(), tx);
            tracing::trace!("[send_request] inserted pending req={} pending_count={}", request_id, reqs.len());
        }

        tracing::trace!("[send_request] encode frame start req={}", request_id);
        let frame = encode_frame(msg_type, payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        tracing::trace!("[send_request] encode frame done req={} frame_len={}", request_id, frame.len());

        {
            let state = self.inner.connection.lock().unwrap();
            if let Some(ref ws) = state.ws {
                if ws.ready_state() == 1 {
                    engine_trace!("[ws] sending frame req={} type={:02X}", request_id, msg_type);
                    ws.send_with_u8_array(&frame)
                        .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                    tracing::trace!("[send_request] frame sent req={}", request_id);
                } else {
                    tracing::trace!("[send_request] NOT_AVAILABLE (ready_state={}) req={}", ws.ready_state(), request_id);
                    return Err(CoordinatorError::NotAvailable);
                }
            } else {
                tracing::trace!("[send_request] NOT_AVAILABLE (no ws) req={}", request_id);
                return Err(CoordinatorError::NotAvailable);
            }
        }

        tracing::trace!("[send_request] awaiting response req={}", request_id);
        let timeout =
            vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(10));

        futures::pin_mut!(timeout);
        futures::pin_mut!(rx);

        match futures::future::select(rx, timeout).await {
            Either::Left((Ok(resp), _)) => {
                tracing::trace!("[send_request] response received req={} resp_len={}", request_id, resp.len());
                Ok(resp)
            }

            Either::Left((Err(_), _)) => {
                tracing::trace!("[send_request] cancelled req={}", request_id);
                Err(CoordinatorError::Internal("request cancelled".into()))
            }

            Either::Right(_) => {
                engine_warn!("[ws] request timed out after 10s req={}", request_id);
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
            let state = self.inner.connection.lock().unwrap();
            state.ws.is_some()
        };
        if !is_connected {
            if let Err(e) = self.connect_and_handshake(namespace, 0, None).await {
                engine_warn!("[WasmWs] Reconnect failed during push: {:?}", e);
                return Err(e);
            }
        }
        if !mutations.is_empty() {
            engine_debug!(
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

        tracing::trace!("[WasmWs] push: send_request start req_id={}", req_id);
        let resp_bin = self.send_request(MSG_PUSH, &req_id, &payload).await?;
        tracing::trace!("[WasmWs] push: send_request done resp_len={}", resp_bin.len());
        tracing::trace!("[WasmWs] push: decode_frame start");
        let (msg_type, payload) =
            decode_frame(&resp_bin).map_err(|e| CoordinatorError::Internal(e))?;
        tracing::trace!("[WasmWs] push: decode_frame done msg_type={:02X}", msg_type);

        if msg_type == MSG_PUSH_ACK {
            tracing::trace!("[WasmWs] push: parse ack start");
            let ack: PushAckPayload = serde_json::from_slice(payload)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            tracing::trace!("[WasmWs] push: parse ack done seq_count={}", ack.sequences.len());
            if let Some(err) = ack.error {
                engine_warn!("[WasmWs] push failed with error: {}", err);
                return Err(CoordinatorError::Internal(err));
            }
            if !ack.sequences.is_empty() {
                engine_debug!(
                    "[WasmWs] push ack success, returned sequences={:?}",
                    ack.sequences
                );
            }
            tracing::trace!("[WasmWs] push: return sequences ok");
            Ok(ack.sequences)
        } else {
            engine_warn!("[WasmWs] push got invalid response type: {:02X}", msg_type);
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
            let state = self.inner.connection.lock().unwrap();
            state.ws.is_some()
        };
        if !is_connected {
            if let Err(e) = self.connect_and_handshake(namespace, after, None).await {
                engine_info!("[WasmWs] Reconnect failed during pull: {:?}", e);
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
                engine_debug!(
                    "[WasmWs] pull returned {} mutations, has_more={}",
                    resp.mutations.len(),
                    resp.has_more
                );
            }
            Ok(resp.mutations)
        } else {
            engine_warn!("[WasmWs] pull got invalid response type: {:02X}", msg_type);
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
            let state = self.inner.connection.lock().unwrap();
            state.ws.is_some()
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
            let state = self.inner.connection.lock().unwrap();
            if let Some(ref ws) = state.ws {
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

    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
        last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        engine_debug!(
            "[WasmWs] register last_sequence={}",
            last_sequence
        );
        // Store our replica_id for self-filtering
        {
            let mut rid = self.inner.replica_id.lock().unwrap();
            *rid = info.replica_id.clone();
        }

        let is_connected = {
            let state = self.inner.connection.lock().unwrap();
            state.ws.is_some()
        };

        if !is_connected {
            self.connect_and_handshake_impl(namespace, last_sequence, Some(&info), true)
                .await?;
        } else {
            let reg = RegisterPayload {
                replica_id: info.replica_id.clone(),
                namespace: namespace.to_string(),
                public_key: info.public_key.clone(),
                schema_version: info.schema_version,
                last_sequence,
                key_version: 1,
            };
            let frame = encode_frame(MSG_REGISTER, &reg)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            let state = self.inner.connection.lock().unwrap();
            if let Some(ref ws) = state.ws {
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

        let state = self.inner.connection.lock().unwrap();
        if let Some(ref ws) = state.ws {
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

    async fn list_snapshots(
        &self,
        _namespace: &str,
    ) -> Result<Vec<Snapshot>, CoordinatorError> {
        let snaps = self.inner.received_snapshots.lock().unwrap().clone();
        engine_debug!("[WasmWs] list_snapshots returning {} snapshots", snaps.len());
        Ok(snaps)
    }

    async fn store_snapshot(
        &self,
        _namespace: &str,
        snapshot: &Snapshot,
    ) -> Result<(), CoordinatorError> {
        let payload = SnapshotPayload {
            doc_id: snapshot.doc_id.clone(),
            record_id: snapshot.record_id.clone(),
            sequence: snapshot.sequence,
            bytes: snapshot.bytes.clone(),
            checksum: snapshot.checksum,
            namespace: _namespace.to_string(),
            schema_version: snapshot.schema_version,
            created_at: snapshot.created_at,
        };
        let frame = encode_frame(MSG_SNAPSHOT, &payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        let state = self.inner.connection.lock().unwrap();
        if let Some(ref ws) = state.ws {
            if ws.ready_state() == 1 {
                ws.send_with_u8_array(&frame)
                    .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                engine_trace!("[WasmWs] stored snapshot doc={} seq={}", snapshot.doc_id, snapshot.sequence);
            }
        }
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        engine_debug!("[WasmWs] disconnect requested");
        let mut state = self.inner.connection.lock().unwrap();
        if let Some(ws) = state.ws.take() {
            ws.set_onopen(None);
            ws.set_onclose(None);
            ws.set_onmessage(None);
            let _ = ws.close();
            engine_debug!("[WasmWs] existing connection closed");
        }
        state.connecting = false;
        state.waiters.clear();
        drop(state);
        // Clear cached snapshots to avoid stale data on reconnect
        self.inner.received_snapshots.lock().unwrap().clear();
        // Bump conn_gen to invalidate background readers from the old connection
        self.inner.conn_gen.fetch_add(1, Ordering::SeqCst);
        engine_debug!("[WasmWs] disconnect complete");
        Ok(())
    }

    async fn generation_id(&self) -> String {
        self.inner.generation_id.lock().unwrap().clone()
    }

    async fn history_preserved(&self) -> bool {
        self.inner.history_preserved.load(Ordering::SeqCst)
    }

    async fn max_sequence(&self) -> u64 {
        self.inner.max_sequence.load(Ordering::SeqCst)
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
