use async_trait::async_trait;
use drift_core::coordinator::traits::{Coordinator, CoordinatorError, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId};
use drift_core::coordinator::ws_proto::*;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use futures::channel::{oneshot, mpsc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, WebSocket};

#[derive(Debug, Clone)]
pub struct WasmWsCoordinator {
    inner: Arc<WasmWsCoordinatorInner>,
}

struct WasmWsCoordinatorInner {
    url: String,
    auth_token: Option<String>,
    ws: Mutex<Option<WebSocket>>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    subscribers: Mutex<Vec<mpsc::Sender<PendingMutation>>>,
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
        Self {
            inner: Arc::new(WasmWsCoordinatorInner {
                url: url.to_string(),
                auth_token,
                ws: Mutex::new(None),
                pending_requests: Mutex::new(HashMap::new()),
                subscribers: Mutex::new(Vec::new()),
            }),
        }
    }

    async fn connect_and_handshake(&self, namespace: &str, after: SequenceId) -> Result<(), CoordinatorError> {
        let ws_url = get_ws_url(&self.inner.url, namespace);
        let ws = WebSocket::new(&ws_url)
            .map_err(|e| CoordinatorError::Internal(format!("Failed to create WebSocket: {:?}", e)))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        // 1. Wait for connection to open
        let (open_rx, close_rx) = {
            let (open_tx, open_rx) = oneshot::channel::<()>();
            let mut open_tx_opt = Some(open_tx);
            let open_callback = Closure::wrap(Box::new(move |_e: web_sys::Event| {
                if let Some(tx) = open_tx_opt.take() {
                    let _ = tx.send(());
                }
            }) as Box<dyn FnMut(web_sys::Event)>);
            ws.set_onopen(Some(open_callback.as_ref().unchecked_ref()));
            open_callback.forget();

            // Also setup onclose/onerror to abort if open fails
            let (close_tx, close_rx) = oneshot::channel::<()>();
            let mut close_tx_opt = Some(close_tx);
            let close_callback = Closure::wrap(Box::new(move |_e: web_sys::Event| {
                if let Some(tx) = close_tx_opt.take() {
                    let _ = tx.send(());
                }
            }) as Box<dyn FnMut(web_sys::Event)>);
            ws.set_onclose(Some(close_callback.as_ref().unchecked_ref()));
            close_callback.forget();

            (open_rx, close_rx)
        };

        futures::select! {
            _ = open_rx.fuse() => {}
            _ = close_rx.fuse() => return Err(CoordinatorError::NotAvailable),
        }

        // Setup message handler
        let mut msg_rx = {
            let (mut msg_tx, msg_rx) = mpsc::channel::<Vec<u8>>(128);
            let msg_callback = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Ok(ab) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let array = js_sys::Uint8Array::new(&ab);
                    let vec = array.to_vec();
                    let _ = msg_tx.try_send(vec);
                }
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);
            ws.set_onmessage(Some(msg_callback.as_ref().unchecked_ref()));
            msg_callback.forget();

            msg_rx
        };

        // 2. Perform AUTH
        let token = self.inner.auth_token.clone().unwrap_or_default();
        let auth = AuthPayload {
            token,
            protocol_version: 1,
            replica_id: format!("client-wasm-{}", uuid::Uuid::new_v4()),
            namespace: namespace.to_string(),
        };
        let auth_frame = encode_frame(MSG_AUTH, &auth)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&auth_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Wait for AUTH_ACK
        let auth_resp = msg_rx.next().await
            .ok_or_else(|| CoordinatorError::Internal("Connection closed during AUTH".to_string()))?;

        let (msg_type, payload) = decode_frame(&auth_resp)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_AUTH_ACK {
            return Err(CoordinatorError::Internal(format!("Expected MSG_AUTH_ACK, got {:02X}", msg_type)));
        }

        let auth_ack: AuthAckPayload = serde_json::from_slice(payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if auth_ack.status != "ok" {
            return Err(CoordinatorError::AuthFailed);
        }

        // 3. Perform REGISTER
        let reg = RegisterPayload {
            replica_id: auth.replica_id.clone(),
            namespace: namespace.to_string(),
            public_key: vec![],
            schema_version: 0,
            last_sequence: after,
        };
        let reg_frame = encode_frame(MSG_REGISTER, &reg)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&reg_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Wait for REGISTER_ACK
        let reg_resp = msg_rx.next().await
            .ok_or_else(|| CoordinatorError::Internal("Connection closed during REGISTER".to_string()))?;

        let (msg_type, _) = decode_frame(&reg_resp)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_REGISTER_ACK {
            return Err(CoordinatorError::Internal(format!("Expected MSG_REGISTER_ACK, got {:02X}", msg_type)));
        }

        // 4. Perform SUBSCRIBE
        let sub = SubscribePayload {
            request_id: uuid::Uuid::new_v4().to_string(),
            namespace: namespace.to_string(),
            after,
        };
        let sub_frame = encode_frame(MSG_SUBSCRIBE, &sub)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&sub_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        // Active connection inner management
        let inner_clone = self.inner.clone();

        // Spawn background message processor
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(bin) = msg_rx.next().await {
                if let Ok((msg_type, payload)) = decode_frame(&bin) {
                    if msg_type == MSG_MUTATION_PUSH {
                        if let Ok(mutat) = serde_json::from_slice::<PendingMutation>(payload) {
                            let mut subs = inner_clone.subscribers.lock().unwrap();
                            subs.retain_mut(|sub| {
                                sub.try_send(mutat.clone()).is_ok()
                            });
                        }
                    } else {
                        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                            if let Some(req_id) = json_val.get("request_id").and_then(|v| v.as_str()) {
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
                drift_core::time_utils::sleep(std::time::Duration::from_secs(30)).await;
                let hb = HeartbeatPayload {
                    replica_id: replica_id_hb.clone(),
                };
                if let Ok(frame) = encode_frame(MSG_HEARTBEAT, &hb) {
                    if ws_clone.send_with_u8_array(&frame).is_err() {
                        break;
                    }
                }
            }
        });

        *self.inner.ws.lock().unwrap() = Some(ws);
        Ok(())
    }

    async fn send_request<T: serde::Serialize>(&self, msg_type: u8, request_id: &str, payload: &T) -> Result<Vec<u8>, CoordinatorError> {
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
                ws.send_with_u8_array(&frame)
                    .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
            } else {
                return Err(CoordinatorError::NotAvailable);
            }
        }

        let resp = rx.await
            .map_err(|_| CoordinatorError::Internal("Request cancelled".to_string()))?;
        Ok(resp)
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
    async fn push(&self, _namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let payload = PushPayload {
            request_id: req_id.clone(),
            mutations,
        };

        let resp_bin = self.send_request(MSG_PUSH, &req_id, &payload).await?;
        let (msg_type, payload) = decode_frame(&resp_bin)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type == MSG_PUSH_ACK {
            let ack: PushAckPayload = serde_json::from_slice(payload)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            if let Some(err) = ack.error {
                return Err(CoordinatorError::Internal(err));
            }
            Ok(ack.sequences)
        } else {
            Err(CoordinatorError::Internal("Invalid response type".to_string()))
        }
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let payload = PullPayload {
            request_id: req_id.clone(),
            namespace: namespace.to_string(),
            after,
            limit,
        };

        let resp_bin = self.send_request(MSG_PULL, &req_id, &payload).await?;
        let (msg_type, payload) = decode_frame(&resp_bin)
            .map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type == MSG_PULL_RESPONSE {
            let resp: PullResponsePayload = serde_json::from_slice(payload)
                .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
            Ok(resp.mutations)
        } else {
            Err(CoordinatorError::Internal("Invalid response type".to_string()))
        }
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        self.connect_and_handshake(namespace, from_sequence).await?;
        
        let (tx, rx) = mpsc::channel::<PendingMutation>(100);
        {
            let mut subs = self.inner.subscribers.lock().unwrap();
            subs.push(tx);
        }

        Ok(Box::new(rx))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let reg = RegisterPayload {
            replica_id: info.replica_id.clone(),
            namespace: namespace.to_string(),
            public_key: info.public_key.clone(),
            schema_version: info.schema_version,
            last_sequence: 0,
        };
        let frame = encode_frame(MSG_REGISTER, &reg)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let ws_lock = self.inner.ws.lock().unwrap();
        if let Some(ref ws) = *ws_lock {
            ws.send_with_u8_array(&frame)
                .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
            Ok(())
        } else {
            Err(CoordinatorError::NotAvailable)
        }
    }

    async fn heartbeat(&self, _namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let hb = HeartbeatPayload {
            replica_id: replica_id.to_string(),
        };
        let frame = encode_frame(MSG_HEARTBEAT, &hb)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        let ws_lock = self.inner.ws.lock().unwrap();
        if let Some(ref ws) = *ws_lock {
            ws.send_with_u8_array(&frame)
                .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
            Ok(())
        } else {
            Err(CoordinatorError::NotAvailable)
        }
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(0)
    }
}
