use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::future::Either;
use futures::{FutureExt, Stream, StreamExt};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use vaultsync_core::coordinator::traits::{
    CoordinatorError, EncryptedMutation, PendingMutation,
};
use vaultsync_core::coordinator::ws_proto::*;
use vaultsync_core::transport::traits::{
    InboundMutation, Transport, TransportError, TransportSource,
};
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, MessageEvent, WebSocket};

/// WebSocket transport for cross-device mutation exchange
///
/// - Connects to the coordinator WS endpoint
/// - Sends mutations as MSG_PUSH frames
/// - Receives mutations as MSG_MUTATION_PUSH frames
/// - Non-mutation messages are routed to the protocol handler via pending_requests
#[derive(Debug)]
pub struct WasmWsTransport {
    inner: Arc<WasmWsTransportInner>,
}

#[derive(Debug)]
struct WasmWsTransportInner {
    url: String,
    auth_token: Option<String>,
    ws: Mutex<Option<WebSocket>>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    /// Stream of all raw WS messages (for protocol layer)
    raw_rx: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    /// Sender for raw messages (cloned to WS onmessage callback)
    raw_tx: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    /// Stream of mutation-specific messages (for Transport trait)
    mutation_tx: Mutex<Option<mpsc::Sender<InboundMutation>>>,
}

impl WasmWsTransport {
    pub fn new(url: &str, auth_token: Option<String>) -> Self {
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(128);
        Self {
            inner: Arc::new(WasmWsTransportInner {
                url: url.to_string(),
                auth_token,
                ws: Mutex::new(None),
                pending_requests: Mutex::new(HashMap::new()),
                raw_rx: Mutex::new(Some(raw_rx)),
                raw_tx: Mutex::new(Some(raw_tx)),
                mutation_tx: Mutex::new(None),
            }),
        }
    }

    /// Get a raw receiver for all WS messages (for protocol layer)
    pub fn take_raw_receiver(&self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.inner.raw_rx.lock().unwrap().take()
    }

    /// Get the pending requests map (for protocol layer)
    pub fn pending_requests(&self) -> &Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>> {
        &self.inner.pending_requests
    }

    /// Connect to the WebSocket server
    pub async fn connect(&self, namespace: &str) -> Result<WebSocket, CoordinatorError> {
        let ws_url = get_ws_url(&self.inner.url, namespace);
        engine_debug!("[WasmWs] connecting to url={}", ws_url);
        let ws = WebSocket::new(&ws_url)
            .map_err(|e| CoordinatorError::Internal(format!("Failed to create WebSocket: {:?}", e)))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        // Wait for connection to open
        let (open_rx, close_rx) = {
            let (open_tx, open_rx) = oneshot::channel::<()>();
            let open_tx_cell = std::cell::RefCell::new(Some(open_tx));
            let open_callback = Closure::wrap(Box::new(move |_e: web_sys::Event| {
                engine_debug!("[WasmWs] WebSocket opened!");
                if let Some(tx) = open_tx_cell.borrow_mut().take() {
                    let _ = tx.send(());
                }
            }) as Box<dyn Fn(web_sys::Event)>);
            ws.set_onopen(Some(open_callback.as_ref().unchecked_ref()));
            open_callback.forget();

            let (close_tx, close_rx) = oneshot::channel::<()>();
            let close_tx_cell = std::cell::RefCell::new(Some(close_tx));
            let inner_clone = self.inner.clone();
            let close_callback = Closure::wrap(Box::new(move |e: web_sys::Event| {
                engine_debug!("[WasmWs] WebSocket closed/failed: {:?}", e);
                if let Some(tx) = close_tx_cell.borrow_mut().take() {
                    let _ = tx.send(());
                }
                *inner_clone.ws.lock().unwrap() = None;
            }) as Box<dyn Fn(web_sys::Event)>);
            ws.set_onclose(Some(close_callback.as_ref().unchecked_ref()));
            close_callback.forget();

            (open_rx, close_rx)
        };

        futures::select! {
            _ = open_rx.fuse() => {
                engine_debug!("[WasmWs] connected!");
            }
            _ = close_rx.fuse() => {
                engine_debug!("[WasmWs] connection failed!");
                return Err(CoordinatorError::NotAvailable);
            }
        }

        // Setup message handler for ALL WS messages
        let raw_tx_rc = Rc::new(RefCell::new(self.inner.raw_tx.lock().unwrap().clone()));
        let mutation_tx_rc = Rc::new(RefCell::new(self.inner.mutation_tx.lock().unwrap().clone()));
        let msg_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(ab) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&ab);
                let vec = array.to_vec();

                if let Ok((msg_type, payload)) = decode_frame(&vec) {
                    if msg_type == MSG_MUTATION_PUSH {
                        // Forward mutation to the Transport trait's incoming stream
                        if let Ok(mutat) = serde_json::from_slice::<PendingMutation>(payload) {
                            let seq = mutat.sequence;
                            if let Some(ref mut tx) = *mutation_tx_rc.borrow_mut() {
                                let inbound = InboundMutation {
                                    source: TransportSource::WebSocket,
                                    mutation: mutat,
                                    server_seq: Some(seq),
                                };
                                let _ = tx.try_send(inbound);
                            }
                            return; // Don't forward mutation to raw stream
                        }
                    }

                    // If it has a request_id, route to pending_requests
                    if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                        if let Some(_req_id) = json_val.get("request_id").and_then(|v| v.as_str()) {
                            // The protocol layer will pick this up from raw_rx
                        }
                    }
                }

                // Forward to raw stream for protocol layer
                if let Some(ref mut tx) = *raw_tx_rc.borrow_mut() {
                    let _ = tx.try_send(vec);
                }
            }
        }) as Box<dyn Fn(MessageEvent)>);

        ws.set_onmessage(Some(msg_callback.as_ref().unchecked_ref()));
        msg_callback.forget();

        // Store the WebSocket
        {
            let mut ws_lock = self.inner.ws.lock().unwrap();
            if let Some(old_ws) = ws_lock.take() {
                old_ws.set_onopen(None);
                old_ws.set_onclose(None);
                old_ws.set_onmessage(None);
                let _ = old_ws.close();
            }
            *ws_lock = Some(ws.clone());
        }

        Ok(ws)
    }

    /// Send raw bytes over the WebSocket
    pub fn send_raw(&self, data: &[u8]) -> Result<(), CoordinatorError> {
        let ws_lock = self.inner.ws.lock().unwrap();
        if let Some(ref ws) = *ws_lock {
            if ws.ready_state() == 1 {
                ws.send_with_u8_array(data)
                    .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;
                Ok(())
            } else {
                Err(CoordinatorError::NotAvailable)
            }
        } else {
            Err(CoordinatorError::NotAvailable)
        }
    }

    /// Send a request and wait for a response (request/response pattern)
    pub async fn send_request<T: serde::Serialize>(
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

        self.send_raw(&frame)?;

        let timeout = vaultsync_core::time_utils::sleep(std::time::Duration::from_secs(10));

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

    /// Send an auth frame and wait for ack
    pub async fn authenticate(
        &self,
        ws: &WebSocket,
        namespace: &str,
        replica_id: &str,
        msg_rx: &mut mpsc::Receiver<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        let token = self.inner.auth_token.clone().unwrap_or_default();
        let auth = AuthPayload {
            token,
            protocol_version: 1,
            replica_id: replica_id.to_string(),
            namespace: namespace.to_string(),
        };
        let auth_frame = encode_frame(MSG_AUTH, &auth)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&auth_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        let auth_resp = recv_with_timeout(msg_rx, 5).await?;
        let (msg_type, payload) =
            decode_frame(&auth_resp).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_AUTH_ACK {
            return Err(CoordinatorError::Internal(format!(
                "Expected MSG_AUTH_ACK, got {:02X}",
                msg_type
            )));
        }

        let auth_ack: AuthAckPayload = serde_json::from_slice(payload)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;

        if auth_ack.status != "ok" {
            return Err(CoordinatorError::AuthFailed);
        }

        Ok(())
    }

    /// Send a register frame and wait for ack. Returns the raw ack payload.
    pub async fn register_with_server<F>(
        &self,
        ws: &WebSocket,
        namespace: &str,
        info: &vaultsync_core::coordinator::traits::ReplicaInfo,
        msg_rx: &mut mpsc::Receiver<Vec<u8>>,
        on_ack: F,
    ) -> Result<(), CoordinatorError>
    where
        F: FnOnce(&[u8]) -> Result<(), CoordinatorError>,
    {
        let reg = RegisterPayload {
            replica_id: info.replica_id.clone(),
            namespace: namespace.to_string(),
            public_key: info.public_key.clone(),
            schema_version: info.schema_version,
            last_sequence: 0,
            key_version: 1,
        };
        let reg_frame = encode_frame(MSG_REGISTER, &reg)
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        ws.send_with_u8_array(&reg_frame)
            .map_err(|e| CoordinatorError::Internal(format!("{:?}", e)))?;

        let reg_resp = recv_with_timeout(msg_rx, 5).await?;
        let (msg_type, payload) =
            decode_frame(&reg_resp).map_err(|e| CoordinatorError::Internal(e))?;

        if msg_type != MSG_REGISTER_ACK {
            return Err(CoordinatorError::Internal(format!(
                "Expected MSG_REGISTER_ACK, got {:02X}",
                msg_type
            )));
        }

        on_ack(payload)
    }
}

#[async_trait]
impl Transport for WasmWsTransport {
    async fn send(
        &self,
        namespace: &str,
        mutation: &EncryptedMutation,
    ) -> Result<(), TransportError> {
        // Check if connected
        let is_connected = {
            let ws_lock = self.inner.ws.lock().unwrap();
            ws_lock.is_some()
        };

        if !is_connected {
            // Try to connect (best-effort)
            if self.connect(namespace).await.is_err() {
                return Err(TransportError::NotAvailable);
            }
        }

        let req_id = uuid::Uuid::new_v4().to_string();
        let payload = PushPayload {
            request_id: req_id.clone(),
            mutations: vec![mutation.clone()],
        };

        let resp_bin = self
            .send_request(MSG_PUSH, &req_id, &payload)
            .await
            .map_err(|e| TransportError::SendFailed(format!("{:?}", e)))?;

        let (_msg_type, _payload) =
            decode_frame(&resp_bin).map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn incoming(
        &self,
        _namespace: &str,
    ) -> Result<Box<dyn Stream<Item = InboundMutation> + Send + Unpin>, TransportError> {
        let (tx, rx) = mpsc::channel::<InboundMutation>(128);
        *self.inner.mutation_tx.lock().unwrap() = Some(tx);
        Ok(Box::new(rx))
    }

    fn is_available(&self) -> bool {
        let ws_lock = self.inner.ws.lock().unwrap();
        ws_lock.is_some()
    }

    fn name(&self) -> &'static str {
        "WebSocket"
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
