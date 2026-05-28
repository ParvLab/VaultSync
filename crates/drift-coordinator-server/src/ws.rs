use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use std::borrow::Cow;
use tracing::{error, info, warn};

use drift_core::coordinator::traits::ReplicaInfo;
use drift_core::coordinator::ws_proto::*;
use crate::state::AppState;

async fn validate_token(state: &AppState, namespace: &str, token: &str) -> bool {
    // 1. Check global static token if configured
    if let Some(ref global_token) = state.config.auth_token {
        return token == global_token;
    }

    // 2. Check per-namespace token store if configured
    if let Some(ref token_store) = state.token_store {
        if state.config.admin_token.is_some() {
            if let Ok(valid) = token_store.validate_token(namespace, token).await {
                return valid;
            }
            return false;
        }
    }

    true
}

pub async fn ws_handler(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_session(state, ns, socket))
}

async fn handle_ws_session(state: AppState, ns: String, socket: WebSocket) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    
    // Dedicated channel for outgoing messages
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(128);
    
    // Spawn writer task
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = ws_sender.send(msg).await {
                warn!("Failed to send WS message to client: {:?}", e);
                break;
            }
        }
    });

    // 1. AUTH handshake
    let auth_msg = match tokio::time::timeout(std::time::Duration::from_secs(5), ws_receiver.next()).await {
        Ok(Some(Ok(Message::Binary(bin)))) => bin,
        _ => {
            warn!("WS connection closed or timed out before AUTH frame received");
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4001,
                reason: Cow::Borrowed("Auth timeout"),
            }))).await;
            return;
        }
    };

    let (msg_type, payload) = match decode_frame(&auth_msg) {
        Ok(res) => res,
        Err(e) => {
            warn!("Failed to decode AUTH frame: {}", e);
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4001,
                reason: Cow::Borrowed("Invalid frame header"),
            }))).await;
            return;
        }
    };

    if msg_type != MSG_AUTH {
        warn!("Expected MSG_AUTH (0x01), got 0x{:02X}", msg_type);
        let _ = tx.send(Message::Close(Some(CloseFrame {
            code: 4001,
            reason: Cow::Borrowed("Expected AUTH frame first"),
        }))).await;
        return;
    }

    let auth_payload: AuthPayload = match serde_json::from_slice(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to parse AuthPayload JSON: {}", e);
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4001,
                reason: Cow::Borrowed("Invalid AUTH payload"),
            }))).await;
            return;
        }
    };

    if auth_payload.namespace != ns {
        warn!("AUTH namespace mismatch: expected '{}', got '{}'", ns, auth_payload.namespace);
        let ack = AuthAckPayload {
            status: "error".to_string(),
            error: Some("NAMESPACE_MISMATCH".to_string()),
            session_id: "".to_string(),
        };
        if let Ok(ack_frame) = encode_frame(MSG_AUTH_ACK, &ack) {
            let _ = tx.send(Message::Binary(ack_frame)).await;
        }
        let _ = tx.send(Message::Close(Some(CloseFrame {
            code: 4001,
            reason: Cow::Borrowed("Namespace mismatch"),
        }))).await;
        return;
    }

    if !validate_token(&state, &ns, &auth_payload.token).await {
        warn!("AUTH failed for namespace '{}'", ns);
        let ack = AuthAckPayload {
            status: "error".to_string(),
            error: Some("INVALID_TOKEN".to_string()),
            session_id: "".to_string(),
        };
        if let Ok(ack_frame) = encode_frame(MSG_AUTH_ACK, &ack) {
            let _ = tx.send(Message::Binary(ack_frame)).await;
        }
        let _ = tx.send(Message::Close(Some(CloseFrame {
            code: 4001,
            reason: Cow::Borrowed("Invalid token"),
        }))).await;
        return;
    }

    // Auth successful
    let session_id = uuid::Uuid::new_v4().to_string();
    let ack = AuthAckPayload {
        status: "ok".to_string(),
        error: None,
        session_id: session_id.clone(),
    };
    let ack_frame = encode_frame(MSG_AUTH_ACK, &ack).expect("Failed to encode MSG_AUTH_ACK");
    if tx.send(Message::Binary(ack_frame)).await.is_err() {
        return;
    }

    // 2. REGISTER handshake
    let register_msg = match tokio::time::timeout(std::time::Duration::from_secs(5), ws_receiver.next()).await {
        Ok(Some(Ok(Message::Binary(bin)))) => bin,
        _ => {
            warn!("WS connection closed or timed out before REGISTER frame received");
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4002,
                reason: Cow::Borrowed("Register timeout"),
            }))).await;
            return;
        }
    };

    let (msg_type, payload) = match decode_frame(&register_msg) {
        Ok(res) => res,
        Err(e) => {
            warn!("Failed to decode REGISTER frame: {}", e);
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4002,
                reason: Cow::Borrowed("Invalid register frame"),
            }))).await;
            return;
        }
    };

    if msg_type != MSG_REGISTER {
        warn!("Expected MSG_REGISTER (0x03), got 0x{:02X}", msg_type);
        let _ = tx.send(Message::Close(Some(CloseFrame {
            code: 4002,
            reason: Cow::Borrowed("Expected REGISTER frame"),
        }))).await;
        return;
    }

    let reg_payload: RegisterPayload = match serde_json::from_slice(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to parse RegisterPayload JSON: {}", e);
            let _ = tx.send(Message::Close(Some(CloseFrame {
                code: 4002,
                reason: Cow::Borrowed("Invalid REGISTER payload"),
            }))).await;
            return;
        }
    };

    let replica_info = ReplicaInfo {
        replica_id: reg_payload.replica_id.clone(),
        namespace: ns.clone(),
        public_key: reg_payload.public_key,
        schema_version: reg_payload.schema_version,
    };

    let reg_result = state.coordinator.register(&ns, replica_info).await;
    let mut reg_ack = RegisterAckPayload {
        status: "ok".to_string(),
        coordinator_sequence: 0,
        snapshot_available: false,
        snapshot_sequence: 0,
        snapshot_url: None,
        error: None,
    };

    if let Err(e) = reg_result {
        error!("Failed to register replica: {:?}", e);
        reg_ack.status = "error".to_string();
        reg_ack.error = Some(format!("{:?}", e));
    } else if let Ok(seq) = state.coordinator.schema_version(&ns).await {
        reg_ack.coordinator_sequence = seq;
    }

    let ack_frame = encode_frame(MSG_REGISTER_ACK, &reg_ack).expect("Failed to encode MSG_REGISTER_ACK");
    if tx.send(Message::Binary(ack_frame)).await.is_err() {
        return;
    }

    // Keep track of active subscription cancellation token
    let mut active_sub_tx: Option<tokio::sync::oneshot::Sender<()>> = None;

    // 3. Message processing loop with 90s heartbeat timeout
    loop {
        let next_msg = tokio::time::timeout(
            std::time::Duration::from_secs(90),
            ws_receiver.next()
        ).await;

        let msg = match next_msg {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                warn!("WS connection error: {:?}", e);
                break;
            }
            Ok(None) => {
                info!("WS connection closed by client");
                break;
            }
            Err(_) => {
                warn!("WS connection timed out (no heartbeat for 90s)");
                let _ = tx.send(Message::Close(Some(CloseFrame {
                    code: 4000,
                    reason: Cow::Borrowed("Heartbeat timeout"),
                }))).await;
                break;
            }
        };

        match msg {
            Message::Binary(bin) => {
                let (msg_type, payload) = match decode_frame(&bin) {
                    Ok(res) => res,
                    Err(e) => {
                        warn!("Failed to decode binary frame: {}", e);
                        continue;
                    }
                };

                match msg_type {
                    MSG_PUSH => {
                        if let Ok(push) = serde_json::from_slice::<PushPayload>(payload) {
                            let res = state.coordinator.push(&ns, push.mutations).await;
                            let (sequences, error) = match res {
                                Ok(seqs) => (seqs, None),
                                Err(e) => (vec![], Some(format!("{:?}", e))),
                            };
                            let ack = PushAckPayload {
                                request_id: push.request_id,
                                sequences,
                                error,
                            };
                            if let Ok(frame) = encode_frame(MSG_PUSH_ACK, &ack) {
                                let _ = tx.send(Message::Binary(frame)).await;
                            }
                        }
                    }
                    MSG_PULL => {
                        if let Ok(pull) = serde_json::from_slice::<PullPayload>(payload) {
                            let res = state.coordinator.pull(&ns, pull.after, pull.limit).await;
                            let (mutations, has_more, error) = match res {
                                Ok(muts) => {
                                    let has_more = muts.len() == pull.limit;
                                    (muts, has_more, None)
                                }
                                Err(e) => (vec![], false, Some(format!("{:?}", e))),
                            };

                            if let Some(err_msg) = error {
                                let err_payload = ErrorPayload {
                                    request_id: Some(pull.request_id),
                                    code: "PULL_FAILED".to_string(),
                                    message: err_msg,
                                    retryable: true,
                                };
                                if let Ok(frame) = encode_frame(MSG_ERROR, &err_payload) {
                                    let _ = tx.send(Message::Binary(frame)).await;
                                }
                            } else {
                                let resp = PullResponsePayload {
                                    request_id: pull.request_id,
                                    mutations,
                                    has_more,
                                };
                                if let Ok(frame) = encode_frame(MSG_PULL_RESPONSE, &resp) {
                                    let _ = tx.send(Message::Binary(frame)).await;
                                }
                            }
                        }
                    }
                    MSG_SCHEMA_SYNC => {
                        if let Ok(sync) = serde_json::from_slice::<SchemaSyncPayload>(payload) {
                            let current_v = state.coordinator.schema_version(&ns).await.unwrap_or(0);
                            let status = if sync.version == current_v {
                                "ok".to_string()
                            } else {
                                "mismatch".to_string()
                            };
                            let resp = SchemaMigrationPayload {
                                status,
                                current_version: current_v,
                                error: None,
                            };
                            if let Ok(frame) = encode_frame(MSG_SCHEMA_MIGRATION, &resp) {
                                let _ = tx.send(Message::Binary(frame)).await;
                            }
                        }
                    }
                    MSG_SUBSCRIBE => {
                        if let Ok(sub) = serde_json::from_slice::<SubscribePayload>(payload) {
                            if let Some(cancel) = active_sub_tx.take() {
                                let _ = cancel.send(());
                            }

                            let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
                            active_sub_tx = Some(cancel_tx);

                            let stream_res = state.coordinator.subscribe(&ns, sub.after).await;
                            match stream_res {
                                Ok(stream) => {
                                    let tx_clone = tx.clone();
                                    tokio::spawn(async move {
                                        let mut pinned_stream = std::pin::Pin::from(stream);
                                        loop {
                                            tokio::select! {
                                                _ = &mut cancel_rx => {
                                                    break;
                                                }
                                                next = pinned_stream.next() => {
                                                    match next {
                                                        Some(mutation) => {
                                                            if let Ok(frame) = encode_frame(MSG_MUTATION_PUSH, &mutation) {
                                                                if tx_clone.send(Message::Binary(frame)).await.is_err() {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        None => break,
                                                    }
                                                }
                                            }
                                        }
                                    });
                                }
                                Err(e) => {
                                    let err_payload = ErrorPayload {
                                        request_id: Some(sub.request_id),
                                        code: "SUBSCRIBE_FAILED".to_string(),
                                        message: format!("{:?}", e),
                                        retryable: true,
                                    };
                                    if let Ok(frame) = encode_frame(MSG_ERROR, &err_payload) {
                                        let _ = tx.send(Message::Binary(frame)).await;
                                    }
                                }
                            }
                        }
                    }
                    MSG_HEARTBEAT => {
                        if let Ok(hb) = serde_json::from_slice::<HeartbeatPayload>(payload) {
                            let _ = state.coordinator.heartbeat(&ns, &hb.replica_id).await;
                            let ack = HeartbeatAckPayload {};
                            if let Ok(frame) = encode_frame(MSG_HEARTBEAT_ACK, &ack) {
                                let _ = tx.send(Message::Binary(frame)).await;
                            }
                        }
                    }
                    _ => {
                        warn!("Received unexpected message type over WS: 0x{:02X}", msg_type);
                    }
                }
            }
            Message::Close(_) => {
                info!("Client initiated close");
                break;
            }
            Message::Ping(ping) => {
                let _ = tx.send(Message::Pong(ping)).await;
            }
            _ => {}
        }
    }

    if let Some(cancel) = active_sub_tx.take() {
        let _ = cancel.send(());
    }
    writer_task.abort();
}
