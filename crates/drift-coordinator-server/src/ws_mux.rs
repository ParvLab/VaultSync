use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::{error, info, warn};

use drift_core::coordinator::traits::ReplicaInfo;
use drift_core::coordinator::ws_proto::*;
use crate::state::AppState;

pub async fn ws_mux_handler(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_mux_session(state, socket))
}

async fn handle_ws_mux_session(state: AppState, socket: WebSocket) {
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

    let mut registered_namespaces = HashMap::<String, String>::new(); // namespace -> replica_id
    let mut active_subs = HashMap::<String, tokio::sync::oneshot::Sender<()>>::new(); // namespace -> cancel sender

    // Main message processing loop with 90s heartbeat timeout
    loop {
        let next_msg = tokio::time::timeout(
            std::time::Duration::from_secs(90),
            ws_receiver.next()
        ).await;

        let msg = match next_msg {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                warn!("Mux WS connection error: {:?}", e);
                break;
            }
            Ok(None) => {
                info!("Mux WS connection closed by client");
                break;
            }
            Err(_) => {
                warn!("Mux WS connection timed out (no heartbeat for 90s)");
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
                    MSG_NAMESPACE_ADD => {
                        if let Ok(add_payload) = serde_json::from_slice::<NamespaceAddPayload>(payload) {
                            let ns = add_payload.namespace.clone();
                            let replica_id = add_payload.replica_id.clone();
                            
                            let replica_info = ReplicaInfo {
                                replica_id: replica_id.clone(),
                                namespace: ns.clone(),
                                public_key: add_payload.public_key,
                                schema_version: add_payload.schema_version,
                            };
                            
                            let reg_result = state.coordinator.register(&ns, replica_info).await;
                            
                            let mut ack = NamespaceAckPayload {
                                namespace: ns.clone(),
                                status: "ok".to_string(),
                                coordinator_sequence: 0,
                                snapshot_available: false,
                                snapshot_sequence: 0,
                                error: None,
                            };
                            
                            let mut available_snapshots = Vec::new();
                            if let Err(e) = reg_result {
                                error!("Failed to register replica for namespace {}: {:?}", ns, e);
                                ack.status = "error".to_string();
                                ack.error = Some(format!("{:?}", e));
                            } else {
                                if let Ok(seq) = state.coordinator.schema_version(&ns).await {
                                    ack.coordinator_sequence = seq;
                                }
                                if let Ok(snaps) = state.coordinator.list_snapshots(&ns).await {
                                    available_snapshots = snaps.into_iter()
                                        .filter(|s| s.sequence > add_payload.last_sequence)
                                        .collect::<Vec<_>>();
                                    if !available_snapshots.is_empty() {
                                        ack.snapshot_available = true;
                                        ack.snapshot_sequence = available_snapshots.iter().map(|s| s.sequence).max().unwrap_or(0);
                                    }
                                }
                            }
                            
                            let ack_frame = encode_frame(MSG_NAMESPACE_ACK, &ack).expect("Failed to encode MSG_NAMESPACE_ACK");
                            if tx.send(Message::Binary(ack_frame)).await.is_err() {
                                break;
                            }
                            
                            for snap in available_snapshots {
                                let snap_payload = SnapshotPayload {
                                    doc_id: snap.doc_id,
                                    record_id: snap.record_id,
                                    sequence: snap.sequence,
                                    bytes: snap.bytes,
                                    checksum: snap.checksum,
                                    namespace: ns.clone(),
                                };
                                if let Ok(snap_frame) = encode_frame(MSG_SNAPSHOT, &snap_payload) {
                                    if tx.send(Message::Binary(snap_frame)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            
                            // Register session in app state
                            state.sessions.sessions.write().await.insert((ns.clone(), replica_id.clone()), tx.clone());
                            registered_namespaces.insert(ns, replica_id);
                        }
                    }
                    MSG_NAMESPACE_DROP => {
                        if let Ok(drop_payload) = serde_json::from_slice::<NamespaceDropPayload>(payload) {
                            let ns = drop_payload.namespace.clone();
                            if let Some(cancel) = active_subs.remove(&ns) {
                                let _ = cancel.send(());
                            }
                            if let Some(replica_id) = registered_namespaces.remove(&ns) {
                                state.sessions.sessions.write().await.remove(&(ns, replica_id));
                            }
                        }
                    }
                    MSG_PUSH => {
                        if let Ok(push) = serde_json::from_slice::<PushPayload>(payload) {
                            let ns = match push.mutations.first() {
                                Some(m) => m.namespace.clone(),
                                None => "".to_string(),
                            };
                            if !ns.is_empty() {
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
                    }
                    MSG_PULL => {
                        if let Ok(pull) = serde_json::from_slice::<PullPayload>(payload) {
                            let ns = pull.namespace.clone();
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
                            let ns = sync.namespace.clone();
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
                                namespace: ns.clone(),
                            };
                            if let Ok(frame) = encode_frame(MSG_SCHEMA_MIGRATION, &resp) {
                                let _ = tx.send(Message::Binary(frame)).await;
                            }
                        }
                    }
                    MSG_SUBSCRIBE => {
                        if let Ok(sub) = serde_json::from_slice::<SubscribePayload>(payload) {
                            let ns = sub.namespace.clone();
                            if let Some(cancel) = active_subs.remove(&ns) {
                                let _ = cancel.send(());
                            }

                            let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
                            active_subs.insert(ns.clone(), cancel_tx);

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
                            if !hb.namespace.is_empty() {
                                let _ = state.coordinator.heartbeat(&hb.namespace, &hb.replica_id).await;
                            } else {
                                for (ns, rep_id) in &registered_namespaces {
                                    if rep_id == &hb.replica_id {
                                        let _ = state.coordinator.heartbeat(ns, rep_id).await;
                                    }
                                }
                            }
                            let ack = HeartbeatAckPayload {};
                            if let Ok(frame) = encode_frame(MSG_HEARTBEAT_ACK, &ack) {
                                let _ = tx.send(Message::Binary(frame)).await;
                            }
                        }
                    }
                    MSG_SNAPSHOT => {
                        if let Ok(snap) = serde_json::from_slice::<SnapshotPayload>(payload) {
                            let ns = snap.namespace.clone();
                            let snapshot = drift_core::crdt::snapshot::Snapshot {
                                doc_id: snap.doc_id,
                                record_id: snap.record_id,
                                schema_version: 0,
                                sequence: snap.sequence,
                                created_at: 0,
                                bytes: snap.bytes,
                                checksum: snap.checksum,
                            };
                            let _ = state.coordinator.store_snapshot(&ns, &snapshot).await;
                        }
                    }
                    MSG_P2P_SIGNAL => {
                        if let Ok(sig) = serde_json::from_slice::<P2PSignalPayload>(payload) {
                            let sessions_guard = state.sessions.sessions.read().await;
                            let mut sent = false;
                            for (ns, sender_replica_id) in &registered_namespaces {
                                if let Some(target_tx) = sessions_guard.get(&(ns.clone(), sig.target_replica_id.clone())) {
                                    let ack = P2PSignalAckPayload {
                                        sender_replica_id: sender_replica_id.clone(),
                                        signal_type: sig.signal_type.clone(),
                                        data: sig.data.clone(),
                                    };
                                    if let Ok(ack_frame) = encode_frame(MSG_P2P_SIGNAL_ACK, &ack) {
                                        let _ = target_tx.send(Message::Binary(ack_frame)).await;
                                    }
                                    sent = true;
                                    break;
                                }
                            }
                            if !sent {
                                warn!("WebRTC signaling target replica not found: {}", sig.target_replica_id);
                            }
                        }
                    }
                    _ => {
                        warn!("Received unexpected message type over Mux WS: 0x{:02X}", msg_type);
                    }
                }
            }
            Message::Close(_) => {
                info!("Client initiated close on Mux WS");
                break;
            }
            Message::Ping(ping) => {
                let _ = tx.send(Message::Pong(ping)).await;
            }
            _ => {}
        }
    }

    // Cleanup
    for (_, cancel) in active_subs {
        let _ = cancel.send(());
    }
    let mut sessions_guard = state.sessions.sessions.write().await;
    for (ns, rep_id) in registered_namespaces {
        sessions_guard.remove(&(ns, rep_id));
    }
    writer_task.abort();
}
