use worker::*;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct WebSocketAttachment {
    replica_id: String,
    namespace: String,
    authenticated: bool,
    #[serde(default)]
    namespaces: HashMap<String, String>,
}

#[derive(Deserialize)]
struct D1MutationRow {
    id: String,
    namespace: String,
    doc_id: String,
    record_id: String,
    encrypted_blob: Vec<u8>,
    timestamp: i64,
    sequence: i64,
    #[serde(default)]
    key_version: i64,
}

#[derive(Deserialize)]
struct D1SchemaVersionRow {
    version: i64,
}

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    let url = req.url()?;
    let path = url.path();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    
    let ns = if segments.len() >= 2 && segments[0] == "namespace" {
        segments[1]
    } else if segments.len() == 1 && segments[0] == "ws" {
        "__mux__"
    } else {
        "default"
    };

    let ns_do = env.durable_object("NAMESPACE_DO")?;
    let id = ns_do.id_from_name(ns)?;
    let stub = id.get_stub()?;
    
    stub.fetch_with_request(req).await
}

#[durable_object]
pub struct NamespaceDurableObject {
    state: State,
    env: Env,
}

impl DurableObject for NamespaceDurableObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        let path = url.path();
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        
        if segments.len() == 1 && segments[0] == "ws" {
            let pairs = WebSocketPair::new()?;
            let client = pairs.client;
            let server = pairs.server;

            self.state.accept_web_socket(&server);
            return Response::from_websocket(client);
        }

        if segments.len() < 3 {
            return Response::error("Not Found", 404);
        }
        
        let ns = segments[1];
        let method = segments[2];

        // Handle WebSocket upgrade
        if method == "ws" {
            let pairs = WebSocketPair::new()?;
            let client = pairs.client;
            let server = pairs.server;

            self.state.accept_web_socket(&server);
            return Response::from_websocket(client);
        }

        let db = self.env.d1("DB")?;
        
        match method {
            "push" => {
                if req.method() != Method::Post {
                    return Response::error("Method Not Allowed", 405);
                }
                let mutations: Vec<drift_core::coordinator::traits::EncryptedMutation> = req.json().await?;
                
                let mut sequences = Vec::new();
                for m in mutations {
                    let stmt = db.prepare("INSERT INTO mutations (id, namespace, replica_id, doc_id, record_id, encrypted_blob, timestamp, key_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)");
                    let blob_js = js_sys::Uint8Array::from(m.encrypted_blob.as_slice());
                    stmt.bind(&[
                        m.id.clone().into(),
                        ns.into(),
                        m.replica_id.clone().into(),
                        m.doc_id.clone().into(),
                        m.record_id.clone().into(),
                        blob_js.into(),
                        (m.timestamp as i64).into(),
                        (m.key_version as i64).into(),
                    ])?.run().await?;
                    
                    let last_row: serde_json::Value = db.prepare("SELECT last_insert_rowid() as seq")
                        .first(None).await?
                        .ok_or_else(|| worker::Error::from("Failed to get row ID"))?;
                    let seq = last_row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
                    sequences.push(seq);
                }
                
                Response::from_json(&sequences)
            }
            "pull" => {
                let query = url.query_pairs().collect::<HashMap<_, _>>();
                let after_str = query.get("after").map(|s| s.as_ref()).unwrap_or("0");
                let limit_str = query.get("limit").map(|s| s.as_ref()).unwrap_or("100");
                
                let after: i64 = after_str.parse().unwrap_or(0);
                let limit: i64 = limit_str.parse().unwrap_or(100);

                let stmt = db.prepare(
                    "SELECT id, namespace, doc_id, record_id, encrypted_blob, timestamp, sequence, key_version
                     FROM mutations
                     WHERE namespace = ?1 AND sequence > ?2
                     ORDER BY sequence ASC
                     LIMIT ?3"
                );
                
                let rows: Vec<D1MutationRow> = stmt.bind(&[ns.into(), after.into(), limit.into()])?
                    .all().await?
                    .results()?;
                
                let pending: Vec<drift_core::coordinator::traits::PendingMutation> = rows.into_iter().map(|r| {
                    drift_core::coordinator::traits::PendingMutation {
                        id: r.id,
                        namespace: r.namespace,
                        sequence: r.sequence as u64,
                        doc_id: r.doc_id,
                        record_id: r.record_id,
                        encrypted_blob: r.encrypted_blob,
                        timestamp: r.timestamp as u64,
                        key_version: r.key_version as u64,
                    }
                }).collect();
                
                Response::from_json(&pending)
            }
            "register" => {
                if req.method() != Method::Post {
                    return Response::error("Method Not Allowed", 405);
                }
                let info: drift_core::coordinator::traits::ReplicaInfo = req.json().await?;
                
                let stmt = db.prepare(
                    "INSERT OR REPLACE INTO replicas (replica_id, namespace, public_key, schema_version, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                );
                let pub_key_js = js_sys::Uint8Array::from(info.public_key.as_slice());
                let now = Date::now().as_millis() as i64;
                stmt.bind(&[
                    info.replica_id.into(),
                    ns.into(),
                    pub_key_js.into(),
                    (info.schema_version as i64).into(),
                    now.into(),
                ])?.run().await?;
                
                let schema_stmt = db.prepare(
                    "INSERT INTO schema_versions (namespace, version)
                     VALUES (?1, ?2)
                     ON CONFLICT(namespace) DO UPDATE SET version = MAX(version, excluded.version)"
                );
                schema_stmt.bind(&[ns.into(), (info.schema_version as i64).into()])?.run().await?;
                
                Response::ok("")
            }
            "heartbeat" => {
                if req.method() != Method::Post {
                    return Response::error("Method Not Allowed", 405);
                }
                let query = url.query_pairs().collect::<HashMap<_, _>>();
                let replica_id = query.get("replica_id").map(|s| s.as_ref()).unwrap_or("");
                
                let now = Date::now().as_millis() as i64;
                let stmt = db.prepare("UPDATE replicas SET last_seen = ?1 WHERE namespace = ?2 AND replica_id = ?3");
                stmt.bind(&[now.into(), ns.into(), replica_id.into()])?.run().await?;
                
                Response::ok("")
            }
            "schema_version" => {
                let stmt = db.prepare("SELECT version FROM schema_versions WHERE namespace = ?1");
                let row: Option<D1SchemaVersionRow> = stmt.bind(&[ns.into()])?.first(None).await?;
                let version = row.map(|r| r.version).unwrap_or(0);
                
                Response::from_json(&version.to_string())
            }
            "snapshot" => {
                if req.method() != Method::Get {
                    return Response::error("Method Not Allowed", 405);
                }
                let doc_id = segments.get(3).copied().unwrap_or("");
                let record_id = segments.get(4).copied().unwrap_or("");

                #[derive(Deserialize)]
                struct D1SnapshotBytesRow {
                    bytes: Vec<u8>,
                }

                let stmt = db.prepare(
                    "SELECT bytes FROM snapshots WHERE namespace = ?1 AND doc_id = ?2 AND record_id = ?3"
                );
                let row: Option<D1SnapshotBytesRow> = stmt
                    .bind(&[ns.into(), doc_id.into(), record_id.into()])?
                    .first(None)
                    .await?;

                match row {
                    Some(r) => {
                        let headers = worker::Headers::new();
                        headers.set("Content-Type", "application/octet-stream")?;
                        Ok(Response::from_bytes(r.bytes)?.with_headers(headers))
                    }
                    None => Response::error("Not Found", 404),
                }
            }
            "compact" => {
                if req.method() != Method::Post {
                    return Response::error("Method Not Allowed", 405);
                }

                #[derive(Deserialize)]
                struct D1SnapRow {
                    doc_id: String,
                    record_id: String,
                    sequence: i64,
                }

                let snap_stmt = db.prepare(
                    "SELECT doc_id, record_id, sequence FROM snapshots WHERE namespace = ?1"
                );
                let snaps: Vec<D1SnapRow> = snap_stmt.bind(&[ns.into()])?.all().await?.results()?;

                let mut oplog_removed: usize = 0;
                let mut snapshots_collapsed: usize = 0;

                for snap in snaps {
                    let del_stmt = db.prepare(
                        "DELETE FROM mutations \
                         WHERE namespace = ?1 AND doc_id = ?2 AND record_id = ?3 AND sequence <= ?4"
                    );
                    let _result = del_stmt.bind(&[
                        ns.into(),
                        snap.doc_id.into(),
                        snap.record_id.into(),
                        snap.sequence.into(),
                    ])?.run().await?;

                    oplog_removed += 1;
                    snapshots_collapsed += 1;
                }

                #[derive(Serialize)]
                struct CompactionResult {
                    oplog_removed: usize,
                    docs_removed: usize,
                    snapshots_collapsed: usize,
                }
                Response::from_json(&CompactionResult {
                    oplog_removed,
                    docs_removed: 0,
                    snapshots_collapsed,
                })
            }
            "replicas" => {
                #[derive(Deserialize)]
                struct D1ReplicaRow {
                    replica_id: String,
                    namespace: String,
                    public_key: Vec<u8>,
                    schema_version: i64,
                }

                let stmt = db.prepare(
                    "SELECT replica_id, namespace, public_key, schema_version FROM replicas WHERE namespace = ?1"
                );
                let rows: Vec<D1ReplicaRow> = stmt.bind(&[ns.into()])?.all().await?.results()?;

                let replicas: Vec<drift_core::coordinator::traits::ReplicaInfo> = rows.into_iter().map(|r| {
                    drift_core::coordinator::traits::ReplicaInfo {
                        replica_id: r.replica_id,
                        namespace: r.namespace,
                        public_key: r.public_key,
                        schema_version: r.schema_version as u64,
                    }
                }).collect();

                Response::from_json(&replicas)
            }
            _ => Response::error("Not Found", 404),
        }
    }

    async fn websocket_close(&self, _ws: WebSocket, _code: usize, _reason: String, _was_clean: bool) -> Result<()> {
        Ok(())
    }

    async fn websocket_error(&self, _ws: WebSocket, _error: Error) -> Result<()> {
        Ok(())
    }

    async fn websocket_message(&self, ws: WebSocket, message: worker::durable::WebSocketIncomingMessage) -> Result<()> {
        let bin = match message {
            worker::durable::WebSocketIncomingMessage::Binary(b) => b,
            _ => return Ok(()),
        };

        let (msg_type, payload) = match drift_core::coordinator::ws_proto::decode_frame(&bin) {
            Ok(res) => res,
            Err(_) => return Ok(()),
        };

        let db = self.env.d1("DB")?;

        match msg_type {
            drift_core::coordinator::ws_proto::MSG_AUTH => {
                let auth_payload: drift_core::coordinator::ws_proto::AuthPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };
                
                let mut namespaces = HashMap::new();
                namespaces.insert(auth_payload.namespace.clone(), auth_payload.replica_id.clone());
                let attachment = WebSocketAttachment {
                    replica_id: auth_payload.replica_id.clone(),
                    namespace: auth_payload.namespace.clone(),
                    authenticated: true,
                    namespaces,
                };
                ws.serialize_attachment(&attachment)?;

                let ack = drift_core::coordinator::ws_proto::AuthAckPayload {
                    status: "ok".to_string(),
                    error: None,
                    session_id: uuid::Uuid::new_v4().to_string(),
                };
                let ack_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_AUTH_ACK,
                    &ack
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&ack_frame)?;
            }
            drift_core::coordinator::ws_proto::MSG_NAMESPACE_ADD => {
                let add_payload: drift_core::coordinator::ws_proto::NamespaceAddPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let mut attachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) => a,
                    None => WebSocketAttachment {
                        replica_id: add_payload.replica_id.clone(),
                        namespace: add_payload.namespace.clone(),
                        authenticated: true,
                        namespaces: HashMap::new(),
                    },
                };
                attachment.namespaces.insert(add_payload.namespace.clone(), add_payload.replica_id.clone());
                ws.serialize_attachment(&attachment)?;

                let stmt = db.prepare(
                    "INSERT OR REPLACE INTO replicas (replica_id, namespace, public_key, schema_version, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                );
                let pub_key_js = js_sys::Uint8Array::from(add_payload.public_key.as_slice());
                let now = Date::now().as_millis() as i64;
                stmt.bind(&[
                    add_payload.replica_id.clone().into(),
                    add_payload.namespace.clone().into(),
                    pub_key_js.into(),
                    (add_payload.schema_version as i64).into(),
                    now.into(),
                ])?.run().await?;

                let schema_stmt = db.prepare(
                    "INSERT INTO schema_versions (namespace, version)
                     VALUES (?1, ?2)
                     ON CONFLICT(namespace) DO UPDATE SET version = MAX(version, excluded.version)"
                );
                schema_stmt.bind(&[add_payload.namespace.clone().into(), (add_payload.schema_version as i64).into()])?.run().await?;

                let mut ack = drift_core::coordinator::ws_proto::NamespaceAckPayload {
                    namespace: add_payload.namespace.clone(),
                    status: "ok".to_string(),
                    coordinator_sequence: 0,
                    snapshot_available: false,
                    snapshot_sequence: 0,
                    error: None,
                };
                if let Ok(Some(row)) = db.prepare("SELECT version FROM schema_versions WHERE namespace = ?1")
                    .bind(&[add_payload.namespace.clone().into()])?.first::<D1SchemaVersionRow>(None).await
                {
                    ack.coordinator_sequence = row.version as u64;
                }

                let mut available_snapshots = Vec::new();
                let snap_stmt = db.prepare("SELECT bytes FROM snapshots WHERE namespace = ?1");
                
                #[derive(Deserialize)]
                struct D1SnapshotRow {
                    bytes: Vec<u8>,
                }
                
                if let Ok(rows) = snap_stmt.bind(&[add_payload.namespace.clone().into()])?.all().await {
                    if let Ok(row_results) = rows.results::<D1SnapshotRow>() {
                        for row in row_results {
                            if let Ok(snap) = drift_core::crdt::snapshot::Snapshot::decode(&row.bytes) {
                                if snap.sequence > add_payload.last_sequence {
                                    available_snapshots.push(snap);
                                }
                            }
                        }
                    }
                }
                
                if !available_snapshots.is_empty() {
                    ack.snapshot_available = true;
                    ack.snapshot_sequence = available_snapshots.iter().map(|s| s.sequence).max().unwrap_or(0);
                }

                let ack_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_NAMESPACE_ACK,
                    &ack
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&ack_frame)?;

                for snap in available_snapshots {
                    let snap_payload = drift_core::coordinator::ws_proto::SnapshotPayload {
                        doc_id: snap.doc_id,
                        record_id: snap.record_id,
                        sequence: snap.sequence,
                        bytes: snap.bytes,
                        checksum: snap.checksum,
                        namespace: add_payload.namespace.clone(),
                    };
                    if let Ok(snap_frame) = drift_core::coordinator::ws_proto::encode_frame(
                        drift_core::coordinator::ws_proto::MSG_SNAPSHOT,
                        &snap_payload
                    ) {
                        let _ = ws.send_with_bytes(&snap_frame);
                    }
                }
            }
            drift_core::coordinator::ws_proto::MSG_NAMESPACE_DROP => {
                let drop_payload: drift_core::coordinator::ws_proto::NamespaceDropPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };
                if let Some(mut attachment) = ws.deserialize_attachment::<WebSocketAttachment>()? {
                    attachment.namespaces.remove(&drop_payload.namespace);
                    ws.serialize_attachment(&attachment)?;
                }
            }
            drift_core::coordinator::ws_proto::MSG_REGISTER => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let reg_payload: drift_core::coordinator::ws_proto::RegisterPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let stmt = db.prepare(
                    "INSERT OR REPLACE INTO replicas (replica_id, namespace, public_key, schema_version, last_seen)
                     VALUES (?1, ?2, ?3, ?4, ?5)"
                );
                let pub_key_js = js_sys::Uint8Array::from(reg_payload.public_key.as_slice());
                let now = Date::now().as_millis() as i64;
                stmt.bind(&[
                    reg_payload.replica_id.clone().into(),
                    attachment.namespace.clone().into(),
                    pub_key_js.into(),
                    (reg_payload.schema_version as i64).into(),
                    now.into(),
                ])?.run().await?;

                let schema_stmt = db.prepare(
                    "INSERT INTO schema_versions (namespace, version)
                     VALUES (?1, ?2)
                     ON CONFLICT(namespace) DO UPDATE SET version = MAX(version, excluded.version)"
                );
                schema_stmt.bind(&[attachment.namespace.clone().into(), (reg_payload.schema_version as i64).into()])?.run().await?;

                let mut reg_ack = drift_core::coordinator::ws_proto::RegisterAckPayload {
                    status: "ok".to_string(),
                    coordinator_sequence: 0,
                    snapshot_available: false,
                    snapshot_sequence: 0,
                    snapshot_url: None,
                    error: None,
                };
                if let Ok(Some(row)) = db.prepare("SELECT version FROM schema_versions WHERE namespace = ?1")
                    .bind(&[attachment.namespace.clone().into()])?.first::<D1SchemaVersionRow>(None).await
                {
                    reg_ack.coordinator_sequence = row.version as u64;
                }

                let mut available_snapshots = Vec::new();
                let snap_stmt = db.prepare("SELECT bytes FROM snapshots WHERE namespace = ?1");
                
                #[derive(Deserialize)]
                struct D1SnapshotRow {
                    bytes: Vec<u8>,
                }
                
                if let Ok(rows) = snap_stmt.bind(&[attachment.namespace.clone().into()])?.all().await {
                    if let Ok(row_results) = rows.results::<D1SnapshotRow>() {
                        for row in row_results {
                            if let Ok(snap) = drift_core::crdt::snapshot::Snapshot::decode(&row.bytes) {
                                if snap.sequence > reg_payload.last_sequence {
                                    available_snapshots.push(snap);
                                }
                            }
                        }
                    }
                }
                
                if !available_snapshots.is_empty() {
                    reg_ack.snapshot_available = true;
                    reg_ack.snapshot_sequence = available_snapshots.iter().map(|s| s.sequence).max().unwrap_or(0);
                }

                let ack_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_REGISTER_ACK,
                    &reg_ack
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&ack_frame)?;

                for snap in available_snapshots {
                    let snap_payload = drift_core::coordinator::ws_proto::SnapshotPayload {
                        doc_id: snap.doc_id,
                        record_id: snap.record_id,
                        sequence: snap.sequence,
                        bytes: snap.bytes,
                        checksum: snap.checksum,
                        namespace: attachment.namespace.clone(),
                    };
                    if let Ok(snap_frame) = drift_core::coordinator::ws_proto::encode_frame(
                        drift_core::coordinator::ws_proto::MSG_SNAPSHOT,
                        &snap_payload
                    ) {
                        let _ = ws.send_with_bytes(&snap_frame);
                    }
                }
            }
            drift_core::coordinator::ws_proto::MSG_PUSH => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let push: drift_core::coordinator::ws_proto::PushPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let ns = match push.mutations.first() {
                    Some(m) => m.namespace.clone(),
                    None => attachment.namespace.clone(),
                };

                let mut sequences = Vec::new();
                let mut pending_to_broadcast = Vec::new();

                for m in push.mutations {
                    let stmt = db.prepare("INSERT INTO mutations (id, namespace, replica_id, doc_id, record_id, encrypted_blob, timestamp, key_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)");
                    let blob_js = js_sys::Uint8Array::from(m.encrypted_blob.as_slice());
                    stmt.bind(&[
                        m.id.clone().into(),
                        ns.clone().into(),
                        m.replica_id.clone().into(),
                        m.doc_id.clone().into(),
                        m.record_id.clone().into(),
                        blob_js.into(),
                        (m.timestamp as i64).into(),
                        (m.key_version as i64).into(),
                    ])?.run().await?;

                    let last_row: serde_json::Value = db.prepare("SELECT last_insert_rowid() as seq")
                        .first(None).await?
                        .ok_or_else(|| worker::Error::from("Failed to get row ID"))?;
                    let seq = last_row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
                    sequences.push(seq);

                    pending_to_broadcast.push(drift_core::coordinator::traits::PendingMutation {
                        id: m.id,
                        namespace: ns.clone(),
                        sequence: seq,
                        doc_id: m.doc_id,
                        record_id: m.record_id,
                        encrypted_blob: m.encrypted_blob,
                        timestamp: m.timestamp,
                        key_version: m.key_version,
                    });
                }

                let ack = drift_core::coordinator::ws_proto::PushAckPayload {
                    request_id: push.request_id,
                    sequences,
                    error: None,
                };
                let ack_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_PUSH_ACK,
                    &ack
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&ack_frame)?;

                let all_ws = self.state.get_websockets();
                for socket in all_ws {
                    if socket != ws {
                        if let Ok(Some(sock_attachment)) = socket.deserialize_attachment::<WebSocketAttachment>() {
                            if sock_attachment.authenticated && (sock_attachment.namespace == ns || sock_attachment.namespaces.contains_key(&ns)) {
                                for pm in &pending_to_broadcast {
                                    if let Ok(frame) = drift_core::coordinator::ws_proto::encode_frame(
                                        drift_core::coordinator::ws_proto::MSG_MUTATION_PUSH,
                                        pm
                                    ) {
                                        let _ = socket.send_with_bytes(&frame);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            drift_core::coordinator::ws_proto::MSG_PULL => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let pull: drift_core::coordinator::ws_proto::PullPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let ns = if !pull.namespace.is_empty() { pull.namespace.clone() } else { attachment.namespace.clone() };

                let stmt = db.prepare(
                    "SELECT id, namespace, doc_id, record_id, encrypted_blob, timestamp, sequence, key_version
                     FROM mutations
                     WHERE namespace = ?1 AND sequence > ?2
                     ORDER BY sequence ASC
                     LIMIT ?3"
                );
                let rows: Vec<D1MutationRow> = stmt.bind(&[
                    ns.into(),
                    (pull.after as i64).into(),
                    (pull.limit as i64).into()
                ])?.all().await?.results()?;

                let mutations: Vec<drift_core::coordinator::traits::PendingMutation> = rows.into_iter().map(|r| {
                    drift_core::coordinator::traits::PendingMutation {
                        id: r.id,
                        namespace: r.namespace,
                        sequence: r.sequence as u64,
                        doc_id: r.doc_id,
                        record_id: r.record_id,
                        encrypted_blob: r.encrypted_blob,
                        timestamp: r.timestamp as u64,
                        key_version: r.key_version as u64,
                    }
                }).collect();

                let resp = drift_core::coordinator::ws_proto::PullResponsePayload {
                    request_id: pull.request_id,
                    mutations,
                    has_more: false,
                };
                let resp_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_PULL_RESPONSE,
                    &resp
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&resp_frame)?;
            }
            drift_core::coordinator::ws_proto::MSG_SUBSCRIBE => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let sub: drift_core::coordinator::ws_proto::SubscribePayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let ns = if !sub.namespace.is_empty() { sub.namespace.clone() } else { attachment.namespace.clone() };

                let stmt = db.prepare(
                    "SELECT id, namespace, doc_id, record_id, encrypted_blob, timestamp, sequence, key_version
                     FROM mutations
                     WHERE namespace = ?1 AND sequence > ?2
                     ORDER BY sequence ASC"
                );
                let rows: Vec<D1MutationRow> = stmt.bind(&[
                    ns.into(),
                    (sub.after as i64).into()
                ])?.all().await?.results()?;

                for r in rows {
                    let pm = drift_core::coordinator::traits::PendingMutation {
                        id: r.id,
                        namespace: r.namespace,
                        sequence: r.sequence as u64,
                        doc_id: r.doc_id,
                        record_id: r.record_id,
                        encrypted_blob: r.encrypted_blob,
                        timestamp: r.timestamp as u64,
                        key_version: r.key_version as u64,
                    };
                    if let Ok(frame) = drift_core::coordinator::ws_proto::encode_frame(
                        drift_core::coordinator::ws_proto::MSG_MUTATION_PUSH,
                        &pm
                    ) {
                        let _ = ws.send_with_bytes(&frame);
                    }
                }
            }
            drift_core::coordinator::ws_proto::MSG_HEARTBEAT => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let hb: drift_core::coordinator::ws_proto::HeartbeatPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let now = Date::now().as_millis() as i64;
                
                if !hb.namespace.is_empty() {
                    let stmt = db.prepare("UPDATE replicas SET last_seen = ?1 WHERE namespace = ?2 AND replica_id = ?3");
                    stmt.bind(&[now.into(), hb.namespace.clone().into(), hb.replica_id.into()])?.run().await?;
                } else {
                    let mut namespaces_to_hb = vec![attachment.namespace.clone()];
                    namespaces_to_hb.extend(attachment.namespaces.keys().cloned());
                    for ns in namespaces_to_hb {
                        let stmt = db.prepare("UPDATE replicas SET last_seen = ?1 WHERE namespace = ?2 AND replica_id = ?3");
                        stmt.bind(&[now.into(), ns.into(), hb.replica_id.clone().into()])?.run().await?;
                    }
                }

                let ack = drift_core::coordinator::ws_proto::HeartbeatAckPayload {};
                let ack_frame = drift_core::coordinator::ws_proto::encode_frame(
                    drift_core::coordinator::ws_proto::MSG_HEARTBEAT_ACK,
                    &ack
                ).map_err(|e| worker::Error::from(e.to_string()))?;
                ws.send_with_bytes(&ack_frame)?;
            }
            drift_core::coordinator::ws_proto::MSG_SNAPSHOT => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                
                let (ns, snap) = if let Ok(snap_payload) = serde_json::from_slice::<drift_core::coordinator::ws_proto::SnapshotPayload>(payload) {
                    (snap_payload.namespace.clone(), drift_core::crdt::snapshot::Snapshot {
                        doc_id: snap_payload.doc_id,
                        record_id: snap_payload.record_id,
                        schema_version: 0,
                        sequence: snap_payload.sequence,
                        created_at: 0,
                        bytes: snap_payload.bytes,
                        checksum: snap_payload.checksum,
                    })
                } else if let Ok(snap) = serde_json::from_slice::<drift_core::crdt::snapshot::Snapshot>(payload) {
                    (attachment.namespace.clone(), snap)
                } else {
                    return Ok(());
                };

                let snap_bytes = snap.encode().map_err(|e| worker::Error::from(e.to_string()))?;
                let stmt = db.prepare(
                    "INSERT OR REPLACE INTO snapshots (namespace, doc_id, record_id, sequence, created_at, bytes, checksum)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                );
                let snap_bytes_js = js_sys::Uint8Array::from(snap_bytes.as_slice());
                stmt.bind(&[
                    ns.into(),
                    snap.doc_id.into(),
                    snap.record_id.into(),
                    (snap.sequence as i64).into(),
                    (snap.created_at as i64).into(),
                    snap_bytes_js.into(),
                    (snap.checksum as i64).into(),
                ])?.run().await?;
            }
            drift_core::coordinator::ws_proto::MSG_P2P_SIGNAL => {
                let attachment: WebSocketAttachment = match ws.deserialize_attachment::<WebSocketAttachment>()? {
                    Some(a) if a.authenticated => a,
                    _ => return Ok(()),
                };
                let sig: drift_core::coordinator::ws_proto::P2PSignalPayload = match serde_json::from_slice(payload) {
                    Ok(p) => p,
                    Err(_) => return Ok(()),
                };

                let all_ws = self.state.get_websockets();
                for socket in all_ws {
                    if let Ok(Some(sock_attachment)) = socket.deserialize_attachment::<WebSocketAttachment>() {
                        if sock_attachment.authenticated 
                           && (sock_attachment.namespace == attachment.namespace || sock_attachment.namespaces.keys().any(|k| attachment.namespaces.contains_key(k)))
                           && (sock_attachment.replica_id == sig.target_replica_id || sock_attachment.namespaces.values().any(|v| v == &sig.target_replica_id))
                        {
                            let ack = drift_core::coordinator::ws_proto::P2PSignalAckPayload {
                                sender_replica_id: attachment.replica_id.clone(),
                                signal_type: sig.signal_type,
                                data: sig.data,
                            };
                            if let Ok(frame) = drift_core::coordinator::ws_proto::encode_frame(
                                drift_core::coordinator::ws_proto::MSG_P2P_SIGNAL_ACK,
                                &ack
                            ) {
                                let _ = socket.send_with_bytes(&frame);
                            }
                            break;
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}
