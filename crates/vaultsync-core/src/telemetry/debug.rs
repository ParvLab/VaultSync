use crate::error::VaultSyncError;
use crate::storage::traits::Storage;
use crate::telemetry::metrics::VaultSyncMetrics;
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "telemetry")]
use std::sync::Mutex;

#[cfg(feature = "telemetry")]
struct ServerState {
    namespace: String,
    replica_id: String,
    #[allow(dead_code)]
    keyring: Arc<crate::e2ee::keyring::KeyRing>,
    coordinator: Arc<dyn crate::coordinator::traits::Coordinator>,
    leader_election: Arc<crate::ipc::leader_election::LeaderElection>,
    upload_queue: Arc<crate::sync::upload::UploadQueue>,
    download_queue: Arc<crate::sync::download::DownloadQueue>,
}

#[cfg(feature = "telemetry")]
pub struct DebugApi {
    pub storage: Arc<dyn Storage>,
    pub metrics: Arc<VaultSyncMetrics>,
    server_state: Mutex<Option<ServerState>>,
}

#[cfg(not(feature = "telemetry"))]
pub struct DebugApi {
    pub storage: Arc<dyn Storage>,
    pub metrics: Arc<VaultSyncMetrics>,
}

#[cfg(feature = "telemetry")]
impl DebugApi {
    pub fn new(storage: Arc<dyn Storage>, metrics: Arc<VaultSyncMetrics>) -> Self {
        Self {
            storage,
            metrics,
            server_state: Mutex::new(None),
        }
    }

    pub async fn get_state(&self, namespace: &str) -> Result<Value, VaultSyncError> {
        let sync_state = self.storage.read_sync_state(namespace).await?;
        let metrics = self.metrics.snapshot();
        Ok(serde_json::json!({
            "sync_state": sync_state,
            "metrics": metrics,
        }))
    }

    pub fn start(
        self: &Arc<Self>,
        port: u16,
        namespace: String,
        replica_id: String,
        keyring: Arc<crate::e2ee::keyring::KeyRing>,
        coordinator: Arc<dyn crate::coordinator::traits::Coordinator>,
        leader_election: Arc<crate::ipc::leader_election::LeaderElection>,
        upload_queue: Arc<crate::sync::upload::UploadQueue>,
        download_queue: Arc<crate::sync::download::DownloadQueue>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        {
            let mut state = self.server_state.lock().unwrap();
            *state = Some(ServerState {
                namespace,
                replica_id,
                keyring,
                coordinator,
                leader_election,
                upload_queue,
                download_queue,
            });
        }

        let api_clone = self.clone();
        tokio::spawn(async move {
            use axum::{
                http::StatusCode,
                response::{IntoResponse, Response},
                routing::{get, post},
                Extension, Json, Router,
            };
            use serde_json::json;
            use std::net::SocketAddr;

            async fn handle_state(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let namespace = {
                    let state = api.server_state.lock().unwrap();
                    state
                        .as_ref()
                        .map(|s| s.namespace.clone())
                        .unwrap_or_else(|| "default".to_string())
                };
                match api.get_state(&namespace).await {
                    Ok(val) => Json(val).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }

            async fn handle_oplog(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let namespace_res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok(s.namespace.clone()),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let namespace = match namespace_res {
                    Ok(ns) => ns,
                    Err(resp) => return resp,
                };

                match api.storage.read_pending_oplog(&namespace, 1000).await {
                    Ok(mut pending) => {
                        match api.storage.read_oplog_after_sequence(&namespace, 0).await {
                            Ok(synced) => {
                                pending.extend(synced);
                                pending.sort_by_key(|e| e.created_at);
                                if pending.len() > 1000 {
                                    pending.drain(..pending.len() - 1000);
                                }
                                Json(pending).into_response()
                            }
                            Err(e) => (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": e.to_string()})),
                            )
                                .into_response(),
                        }
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }

            async fn handle_documents(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let namespace_res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok(s.namespace.clone()),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let namespace = match namespace_res {
                    Ok(ns) => ns,
                    Err(resp) => return resp,
                };

                let mut doc_ids = std::collections::HashSet::new();
                match api.storage.read_pending_oplog(&namespace, 10000).await {
                    Ok(pending) => {
                        for entry in pending {
                            doc_ids.insert(entry.doc_id);
                        }
                    }
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": e.to_string()})),
                        )
                            .into_response()
                    }
                }
                match api.storage.read_oplog_after_sequence(&namespace, 0).await {
                    Ok(synced) => {
                        for entry in synced {
                            doc_ids.insert(entry.doc_id);
                        }
                    }
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": e.to_string()})),
                        )
                            .into_response()
                    }
                }

                let mut docs_meta = Vec::new();
                for doc_id in doc_ids {
                    match api.storage.list_documents(&doc_id).await {
                        Ok(records) => {
                            let size: usize = records.iter().map(|(_, bytes)| bytes.len()).sum();
                            docs_meta.push(json!({
                                "doc_id": doc_id,
                                "record_count": records.len(),
                                "total_size_bytes": size,
                            }));
                        }
                        Err(e) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": e.to_string()})),
                            )
                                .into_response()
                        }
                    }
                }

                Json(docs_meta).into_response()
            }

            async fn handle_keys(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let namespace_res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok(s.namespace.clone()),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let namespace = match namespace_res {
                    Ok(ns) => ns,
                    Err(resp) => return resp,
                };

                match api.storage.read_keys(&namespace).await {
                    Ok(keys) => {
                        let meta: Vec<Value> = keys
                            .iter()
                            .map(|k| {
                                json!({
                                    "namespace": k.namespace,
                                    "version": k.version,
                                    "key_len": k.key_bytes.len(),
                                })
                            })
                            .collect();
                        Json(meta).into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }

            async fn handle_replicas(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok((s.namespace.clone(), s.coordinator.clone())),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let (namespace, coordinator) = match res {
                    Ok(val) => val,
                    Err(resp) => return resp,
                };

                match coordinator.list_replicas(&namespace).await {
                    Ok(replicas) => Json(replicas).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("{:?}", e)})),
                    )
                        .into_response(),
                }
            }

            async fn handle_metrics() -> Response {
                use prometheus::Encoder;
                let encoder = prometheus::TextEncoder::new();
                let metric_families = crate::telemetry::metrics::get_registry().gather();
                let mut buffer = Vec::new();
                if encoder.encode(&metric_families, &mut buffer).is_ok() {
                    match String::from_utf8(buffer) {
                        Ok(text) => Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "text/plain; version=0.0.4")
                            .body(axum::body::boxed(axum::body::Full::from(text)))
                            .unwrap(),
                        Err(_) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "invalid UTF-8 in metrics",
                        )
                            .into_response(),
                    }
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to encode metrics",
                    )
                        .into_response()
                }
            }

            async fn handle_traces() -> Response {
                let spans = crate::telemetry::tracing::get_span_buffer().get_all();
                Json(spans).into_response()
            }

            async fn handle_leader(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok((s.replica_id.clone(), s.leader_election.clone())),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let (replica_id, leader_election) = match res {
                    Ok(val) => val,
                    Err(resp) => return resp,
                };

                let is_leader = leader_election.is_leader();
                Json(json!({
                    "leader": is_leader,
                    "replica_id": replica_id,
                }))
                .into_response()
            }

            async fn handle_force_sync(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok((s.upload_queue.clone(), s.download_queue.clone())),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let (upload_queue, download_queue) = match res {
                    Ok(val) => val,
                    Err(resp) => return resp,
                };

                match upload_queue.process_batch().await {
                    Ok(up) => match download_queue.process_batch().await {
                        Ok(down) => Json(json!({
                            "status": "success",
                            "uploaded_mutations": up,
                            "downloaded_mutations": down,
                        }))
                        .into_response(),
                        Err(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": format!("download failed: {e}")})),
                        )
                            .into_response(),
                    },
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("upload failed: {e}")})),
                    )
                        .into_response(),
                }
            }

            async fn handle_force_election(Extension(api): Extension<Arc<DebugApi>>) -> Response {
                let leader_election_res = {
                    let state = api.server_state.lock().unwrap();
                    match state.as_ref() {
                        Some(s) => Ok(s.leader_election.clone()),
                        None => Err((
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "server not started"})),
                        )
                            .into_response()),
                    }
                };
                let leader_election = match leader_election_res {
                    Ok(val) => val,
                    Err(resp) => return resp,
                };

                // Release is automatic when Lease is dropped.
                // Use sync try_acquire for debug endpoint compatibility.
                match leader_election.try_acquire_immediate_sync() {
                    Ok(acquired) => Json(json!({
                        "status": "success",
                        "action": "released and re-tried lock",
                        "acquired": acquired,
                    }))
                    .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }

            let app = Router::new()
                .route("/debug/vaultsync/state", get(handle_state))
                .route("/debug/vaultsync/state/oplog", get(handle_oplog))
                .route("/debug/vaultsync/state/documents", get(handle_documents))
                .route("/debug/vaultsync/state/keys", get(handle_keys))
                .route("/debug/vaultsync/state/replicas", get(handle_replicas))
                .route("/debug/vaultsync/metrics", get(handle_metrics))
                .route("/debug/vaultsync/traces", get(handle_traces))
                .route("/debug/vaultsync/leader", get(handle_leader))
                .route("/debug/vaultsync/force-sync", post(handle_force_sync))
                .route(
                    "/debug/vaultsync/force-election",
                    post(handle_force_election),
                )
                .layer(Extension(api_clone));

            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            tracing::info!(port = port, "Debug HTTP server starting");
            let serve = axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .with_graceful_shutdown(async move {
                    while shutdown_rx.changed().await.is_ok() {
                        if *shutdown_rx.borrow() {
                            tracing::info!("Debug HTTP server received shutdown signal");
                            break;
                        }
                    }
                });

            if let Err(e) = serve.await {
                tracing::error!(error = %e, "Debug HTTP server error");
            }
        });
    }
}

#[cfg(not(feature = "telemetry"))]
impl DebugApi {
    pub fn new(storage: Arc<dyn Storage>, metrics: Arc<VaultSyncMetrics>) -> Self {
        Self { storage, metrics }
    }

    pub async fn get_state(&self, namespace: &str) -> Result<Value, VaultSyncError> {
        let sync_state = self.storage.read_sync_state(namespace).await?;
        let metrics = self.metrics.snapshot();
        Ok(serde_json::json!({
            "sync_state": sync_state,
            "metrics": metrics,
        }))
    }
}
