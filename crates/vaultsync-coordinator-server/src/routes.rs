use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    Json,
};
use futures::StreamExt;
use std::convert::Infallible;
use vaultsync_core::coordinator::traits::{
    EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId,
};

// Helper to validate namespace token auth
async fn authorize_namespace(
    state: &AppState,
    headers: &HeaderMap,
    namespace: &str,
) -> Result<(), StatusCode> {
    // 1. Check global static token if configured
    if let Some(ref global_token) = state.config.auth_token {
        if let Some(auth_header) = headers.get(axum::http::header::AUTHORIZATION) {
            if let Ok(auth_str) = auth_header.to_str() {
                if auth_str == format!("Bearer {}", global_token) {
                    return Ok(());
                }
            }
        }
        return Err(StatusCode::UNAUTHORIZED);
    }

    // 2. Check per-namespace token store if configured
    if let Some(ref token_store) = state.token_store {
        // If an admin token is configured, namespaces MUST be pre-registered and have a token.
        // Therefore, we always require a valid token.
        if state.config.admin_token.is_some() {
            if let Some(auth_header) = headers.get(axum::http::header::AUTHORIZATION) {
                if let Ok(auth_str) = auth_header.to_str() {
                    if auth_str.starts_with("Bearer ") {
                        let token = &auth_str["Bearer ".len()..];
                        if let Ok(true) = token_store.validate_token(namespace, token).await {
                            return Ok(());
                        }
                    }
                }
            }
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    Ok(())
}

// ----------------------------------------------------
// Namespace HTTP API Endpoints
// ----------------------------------------------------

pub async fn push_mutations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(mutations): Json<Vec<EncryptedMutation>>,
) -> Result<Json<Vec<SequenceId>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let seq_ids = state.coordinator.push(&ns, mutations).await.map_err(|e| {
        tracing::error!("Failed to push mutations: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(seq_ids))
}

#[derive(serde::Deserialize)]
pub struct PullQuery {
    pub after: SequenceId,
    pub limit: usize,
}

pub async fn pull_mutations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(query): Query<PullQuery>,
) -> Result<Json<Vec<PendingMutation>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let mutations = state
        .coordinator
        .pull(&ns, query.after, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to pull mutations: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(mutations))
}

pub async fn register_replica(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(info): Json<ReplicaInfo>,
) -> Result<StatusCode, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    state.coordinator.register(&ns, info, 0).await.map_err(|e| {
        tracing::error!("Failed to register replica: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}

pub async fn list_replicas(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Vec<ReplicaInfo>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let replicas = state.coordinator.list_replicas(&ns).await.map_err(|e| {
        tracing::error!("Failed to list replicas: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(replicas))
}

#[derive(serde::Deserialize)]
pub struct HeartbeatQuery {
    pub replica_id: String,
}

pub async fn heartbeat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(query): Query<HeartbeatQuery>,
) -> Result<StatusCode, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    state
        .coordinator
        .heartbeat(&ns, &query.replica_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to send heartbeat: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

pub async fn get_schema_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<u64>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let version = state.coordinator.schema_version(&ns).await.map_err(|e| {
        tracing::error!("Failed to get schema version: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(version))
}

#[derive(serde::Deserialize)]
pub struct EventsQuery {
    pub after: SequenceId,
}

pub async fn events_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let stream = state
        .coordinator
        .subscribe(&ns, query.after)
        .await
        .map_err(|e| {
            tracing::error!("Failed to subscribe to events: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let pinned_stream = std::pin::Pin::from(stream);

    let sse_stream = pinned_stream.map(|mutation| {
        let json = serde_json::to_string(&mutation).unwrap();
        Ok(Event::default().data(json))
    });

    Ok(Sse::new(sse_stream).keep_alive(axum::response::sse::KeepAlive::default()))
}

// ----------------------------------------------------
// Admin API Endpoints
// ----------------------------------------------------

#[derive(serde::Deserialize)]
pub struct AdminRegisterPayload {
    pub token: Option<String>,
}

#[derive(serde::Serialize)]
pub struct AdminRegisterResponse {
    pub namespace: String,
    pub token: String,
}

pub async fn admin_register_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    payload: Option<Json<AdminRegisterPayload>>,
) -> Result<Json<AdminRegisterResponse>, StatusCode> {
    // Verify admin token
    let admin_token = state
        .config
        .admin_token
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    if auth_str != format!("Bearer {}", admin_token) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token_store = state
        .token_store
        .as_ref()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let token = match payload {
        Some(Json(p)) => p.token.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        None => uuid::Uuid::new_v4().to_string(),
    };

    token_store
        .set_token(&ns, &token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AdminRegisterResponse {
        namespace: ns,
        token,
    }))
}

pub async fn admin_compact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<vaultsync_core::sync::compaction::CompactionStats>, StatusCode> {
    // Verify admin token
    let admin_token = state
        .config
        .admin_token
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    if auth_str != format!("Bearer {}", admin_token) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let stats = state.coordinator.compact_oplog(&ns).await.map_err(|e| {
        tracing::error!("Failed to compact oplog: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(stats))
}

pub async fn admin_get_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((ns, doc_id, record_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, StatusCode> {
    // Verify admin token
    let admin_token = state
        .config
        .admin_token
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    if auth_str != format!("Bearer {}", admin_token) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let snapshot = state
        .coordinator
        .get_snapshot(&ns, &doc_id, &record_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get snapshot: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let bytes = snapshot
        .encode()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );

    Ok((headers, bytes))
}

// ----------------------------------------------------
// Replica Key Management Endpoints
// ----------------------------------------------------

#[derive(serde::Deserialize, serde::Serialize)]
pub struct UpdateReplicaKeyPayload {
    pub public_key: Vec<u8>,
    pub key_version: u64,
}

pub async fn update_replica_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((ns, replica_id)): Path<(String, String)>,
    Json(payload): Json<UpdateReplicaKeyPayload>,
) -> Result<StatusCode, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    state
        .coordinator
        .update_replica_key(&ns, &replica_id, payload.public_key, payload.key_version)
        .await
        .map_err(|e| {
            tracing::error!("Failed to update replica key: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

pub async fn get_replica_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((ns, replica_id)): Path<(String, String)>,
) -> Result<Json<Option<UpdateReplicaKeyPayload>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;

    let result = state
        .coordinator
        .get_replica_key(&ns, &replica_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get replica key: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(result.map(|(public_key, key_version)| {
        UpdateReplicaKeyPayload {
            public_key,
            key_version,
        }
    })))
}

pub async fn get_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((ns, doc_id, record_id)): Path<(String, String, String)>,
) -> Result<Json<Option<vaultsync_core::crdt::snapshot::Snapshot>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;
    let snapshot = state
        .coordinator
        .get_snapshot(&ns, &doc_id, &record_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(snapshot))
}

pub async fn store_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((ns, _doc_id, _record_id)): Path<(String, String, String)>,
    Json(snapshot): Json<vaultsync_core::crdt::snapshot::Snapshot>,
) -> Result<StatusCode, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;
    state
        .coordinator
        .store_snapshot(&ns, &snapshot)
        .await
        .map_err(|e| {
            tracing::error!("store_snapshot failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::OK)
}

pub async fn list_snapshots(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Vec<vaultsync_core::crdt::snapshot::Snapshot>>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;
    let snapshots = state
        .coordinator
        .list_snapshots(&ns)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(snapshots))
}

pub async fn compact_oplog(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<vaultsync_core::sync::compaction::CompactionStats>, StatusCode> {
    authorize_namespace(&state, &headers, &ns).await?;
    let stats = state
        .coordinator
        .compact_oplog(&ns)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(stats))
}

// ----------------------------------------------------
// Health Check Endpoint
// ----------------------------------------------------

pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}
