pub mod config;
pub mod routes;
pub mod state;
pub mod token_store;
pub mod ws;
pub mod ws_mux;

use state::AppState;

pub fn build_router(state: AppState) -> axum::Router {
    // Spawn background auto-compaction task if configured
    if !state.config.auto_compact_ns.is_empty() && state.config.auto_compact_interval_minutes > 0 {
        let ns = state.config.auto_compact_ns.clone();
        let coord = state.coordinator.clone();
        let interval_minutes = state.config.auto_compact_interval_minutes;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(
                    interval_minutes * 60,
                ))
                .await;
                match coord.compact_oplog(&ns).await {
                    Ok(stats) => {
                        tracing::info!(
                            "[auto-compact] namespace={} removed={} snapshots={} docs={}",
                            ns,
                            stats.oplog_removed,
                            stats.snapshots_collapsed,
                            stats.docs_removed
                        );
                    }
                    Err(e) => {
                        tracing::warn!("[auto-compact] namespace={} failed: {:?}", ns, e);
                    }
                }
            }
        });
    }

    axum::Router::new()
        .route("/health", axum::routing::get(routes::health_check))
        .route(
            "/namespace/:ns/push",
            axum::routing::post(routes::push_mutations),
        )
        .route(
            "/namespace/:ns/pull",
            axum::routing::get(routes::pull_mutations),
        )
        .route(
            "/namespace/:ns/register",
            axum::routing::post(routes::register_replica),
        )
        .route(
            "/namespace/:ns/replicas",
            axum::routing::get(routes::list_replicas),
        )
        .route(
            "/namespace/:ns/replicas/:replica_id/key",
            axum::routing::put(routes::update_replica_key).get(routes::get_replica_key),
        )
        .route(
            "/namespace/:ns/heartbeat",
            axum::routing::post(routes::heartbeat),
        )
        .route(
            "/namespace/:ns/schema_version",
            axum::routing::get(routes::get_schema_version),
        )
        .route(
            "/namespace/:ns/snapshot/:doc_id/:record_id",
            axum::routing::get(routes::get_snapshot).post(routes::store_snapshot),
        )
        .route(
            "/namespace/:ns/snapshots",
            axum::routing::get(routes::list_snapshots),
        )
        .route(
            "/namespace/:ns/compact",
            axum::routing::post(routes::compact_oplog),
        )
        .route(
            "/namespace/:ns/events",
            axum::routing::get(routes::events_stream),
        )
        .route("/namespace/:ns/ws", axum::routing::get(ws::ws_handler))
        .route("/ws", axum::routing::get(ws_mux::ws_mux_handler))
        .route(
            "/admin/namespace/:ns",
            axum::routing::post(routes::admin_register_namespace),
        )
        .route(
            "/admin/namespace/:ns/compact",
            axum::routing::post(routes::admin_compact),
        )
        .route(
            "/admin/namespace/:ns/snapshot/:doc_id/:record_id",
            axum::routing::get(routes::admin_get_snapshot),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
}
