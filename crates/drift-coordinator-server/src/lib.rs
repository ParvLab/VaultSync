pub mod config;
pub mod routes;
pub mod state;
pub mod token_store;
pub mod ws;
pub mod ws_mux;

use state::AppState;

pub fn build_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/health", axum::routing::get(routes::health_check))
        .route("/namespace/:ns/push", axum::routing::post(routes::push_mutations))
        .route("/namespace/:ns/pull", axum::routing::get(routes::pull_mutations))
        .route("/namespace/:ns/register", axum::routing::post(routes::register_replica))
        .route("/namespace/:ns/replicas", axum::routing::get(routes::list_replicas))
        .route("/namespace/:ns/replicas/:replica_id/key", axum::routing::put(routes::update_replica_key).get(routes::get_replica_key))
        .route("/namespace/:ns/heartbeat", axum::routing::post(routes::heartbeat))
        .route("/namespace/:ns/schema_version", axum::routing::get(routes::get_schema_version))
        .route("/namespace/:ns/events", axum::routing::get(routes::events_stream))
        .route("/namespace/:ns/ws", axum::routing::get(ws::ws_handler))
        .route("/ws", axum::routing::get(ws_mux::ws_mux_handler))
        .route("/admin/namespace/:ns", axum::routing::post(routes::admin_register_namespace))
        .route("/admin/namespace/:ns/compact", axum::routing::post(routes::admin_compact))
        .route("/admin/namespace/:ns/snapshot/:doc_id/:record_id", axum::routing::get(routes::admin_get_snapshot))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
}
