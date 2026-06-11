use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use vaultsync_coordinator_server::{
    build_router,
    config::ServerConfig,
    state::AppState,
    token_store::{MemoryTokenStore, RedisTokenStore, SqliteTokenStore, TokenStore},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse config arguments
    let config = ServerConfig::parse_args();
    tracing::info!(
        "Starting VaultSync Coordinator Server with config: {:?}",
        config
    );

    // Initialize the pluggable coordinator backend
    let coordinator: Arc<dyn vaultsync_core::coordinator::traits::Coordinator> = match config
        .backend
        .as_str()
    {
        "sqlite" => {
            tracing::info!(
                "Initializing SQLite coordinator backend at path: {}",
                config.db_path
            );
            Arc::new(
                vaultsync_coordinator_sqlite::coordinator::SQLiteCoordinator::new(&config.db_path),
            )
        }
        "redis" => {
            tracing::info!(
                "Initializing Redis coordinator backend at URL: {}",
                config.db_url
            );
            let redis_coord =
                vaultsync_coordinator_redis::coordinator::RedisCoordinator::new(&config.db_url)
                    .await
                    .map_err(|e| format!("Redis initialization error: {:?}", e))?;
            Arc::new(redis_coord)
        }
        "memory" | _ => {
            tracing::info!("Initializing In-Memory coordinator backend");
            Arc::new(vaultsync_coordinator_memory::coordinator::InMemoryCoordinator::new())
        }
    };

    // Initialize the token store for authentication if dynamic namespace auth is used (via admin_token)
    let token_store: Option<Arc<dyn TokenStore>> = if config.admin_token.is_some() {
        let store: Arc<dyn TokenStore> = match config.backend.as_str() {
            "sqlite" => {
                tracing::info!(
                    "Initializing SQLite token store at path: {}",
                    config.db_path
                );
                Arc::new(SqliteTokenStore::new(&config.db_path)?)
            }
            "redis" => {
                tracing::info!("Initializing Redis token store at URL: {}", config.db_url);
                Arc::new(RedisTokenStore::new(&config.db_url)?)
            }
            _ => {
                tracing::info!("Initializing In-Memory token store");
                Arc::new(MemoryTokenStore::new())
            }
        };
        store.initialize().await?;
        Some(store)
    } else {
        None
    };

    let state = AppState {
        coordinator,
        token_store,
        config: config.clone(),
        sessions: Default::default(),
    };

    // Build the Axum router
    let app = build_router(state);

    // Bind and start the server
    let addr = format!("{}:{}", config.host, config.port);
    let socket_addr: SocketAddr = addr.parse()?;
    tracing::info!("Server listening on {}", socket_addr);

    axum::Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
