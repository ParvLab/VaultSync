use std::sync::Arc;
use drift_conformance::run_full_conformance_suite;
use drift_coordinator_memory::coordinator::InMemoryCoordinator;
use drift_coordinator_sqlite::coordinator::SQLiteCoordinator;
use drift_coordinator_http::coordinator::{HttpCoordinator, HttpCoordinatorConfig};
use drift_coordinator_redis::coordinator::RedisCoordinator;
use drift_coordinator_postgres::coordinator::PostgresCoordinator;

#[tokio::test]
async fn test_memory_conformance() {
    let coord = Arc::new(InMemoryCoordinator::new());
    run_full_conformance_suite(coord, "memory").await;
}

#[tokio::test]
async fn test_sqlite_conformance() {
    let coord = Arc::new(SQLiteCoordinator::new(":memory:"));
    run_full_conformance_suite(coord, "sqlite").await;
}

#[tokio::test]
async fn test_http_conformance() {
    // Spin up real drift-coordinator-server
    use drift_coordinator_server::{
        config::ServerConfig,
        state::AppState,
        build_router,
    };
    use std::net::SocketAddr;

    let coordinator = Arc::new(InMemoryCoordinator::new());
    let state = AppState {
        coordinator,
        token_store: None,
        config: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            backend: "memory".to_string(),
            db_path: "".to_string(),
            db_url: "".to_string(),
            auth_token: None,
            admin_token: None,
        },
        sessions: Default::default(),
    };

    let app = build_router(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    let local_addr = server.local_addr();

    tokio::spawn(async move {
        let _ = server.await;
    });

    let url = format!("http://{}", local_addr);
    let coord = Arc::new(HttpCoordinator::new(HttpCoordinatorConfig {
        url,
        auth_token: None,
    }));

    run_full_conformance_suite(coord, "http").await;
}

#[tokio::test]
async fn test_redis_conformance() {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    // Attempt connection, skip if offline
    match RedisCoordinator::new(&redis_url).await {
        Ok(coord) => {
            let coord = Arc::new(coord);
            run_full_conformance_suite(coord, "redis").await;
        }
        Err(e) => {
            println!("Skipping Redis conformance tests: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_postgres_conformance() {
    let db_url = std::env::var("DATABASE_URL");
    match db_url {
        Ok(url) => {
            // Attempt to connect and initialize database for the test
            match PostgresCoordinator::new(&url).await {
                Ok(coord) => {
                    let coord = Arc::new(coord);
                    run_full_conformance_suite(coord, "postgres").await;
                }
                Err(e) => {
                    println!("Skipping Postgres conformance tests (connection failed): {:?}", e);
                }
            }
        }
        Err(_) => {
            println!("Skipping Postgres conformance tests (DATABASE_URL not set)");
        }
    }
}
