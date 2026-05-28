use std::sync::Arc;
use tokio::net::TcpListener;
use futures::StreamExt;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};
use drift_core::coordinator::http::{HttpCoordinator, HttpCoordinatorConfig};
use drift_coordinator_server::{
    build_router,
    config::ServerConfig,
    state::AppState,
    token_store::{MemoryTokenStore, TokenStore},
};

async fn spawn_test_server(auth_token: Option<String>, admin_token: Option<String>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://127.0.0.1:{}", addr.port());

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: addr.port(),
        backend: "memory".to_string(),
        db_path: "".to_string(),
        db_url: "".to_string(),
        auth_token,
        admin_token,
    };

    let coordinator = Arc::new(drift_coordinator_memory::coordinator::InMemoryCoordinator::new());
    let token_store: Option<Arc<dyn TokenStore>> = if config.admin_token.is_some() {
        let store: Arc<dyn TokenStore> = Arc::new(MemoryTokenStore::new());
        store.initialize().await.unwrap();
        Some(store)
    } else {
        None
    };

    let state = AppState {
        coordinator,
        token_store,
        config,
        sessions: Default::default(),
    };

    let app = build_router(state);
    let std_listener = listener.into_std().unwrap();
    let handle = tokio::spawn(async move {
        axum::Server::from_tcp(std_listener)
            .unwrap()
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    (url, handle)
}

#[tokio::test]
async fn test_ws_push_pull_flow() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let coord = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    });
    let ns = "test-ws-ns";

    // 1. Subscribe to establish WebSocket connection
    let stream = coord.subscribe(ns, 0).await.unwrap();
    let mut stream = std::pin::Pin::from(stream);

    // 2. Register replica (routes via WS)
    coord.register(ns, ReplicaInfo {
        replica_id: "replica-ws-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![10, 20, 30],
        schema_version: 100,
    }).await.unwrap();

    // 3. Send heartbeat (routes via WS)
    coord.heartbeat(ns, "replica-ws-1").await.unwrap();

    // 4. Push a mutation over WS
    let mutations = vec![EncryptedMutation {
        id: "m-ws-1".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-ws-1".to_string(),
        doc_id: "doc-ws-1".to_string(),
        record_id: "rec-ws-1".to_string(),
        encrypted_blob: vec![200, 201, 202],
        timestamp: 100000,
        schema_version: 100,
    }];
    let seqs = coord.push(ns, mutations).await.unwrap();
    assert_eq!(seqs, vec![1]);

    // 5. Pull mutations over WS
    let pulled = coord.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].id, "m-ws-1");
    assert_eq!(pulled[0].sequence, 1);
    assert_eq!(pulled[0].encrypted_blob, vec![200, 201, 202]);

    // 6. Subscription should receive the mutation in real-time
    let mutation = stream.next().await;
    assert!(mutation.is_some());
    let mutation = mutation.unwrap();
    assert_eq!(mutation.id, "m-ws-1");
    assert_eq!(mutation.sequence, 1);
}

#[tokio::test]
async fn test_ws_token_auth_ok() {
    let token = "ws-secret-token".to_string();
    let (url, _handle) = spawn_test_server(Some(token.clone()), None).await;

    let coord = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: Some(token),
    });

    let res = coord.subscribe("ns", 0).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_ws_token_auth_fail() {
    let token = "ws-secret-token".to_string();
    let (url, _handle) = spawn_test_server(Some(token.clone()), None).await;

    let coord = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: Some("wrong-token".to_string()),
    });

    let res = coord.subscribe("ns", 0).await;
    // Should fail auth, falling back to SSE which will also fail auth, returning an error.
    assert!(res.is_err());
}
