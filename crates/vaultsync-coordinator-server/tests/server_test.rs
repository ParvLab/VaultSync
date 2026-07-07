use futures::StreamExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use vaultsync_coordinator_server::{
    build_router,
    config::ServerConfig,
    state::AppState,
    token_store::{MemoryTokenStore, TokenStore},
};
use vaultsync_core::coordinator::http::{HttpCoordinator, HttpCoordinatorConfig};
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};

async fn spawn_test_server(
    auth_token: Option<String>,
    admin_token: Option<String>,
) -> (String, tokio::task::JoinHandle<()>) {
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
        auto_compact_ns: String::new(),
        auto_compact_interval_minutes: 0,
    };

    let coordinator =
        Arc::new(vaultsync_coordinator_memory::coordinator::InMemoryCoordinator::new());
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
        generation_id: "test".to_string(),
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
async fn test_health() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let client = reqwest::Client::new();
    let resp = client.get(&format!("{}/health", url)).send().await.unwrap();
    assert!(resp.status().is_success());
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn test_push_pull_flow() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let coord = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    });
    let ns = "test-ns";

    // Register a replica
    coord
        .register(
            ns,
            ReplicaInfo {
                replica_id: "replica-1".to_string(),
                namespace: ns.to_string(),
                public_key: vec![1, 2, 3],
                schema_version: 42,
            },
            0,
        )
        .await
        .unwrap();

    // Verify schema version is 42
    let schema_ver = coord.schema_version(ns).await.unwrap();
    assert_eq!(schema_ver, 42);

    // Heartbeat
    coord.heartbeat(ns, "replica-1").await.unwrap();

    // Push a mutation
    let mutations = vec![EncryptedMutation {
        id: "m-1".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![100, 101, 102],
        timestamp: 99999,
        schema_version: 42,
        key_version: 1,
    }];
    let seqs = coord.push(ns, mutations).await.unwrap();
    assert_eq!(seqs, vec![1]);

    // Pull mutations back
    let pulled = coord.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].id, "m-1");
    assert_eq!(pulled[0].sequence, 1);
    assert_eq!(pulled[0].encrypted_blob, vec![100, 101, 102]);
}

#[tokio::test]
async fn test_global_token_auth() {
    let token = "my-secret-global-token".to_string();
    let (url, _handle) = spawn_test_server(Some(token.clone()), None).await;

    // 1. Client with no token should fail
    let coord_unauthorized = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    });
    let res = coord_unauthorized.schema_version("ns").await;
    assert!(res.is_err());

    // 2. Client with correct token should succeed
    let coord_authorized = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: Some(token),
    });
    let res = coord_authorized.schema_version("ns").await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), 0);
}

#[tokio::test]
async fn test_admin_and_namespace_auth() {
    let admin_token = "admin-secret-key".to_string();
    let (url, _handle) = spawn_test_server(None, Some(admin_token.clone())).await;

    let client = reqwest::Client::new();
    let ns = "ns-erp";

    // 1. Register namespace ns-erp via admin API without token should fail (401)
    let resp = client
        .post(&format!("{}/admin/namespace/{}", url, ns))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 2. Register namespace ns-erp via admin API with correct token should succeed and return namespace token
    let resp = client
        .post(&format!("{}/admin/namespace/{}", url, ns))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let reg_res: serde_json::Value = resp.json().await.unwrap();
    let ns_token = reg_res["token"].as_str().unwrap().to_string();
    assert!(!ns_token.is_empty());

    // 3. Client using ns-erp without token should fail
    let coord_no_token = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    });
    let res = coord_no_token.schema_version(ns).await;
    assert!(res.is_err());

    // 4. Client using ns-erp with wrong token should fail
    let coord_wrong_token = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: Some("wrong-token".to_string()),
    });
    let res = coord_wrong_token.schema_version(ns).await;
    assert!(res.is_err());

    // 5. Client using ns-erp with correct token should succeed
    let coord_correct = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: Some(ns_token),
    });
    let res = coord_correct.schema_version(ns).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), 0);
}

#[tokio::test]
async fn test_sse_events() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let coord = HttpCoordinator::new(HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    });
    let ns = "test-sse-ns";

    // Subscribe in a background task
    let stream = coord.subscribe(ns, 0).await.unwrap();
    let mut stream = std::pin::Pin::from(stream);

    // Push a mutation
    coord
        .push(
            ns,
            vec![EncryptedMutation {
                id: "m-sse-1".to_string(),
                namespace: ns.to_string(),
                replica_id: "replica-sse".to_string(),
                doc_id: "doc-sse".to_string(),
                record_id: "rec-sse".to_string(),
                encrypted_blob: vec![7, 7, 7],
                timestamp: 12345,
                schema_version: 1,
                key_version: 1,
            }],
        )
        .await
        .unwrap();

    // Read from the subscription stream
    let mutation = stream.next().await;
    assert!(mutation.is_some());
    let mutation = mutation.unwrap();
    assert_eq!(mutation.id, "m-sse-1");
    assert_eq!(mutation.sequence, 1);
    assert_eq!(mutation.encrypted_blob, vec![7, 7, 7]);
}
