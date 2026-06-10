use std::sync::Arc;
use tokio::net::TcpListener;
use futures::StreamExt;
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};
use vaultsync_core::coordinator::http::{HttpCoordinator, HttpCoordinatorConfig};
use vaultsync_core::coordinator::mux_coordinator::MuxCoordinator;
use vaultsync_coordinator_server::{
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

    let coordinator = Arc::new(vaultsync_coordinator_memory::coordinator::InMemoryCoordinator::new());
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
        key_version: 1,
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

#[tokio::test]
async fn test_mux_connect_single_namespace() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let config = HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    };
    let mux = Arc::new(MuxCoordinator::new(config));
    let ns = "test-mux-ns-1";
    let coord = mux.join_namespace(ns.to_string());

    // 1. Subscribe to establish WebSocket and register namespace
    let stream = coord.subscribe(ns, 0).await.unwrap();
    let mut stream = std::pin::Pin::from(stream);

    // 2. Register replica info
    coord.register(ns, ReplicaInfo {
        replica_id: "replica-mux-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![10, 20, 30],
        schema_version: 1,
    }).await.unwrap();

    // 3. Heartbeat
    coord.heartbeat(ns, "replica-mux-1").await.unwrap();

    // 4. Push a mutation over Mux WS
    let mutations = vec![EncryptedMutation {
        id: "m-mux-1".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-mux-1".to_string(),
        doc_id: "doc-mux-1".to_string(),
        record_id: "rec-mux-1".to_string(),
        encrypted_blob: vec![200, 201, 202],
        timestamp: 100000,
        schema_version: 1,
        key_version: 1,
    }];
    let seqs = coord.push(ns, mutations).await.unwrap();
    assert_eq!(seqs, vec![1]);

    // 5. Pull mutations over Mux WS
    let pulled = coord.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].id, "m-mux-1");
    assert_eq!(pulled[0].sequence, 1);
    assert_eq!(pulled[0].encrypted_blob, vec![200, 201, 202]);

    // 6. Subscription should receive the mutation in real-time
    let mutation = stream.next().await;
    assert!(mutation.is_some());
    let mutation = mutation.unwrap();
    assert_eq!(mutation.id, "m-mux-1");
    assert_eq!(mutation.sequence, 1);
}

#[tokio::test]
async fn test_mux_two_namespaces_isolated() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let config = HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    };
    let mux = Arc::new(MuxCoordinator::new(config));

    let ns1 = "test-mux-ns-1";
    let ns2 = "test-mux-ns-2";

    let coord1 = mux.join_namespace(ns1.to_string());
    let coord2 = mux.join_namespace(ns2.to_string());

    let stream1 = coord1.subscribe(ns1, 0).await.unwrap();
    let mut stream1 = std::pin::Pin::from(stream1);

    let stream2 = coord2.subscribe(ns2, 0).await.unwrap();
    let mut stream2 = std::pin::Pin::from(stream2);

    // Register both replicas
    coord1.register(ns1, ReplicaInfo {
        replica_id: "replica-mux-1".to_string(),
        namespace: ns1.to_string(),
        public_key: vec![1],
        schema_version: 1,
    }).await.unwrap();

    coord2.register(ns2, ReplicaInfo {
        replica_id: "replica-mux-2".to_string(),
        namespace: ns2.to_string(),
        public_key: vec![2],
        schema_version: 1,
    }).await.unwrap();

    // Push to ns1
    let mutations1 = vec![EncryptedMutation {
        id: "m-ns1-1".to_string(),
        namespace: ns1.to_string(),
        replica_id: "replica-mux-1".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![100],
        timestamp: 100000,
        schema_version: 1,
        key_version: 1,
    }];
    coord1.push(ns1, mutations1).await.unwrap();

    // Push to ns2
    let mutations2 = vec![EncryptedMutation {
        id: "m-ns2-1".to_string(),
        namespace: ns2.to_string(),
        replica_id: "replica-mux-2".to_string(),
        doc_id: "doc-2".to_string(),
        record_id: "rec-2".to_string(),
        encrypted_blob: vec![200],
        timestamp: 100000,
        schema_version: 1,
        key_version: 1,
    }];
    coord2.push(ns2, mutations2).await.unwrap();

    // Verify stream1 gets only ns1 mutation
    let mutation = stream1.next().await;
    assert!(mutation.is_some());
    let mutation = mutation.unwrap();
    assert_eq!(mutation.id, "m-ns1-1");
    assert_eq!(mutation.namespace, ns1);

    // Verify stream2 gets only ns2 mutation
    let mutation = stream2.next().await;
    assert!(mutation.is_some());
    let mutation = mutation.unwrap();
    assert_eq!(mutation.id, "m-ns2-1");
    assert_eq!(mutation.namespace, ns2);
}

#[tokio::test]
async fn test_mux_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let url = format!("http://127.0.0.1:{}", port);
    
    let run_server = |std_listener: std::net::TcpListener| {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            backend: "memory".to_string(),
            db_path: "".to_string(),
            db_url: "".to_string(),
            auth_token: None,
            admin_token: None,
        };
        let coordinator = Arc::new(vaultsync_coordinator_memory::coordinator::InMemoryCoordinator::new());
        let state = AppState {
            coordinator,
            token_store: None,
            config,
            sessions: Default::default(),
        };
        let app = build_router(state);
        tokio::spawn(async move {
            axum::Server::from_tcp(std_listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap();
        })
    };

    let std_listener = listener.into_std().unwrap();
    let server_handle = run_server(std_listener);

    let config = HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    };
    let mux = Arc::new(MuxCoordinator::new(config));
    let ns = "test-mux-reconnect";
    let coord = mux.join_namespace(ns.to_string());

    // Subscribe
    let stream = coord.subscribe(ns, 0).await.unwrap();
    let mut stream = std::pin::Pin::from(stream);

    // Register
    coord.register(ns, ReplicaInfo {
        replica_id: "replica-rec".to_string(),
        namespace: ns.to_string(),
        public_key: vec![5],
        schema_version: 1,
    }).await.unwrap();

    // Push to make sure it works
    coord.push(ns, vec![EncryptedMutation {
        id: "m-before".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-rec".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![1],
        timestamp: 100000,
        schema_version: 1,
        key_version: 1,
    }]).await.unwrap();

    let mut_before = stream.next().await.unwrap();
    assert_eq!(mut_before.id, "m-before");

    // Kill the server
    server_handle.abort();
    let _ = server_handle.await;

    // Sleep a bit to allow socket disconnection to register
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Start a new server on the same port
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
    let std_listener = listener.into_std().unwrap();
    let _new_server_handle = run_server(std_listener);

    // The client should automatically reconnect. Let's wait a bit for reconnection to complete.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;

    // Push a new mutation on the reconnected socket
    coord.push(ns, vec![EncryptedMutation {
        id: "m-after".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-rec".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![2],
        timestamp: 100001,
        schema_version: 1,
        key_version: 1,
    }]).await.unwrap();

    // Check if the stream receives it
    let mut_after = stream.next().await.unwrap();
    assert_eq!(mut_after.id, "m-after");
}

#[tokio::test]
async fn test_mux_namespace_drop() {
    let (url, _handle) = spawn_test_server(None, None).await;
    let config = HttpCoordinatorConfig {
        url: url.clone(),
        auth_token: None,
    };
    let mux = Arc::new(MuxCoordinator::new(config));
    let ns = "test-mux-drop";
    let coord = mux.join_namespace(ns.to_string());

    let stream = coord.subscribe(ns, 0).await.unwrap();
    let mut stream = std::pin::Pin::from(stream);

    coord.register(ns, ReplicaInfo {
        replica_id: "replica-drop".to_string(),
        namespace: ns.to_string(),
        public_key: vec![9],
        schema_version: 1,
    }).await.unwrap();

    // Leave the namespace
    coord.leave().await.unwrap();

    // Push a mutation
    coord.push(ns, vec![EncryptedMutation {
        id: "m-after-drop".to_string(),
        namespace: ns.to_string(),
        replica_id: "replica-drop".to_string(),
        doc_id: "doc-1".to_string(),
        record_id: "rec-1".to_string(),
        encrypted_blob: vec![3],
        timestamp: 100002,
        schema_version: 1,
        key_version: 1,
    }]).await.unwrap();

    // The stream should be disconnected (returns None) or timeout without receiving any new mutations.
    // Wait, when we call `leave()`, the server cancels the subscription and the client removes the tx channel.
    // So the stream should return None.
    let result = tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await;
    // It should either return None (stream finished) or timeout (received nothing).
    match result {
        Ok(opt) => assert!(opt.is_none()),
        Err(_) => {} // Timeout is also acceptable since the stream did not receive any mutation
    }
}

