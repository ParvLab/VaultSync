#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use drift_core::storage::traits::StorageConfig;
use drift_coordinator_redis::coordinator::RedisCoordinator;

#[tokio::test]
#[ignore]
async fn test_chaos_network_toxiproxy() {
    let client = reqwest::Client::new();
    
    // Create a proxy for Redis on Toxiproxy
    let create_proxy_res = client.post("http://localhost:8474/proxies")
        .json(&serde_json::json!({
            "name": "redis_proxy",
            "listen": "localhost:8475",
            "upstream": "localhost:6379",
            "enabled": true
        }))
        .send()
        .await;

    if create_proxy_res.is_err() {
        println!("Skipping toxiproxy test because Toxiproxy is not running at localhost:8474.");
        return;
    }

    // Connect client to Redis via Toxiproxy proxy
    let coord = Arc::new(RedisCoordinator::new("redis://localhost:8475").await.unwrap());
    
    let mut config_alice = DriftConfig::default();
    config_alice.namespace = "chaos-toxiproxy-ns".to_string();
    config_alice.replica_id = "alice".to_string();
    config_alice.storage = StorageConfig::InMemory;
    config_alice.sync_interval = Duration::from_millis(50);
    
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let client_alice = DriftClient::new_with_keyring(config_alice, coord.clone(), keyring.clone()).await.unwrap();

    // Inject latency toxic
    let latency_res = client.post("http://localhost:8474/proxies/redis_proxy/toxics")
        .json(&serde_json::json!({
            "name": "latency_toxic",
            "type": "latency",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": {
                "latency": 100,
                "jitter": 20
            }
        }))
        .send()
        .await;
    assert!(latency_res.is_ok());

    // Do write with latency
    let mut fields = HashMap::new();
    fields.insert("latency_key".to_string(), CrdtValue::String("val".to_string()));
    client_alice.insert("doc-1", "rec-1", fields).await.unwrap();
    client_alice.force_sync().await.unwrap();

    // Disable the proxy (simulate complete partition)
    let disable_res = client.post("http://localhost:8474/proxies/redis_proxy")
        .json(&serde_json::json!({
            "enabled": false
        }))
        .send()
        .await;
    assert!(disable_res.is_ok());

    // Write offline
    let mut fields_off = HashMap::new();
    fields_off.insert("offline_key".to_string(), CrdtValue::String("written_offline".to_string()));
    client_alice.update("doc-1", "rec-1", fields_off).await.unwrap();

    // Force sync should time out or return early
    let _ = client_alice.force_sync().await;

    // Enable the proxy back (heal partition)
    let enable_res = client.post("http://localhost:8474/proxies/redis_proxy")
        .json(&serde_json::json!({
            "enabled": true
        }))
        .send()
        .await;
    assert!(enable_res.is_ok());

    // Remove latency toxic
    let _ = client.delete("http://localhost:8474/proxies/redis_proxy/toxics/latency_toxic").send().await;

    // Force sync now should succeed
    let _ = client_alice.force_sync().await;

    // Clean up
    let _ = client.delete("http://localhost:8474/proxies/redis_proxy").send().await;
    client_alice.shutdown().await.unwrap();
}
