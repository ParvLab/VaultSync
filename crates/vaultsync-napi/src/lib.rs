use napi_derive::napi;
use napi::JsFunction;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use vaultsync_core::VaultSyncClient;
use vaultsync_core::crdt::types::CrdtValue;

#[napi]
pub struct NapiVaultSyncClient {
    client: Arc<VaultSyncClient>,
    subscription_handles: Mutex<HashMap<String, vaultsync_core::subscription::engine::SubscriptionHandle>>,
}

#[napi]
impl NapiVaultSyncClient {
    #[napi]
    pub async fn insert(&self, doc_id: String, record_id: String, json: String) -> napi::Result<()> {
        let fields = json_to_fields(&json)?;
        self.client.insert(&doc_id, &record_id, fields).await
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;
        Ok(())
    }

    #[napi]
    pub async fn update(&self, doc_id: String, record_id: String, json: String) -> napi::Result<()> {
        let fields = json_to_fields(&json)?;
        self.client.update(&doc_id, &record_id, fields).await
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?;
        Ok(())
    }

    #[napi]
    pub async fn delete(&self, doc_id: String, record_id: String) -> napi::Result<()> {
        self.client.delete(&doc_id, &record_id).await
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;
        Ok(())
    }

    #[napi]
    pub async fn get(&self, doc_id: String, record_id: String) -> napi::Result<Option<String>> {
        let doc_opt = self.client.get(&doc_id, &record_id).await
            .map_err(|e| napi::Error::from_reason(format!("Get failed: {:?}", e)))?;
        
        match doc_opt {
            Some(fields) => {
                let mut map = serde_json::Map::new();
                for (k, v) in fields {
                    let json_val = match v {
                        CrdtValue::String(s) => serde_json::Value::String(s),
                        CrdtValue::Number(n) => serde_json::Value::Number(serde_json::Number::from_f64(n).unwrap_or_else(|| serde_json::Number::from(0))),
                        CrdtValue::Boolean(b) => serde_json::Value::Bool(b),
                        CrdtValue::Null => serde_json::Value::Null,
                        _ => serde_json::Value::Null,
                    };
                    map.insert(k, json_val);
                }
                let json_str = serde_json::to_string(&serde_json::Value::Object(map))
                    .map_err(|e| napi::Error::from_reason(format!("Serialize failed: {:?}", e)))?;
                Ok(Some(json_str))
            }
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn find(&self, doc_id: String) -> napi::Result<Vec<String>> {
        let records = self.client.find(&doc_id, None).await
            .map_err(|e| napi::Error::from_reason(format!("Find failed: {:?}", e)))?;
        
        let mut results = Vec::new();
        for fields in records {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                let json_val = match v {
                    CrdtValue::String(s) => serde_json::Value::String(s),
                    CrdtValue::Number(n) => serde_json::Value::Number(serde_json::Number::from_f64(n).unwrap_or_else(|| serde_json::Number::from(0))),
                    CrdtValue::Boolean(b) => serde_json::Value::Bool(b),
                    CrdtValue::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                map.insert(k, json_val);
            }
            let json_str = serde_json::to_string(&serde_json::Value::Object(map))
                .map_err(|e| napi::Error::from_reason(format!("Serialize failed: {:?}", e)))?;
            results.push(json_str);
        }
        Ok(results)
    }

    #[napi]
    pub fn subscribe(&self, doc_id: String, callback: JsFunction) -> napi::Result<String> {
        let ts_fn: ThreadsafeFunction<(String, String)> = callback
            .create_threadsafe_function(0, |ctx: napi::threadsafe_function::ThreadSafeCallContext<(String, String)>| {
                let record_id = ctx.env.create_string(&ctx.value.0)?;
                let json_str = ctx.env.create_string(&ctx.value.1)?;
                Ok(vec![record_id, json_str])
            })?;
        
        let handle = self.client.subscribe(&doc_id, Box::new(move |_doc_id, record_id, fields| {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                let json_val = match v {
                    CrdtValue::String(s) => serde_json::Value::String(s.clone()),
                    CrdtValue::Number(n) => serde_json::Value::Number(serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0))),
                    CrdtValue::Boolean(b) => serde_json::Value::Bool(*b),
                    CrdtValue::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                map.insert(k.clone(), json_val);
            }
            if let Ok(json_str) = serde_json::to_string(&serde_json::Value::Object(map)) {
                let _ = ts_fn.call(
                    Ok((record_id.to_string(), json_str)),
                    ThreadsafeFunctionCallMode::Blocking,
                );
            }
        }));

        let sub_id = uuid::Uuid::new_v4().to_string();
        self.subscription_handles.lock().unwrap().insert(sub_id.clone(), handle);
        Ok(sub_id)
    }

    #[napi]
    pub fn unsubscribe(&self, sub_id: String) -> napi::Result<()> {
        if let Some(handle) = self.subscription_handles.lock().unwrap().remove(&sub_id) {
            self.client.unsubscribe(handle)
                .map_err(|e| napi::Error::from_reason(format!("Unsubscribe failed: {:?}", e)))?;
        }
        Ok(())
    }

    #[napi]
    pub async fn sync_status(&self) -> napi::Result<String> {
        let state = self.client.sync_status().await
            .map_err(|e| napi::Error::from_reason(format!("Sync status failed: {:?}", e)))?;
        let pending = self.client.pending_uploads().await
            .map_err(|e| napi::Error::from_reason(format!("Pending uploads failed: {:?}", e)))?;

        let connected = state.connection_status != vaultsync_core::sync::state::ConnectionStatus::Disconnected;

        let mut map = serde_json::Map::new();
        map.insert("connected".to_string(), serde_json::Value::Bool(connected));
        map.insert("pendingMutations".to_string(), serde_json::Value::Number(serde_json::Number::from(pending)));
        map.insert("lastSyncedSequence".to_string(), serde_json::Value::Number(serde_json::Number::from(state.last_synced_sequence)));

        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| napi::Error::from_reason(format!("Serialize failed: {:?}", e)))?;
        Ok(json_str)
    }

    #[napi]
    pub async fn shutdown(&self) -> napi::Result<()> {
        self.client.shutdown().await
            .map_err(|e| napi::Error::from_reason(format!("Shutdown failed: {:?}", e)))?;
        Ok(())
    }

    #[napi]
    pub fn is_leader(&self) -> bool {
        self.client.leader_election.is_leader()
    }

    #[napi]
    pub async fn rotate_keys(&self) -> napi::Result<i64> {
        self.client.rotate_keys().await
            .map_err(|e| napi::Error::from_reason(format!("Rotate keys failed: {:?}", e)))?;
        let active = self.client.active_key_version();
        Ok(active as i64)
    }

    #[napi]
    pub fn active_key_version(&self) -> i64 {
        self.client.active_key_version() as i64
    }

    #[napi]
    pub fn list_key_versions(&self) -> napi::Result<String> {
        let keys = self.client.list_key_versions();
        let active = self.client.active_key_version();
        let mut list = Vec::new();
        for key in keys {
            list.push(serde_json::json!({
                "version": key.version,
                "createdAt": key.created_at * 1000,
                "isActive": key.version == active,
            }));
        }
        let json_str = serde_json::to_string(&list)
            .map_err(|e| napi::Error::from_reason(format!("Serialize failed: {:?}", e)))?;
        Ok(json_str)
    }

    #[napi]
    pub fn prune_key_versions(&self, keep_versions: i32) -> napi::Result<()> {
        self.client.prune_key_versions(keep_versions as u64);
        Ok(())
    }
}

#[napi]
pub async fn create_client(
    namespace: String,
    replica_id: String,
    storage_path: Option<String>,
    coordinator_endpoint: Option<String>,
    auth_token: Option<String>,
) -> napi::Result<NapiVaultSyncClient> {
    let mut config = vaultsync_core::VaultSyncConfig::default();
    config.namespace = namespace;
    config.replica_id = replica_id;
    if let Some(endpoint) = coordinator_endpoint {
        config.coordinator_endpoint = endpoint;
    }
    config.auth_token = auth_token;
    if let Some(path) = storage_path {
        config.storage = vaultsync_core::storage::traits::StorageConfig::Sqlite { path };
    } else {
        config.storage = vaultsync_core::storage::traits::StorageConfig::InMemory;
    }

    let client = VaultSyncClient::connect(config).await
        .map_err(|e| napi::Error::from_reason(format!("Failed to connect client: {:?}", e)))?;

    Ok(NapiVaultSyncClient {
        client: Arc::new(client),
        subscription_handles: Mutex::new(HashMap::new()),
    })
}

fn json_to_fields(json: &str) -> napi::Result<HashMap<String, CrdtValue>> {
    let val: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| napi::Error::from_reason(format!("invalid json: {e:?}")))?;
    
    let map = match val {
        serde_json::Value::Object(obj) => obj,
        _ => return Err(napi::Error::from_reason("expected json object")),
    };

    let mut fields = HashMap::new();
    for (k, v) in map {
        let crdt_val = match v {
            serde_json::Value::String(s) => CrdtValue::String(s),
            serde_json::Value::Number(n) => CrdtValue::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Bool(b) => CrdtValue::Boolean(b),
            _ => CrdtValue::Null,
        };
        fields.insert(k, crdt_val);
    }
    Ok(fields)
}
