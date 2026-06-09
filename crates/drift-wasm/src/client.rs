use wasm_bindgen::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use crate::storage::BrowserStorage;
use drift_core::coordinator::memory::InMemoryCoordinator;
use drift_core::e2ee::keyring::KeyRing;

use wasm_bindgen::JsCast;

#[wasm_bindgen]
pub struct WasmDriftClient {
    client: Arc<DriftClient>,
}

#[wasm_bindgen]
impl WasmDriftClient {
    pub async fn new(namespace: &str, replica_id: &str) -> Result<WasmDriftClient, JsValue> {
        let mut config = DriftConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = replica_id.to_string();
        config.sync_interval = std::time::Duration::from_millis(200);
        config.retry.initial_delay = std::time::Duration::from_millis(50);
        
        let db_name = format!("{}_db", namespace);
        let storage = Arc::new(BrowserStorage::new(&db_name).await
            .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?);

        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());

        let client = Arc::new(DriftClient::new_with_storage(
            config,
            coordinator,
            keyring,
            storage,
        )
        .await
        .map_err(|e| JsValue::from_str(&format!("Client failed: {:?}", e)))?);

        // Setup BroadcastChannel for cross-tab sync
        let channel_name = format!("drift-ipc-{}", namespace);
        let channel = web_sys::BroadcastChannel::new(&channel_name)
            .map_err(|e| JsValue::from_str(&format!("Failed to create BroadcastChannel: {:?}", e)))?;

        let client_clone = client.clone();
        let onmessage = wasm_bindgen::closure::Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
            if let Some(msg_str) = e.data().as_string() {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&msg_str) {
                    if let (Some(doc_id), Some(record_id)) = (
                        val.get("doc_id").and_then(|v| v.as_str()),
                        val.get("record_id").and_then(|v| v.as_str())
                    ) {
                        let doc_id = doc_id.to_string();
                        let record_id = record_id.to_string();
                        let client = client_clone.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            if let Ok(Some(state)) = client.get(&doc_id, &record_id).await {
                                client.fire_local_subscription(&doc_id, &record_id, &state);
                            }
                        });
                    }
                }
            }
        }) as Box<dyn FnMut(web_sys::MessageEvent)>);

        channel.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        let channel_send = channel.clone();
        client.set_change_listener(Arc::new(move |doc_id, record_id| {
            let msg = serde_json::json!({
                "doc_id": doc_id,
                "record_id": record_id,
            }).to_string();
            let _ = channel_send.post_message(&JsValue::from_str(&msg));
        }));

        Ok(Self { client })
    }

    pub async fn new_with_coordinator(
        namespace: &str,
        replica_id: &str,
        coordinator_url: &str,
        auth_token: Option<String>,
    ) -> Result<WasmDriftClient, JsValue> {
        let mut config = DriftConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = replica_id.to_string();
        config.sync_interval = std::time::Duration::from_millis(200);
        config.retry.initial_delay = std::time::Duration::from_millis(50);
        
        let db_name = format!("{}_db", namespace);
        let storage = Arc::new(BrowserStorage::new(&db_name).await
            .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?);

        let coordinator = Arc::new(crate::ws_coordinator::WasmWsCoordinator::new(
            coordinator_url,
            auth_token,
        ));
        let keyring = Arc::new(KeyRing::generate());

        let client = Arc::new(DriftClient::new_with_storage(
            config,
            coordinator,
            keyring,
            storage,
        )
        .await
        .map_err(|e| JsValue::from_str(&format!("Client failed: {:?}", e)))?);

        // Setup BroadcastChannel for cross-tab sync
        let channel_name = format!("drift-ipc-{}", namespace);
        let channel = web_sys::BroadcastChannel::new(&channel_name)
            .map_err(|e| JsValue::from_str(&format!("Failed to create BroadcastChannel: {:?}", e)))?;

        let client_clone = client.clone();
        let onmessage = wasm_bindgen::closure::Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
            if let Some(msg_str) = e.data().as_string() {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&msg_str) {
                    if let (Some(doc_id), Some(record_id)) = (
                        val.get("doc_id").and_then(|v| v.as_str()),
                        val.get("record_id").and_then(|v| v.as_str())
                    ) {
                        let doc_id = doc_id.to_string();
                        let record_id = record_id.to_string();
                        let client = client_clone.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            if let Ok(Some(state)) = client.get(&doc_id, &record_id).await {
                                client.fire_local_subscription(&doc_id, &record_id, &state);
                            }
                        });
                    }
                }
            }
        }) as Box<dyn FnMut(web_sys::MessageEvent)>);

        channel.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        let channel_send = channel.clone();
        client.set_change_listener(Arc::new(move |doc_id, record_id| {
            let msg = serde_json::json!({
                "doc_id": doc_id,
                "record_id": record_id,
            }).to_string();
            let _ = channel_send.post_message(&JsValue::from_str(&msg));
        }));

        Ok(Self { client })
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        let fields = json_to_fields(json)?;
        self.client.insert(doc_id, record_id, fields).await
            .map_err(|e| JsValue::from_str(&format!("Insert failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        let fields = json_to_fields(json)?;
        self.client.update(doc_id, record_id, fields).await
            .map_err(|e| JsValue::from_str(&format!("Update failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        self.client.delete(doc_id, record_id).await
            .map_err(|e| JsValue::from_str(&format!("Delete failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<JsValue, JsValue> {
        let doc_opt = self.client.get(doc_id, record_id).await
            .map_err(|e| JsValue::from_str(&format!("Get failed: {:?}", e)))?;
        
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
                    .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
                Ok(JsValue::from_str(&json_str))
            }
            None => Ok(JsValue::NULL),
        }
    }

    pub async fn shutdown(&self) -> Result<(), JsValue> {
        self.client.shutdown().await
            .map_err(|e| JsValue::from_str(&format!("Shutdown failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn find(&self, doc_id: &str) -> Result<js_sys::Array, JsValue> {
        let records = self.client.find(doc_id, None).await
            .map_err(|e| JsValue::from_str(&format!("Find failed: {:?}", e)))?;
        
        let arr = js_sys::Array::new();
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
                .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
            arr.push(&JsValue::from_str(&json_str));
        }
        Ok(arr)
    }

    pub async fn sync_status(&self) -> Result<JsValue, JsValue> {
        let state = self.client.sync_status().await
            .map_err(|e| JsValue::from_str(&format!("Sync status failed: {:?}", e)))?;
        let pending = self.client.pending_uploads().await
            .map_err(|e| JsValue::from_str(&format!("Pending uploads failed: {:?}", e)))?;

        let connected = state.connection_status != drift_core::sync::state::ConnectionStatus::Disconnected;

        let mut map = serde_json::Map::new();
        map.insert("connected".to_string(), serde_json::Value::Bool(connected));
        map.insert("pendingMutations".to_string(), serde_json::Value::Number(serde_json::Number::from(pending)));
        map.insert("lastSyncedSequence".to_string(), serde_json::Value::Number(serde_json::Number::from(state.last_synced_sequence)));

        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn subscribe(&self, doc_id: &str, callback: js_sys::Function) -> WasmSubscriptionHandle {
        let send_cb = SendFunction(callback);
        let handle = self.client.subscribe(doc_id, Box::new(move |_doc_id, record_id, fields| {
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
                let record_id_js = JsValue::from_str(record_id);
                let json_js = JsValue::from_str(&json_str);
                let _ = send_cb.0.call2(&JsValue::NULL, &record_id_js, &json_js);
            }
        }));

        WasmSubscriptionHandle { handle: Some(handle) }
    }

    pub fn unsubscribe(&self, handle: &mut WasmSubscriptionHandle) -> Result<(), JsValue> {
        handle.cancel(self)
    }

    pub async fn rotate_keys(&self) -> Result<JsValue, JsValue> {
        self.client.rotate_keys().await
            .map_err(|e| JsValue::from_str(&format!("Rotate keys failed: {:?}", e)))?;
        let active = self.client.active_key_version();
        Ok(JsValue::from_f64(active as f64))
    }

    pub fn active_key_version(&self) -> u64 {
        self.client.active_key_version()
    }

    pub fn list_key_versions(&self) -> Result<JsValue, JsValue> {
        let keys = self.client.list_key_versions();
        let active = self.client.active_key_version();
        let mut list = Vec::new();
        for key in keys {
            list.push(serde_json::json!({
                "version": key.version,
                "createdAt": key.created_at * 1000, // convert to ms for JS Date
                "isActive": key.version == active,
            }));
        }
        let json_str = serde_json::to_string(&list)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn prune_key_versions(&self, keep_versions: u32) -> Result<(), JsValue> {
        self.client.prune_key_versions(keep_versions as u64);
        Ok(())
    }

    pub async fn define_schema(&self, doc_id: &str, schema_json: &str) -> Result<(), JsValue> {
        let schema: drift_core::schema::registry::DocumentSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsValue::from_str(&format!("Invalid schema JSON: {:?}", e)))?;
        self.client.define_schema(doc_id, schema).await
            .map_err(|e| JsValue::from_str(&format!("Define schema failed: {:?}", e)))?;
        Ok(())
    }
}

#[wasm_bindgen]
pub struct WasmSubscriptionHandle {
    handle: Option<drift_core::subscription::engine::SubscriptionHandle>,
}

#[wasm_bindgen]
impl WasmSubscriptionHandle {
    pub fn cancel(&mut self, client: &WasmDriftClient) -> Result<(), JsValue> {
        if let Some(h) = self.handle.take() {
            client.client.unsubscribe(h)
                .map_err(|e| JsValue::from_str(&format!("Unsubscribe failed: {:?}", e)))?;
        }
        Ok(())
    }
}

struct SendFunction(js_sys::Function);

unsafe impl Send for SendFunction {}
unsafe impl Sync for SendFunction {}


fn json_to_fields(json: &str) -> Result<HashMap<String, CrdtValue>, JsValue> {
    let val: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| JsValue::from_str(&format!("invalid json: {e:?}")))?;
    
    let map = match val {
        serde_json::Value::Object(obj) => obj,
        _ => return Err(JsValue::from_str("expected json object")),
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
