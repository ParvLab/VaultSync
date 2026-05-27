use wasm_bindgen::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use crate::storage::BrowserStorage;
use drift_core::coordinator::memory::InMemoryCoordinator;
use drift_core::e2ee::keyring::KeyRing;

#[wasm_bindgen]
pub struct WasmDriftClient {
    client: DriftClient,
}

#[wasm_bindgen]
impl WasmDriftClient {
    pub async fn new(namespace: &str, replica_id: &str) -> Result<WasmDriftClient, JsValue> {
        let mut config = DriftConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = replica_id.to_string();
        
        let db_name = format!("{}_db", namespace);
        let storage = Arc::new(BrowserStorage::new(&db_name).await
            .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?);

        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());

        let client = DriftClient::new_with_storage(
            config,
            coordinator,
            keyring,
            storage,
        )
        .await
        .map_err(|e| JsValue::from_str(&format!("Client failed: {:?}", e)))?;

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
        let json = serde_json::to_string(&state)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
        Ok(JsValue::from_str(&json))
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
