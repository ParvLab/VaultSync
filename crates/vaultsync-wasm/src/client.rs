use crate::storage::BrowserStorage;
use std::collections::HashMap;
use std::sync::Arc;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::e2ee::keyring::KeyRing;
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (console_log_str(&format!($($t)*)));
}

#[wasm_bindgen]
pub struct WasmVaultSyncClient {
    client: Arc<VaultSyncClient>,
    presence: Option<crate::presence::PresenceManager>,
}

#[wasm_bindgen]
impl WasmVaultSyncClient {
    pub async fn new(
        namespace: &str,
        replica_id: &str,
        db_name: Option<String>,
        storage_backend: Option<String>,
    ) -> Result<WasmVaultSyncClient, JsValue> {
        let mut config = VaultSyncConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = replica_id.to_string();
        config.sync_interval = std::time::Duration::from_millis(200);
        config.retry.initial_delay = std::time::Duration::from_millis(50);

        let final_db_name = db_name.unwrap_or_else(|| format!("{}_db", namespace));
        let storage = Arc::new(
            BrowserStorage::new(&final_db_name, storage_backend.as_deref())
                .await
                .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?,
        );

        config.storage = match &*storage {
            BrowserStorage::Opfs(_) | BrowserStorage::Idb(_) => StorageConfig::Wasm,
        };

        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());

        let client = Arc::new(
            VaultSyncClient::new_with_storage(config, coordinator, keyring, storage)
                .await
                .map_err(|e| JsValue::from_str(&format!("Client failed: {:?}", e)))?,
        );

        Ok(Self {
            client,
            presence: None,
        })
    }

    pub async fn new_with_coordinator(
        namespace: &str,
        replica_id: &str,
        coordinator_url: &str,
        auth_token: Option<String>,
        db_name: Option<String>,
        storage_backend: Option<String>,
    ) -> Result<WasmVaultSyncClient, JsValue> {
        console_log!("[1/9] creating browser storage");
        let mut config = VaultSyncConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = replica_id.to_string();
        config.sync_interval = std::time::Duration::from_millis(200);
        config.retry.initial_delay = std::time::Duration::from_millis(50);

        let final_db_name = db_name.unwrap_or_else(|| format!("{}_db", namespace));
        let storage = Arc::new(
            BrowserStorage::new(&final_db_name, storage_backend.as_deref())
                .await
                .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?,
        );

        config.storage = match &*storage {
            BrowserStorage::Opfs(_) | BrowserStorage::Idb(_) => StorageConfig::Wasm,
        };

        console_log!("[2/9] creating ws coordinator");
        let coordinator = Arc::new(crate::ws_coordinator::WasmWsCoordinator::new(
            coordinator_url,
            auth_token,
        ));
        // Clone the Arc BEFORE coercing to dyn Coordinator, so we keep a concrete reference
        let coordinator_for_client: Arc<dyn vaultsync_core::coordinator::traits::Coordinator> =
            coordinator.clone();
        let keyring = Arc::new(KeyRing::generate());

        console_log!("[3/9] creating sync client");
        let client = Arc::new(
            VaultSyncClient::new_with_storage_skip_init(config, coordinator_for_client, keyring, storage)
                .await
                .map_err(|e| JsValue::from_str(&format!("Client failed: {:?}", e)))?,
        );

        // Wire WS mutation push → download notification BEFORE initialize (no race)
        coordinator.set_download_notify(client.events.download_notify.clone());

        // Now start WS connection (background processor starts here, sender already set)
        client.initialize().await.map_err(|e| {
            JsValue::from_str(&format!("Initialize failed: {:?}", e))
        })?;

        // Bootstrap: same event-driven path as WS push notifications
        client.events.notify_download(None);

        // Create presence manager for cross-tab awareness
        let presence = crate::presence::PresenceManager::new(namespace, replica_id).ok();

        console_log!("[9/9] client ready");
        Ok(Self {
            client,
            presence,
        })
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        let fields = json_to_fields(json)?;
        self.client
            .insert(doc_id, record_id, fields)
            .await
            .map_err(|e| JsValue::from_str(&format!("Insert failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        let fields = json_to_fields(json)?;
        self.client
            .update(doc_id, record_id, fields)
            .await
            .map_err(|e| JsValue::from_str(&format!("Update failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        self.client
            .delete(doc_id, record_id)
            .await
            .map_err(|e| JsValue::from_str(&format!("Delete failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<JsValue, JsValue> {
        let doc_opt = self
            .client
            .get(doc_id, record_id)
            .await
            .map_err(|e| JsValue::from_str(&format!("Get failed: {:?}", e)))?;

        match doc_opt {
            Some(fields) => {
                let mut map = serde_json::Map::new();
                for (k, v) in fields {
                    let json_val = match v {
                        CrdtValue::String(s) => serde_json::Value::String(s),
                        CrdtValue::Number(n) => serde_json::Value::Number(
                            serde_json::Number::from_f64(n)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
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
        self.client
            .shutdown()
            .await
            .map_err(|e| JsValue::from_str(&format!("Shutdown failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn fire_subscription(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        if let Ok(Some(state)) = self.client.get(doc_id, record_id).await {
            self.client.fire_local_subscription(doc_id, record_id, &state);
        }
        Ok(())
    }

    pub async fn find(&self, doc_id: &str) -> Result<js_sys::Array, JsValue> {
        let records = self
            .client
            .find(doc_id, None)
            .await
            .map_err(|e| JsValue::from_str(&format!("Find failed: {:?}", e)))?;

        let arr = js_sys::Array::new();
        for fields in records {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                let json_val = match v {
                    CrdtValue::String(s) => serde_json::Value::String(s),
                    CrdtValue::Number(n) => serde_json::Value::Number(
                        serde_json::Number::from_f64(n)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
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
        let state = self
            .client
            .sync_status()
            .await
            .map_err(|e| JsValue::from_str(&format!("Sync status failed: {:?}", e)))?;
        let pending = self
            .client
            .pending_uploads()
            .await
            .map_err(|e| JsValue::from_str(&format!("Pending uploads failed: {:?}", e)))?;

        let connected =
            state.connection_status != vaultsync_core::sync::state::ConnectionStatus::Disconnected;

        let metrics = self.client.metrics.snapshot();

        let mut map = serde_json::Map::new();
        map.insert("connected".to_string(), serde_json::Value::Bool(connected));
        map.insert(
            "pendingMutations".to_string(),
            serde_json::Value::Number(serde_json::Number::from(pending)),
        );
        map.insert(
            "lastSyncedSequence".to_string(),
            serde_json::Value::Number(serde_json::Number::from(state.last_synced_sequence)),
        );
        map.insert(
            "optimisticWrites".to_string(),
            serde_json::Value::Number(serde_json::Number::from(metrics.optimistic_writes)),
        );
        map.insert(
            "pushMutationsReceived".to_string(),
            serde_json::Value::Number(serde_json::Number::from(metrics.push_mutations_received)),
        );
        map.insert(
            "snapshotsApplied".to_string(),
            serde_json::Value::Number(serde_json::Number::from(metrics.snapshots_applied)),
        );
        map.insert(
            "activePeers".to_string(),
            serde_json::Value::Number(serde_json::Number::from(metrics.active_peers)),
        );

        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn subscribe(&self, doc_id: &str, callback: js_sys::Function) -> WasmSubscriptionHandle {
        let send_cb = SendFunction(JsValue::from(callback));
        let handle = self.client.subscribe(
            doc_id,
            Box::new(move |_doc_id, record_id, fields| {
                let mut map = serde_json::Map::new();
                for (k, v) in fields {
                    let json_val = match v {
                        CrdtValue::String(s) => serde_json::Value::String(s.clone()),
                        CrdtValue::Number(n) => serde_json::Value::Number(
                            serde_json::Number::from_f64(*n)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                        CrdtValue::Boolean(b) => serde_json::Value::Bool(*b),
                        CrdtValue::Null => serde_json::Value::Null,
                        _ => serde_json::Value::Null,
                    };
                    map.insert(k.clone(), json_val);
                }
                if let Ok(json_str) = serde_json::to_string(&serde_json::Value::Object(map)) {
                    let record_id_owned = record_id.to_string();
                    let cb_clone = send_cb.0.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        let func: js_sys::Function = cb_clone.unchecked_into();
                        let record_id_js = JsValue::from_str(&record_id_owned);
                        let json_js = JsValue::from_str(&json_str);
                        let _ = func.call2(&JsValue::NULL, &record_id_js, &json_js);
                    });
                }
            }),
        );

        WasmSubscriptionHandle {
            handle: Some(handle),
        }
    }

    pub fn unsubscribe(&self, handle: &mut WasmSubscriptionHandle) -> Result<(), JsValue> {
        handle.cancel(self)
    }

    pub async fn rotate_keys(&self) -> Result<JsValue, JsValue> {
        self.client
            .rotate_keys()
            .await
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
        let schema: vaultsync_core::schema::registry::DocumentSchema =
            serde_json::from_str(schema_json)
                .map_err(|e| JsValue::from_str(&format!("Invalid schema JSON: {:?}", e)))?;
        self.client
            .define_schema(doc_id, schema)
            .await
            .map_err(|e| JsValue::from_str(&format!("Define schema failed: {:?}", e)))?;
        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.client.leader_election.is_leader()
    }

    /// Returns a clone of the PresenceManager if available.
    #[wasm_bindgen]
    pub fn presence(&self) -> Option<crate::presence::PresenceManager> {
        self.presence.clone()
    }

    /// Returns a JSON snapshot of all metrics counters.
    #[wasm_bindgen(js_name = metricsSnapshot)]
    pub fn metrics_snapshot(&self) -> String {
        let snapshot = self.client.metrics.snapshot();
        serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string())
    }
}

#[wasm_bindgen]
pub struct WasmSubscriptionHandle {
    handle: Option<vaultsync_core::subscription::engine::SubscriptionHandle>,
}

#[wasm_bindgen]
impl WasmSubscriptionHandle {
    pub fn cancel(&mut self, client: &WasmVaultSyncClient) -> Result<(), JsValue> {
        if let Some(h) = self.handle.take() {
            client
                .client
                .unsubscribe(h)
                .map_err(|e| JsValue::from_str(&format!("Unsubscribe failed: {:?}", e)))?;
        }
        Ok(())
    }
}

struct SendFunction(JsValue);

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
