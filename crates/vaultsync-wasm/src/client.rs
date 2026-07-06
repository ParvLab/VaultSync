use crate::storage::BrowserStorage;
use std::collections::HashMap;
use std::sync::Arc;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::e2ee::keyring::KeyRing;
use vaultsync_core::storage::compaction::CompactionPolicy;
use vaultsync_core::storage::manager::{DefaultStorageManager, StorageManager};
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::workspace::WorkspaceManager;
use vaultsync_core::working_set::WorkingSetManager;
use vaultsync_core::replication::planner::ReplicationPlanner;
use vaultsync_core::event_bus::EventBus;
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = debug)]
    fn console_debug_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = trace)]
    fn console_trace_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    fn console_warn_str(s: &str);
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error_str(s: &str);
}

macro_rules! console_error {
    ($($t:tt)*) => (console_error_str(&format!($($t)*)));
}

macro_rules! console_log {
    ($($t:tt)*) => (console_log_str(&format!($($t)*)));
}

macro_rules! console_debug {
    ($($t:tt)*) => (console_debug_str(&format!($($t)*)));
}

macro_rules! console_trace {
    ($($t:tt)*) => (console_trace_str(&format!($($t)*)));
}

macro_rules! console_warn {
    ($($t:tt)*) => (console_warn_str(&format!($($t)*)));
}

#[wasm_bindgen]
pub struct WasmVaultSyncClient {
    client: Arc<VaultSyncClient>,
    storage_manager: Arc<dyn StorageManager>,
    workspace_manager: Arc<WorkspaceManager>,
    working_set_manager: Arc<WorkingSetManager>,
    replication_planner: Arc<ReplicationPlanner>,
    event_bus: Arc<EventBus>,
    presence: Option<crate::presence::PresenceManager>,
    cross_tab_channel: Option<web_sys::BroadcastChannel>,
    tab_id: String,
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
        config.coordinator_mode = vaultsync_core::CoordinatorMode::Offline;
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

        // Clean up tombstoned pages left by previous versions
        match storage.cleanup_tombstoned_pages().await {
            Ok(n) => {
                if n > 0 {
                    console_debug!("[cleanup] removed {} tombstoned pages", n);
                }
            }
            Err(e) => console_warn!("[cleanup] tombstoned page cleanup: {:?}", e),
        }

        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
        );

        let workspace_manager = Arc::new(WorkspaceManager::new());
        let working_set_manager = Arc::new(WorkingSetManager::new());
        let replication_planner = Arc::new(ReplicationPlanner::new());
        let event_bus = Arc::new(EventBus::new());

        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());

        let client = match VaultSyncClient::new_with_storage(config, coordinator, keyring, storage).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                console_error!("Offline client construction failed: {:?}", e);
                return Err(JsValue::from_str(&format!("Client failed: {:?}", e)));
            }
        };

        // Settle leader election (Web Locks API - async, need to poll)
        let _ = client.leader_election.try_acquire();
        for _ in 0..30 {
            if client.leader_election.is_leader() {
                console_debug!("[LeaderElection] Leadership acquired for tab={}", replica_id);
                break;
            }
            vaultsync_core::time_utils::sleep(std::time::Duration::from_millis(50)).await;
        }
        if !client.leader_election.is_leader() {
            console_debug!("[LeaderElection] Acting as follower for tab={}", replica_id);
        }

        let channel_name = format!("vaultsync-ipc-{}", namespace);
        let cross_tab_channel = web_sys::BroadcastChannel::new(&channel_name).ok();

        Ok(Self {
            client,
            storage_manager,
            workspace_manager,
            working_set_manager,
            replication_planner,
            event_bus,
            presence: None,
            cross_tab_channel,
            tab_id: replica_id.to_string(),
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
        let _t0 = js_sys::Date::now();
        let t = || -> f64 { js_sys::Date::now() - _t0 };
        let phase_log = |name: &str| {
            console_log!("[{:.0} ms] {}", t(), name);
        };
        phase_log("startup");
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

        // Clean up tombstoned pages left by previous versions
        match storage.cleanup_tombstoned_pages().await {
            Ok(n) => {
                if n > 0 {
                    console_debug!("[cleanup] removed {} tombstoned pages", n);
                }
            }
            Err(e) => console_warn!("[cleanup] tombstoned page cleanup: {:?}", e),
        }

        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
        );
        phase_log("storage_manager ready");

        let workspace_manager = Arc::new(WorkspaceManager::new());
        phase_log("workspace_manager ready");

        let working_set_manager = Arc::new(WorkingSetManager::new());
        phase_log("working_set_manager ready");

        let replication_planner = Arc::new(ReplicationPlanner::new());
        phase_log("replication_planner ready");

        let event_bus = Arc::new(EventBus::new());
        phase_log("event_bus ready");

        phase_log("storage ready");
        let coordinator = Arc::new(crate::ws_coordinator::WasmWsCoordinator::new(
            coordinator_url,
            auth_token,
        ));
        phase_log("coordinator ready");

        // Clone the Arc BEFORE coercing to dyn Coordinator, so we keep a concrete reference
        let coordinator_for_client: Arc<dyn vaultsync_core::coordinator::traits::Coordinator> =
            coordinator.clone();
        let keyring = Arc::new(KeyRing::generate());
        phase_log("keyring ready");

        let client = match VaultSyncClient::new_with_storage_skip_init(config, coordinator_for_client, keyring, storage).await {
            Ok(client) => {
                phase_log("core client built (skip_init)");
                Arc::new(client)
            }
            Err(e) => {
                console_error!("Client construction failed: {:?}", e);
                return Err(JsValue::from_str(&format!("Client failed: {:?}", e)));
            }
        };
        phase_log("client built");

        // Wire WS mutation push → download/upload notifications BEFORE initialize (no race)
        coordinator.set_download_notify(client.events.download_notify.clone());
        coordinator.set_upload_notify(client.events.upload_notify.clone());

        // Now start WS connection (background processor starts here, sender already set)
        phase_log("initialize start");
        if let Err(e) = client.initialize().await {
            console_error!("Initialize failed: {:?}", e);
            return Err(JsValue::from_str(&format!("Initialize failed: {:?}", e)));
        }
        phase_log("initialize done");

        // NOTE: No bootstrap notify_download needed — download worker starts
        // after subscribe completes in initialize(), already synchronized.

        // Create presence manager for cross-tab awareness
        let presence = crate::presence::PresenceManager::new(namespace, replica_id).ok();
        phase_log("presence ready");

        // Settle leader election (timed)
        let _ = client.leader_election.try_acquire();
        let le_t0 = js_sys::Date::now();
        let mut le_attempts = 0u32;
        for _ in 0..30 {
            if client.leader_election.is_leader() {
                le_attempts += 1;
                break;
            }
            le_attempts += 1;
            vaultsync_core::time_utils::sleep(std::time::Duration::from_millis(50)).await;
        }
        let le_elapsed = (js_sys::Date::now() - le_t0) as u64;
        if client.leader_election.is_leader() {
            console_log!("[{:.0} ms] leader election acquired attempts={} elapsed={}ms", t(), le_attempts, le_elapsed);
        } else {
            console_debug!("[LeaderElection] Acting as follower for tab={} attempts={} elapsed={}ms", replica_id, le_attempts, le_elapsed);
        }

        console_log!("[{:.0} ms] READY", t());

        let channel_name = format!("vaultsync-ipc-{}", namespace);
        let cross_tab_channel = web_sys::BroadcastChannel::new(&channel_name).ok();

        Ok(Self {
            client,
            storage_manager,
            workspace_manager,
            working_set_manager,
            replication_planner,
            event_bus,
            presence,
            cross_tab_channel,
            tab_id: replica_id.to_string(),
        })
    }

    fn broadcast_invalidation(&self, doc_id: &str, record_id: &str) {
        if let Some(ref bc) = self.cross_tab_channel {
            let msg = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&msg, &"type".into(), &"invalidate".into());
            let _ = js_sys::Reflect::set(&msg, &"doc_id".into(), &doc_id.into());
            let _ = js_sys::Reflect::set(&msg, &"record_id".into(), &record_id.into());
            let _ = js_sys::Reflect::set(&msg, &"tab_id".into(), &self.tab_id.clone().into());
            if let Ok(json) = js_sys::JSON::stringify(&msg) {
                console_debug!(
                    "[BC invalidate] tab_id={} doc_id={} record_id={} payload={}",
                    self.tab_id, doc_id, record_id, json,
                );
                let _ = bc.post_message(&json);
            }
        } else {
            console_debug!("[BC invalidate] skipped (no channel) tab_id={} doc_id={} record_id={}", self.tab_id, doc_id, record_id);
        }
    }

    fn send_command(&self, verb: &str, doc_id: &str, record_id: &str, json: &str) {
        if let Some(ref bc) = self.cross_tab_channel {
            let msg = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&msg, &"type".into(), &verb.into());
            let _ = js_sys::Reflect::set(&msg, &"doc_id".into(), &doc_id.into());
            let _ = js_sys::Reflect::set(&msg, &"record_id".into(), &record_id.into());
            let _ = js_sys::Reflect::set(&msg, &"tab_id".into(), &self.tab_id.clone().into());
            let _ = js_sys::Reflect::set(&msg, &"payload".into(), &json.into());
            let request_id = uuid::Uuid::new_v4().to_string();
            let _ = js_sys::Reflect::set(&msg, &"request_id".into(), &request_id.clone().into());
            if let Ok(json_str) = js_sys::JSON::stringify(&msg) {
                console_debug!(
                    "[BC command] verb={} tab_id={} doc_id={} record_id={} request_id={}",
                    verb, self.tab_id, doc_id, record_id, request_id,
                );
                let _ = bc.post_message(&json_str);
            }
        } else {
            console_debug!("[BC command] skipped (no channel) verb={} tab_id={} doc_id={} record_id={}", verb, self.tab_id, doc_id, record_id);
        }
    }

    /// Leader processes a command from a follower: executes the mutation through the normal
    /// VaultSyncClient pipeline, then broadcasts invalidation to all tabs.
    pub async fn handle_command(
        &self,
        verb: &str,
        doc_id: &str,
        record_id: &str,
        json: &str,
    ) -> Result<(), JsValue> {
        console_debug!("[leader] handle_command verb={} doc={} record={} tab={}", verb, doc_id, record_id, self.tab_id);
        let t0 = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now())
            .unwrap_or(0.0);
        let result = match verb {
            "insert" => {
                let fields = json_to_fields(json)?;
                let keys: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
                console_trace!("[BC] LEADER_INSERT_BEGIN doc={} record={} field_count={} keys=[{}] payload_len={} payload={}", doc_id, record_id, fields.len(), keys.join(","), json.len(), safe_utf8_slice(json, 300));
                self.client.insert(doc_id, record_id, fields).await
            }
            "update" => {
                let fields = json_to_fields(json)?;
                let keys: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
                console_trace!("[BC] LEADER_UPDATE_BEGIN doc={} record={} field_count={} keys=[{}] payload_len={} payload={}", doc_id, record_id, fields.len(), keys.join(","), json.len(), safe_utf8_slice(json, 300));
                self.client.update(doc_id, record_id, fields).await
            }
            "delete" => self.client.delete(doc_id, record_id).await,
            _ => return Err(JsValue::from_str(&format!("Unknown command verb: {}", verb))),
        };
        let elapsed = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now() - t0)
            .unwrap_or(0.0);
        match result {
            Ok(()) => {
                console_debug!("[leader] command done verb={} doc={} record={} elapsed={:.1}ms", verb, doc_id, record_id, elapsed);
                self.broadcast_invalidation(doc_id, record_id);
                Ok(())
            }
            Err(e) => {
                console_warn!("[leader] command failed verb={} doc={} record={} elapsed={:.1}ms err={:?}", verb, doc_id, record_id, elapsed, e);
                Err(JsValue::from_str(&format!("Command {} failed: {:?}", verb, e)))
            }
        }
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        if self.client.leader_election.is_leader() {
            let fields = json_to_fields(json)?;
            self.client
                .insert(doc_id, record_id, fields)
                .await
                .map_err(|e| JsValue::from_str(&format!("Insert failed: {:?}", e)))?;
            self.broadcast_invalidation(doc_id, record_id);
            Ok(())
        } else {
            console_trace!("[BC] FOLLOW_INSERT_BEGIN doc={} record={} payload_len={} payload={}", doc_id, record_id, json.len(), safe_utf8_slice(json, 500));
            self.send_command("insert", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                self.client.fire_local_subscription(doc_id, record_id, &fields);
            }
            Ok(())
        }
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        if self.client.leader_election.is_leader() {
            let fields = json_to_fields(json)?;
            self.client
                .update(doc_id, record_id, fields)
                .await
                .map_err(|e| JsValue::from_str(&format!("Update failed: {:?}", e)))?;
            self.broadcast_invalidation(doc_id, record_id);
            Ok(())
        } else {
            console_trace!("[BC] FOLLOW_UPDATE_BEGIN doc={} record={} payload_len={} payload={}", doc_id, record_id, json.len(), safe_utf8_slice(json, 500));
            self.send_command("update", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                self.client.fire_local_subscription(doc_id, record_id, &fields);
            }
            Ok(())
        }
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        if self.client.leader_election.is_leader() {
            self.client
                .delete(doc_id, record_id)
                .await
                .map_err(|e| JsValue::from_str(&format!("Delete failed: {:?}", e)))?;
            self.broadcast_invalidation(doc_id, record_id);
            Ok(())
        } else {
            console_debug!("[Follower delete] sending command doc={} record={}", doc_id, record_id);
            self.send_command("delete", doc_id, record_id, "");
            Ok(())
        }
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<JsValue, JsValue> {
        console_debug!("[get] doc={} record={}", doc_id, record_id);
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
        match self.client.get(doc_id, record_id).await {
            Ok(Some(state)) => {
                let keys: Vec<String> = state.keys().cloned().collect();
                console_debug!(
                    "[BC fire_subscription] doc={} record={} fields={} keys={:?}",
                    doc_id, record_id, state.len(), keys,
                );
                self.client.fire_local_subscription(doc_id, record_id, &state);
            }
            Ok(None) => {
                console_debug!("[BC fire_subscription] doc={} record={} not_found=true", doc_id, record_id);
            }
            Err(e) => {
                console_debug!("[BC fire_subscription] doc={} record={} error={:?}", doc_id, record_id, e);
            }
        }
        Ok(())
    }

    pub async fn find(&self, doc_id: &str) -> Result<js_sys::Array, JsValue> {
        console_debug!("[timing] find call doc={} t={:.0}ms", doc_id, js_sys::Date::now());
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
        console_debug!("[timing] subscribe call doc={} t={:.0}ms", doc_id, js_sys::Date::now());
        static NOTIFY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let send_cb = SendFunction(JsValue::from(callback));
        let handle = self.client.subscribe(
            doc_id,
            Box::new(move |_doc_id, record_id, fields| {
                let notify_seq = NOTIFY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                console_debug!(
                    "[notify] seq={} record={} phase=wasm_callback t={:.0}ms",
                    notify_seq,
                    record_id,
                    js_sys::Date::now()
                );
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
                    let seq = notify_seq;
                    wasm_bindgen_futures::spawn_local(async move {
                        console_debug!(
                            "[notify] seq={} record={} phase=spawn_local t={:.0}ms",
                            seq,
                            record_id_owned,
                            js_sys::Date::now()
                        );
                        if cb_clone.is_null() || cb_clone.is_undefined() {
                            console_warn!("[notify] seq={} record={} null_callback=true", seq, record_id_owned);
                            return;
                        }
                        let func: js_sys::Function = cb_clone.unchecked_into();
                        let record_id_js = JsValue::from_str(&record_id_owned);
                        let json_js = JsValue::from_str(&json_str);
                        let seq_js = JsValue::from_f64(seq as f64);
                        if let Err(e) = func.call3(&JsValue::NULL, &record_id_js, &json_js, &seq_js) {
                            console_error!("[notify] seq={} record={} callback_error={:?}", seq, record_id_owned, e);
                        }
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

    /// Runs compaction on the given namespace, returns JSON stats.
    #[wasm_bindgen(js_name = compactNamespace)]
    pub async fn compact_namespace(&self, namespace: &str) -> Result<String, JsValue> {
        let stats = self
            .storage_manager
            .compact_namespace(namespace)
            .await
            .map_err(|e| JsValue::from_str(&format!("Compaction failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Runs lifecycle (tombstone cleanup) on the given namespace, returns JSON stats.
    #[wasm_bindgen(js_name = runLifecycle)]
    pub async fn run_lifecycle(&self, namespace: &str) -> Result<String, JsValue> {
        let stats = self
            .storage_manager
            .run_lifecycle(namespace)
            .await
            .map_err(|e| JsValue::from_str(&format!("Lifecycle failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Returns JSON with storage statistics (page count, segment state, etc.).
    #[wasm_bindgen(js_name = storageStats)]
    pub async fn storage_stats(&self) -> Result<String, JsValue> {
        let stats = self
            .storage_manager
            .stats()
            .await
            .map_err(|e| JsValue::from_str(&format!("Stats failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Runs the resource manager sweep (recompute access scores, promote/demote tiers).
    /// Returns JSON with tier byte counts, promotions, demotions, eviction candidates.
    #[wasm_bindgen(js_name = resourceSweep)]
    pub async fn resource_sweep(&self) -> Result<String, JsValue> {
        let stats = self
            .storage_manager
            .run_resource_sweep()
            .await
            .map_err(|e| JsValue::from_str(&format!("Resource sweep failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Returns the workspace namespace proxy for CRUD operations.
    #[wasm_bindgen(js_name = workspace)]
    pub fn workspace(&self) -> WorkspaceNamespace {
        WorkspaceNamespace {
            manager: self.workspace_manager.clone(),
        }
    }

    /// Returns the working sets namespace proxy for CRUD operations.
    #[wasm_bindgen(js_name = workingSets)]
    pub fn working_sets(&self) -> WorkingSetsNamespace {
        WorkingSetsNamespace {
            manager: self.working_set_manager.clone(),
        }
    }

    /// Returns the replication namespace proxy.
    #[wasm_bindgen(js_name = replication)]
    pub fn replication(&self) -> ReplicationNamespace {
        ReplicationNamespace {
            planner: self.replication_planner.clone(),
        }
    }

    /// Returns the event bus for publishing/subscribing to engine events.
    #[wasm_bindgen(js_name = events)]
    pub fn events(&self) -> EventBusProxy {
        EventBusProxy {
            bus: self.event_bus.clone(),
        }
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

#[wasm_bindgen]
pub struct WorkspaceNamespace {
    manager: Arc<WorkspaceManager>,
}

#[wasm_bindgen]
impl WorkspaceNamespace {
    #[wasm_bindgen(js_name = create)]
    pub fn create(
        &self,
        name: &str,
        namespace: &str,
        schema_json: &str,
        retention_json: &str,
    ) -> Result<u64, JsValue> {
        let schema: vaultsync_core::workspace::WorkspaceSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsValue::from_str(&format!("Invalid schema: {:?}", e)))?;
        let retention: vaultsync_core::workspace::RetentionPolicy = serde_json::from_str(retention_json)
            .map_err(|e| JsValue::from_str(&format!("Invalid retention: {:?}", e)))?;
        let now = js_sys::Date::now() as u64;
        let id = self.manager.create_workspace(
            name,
            namespace,
            schema,
            vaultsync_core::workspace::ReplicationPolicy::FullSync,
            retention,
            now,
        );
        Ok(id.0)
    }

    #[wasm_bindgen(js_name = list)]
    pub fn list(&self) -> Result<String, JsValue> {
        let list = self.manager.list_workspaces();
        serde_json::to_string(&list)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = get)]
    pub fn get(&self, id: u64) -> Result<String, JsValue> {
        let ws = self.manager.get_workspace(&vaultsync_core::workspace::WorkspaceId(id));
        match ws {
            Some(ws) => serde_json::to_string(&ws)
                .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e))),
            None => Ok("null".to_string()),
        }
    }

    #[wasm_bindgen(js_name = update)]
    pub fn update(&self, id: u64, name: Option<String>, retention_json: Option<String>) -> Result<bool, JsValue> {
        let retention = match retention_json {
            Some(json) => Some(serde_json::from_str::<vaultsync_core::workspace::RetentionPolicy>(&json)
                .map_err(|e| JsValue::from_str(&format!("Invalid retention: {:?}", e)))?),
            None => None,
        };
        let now = js_sys::Date::now() as u64;
        Ok(self.manager.update_workspace(
            &vaultsync_core::workspace::WorkspaceId(id),
            name.as_deref(),
            None,
            None,
            retention,
            now,
        ))
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete(&self, id: u64) -> bool {
        self.manager.delete_workspace(&vaultsync_core::workspace::WorkspaceId(id))
    }
}

#[wasm_bindgen]
pub struct WorkingSetsNamespace {
    manager: Arc<WorkingSetManager>,
}

#[wasm_bindgen]
impl WorkingSetsNamespace {
    #[wasm_bindgen(js_name = create)]
    pub fn create(
        &self,
        name: &str,
        filter_json: &str,
        workspace_id: Option<u64>,
    ) -> Result<u64, JsValue> {
        let filter: vaultsync_core::working_set::QueryFilter = serde_json::from_str(filter_json)
            .map_err(|e| JsValue::from_str(&format!("Invalid filter: {:?}", e)))?;
        let now = js_sys::Date::now() as u64;
        let id = self.manager.create_set(
            name,
            filter,
            vaultsync_core::working_set::WorkingSetPolicy::Manual,
            workspace_id,
            now,
        );
        Ok(id.0)
    }

    #[wasm_bindgen(js_name = list)]
    pub fn list(&self) -> Result<String, JsValue> {
        let list = self.manager.list_sets();
        serde_json::to_string(&list)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = setActive)]
    pub fn set_active(&self, id: u64) -> bool {
        let now = js_sys::Date::now() as u64;
        self.manager.set_active(&vaultsync_core::working_set::WorkingSetId(id), now)
    }

    #[wasm_bindgen(js_name = getActive)]
    pub fn get_active(&self) -> JsValue {
        match self.manager.get_active() {
            Some(id) => JsValue::from_f64(id.0 as f64),
            None => JsValue::NULL,
        }
    }

    #[wasm_bindgen(js_name = clearActive)]
    pub fn clear_active(&self) {
        self.manager.clear_active();
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete(&self, id: u64) -> bool {
        self.manager.delete_set(&vaultsync_core::working_set::WorkingSetId(id))
    }
}

#[wasm_bindgen]
pub struct ReplicationNamespace {
    planner: Arc<ReplicationPlanner>,
}

#[wasm_bindgen]
impl ReplicationNamespace {
    #[wasm_bindgen(js_name = predictNext)]
    pub fn predict_next(&self, limit: usize) -> Result<String, JsValue> {
        let predictions = self.planner.predict_next_documents(limit);
        serde_json::to_string(&predictions)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = pendingCount)]
    pub fn pending_count(&self) -> usize {
        self.planner.pending_count()
    }

    #[wasm_bindgen(js_name = clearPending)]
    pub fn clear_pending(&self) {
        self.planner.clear_pending();
    }
}

#[wasm_bindgen]
pub struct EventBusProxy {
    bus: Arc<EventBus>,
}

#[wasm_bindgen]
impl EventBusProxy {
    #[wasm_bindgen(js_name = subscriberCount)]
    pub fn subscriber_count(&self) -> usize {
        self.bus.subscriber_count()
    }
}

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

/// Truncate a &str to at most `max` characters, respecting UTF-8 boundaries.
/// Returns the truncated string. Never panics on multi-byte boundaries.
fn safe_utf8_slice(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}
