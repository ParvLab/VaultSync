use crate::broadcast_manager::BroadcastManager;
use crate::capability::{CapabilityManager, RuntimeCapability};
use crate::follower::MirrorRuntime;
use crate::ipc::WasmIPC;
use crate::runtime::Runtime;
use crate::maintenance_runtime::MaintenanceRuntime;
use crate::runtime_host::VaultRuntime;
use crate::storage_runtime::StorageRuntime;
use crate::storage::BrowserStorage;
use crate::persistence_engine::{OpfsPersistenceEngine, PersistenceEngine};
use crate::upload_scheduler::{PendingUpload, UploadAction};
use crate::compaction_scheduler::CompactionScheduler;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
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
use vaultsync_core::VaultSyncError;
use futures::stream::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen]
#[repr(u32)]
pub enum ClientEvent {
    StatusDirty = 0,
}

#[wasm_bindgen]
pub struct VaultSyncRuntime {
    /// Core sync engine (leader mode only)
    client: Option<Arc<VaultSyncClient>>,
    /// Storage management (leader mode only)
    storage_manager: Option<Arc<dyn StorageManager>>,
    workspace_manager: Option<Arc<WorkspaceManager>>,
    working_set_manager: Option<Arc<WorkingSetManager>>,
    replication_planner: Option<Arc<ReplicationPlanner>>,
    event_bus: Option<Arc<EventBus>>,
    presence: Option<crate::presence::PresenceManager>,
    cross_tab_channel: Option<web_sys::BroadcastChannel>,
    tab_id: String,
    event_callback: Arc<Mutex<Option<SendFunction>>>,
    // Engine V2 modules (leader mode only)
    runtime: Option<Arc<Runtime>>,
    /// Phase 5: VaultRuntime — single top-level runtime host wrapping legacy + new services
    vault_runtime: Option<Arc<VaultRuntime>>,
    capability_manager: Option<Arc<CapabilityManager>>,
    broadcast_manager: Option<Arc<BroadcastManager>>,
    /// Phase 4 v2: RuntimeCoordinator — state machine, HELLO/DISCOVER protocol, BC routing
    runtime_coordinator: Option<Arc<crate::runtime_coordinator::RuntimeCoordinator>>,
    persistence_engine: Option<Arc<dyn PersistenceEngine>>,
    /// Phase 6: CompactionScheduler — auto-triggered compaction
    compaction_scheduler: Option<Arc<CompactionScheduler>>,
    /// Phase 4: MirrorRuntime (follower mode — no storage/coordinator/recovery)
    mirror: Option<Arc<MirrorRuntime>>,
}

#[wasm_bindgen]
impl VaultSyncRuntime {
    pub async fn new(
        namespace: &str,
        replica_id: &str,
        db_name: Option<String>,
        storage_backend: Option<String>,
    ) -> Result<VaultSyncRuntime, JsValue> {
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
        let storage_for_engine = storage.clone();

        config.storage = match &*storage {
            BrowserStorage::Opfs(_) | BrowserStorage::Idb(_) => StorageConfig::Wasm,
        };

        // Phase 3: Wire StorageRuntime into bootstrap (offline path)
        let mut vault_runtime: Option<Arc<VaultRuntime>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            let pages = opfs.pages().clone();
            let metrics = Arc::new(crate::metrics::RuntimeMetrics::default());
            match StorageRuntime::new(pages, metrics.clone()).await {
                Ok(storage_runtime) => {
                    opfs.set_storage_runtime(storage_runtime.clone());
                    let runtime = Runtime::new(replica_id.to_string());
                    let scheduler = storage_runtime.scheduler.clone();
                    let vr = VaultRuntime::new(runtime, storage_runtime, scheduler, metrics);
                    Some(vr)
                }
                Err(e) => {
                    engine_debug!("[StorageRuntime] init deferred: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Clean up tombstoned pages left by previous versions
        match storage.cleanup_tombstoned_pages().await {
            Ok(n) => {
                if n > 0 {
                    engine_debug!("[cleanup] removed {} tombstoned pages", n);
                }
            }
            Err(e) => engine_warn!("[cleanup] tombstoned page cleanup: {:?}", e),
        }

        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
        );

        // Phase 4.0: Attach MaintenanceRuntime to VaultRuntime (best-effort)
        if let Some(ref vr) = vault_runtime {
            let mr = MaintenanceRuntime::new(
                vr.storage().clone(),
                storage_manager.clone(),
                vr.runtime().clone(),
                vr.metrics().clone(),
            );
            vault_runtime = Some(vr.clone().with_maintenance(mr));
            engine_debug!("[maintenance] attached to VaultRuntime");
        }

        let workspace_manager = Arc::new(WorkspaceManager::new());
        let working_set_manager = Arc::new(WorkingSetManager::new());
        let replication_planner = Arc::new(ReplicationPlanner::new());
        let event_bus = Arc::new(EventBus::new());

        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());

        let client = match VaultSyncClient::new_with_storage(config, coordinator, keyring, storage.clone()).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                engine_error!("Offline client construction failed: {:?}", e);
                return Err(JsValue::from_str(&format!("Client failed: {:?}", e)));
            }
        };

        // Settle leader election (Web Locks API - async, need to poll)
        let _ = client.leader_election.try_acquire();
        for _ in 0..30 {
            if client.leader_election.is_leader() {
                engine_debug!("[LeaderElection] Leadership acquired for tab={}", replica_id);
                break;
            }
            vaultsync_core::time_utils::sleep(std::time::Duration::from_millis(50)).await;
        }
        if !client.leader_election.is_leader() {
            engine_debug!("[LeaderElection] Acting as follower for tab={}", replica_id);
        }

        let channel_name = format!("vaultsync-ipc-{}", namespace);
        let cross_tab_channel = web_sys::BroadcastChannel::new(&channel_name).ok();

        let event_callback: Arc<Mutex<Option<SendFunction>>> = Arc::new(Mutex::new(None));

        // Engine V2: initialize Runtime and managers
        let runtime = Runtime::new(replica_id.to_string());
        let capability_manager = Arc::new(CapabilityManager::new());

        // Engine V2/V3: create PersistenceEngine if OPFS backend (best-effort, skip on failure)
        // Phase 3: Runtime owns the PageStore instances — set them here so all subsystems share them
        let persistence_engine: Option<Arc<dyn PersistenceEngine>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            match Self::build_persistence_engine(opfs.clone().into(), runtime.clone()).await {
                Ok(engine) => {
                    Some(engine)
                }
                Err(e) => {
                    engine_warn!("[engine] persistence_engine init skipped: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Engine V2: restore WAL checkpoint from OPFS
        if let Some(ref pe) = persistence_engine {
            match pe.load_checkpoint().await {
                Ok((seq, count)) if seq > 0 => {
                    runtime.metadata_store.lock().unwrap().cursor = seq;
                    runtime.metadata_store.lock().unwrap().pending_count = count;
                    engine_info!("[engine] restored WAL checkpoint: seq={} pending={}", seq, count);
                }
                Ok(_) => {}
                Err(e) => engine_warn!("[engine] load_checkpoint: {:?}", e),
            }
        }

        let broadcast_manager = cross_tab_channel.clone().map(|bc| {
            let ipc = Arc::new(WasmIPC::new_with_channel(bc));
            Arc::new(BroadcastManager::new(runtime.clone(), ipc, Arc::new(crate::metrics::RuntimeMetrics::default())))
        });

        // Wire CapabilityManager based on leader election
        let is_leader = client.leader_election.is_leader();
        if is_leader {
            capability_manager.set(RuntimeCapability::Leader);
            runtime.set_status(crate::runtime::RuntimeStatus::Leader);
        } else {
            capability_manager.set(RuntimeCapability::Follower);
        }

        // Phase 4 v2: Create RuntimeCoordinator
        let runtime_coordinator = broadcast_manager.clone().map(|bm| {
            Arc::new(crate::runtime_coordinator::RuntimeCoordinator::new(
                runtime.clone(),
                bm,
                Arc::new(crate::metrics::RuntimeMetrics::default()),
                replica_id.to_string(),
            ))
        });

        if let Some(ref coord) = runtime_coordinator {
            if is_leader {
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Leading);
            } else {
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Follower);
            }
        }

        let compaction_scheduler = if is_leader {
            Some(Arc::new(CompactionScheduler::new(
                runtime.clone(),
                storage_manager.clone(),
                namespace,
            )))
        } else {
            None
        };

        // Wire compaction_scheduler into VaultRuntime's EngineContext
        if let (Some(vr), Some(cs)) = (vault_runtime.clone(), compaction_scheduler.clone()) {
            vault_runtime = Some(vr.with_compaction(cs));
        }

        let result = Self {
            client: Some(client),
            storage_manager: Some(storage_manager),
            workspace_manager: Some(workspace_manager),
            working_set_manager: Some(working_set_manager),
            replication_planner: Some(replication_planner),
            event_bus: Some(event_bus),
            presence: None,
            cross_tab_channel,
            tab_id: replica_id.to_string(),
            event_callback,
            runtime: Some(runtime.clone()),
            vault_runtime,
            capability_manager: Some(capability_manager),
            broadcast_manager: broadcast_manager.clone(),
            runtime_coordinator,
            persistence_engine,
            compaction_scheduler,
            mirror: None,
        };

        // Phase II: new leader announces via BC
        if is_leader {
            if let Some(ref bm) = broadcast_manager {
                bm.broadcast_leader_announcement(replica_id);
            }
        }

        Self::spawn_pending_listener(&result);

        Ok(result)
    }

    /// Phase 4: Construct a follower-only client (no storage, no coordinator, no recovery).
    /// Used when a leader is detected on BC during startup.
    #[allow(unused_variables)]
    async fn new_follower(
        namespace: &str,
        replica_id: &str,
        cross_tab_channel: web_sys::BroadcastChannel,
        mirror: Arc<MirrorRuntime>,
    ) -> Result<VaultSyncRuntime, JsValue> {
        let event_callback: Arc<Mutex<Option<SendFunction>>> = Arc::new(Mutex::new(None));

        let capability_manager = Arc::new(CapabilityManager::new());
        capability_manager.set(RuntimeCapability::Follower);

        let result = Self {
            client: None,
            storage_manager: None,
            workspace_manager: None,
            working_set_manager: None,
            replication_planner: None,
            event_bus: None,
            presence: None,
            cross_tab_channel: Some(cross_tab_channel),
            tab_id: replica_id.to_string(),
            event_callback,
            runtime: None,
            vault_runtime: None,
            capability_manager: Some(capability_manager),
            broadcast_manager: None,
            runtime_coordinator: None,
            persistence_engine: None,
            compaction_scheduler: None,
            mirror: Some(mirror),
        };

        Ok(result)
    }

    /// Phase 4: Handle BC messages for MirrorRuntime (follower) mode.
    /// Processes HEARTBEAT, MUTATION, RUNTIME_SNAPSHOT, LEADER_ELECTED messages.
    fn handle_mirror_bc_message(mirror: &MirrorRuntime, msg_str: &str) {
        if msg_str.starts_with("HEARTBEAT|") {
            let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
            if parts.len() >= 5 {
                let runtime_gen: u64 = parts[1].parse().unwrap_or(0);
                let bus_gen: u64 = parts[2].parse().unwrap_or(0);
                let cursor: u64 = parts[3].parse().unwrap_or(0);
                let pending_count: u64 = parts[4].parse().unwrap_or(0);
                mirror.update_heartbeat(runtime_gen, bus_gen, cursor, pending_count);
            }
            return;
        }

        if msg_str.starts_with("MUTATION|") {
            let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
            if parts.len() >= 5 {
                let doc_id = parts[1];
                let record_id = parts[2];
                let fields_str = parts[3];
                if let Ok(fields) = json_to_fields(fields_str) {
                    // Phase 3: Apply to shared DocumentStore
                    for (field, value) in &fields {
                        mirror.apply_mutation(doc_id, record_id, field, value.clone());
                    }
                    // Fire subscriptions
                    let fields_copy = fields;
                    mirror.fire_subscription(doc_id, record_id, &fields_copy);
                }
            }
            return;
        }

        if msg_str.starts_with("LEADER_ELECTED|") {
            let parts: Vec<&str> = msg_str.splitn(2, '|').collect();
            let other_tab = parts.get(1).unwrap_or(&"");
            if other_tab != &mirror.tab_id {
                engine_info!("[MirrorRuntime] leader elected: tab={}", other_tab);
            }
            return;
        }
    }

    pub async fn new_with_coordinator(
        namespace: &str,
        replica_id: &str,
        coordinator_url: &str,
        auth_token: Option<String>,
        db_name: Option<String>,
        storage_backend: Option<String>,
    ) -> Result<VaultSyncRuntime, JsValue> {
        let _t0 = js_sys::Date::now();
        let t = || -> f64 { js_sys::Date::now() - _t0 };
        let phase_log = |name: &str| {
            engine_info!("[{:.0} ms] {}", t(), name);
        };

        // Create BC channel early for discovery (before any expensive operations)
        let channel_name = format!("vaultsync-ipc-{}", namespace);
        let cross_tab_channel = web_sys::BroadcastChannel::new(&channel_name).ok();

        // Phase 4 v3: DiscoveryProtocol — lightweight HELLO/DISCOVER protocol
        // Order: create handler → send HELLO → wait for DISCOVER → found? → MirrorRuntime → else → full init
        //
        // CRITICAL: The BC handler is registered BEFORE send_hello() so that
        // other tabs running concurrently can immediately respond to our HELLO,
        // and we can respond to THEIR HELLO while still building.
        let boot_state = if cross_tab_channel.is_some() {
                let bs = std::sync::Arc::new(
                    crate::discovery_protocol::SharedBootState::new(replica_id.to_string())
                );
                let bc = cross_tab_channel.as_ref().unwrap();
                let protocol = crate::discovery_protocol::DiscoveryProtocol::new(
                    bc.clone(),
                    replica_id.to_string(),
                );

                // 1. Set BUILDING state immediately — other tabs that receive our
                //    HELLO will get a BUILDING response instead of silence.
                bs.state.store(crate::discovery_protocol::SharedBootState::BUILDING, Ordering::Relaxed);

                // 2. Register lightweight BC handler (responds to HELLO, tracks DISCOVER/READY)
                let handler = protocol.create_lightweight_handler(&bs);
                cross_tab_channel.as_ref().unwrap().set_onmessage(
                    Some(handler.as_ref().unchecked_ref())
                );
                handler.forget(); // lives until replaced by full BC handler

                // 3. Send HELLO
                protocol.send_hello();

                engine_debug!("[Discovery] sent HELLO, waiting for DISCOVER");

                // 4. Wait for DISCOVER/READY (2500ms max — covers engine build time)
                //    If we receive BUILDING from another tab, the timeout resets.
                //    This handles the race: another tab may be building while we start.
                let leader = protocol.wait_for_leader(&bs, 2500).await;

                if let Some(ref l) = leader {
                    engine_info!("[Discovery] leader found: tab={} boot={} gen={} cursor={} — fast-path bootstrap", l.tab_id, l.boot_id, l.generation, l.cursor);
                    *bs.result.lock().unwrap() = Some(l.clone());
                    bs.found.store(true, Ordering::Relaxed);
                    // Stay in BUILDING state — our engine build is cancelled by MirrorRuntime path below
                } else {
                    engine_debug!("[Discovery] no leader found after waiting {:.0}ms — becoming leader", js_sys::Date::now() - t());
                    // State is already BUILDING from step 1
                    protocol.announce_building();
                }
                drop(protocol);
                Some(bs)
            } else {
                None
            };

        // Fast-path: leader exists, build MirrorRuntime
        if let Some(ref bs) = boot_state {
            if bs.found.load(Ordering::Relaxed) {
                engine_info!("[Discovery] taking MirrorRuntime fast-path — engine build skipped");
                let bc = cross_tab_channel.as_ref().unwrap().clone();
                let leader = bs.result.lock().unwrap().take().unwrap();

                // Create shared DocumentStore for MirrorRuntime
                let doc_store = Arc::new(std::sync::Mutex::new(
                    crate::runtime::DocumentStore::new()
                ));

                // Set up permanent BC handler for MirrorRuntime
                let mirror = MirrorRuntime::new(replica_id, doc_store);
                let mirror_clone = mirror.clone();
                let mirror_handler = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                    if let Some(msg_str) = e.data().as_string() {
                        Self::handle_mirror_bc_message(&mirror_clone, &msg_str);
                    }
                }) as Box<dyn FnMut(web_sys::MessageEvent)>);
                bc.set_onmessage(Some(mirror_handler.as_ref().unchecked_ref()));
                mirror_handler.forget();

                phase_log("mirror runtime ready");
                return Self::new_follower(namespace, replica_id, bc, mirror).await;
            }
        }

        // No leader found — build full engine and become leader
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
        let storage_for_engine = storage.clone();

        config.storage = match &*storage {
            BrowserStorage::Opfs(_) | BrowserStorage::Idb(_) => StorageConfig::Wasm,
        };

        // Phase 3: Wire StorageRuntime into bootstrap (leader path)
        let mut vault_runtime: Option<Arc<VaultRuntime>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            let pages = opfs.pages().clone();
            let metrics = Arc::new(crate::metrics::RuntimeMetrics::default());
            match StorageRuntime::new(pages, metrics.clone()).await {
                Ok(storage_runtime) => {
                    opfs.set_storage_runtime(storage_runtime.clone());
                    let runtime = Runtime::new(replica_id.to_string());
                    let scheduler = storage_runtime.scheduler.clone();
                    let vr = VaultRuntime::new(runtime, storage_runtime, scheduler, metrics);
                    Some(vr)
                }
                Err(e) => {
                    engine_debug!("[StorageRuntime] init deferred: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Clean up tombstoned pages left by previous versions
        match storage.cleanup_tombstoned_pages().await {
            Ok(n) => {
                if n > 0 {
                    engine_debug!("[cleanup] removed {} tombstoned pages", n);
                }
            }
            Err(e) => engine_warn!("[cleanup] tombstoned page cleanup: {:?}", e),
        }

        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
        );
        phase_log("storage_manager ready");

        // Phase 4.0: Attach MaintenanceRuntime to VaultRuntime (best-effort)
        if let Some(ref vr) = vault_runtime {
            let mr = MaintenanceRuntime::new(
                vr.storage().clone(),
                storage_manager.clone(),
                vr.runtime().clone(),
                vr.metrics().clone(),
            );
            vault_runtime = Some(vr.clone().with_maintenance(mr));
            engine_debug!("[maintenance] attached to VaultRuntime");
        }

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
                engine_error!("Client construction failed: {:?}", e);
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
            engine_error!("Initialize failed: {:?}", e);
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
            engine_info!("[{:.0} ms] leader election acquired attempts={} elapsed={}ms", t(), le_attempts, le_elapsed);
        } else {
            engine_debug!("[LeaderElection] Acting as follower for tab={} attempts={} elapsed={}ms", replica_id, le_attempts, le_elapsed);
        }

        engine_info!("[{:.0} ms] READY", t());

        let event_callback: Arc<Mutex<Option<SendFunction>>> = Arc::new(Mutex::new(None));

        // Engine V2: Runtime::new() already returns Arc<Runtime>
        let runtime = Runtime::new(replica_id.to_string());
        let capability_manager = Arc::new(CapabilityManager::new());

        // Engine V2/V3: create PersistenceEngine if OPFS backend (best-effort, skip on failure)
        // Phase 3: Runtime owns the PageStore instances — set them here so all subsystems share them
        let persistence_engine: Option<Arc<dyn PersistenceEngine>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            match Self::build_persistence_engine(opfs.clone().into(), runtime.clone()).await {
                Ok(engine) => {
                    Some(engine)
                }
                Err(e) => {
                    engine_warn!("[engine] persistence_engine init skipped: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Engine V2: restore WAL checkpoint from OPFS
        if let Some(ref pe) = persistence_engine {
            match pe.load_checkpoint().await {
                Ok((seq, count)) if seq > 0 => {
                    runtime.metadata_store.lock().unwrap().cursor = seq;
                    runtime.metadata_store.lock().unwrap().pending_count = count;
                    engine_info!("[engine] restored WAL checkpoint: seq={} pending={}", seq, count);
                }
                Ok(_) => {}
                Err(e) => engine_warn!("[engine] load_checkpoint: {:?}", e),
            }
        }

        let broadcast_manager = cross_tab_channel.clone().map(|bc| {
            let ipc = Arc::new(WasmIPC::new_with_channel(bc));
            Arc::new(BroadcastManager::new(runtime.clone(), ipc, Arc::new(crate::metrics::RuntimeMetrics::default())))
        });

        // Wire CapabilityManager based on leader election
        if client.leader_election.is_leader() {
            capability_manager.set(RuntimeCapability::Leader);
            runtime.set_status(crate::runtime::RuntimeStatus::Leader);
        } else {
            capability_manager.set(RuntimeCapability::Follower);
        }

        // Phase II: check leader before moving client into result
        let is_leader = client.leader_election.is_leader();

        // Phase 4 v2: Create RuntimeCoordinator
        let runtime_coordinator = broadcast_manager.clone().map(|bm| {
            Arc::new(crate::runtime_coordinator::RuntimeCoordinator::new(
                runtime.clone(),
                bm,
                Arc::new(crate::metrics::RuntimeMetrics::default()),
                replica_id.to_string(),
            ))
        });

        if let Some(ref coord) = runtime_coordinator {
            if is_leader {
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Leading);
            } else {
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Follower);
            }
        }

        let compaction_scheduler = if is_leader {
            Some(Arc::new(CompactionScheduler::new(
                runtime.clone(),
                storage_manager.clone(),
                namespace,
            )))
        } else {
            None
        };

        // Wire compaction_scheduler into VaultRuntime's EngineContext
        if let (Some(vr), Some(cs)) = (vault_runtime.clone(), compaction_scheduler.clone()) {
            vault_runtime = Some(vr.with_compaction(cs));
        }

        let result = Self {
            client: Some(client),
            storage_manager: Some(storage_manager),
            workspace_manager: Some(workspace_manager),
            working_set_manager: Some(working_set_manager),
            replication_planner: Some(replication_planner),
            event_bus: Some(event_bus),
            presence: None,
            cross_tab_channel: cross_tab_channel.clone(),
            tab_id: replica_id.to_string(),
            event_callback,
            runtime: Some(runtime.clone()),
            vault_runtime,
            capability_manager: Some(capability_manager),
            broadcast_manager: broadcast_manager.clone(),
            runtime_coordinator,
            persistence_engine,
            compaction_scheduler,
            mirror: None,
        };

        // Phase II: new leader announces via BC
        if is_leader {
            if let Some(ref bs) = boot_state {
                bs.state.store(crate::discovery_protocol::SharedBootState::LEADER, Ordering::Relaxed);
            }
            if let Some(ref bc) = cross_tab_channel {
                let protocol = crate::discovery_protocol::DiscoveryProtocol::new(
                    bc.clone(),
                    replica_id.to_string(),
                );
                let cursor = runtime.metadata_store.lock().unwrap().cursor;
                protocol.announce_ready(cursor);
            }
        }

        Self::spawn_pending_listener(&result);
        Self::spawn_bc_message_handler(&result);

        Ok(result)
    }

    /// Helper: build OpfsPersistenceEngine from an OpfsStorage Arc.
    /// Uses the already-opened PageStore instances from PagesDir (single instance per store).
    async fn build_persistence_engine(
        storage: Arc<crate::storage::OpfsStorage>,
        runtime: Arc<Runtime>,
    ) -> Result<Arc<dyn PersistenceEngine>, VaultSyncError> {
        let doc_data = storage.pages().doc_data.clone();
        let oplog = storage.pages().oplog.clone();
        let engine = OpfsPersistenceEngine::new(storage, doc_data, oplog).with_runtime(runtime);
        Ok(Arc::new(engine))
    }

    fn broadcast_invalidation(&self, doc_id: &str, record_id: &str) {
        if let Some(ref bc) = self.cross_tab_channel {
            let msg = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&msg, &"type".into(), &"invalidate".into());
            let _ = js_sys::Reflect::set(&msg, &"doc_id".into(), &doc_id.into());
            let _ = js_sys::Reflect::set(&msg, &"record_id".into(), &record_id.into());
            let _ = js_sys::Reflect::set(&msg, &"tab_id".into(), &self.tab_id.clone().into());
            if let Ok(json) = js_sys::JSON::stringify(&msg) {
                engine_debug!(
                    "[BC invalidate] tab_id={} doc_id={} record_id={} payload={}",
                    self.tab_id, doc_id, record_id, json,
                );
                let _ = bc.post_message(&json);
            }
        } else {
            engine_debug!("[BC invalidate] skipped (no channel) tab_id={} doc_id={} record_id={}", self.tab_id, doc_id, record_id);
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
                engine_debug!(
                    "[BC command] verb={} tab_id={} doc_id={} record_id={} request_id={}",
                    verb, self.tab_id, doc_id, record_id, request_id,
                );
                let _ = bc.post_message(&json_str);
            }
        } else {
            engine_debug!("[BC command] skipped (no channel) verb={} tab_id={} doc_id={} record_id={}", verb, self.tab_id, doc_id, record_id);
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
        // Engine V2: capability check
        if let Some(ref cm) = self.capability_manager {
            if !cm.can_write() {
                return Err(JsValue::from_str("cannot execute command: runtime role is read-only"));
            }
        }
        engine_debug!("[leader] handle_command verb={} doc={} record={} tab={}", verb, doc_id, record_id, self.tab_id);
        let t0 = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now())
            .unwrap_or(0.0);
        let result = match verb {
            "insert" => {
                let fields = json_to_fields(json)?;
                let keys: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
                engine_trace!("[BC] LEADER_INSERT_BEGIN doc={} record={} field_count={} keys=[{}] payload_len={} payload={}", doc_id, record_id, fields.len(), keys.join(","), json.len(), safe_utf8_slice(json, 300));
                // Engine V2: Runtime first
                if let Some(ref rt) = self.runtime {
                    for (field, value) in &fields {
                        rt.set_field(doc_id, record_id, field, value.clone());
                    }
                }
                self.client.as_ref().unwrap().insert(doc_id, record_id, fields).await
            }
            "update" => {
                let fields = json_to_fields(json)?;
                let keys: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
                engine_trace!("[BC] LEADER_UPDATE_BEGIN doc={} record={} field_count={} keys=[{}] payload_len={} payload={}", doc_id, record_id, fields.len(), keys.join(","), json.len(), safe_utf8_slice(json, 300));
                // Engine V2: Runtime first
                if let Some(ref rt) = self.runtime {
                    for (field, value) in &fields {
                        rt.set_field(doc_id, record_id, field, value.clone());
                    }
                }
                self.client.as_ref().unwrap().update(doc_id, record_id, fields).await
            }
            "delete" => {
                if let Some(ref rt) = self.runtime {
                    rt.delete_field(doc_id, record_id, "__deleted__");
                }
                self.client.as_ref().unwrap().delete(doc_id, record_id).await
            }
            _ => return Err(JsValue::from_str(&format!("Unknown command verb: {}", verb))),
        };
        let elapsed = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now() - t0)
            .unwrap_or(0.0);
        match result {
            Ok(()) => {
                engine_debug!("[leader] command done verb={} doc={} record={} elapsed={:.1}ms", verb, doc_id, record_id, elapsed);
                self.broadcast_invalidation(doc_id, record_id);
                Ok(())
            }
            Err(e) => {
                engine_warn!("[leader] command failed verb={} doc={} record={} elapsed={:.1}ms err={:?}", verb, doc_id, record_id, elapsed, e);
                Err(JsValue::from_str(&format!("Command {} failed: {:?}", verb, e)))
            }
        }
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        // Phase 4: MirrorRuntime mode — send BC command to leader
        if self.mirror.is_some() {
            engine_trace!("[MirrorRuntime] insert doc={} record={}", doc_id, record_id);
            self.send_command("insert", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                for (field, value) in &fields {
                    self.mirror.as_ref().unwrap().apply_mutation(doc_id, record_id, field, value.clone());
                }
            }
            return Ok(());
        }

        let is_leader = self.client.as_ref().map(|c| c.leader_election.is_leader()).unwrap_or(false);
        if is_leader {
            // Engine V2: capability check
            if let Some(ref cm) = self.capability_manager {
                if !cm.can_write() {
                    return Err(JsValue::from_str("cannot write: runtime role is read-only"));
                }
            }

            let fields = json_to_fields(json)?;

            // Engine V2: Runtime first (in-memory DocumentStore + WAL), then persist
            if let Some(ref rt) = self.runtime {
                for (field, value) in &fields {
                    rt.set_field(doc_id, record_id, field, value.clone());
                }
                // Phase 5: Schedule via UploadScheduler
                rt.upload_scheduler.schedule(UploadAction::Insert, doc_id, record_id, fields.clone());
            }

            self.client
                .as_ref()
                .unwrap()
                .insert(doc_id, record_id, fields)
                .await
                .map_err(|e| JsValue::from_str(&format!("Insert failed: {:?}", e)))?;

            // Engine V2: checkpoint WAL if threshold reached
            if let (Some(ref rt), Some(ref pe)) = (self.runtime.clone(), self.persistence_engine.clone()) {
                if rt.wal.lock().unwrap().needs_checkpoint() {
                    if let Err(e) = pe.checkpoint_wal().await {
                        engine_warn!("[engine] checkpoint_wal failed: {:?}", e);
                    }
                }
            }

            // Engine V2: broadcast structured mutation
            if let Some(ref bm) = self.broadcast_manager {
                bm.broadcast_mutation(doc_id, record_id, json);
            } else {
                self.broadcast_invalidation(doc_id, record_id);
            }
            Ok(())
        } else {
            engine_trace!("[BC] FOLLOW_INSERT_BEGIN doc={} record={} payload_len={} payload={}", doc_id, record_id, json.len(), safe_utf8_slice(json, 500));
            self.send_command("insert", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                // Engine V2: apply follower mutation to local Runtime cache
                if let Some(ref rt) = self.runtime {
                    for (field, value) in &fields {
                        rt.set_field(doc_id, record_id, field, value.clone());
                    }
                }
                self.client.as_ref().unwrap().fire_local_subscription(doc_id, record_id, &fields);
            }
            Ok(())
        }
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, json: &str) -> Result<(), JsValue> {
        // Phase 4: MirrorRuntime mode — send BC command to leader
        if self.mirror.is_some() {
            engine_trace!("[MirrorRuntime] update doc={} record={}", doc_id, record_id);
            self.send_command("update", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                for (field, value) in &fields {
                    self.mirror.as_ref().unwrap().apply_mutation(doc_id, record_id, field, value.clone());
                }
            }
            return Ok(());
        }

        let is_leader = self.client.as_ref().map(|c| c.leader_election.is_leader()).unwrap_or(false);
        if is_leader {
            // Engine V2: capability check
            if let Some(ref cm) = self.capability_manager {
                if !cm.can_write() {
                    return Err(JsValue::from_str("cannot write: runtime role is read-only"));
                }
            }

            let fields = json_to_fields(json)?;

            // Engine V2: Runtime first
            if let Some(ref rt) = self.runtime {
                for (field, value) in &fields {
                    rt.set_field(doc_id, record_id, field, value.clone());
                }
                // Phase 5: Schedule via UploadScheduler
                rt.upload_scheduler.schedule(UploadAction::Update, doc_id, record_id, fields.clone());
            }

            self.client
                .as_ref()
                .unwrap()
                .update(doc_id, record_id, fields)
                .await
                .map_err(|e| JsValue::from_str(&format!("Update failed: {:?}", e)))?;

            // Engine V2: checkpoint WAL if threshold reached
            if let (Some(ref rt), Some(ref pe)) = (self.runtime.clone(), self.persistence_engine.clone()) {
                if rt.wal.lock().unwrap().needs_checkpoint() {
                    if let Err(e) = pe.checkpoint_wal().await {
                        engine_warn!("[engine] checkpoint_wal failed: {:?}", e);
                    }
                }
            }

            // Engine V2: broadcast structured mutation
            if let Some(ref bm) = self.broadcast_manager {
                bm.broadcast_mutation(doc_id, record_id, json);
            } else {
                self.broadcast_invalidation(doc_id, record_id);
            }
            Ok(())
        } else {
            engine_trace!("[BC] FOLLOW_UPDATE_BEGIN doc={} record={} payload_len={} payload={}", doc_id, record_id, json.len(), safe_utf8_slice(json, 500));
            self.send_command("update", doc_id, record_id, json);
            if let Ok(fields) = json_to_fields(json) {
                if let Some(ref rt) = self.runtime {
                    for (field, value) in &fields {
                        rt.set_field(doc_id, record_id, field, value.clone());
                    }
                }
                self.client.as_ref().unwrap().fire_local_subscription(doc_id, record_id, &fields);
            }
            Ok(())
        }
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        // Phase 4: MirrorRuntime mode — send BC command to leader
        if self.mirror.is_some() {
            engine_debug!("[MirrorRuntime] delete doc={} record={}", doc_id, record_id);
            self.send_command("delete", doc_id, record_id, "");
            self.mirror.as_ref().unwrap().remove_record(doc_id, record_id);
            return Ok(());
        }

        let is_leader = self.client.as_ref().map(|c| c.leader_election.is_leader()).unwrap_or(false);
        if is_leader {
            // Engine V2: capability check
            if let Some(ref cm) = self.capability_manager {
                if !cm.can_write() {
                    return Err(JsValue::from_str("cannot write: runtime role is read-only"));
                }
            }

            // Engine V2: Runtime first
            if let Some(ref rt) = self.runtime {
                rt.delete_field(doc_id, record_id, "__deleted__");
                // Phase 5: Schedule delete via UploadScheduler
                rt.upload_scheduler.schedule_delete(doc_id, record_id);
            }

            self.client
                .as_ref()
                .unwrap()
                .delete(doc_id, record_id)
                .await
                .map_err(|e| JsValue::from_str(&format!("Delete failed: {:?}", e)))?;

            // Engine V2: checkpoint WAL if threshold reached
            if let (Some(ref rt), Some(ref pe)) = (self.runtime.clone(), self.persistence_engine.clone()) {
                if rt.wal.lock().unwrap().needs_checkpoint() {
                    if let Err(e) = pe.checkpoint_wal().await {
                        engine_warn!("[engine] checkpoint_wal failed: {:?}", e);
                    }
                }
            }

            // Engine V2: broadcast structured mutation
            if let Some(ref bm) = self.broadcast_manager {
                bm.broadcast_mutation(doc_id, record_id, "{\"__deleted__\":true}");
            } else {
                self.broadcast_invalidation(doc_id, record_id);
            }
            Ok(())
        } else {
            engine_debug!("[Follower delete] sending command doc={} record={}", doc_id, record_id);
            self.send_command("delete", doc_id, record_id, "");
            Ok(())
        }
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<JsValue, JsValue> {
        // Phase 4: MirrorRuntime mode — read from cache
        if let Some(ref mirror) = self.mirror {
            if let Some(fields) = mirror.get(doc_id, record_id) {
                engine_trace!("[get] from MirrorRuntime cache doc={} record={}", doc_id, record_id);
                return fields_to_jsvalue(&fields);
            }
            engine_trace!("[get] from MirrorRuntime cache doc={} record={} not_found", doc_id, record_id);
            return Ok(JsValue::NULL);
        }

        // Phase 3: check Runtime's in-memory document store first (O(1), no OPFS scan)
        if let Some(ref rt) = self.runtime {
            let store = rt.document_store.lock().unwrap();
            if let Some(fields) = store.get_record(doc_id, record_id) {
                let fields_owned = fields.clone();
                drop(store);
                engine_trace!("[get] from Runtime cache doc={} record={}", doc_id, record_id);
                return fields_to_jsvalue(&fields_owned);
            }
            drop(store);
        }
        engine_debug!("[get] from OPFS doc={} record={}", doc_id, record_id);
        let doc_opt = self
            .client
            .as_ref()
            .unwrap()
            .get(doc_id, record_id)
            .await
            .map_err(|e| JsValue::from_str(&format!("Get failed: {:?}", e)))?;

        match doc_opt {
            Some(fields) => fields_to_jsvalue(&fields),
            None => Ok(JsValue::NULL),
        }
    }

    pub async fn shutdown(&self) -> Result<(), JsValue> {
        if let Some(ref mirror) = self.mirror {
            mirror.clear();
            engine_info!("[MirrorRuntime] shutdown");
            return Ok(());
        }
        self.client
            .as_ref()
            .unwrap()
            .shutdown()
            .await
            .map_err(|e| JsValue::from_str(&format!("Shutdown failed: {:?}", e)))?;
        Ok(())
    }

    pub async fn fire_subscription(&self, doc_id: &str, record_id: &str) -> Result<(), JsValue> {
        // Phase 4: MirrorRuntime mode — fire from cache
        if let Some(ref mirror) = self.mirror {
            if let Some(fields) = mirror.get(doc_id, record_id) {
                mirror.fire_subscription(doc_id, record_id, &fields);
            }
            return Ok(());
        }

        let client = self.client.as_ref().unwrap();
        match client.get(doc_id, record_id).await {
            Ok(Some(state)) => {
                let keys: Vec<String> = state.keys().cloned().collect();
                engine_debug!(
                    "[BC fire_subscription] doc={} record={} fields={} keys={:?}",
                    doc_id, record_id, state.len(), keys,
                );
                client.fire_local_subscription(doc_id, record_id, &state);
            }
            Ok(None) => {
                engine_debug!("[BC fire_subscription] doc={} record={} not_found=true", doc_id, record_id);
            }
            Err(e) => {
                engine_debug!("[BC fire_subscription] doc={} record={} error={:?}", doc_id, record_id, e);
            }
        }
        Ok(())
    }

    pub async fn find(&self, doc_id: &str) -> Result<js_sys::Array, JsValue> {
        // Phase 4: MirrorRuntime mode — read from cache
        if let Some(ref mirror) = self.mirror {
            let records = mirror.query_doc(doc_id);
            let arr = js_sys::Array::new();
            for fields in records {
                let json_str = fields_to_json_string(&fields)
                    .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
                arr.push(&JsValue::from_str(&json_str));
            }
            return Ok(arr);
        }

        // Phase 3: check Runtime's in-memory document store first (O(1), no OPFS scan)
        if let Some(ref rt) = self.runtime {
            let store = rt.document_store.lock().unwrap();
            let records: Vec<HashMap<String, CrdtValue>> = store.query_doc(doc_id)
                .into_iter()
                .map(|r| r.clone())
                .collect();
            if !records.is_empty() {
                drop(store);
                engine_trace!("[find] from Runtime cache doc={} count={}", doc_id, records.len());
                let arr = js_sys::Array::new();
                for fields in records {
                    let json_str = fields_to_json_string(&fields)
                        .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
                    arr.push(&JsValue::from_str(&json_str));
                }
                return Ok(arr);
            }
            drop(store);
        }
        engine_debug!("[find] from OPFS doc={}", doc_id);
        let records = self
            .client
            .as_ref()
            .unwrap()
            .find(doc_id, None)
            .await
            .map_err(|e| JsValue::from_str(&format!("Find failed: {:?}", e)))?;

        let arr = js_sys::Array::new();
        for fields in records {
            let json_str = fields_to_json_string(&fields)
                .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
            arr.push(&JsValue::from_str(&json_str));
        }
        Ok(arr)
    }

    pub async fn sync_status(&self) -> Result<JsValue, JsValue> {
        // Phase 4: MirrorRuntime mode — return basic status
        if let Some(ref mirror) = self.mirror {
            let mut map = serde_json::Map::new();
            map.insert("connected".to_string(), serde_json::Value::Bool(mirror.leader_alive(5000)));
            map.insert("pendingMutations".to_string(), serde_json::Value::Number(serde_json::Number::from(mirror.pending_count.load(std::sync::atomic::Ordering::Acquire))));
            return Ok(JsValue::from_str(&serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".to_string())));
        }

        let state = self
            .client
            .as_ref()
            .unwrap()
            .sync_status()
            .await
            .map_err(|e| JsValue::from_str(&format!("Sync status failed: {:?}", e)))?;
        let pending = self
            .client
            .as_ref()
            .unwrap()
            .pending_uploads()
            .await
            .map_err(|e| JsValue::from_str(&format!("Pending uploads failed: {:?}", e)))?;
        engine_debug!("[pending] js_query returns={}", pending);

        let connected =
            state.connection_status != vaultsync_core::sync::state::ConnectionStatus::Disconnected;

        let metrics = self.client.as_ref().unwrap().metrics.snapshot();

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

    /// Engine V2: expose Runtime + PersistenceEngine health stats to JS
    pub fn runtime_storage_stats(&self) -> Result<JsValue, JsValue> {
        let mut map = serde_json::Map::new();

        if let Some(ref rt) = self.runtime {
            let meta = rt.metadata_store.lock().unwrap();
            map.insert("cursor".to_string(), serde_json::Value::Number(serde_json::Number::from(meta.cursor)));
            map.insert("pendingCount".to_string(), serde_json::Value::Number(serde_json::Number::from(meta.pending_count as u64)));
            drop(meta);
            let wal = rt.wal.lock().unwrap();
            map.insert("walLength".to_string(), serde_json::Value::Number(serde_json::Number::from(wal.len() as u64)));
            map.insert("walLastCheckpoint".to_string(), serde_json::Value::Number(serde_json::Number::from(wal.last_checkpoint_time())));
        }
        if let Some(ref pe) = self.persistence_engine {
            let health = pe.health();
            map.insert("isHealthy".to_string(), serde_json::Value::Bool(health.wal_ok && health.manifest_ok && !health.corruption_detected));
            map.insert("opfsAvailable".to_string(), serde_json::Value::Bool(health.opfs_available));
            map.insert("manifestOk".to_string(), serde_json::Value::Bool(health.manifest_ok));
            map.insert("walOk".to_string(), serde_json::Value::Bool(health.wal_ok));
            map.insert("pageCacheUsed".to_string(), serde_json::Value::Number(serde_json::Number::from(health.page_cache_usage.0 as u64)));
            map.insert("pageCacheCapacity".to_string(), serde_json::Value::Number(serde_json::Number::from(health.page_cache_usage.1 as u64)));
            map.insert("lastCheckpointSeq".to_string(), serde_json::Value::Number(serde_json::Number::from(health.last_checkpoint)));
            map.insert("corruptionDetected".to_string(), serde_json::Value::Bool(health.corruption_detected));
        }

        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| JsValue::from_str(&format!("Serialize error: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn on_event(&self, callback: js_sys::Function) {
        *self.event_callback.lock().unwrap() = Some(SendFunction(JsValue::from(callback)));
    }

    /// Engine V2/V3: Listen for messages on BC — routes all messages through RuntimeCoordinator first.
    fn spawn_bc_message_handler(this: &Self) {
        if let Some(ref bc) = this.cross_tab_channel {
            let rt = this.runtime.clone();
            let client = this.client.clone();
            let bm = this.broadcast_manager.clone();
            let tab_id = this.tab_id.clone();
            let cm = this.capability_manager.clone();
            let coord = this.runtime_coordinator.clone();

            let onmsg = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Some(msg_str) = e.data().as_string() {
                    // Phase 4 v2: Route protocol messages through RuntimeCoordinator first
                    if let Some(ref c) = coord {
                        if c.handle_bc_message(&msg_str, &tab_id) {
                            // Coordinator handled it (HELLO, DISCOVER, SNAPSHOT_REQUEST, etc.)
                            return;
                        }
                    }
                    // Engine V2 MUTATION: apply directly to Runtime cache
                    if msg_str.starts_with("MUTATION|") {
                        if let Some(ref rt) = rt {
                            let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
                            if parts.len() >= 5 {
                                let doc_id = parts[2];
                                let record_id = parts[3];
                                let fields_str = parts[4];
                                if let Ok(fields) = json_to_fields(fields_str) {
                                    for (field, value) in &fields {
                                        rt.set_field(doc_id, record_id, field, value.clone());
                                    }
                                }
                            }
                        }
                        return;
                    }

                    // LEADER_ELECTED: another tab won election
                    if msg_str.starts_with("LEADER_ELECTED|") {
                        let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
                        let other_tab = parts.get(1).unwrap_or(&"");
                        if other_tab != &tab_id {
                            if let Some(ref cm) = cm {
                                if cm.is_leader() {
                                    cm.set(RuntimeCapability::Follower);
                                    if let Some(ref rt) = rt {
                                        rt.set_status(crate::runtime::RuntimeStatus::Follower);
                                    }
                                    engine_info!("[BC] demoted by LEADER_ELECTED from tab={}", other_tab);
                                }
                            }
                            // Send FOLLOWER_ATTACH
                            if let Some(ref bm) = bm {
                                let msg = format!("FOLLOWER_ATTACH|{}|{}|leader", tab_id, 1);
                                let _ = bm.channel.send(&msg);
                            }
                        }
                        return;
                    }

                    // LEADER_TRANSFER: sync state from old leader
                    if msg_str.starts_with("LEADER_TRANSFER|") {
                        let parts: Vec<&str> = msg_str.splitn(4, '|').collect();
                        if parts.len() >= 4 {
                            if let Some(ref rt) = rt {
                                let mut meta = rt.metadata_store.lock().unwrap();
                                meta.runtime_gen = crate::runtime::RuntimeGeneration(parts[1].parse().unwrap_or(0));
                                meta.bus_gen = crate::runtime::BusGeneration(parts[2].parse().unwrap_or(0));
                                meta.cursor = parts[3].parse().unwrap_or(0);
                            }
                        }
                        return;
                    }

                    // Legacy invalidation: trigger subscription re-fire
                    if let Some(ref client) = client {
                        if let Ok(val) = js_sys::JSON::parse(&msg_str) {
                            let msg_type = js_sys::Reflect::get(&val, &"type".into())
                                .ok().and_then(|v| v.as_string());
                            if msg_type.as_deref() == Some("invalidate") {
                                let doc_id = js_sys::Reflect::get(&val, &"doc_id".into())
                                    .ok().and_then(|v| v.as_string()).unwrap_or_default();
                                let record_id = js_sys::Reflect::get(&val, &"record_id".into())
                                    .ok().and_then(|v| v.as_string()).unwrap_or_default();
                                let c = client.clone();
                                let _ = wasm_bindgen_futures::spawn_local(
                                    async move {
                                        let _ = c.fire_local_subscription(&doc_id, &record_id, &HashMap::new());
                                    }
                                );
                            }
                        }
                    }
                }
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);

            bc.set_onmessage(Some(onmsg.as_ref().unchecked_ref()));
            onmsg.forget();
        }
    }

    fn spawn_pending_listener(this: &Self) {
        let rx = this.client.as_ref().and_then(|c| c.take_pending_count_rx());
        match rx {
            Some(rx) => {
                let cb = this.event_callback.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    futures::pin_mut!(rx);
                    while let Some(count) = rx.next().await {
                        engine_trace!("[event] pending_count={}", count);
                        let func = {
                            let guard = cb.lock().unwrap();
                            guard.as_ref().map(|sf| sf.0.clone())
                        };
                        match func {
                            Some(val) => {
                                let f: js_sys::Function = val.unchecked_into();
                                let _ = f.call1(
                                    &JsValue::NULL,
                                    &JsValue::from(ClientEvent::StatusDirty as u32),
                                );
                            }
                            None => break,
                        }
                    }
                    engine_debug!("[event] pending_listener ended");
                });
            }
            None => {}
        }
    }

    pub fn subscribe(&self, doc_id: &str, callback: js_sys::Function) -> WasmSubscriptionHandle {
        // Phase 4: MirrorRuntime mode — subscribe via MirrorRuntime's callback list
        if let Some(ref mirror) = self.mirror {
            engine_debug!("[MirrorRuntime] subscribe doc={}", doc_id);
            let sub_id = mirror.subscribe(doc_id, callback);
            return WasmSubscriptionHandle {
                handle: None,
                mirror_unsub: Some((mirror.clone(), doc_id.to_string(), sub_id)),
            };
        }

        // Leader mode — use core VaultSyncClient subscription
        engine_debug!("[timing] subscribe call doc={} t={:.0}ms", doc_id, js_sys::Date::now());
        static NOTIFY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let send_cb = SendFunction(JsValue::from(callback));
        let handle = self.client.as_ref().unwrap().subscribe(
            doc_id,
            Box::new(move |_doc_id, record_id, fields| {
                let notify_seq = NOTIFY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                engine_debug!(
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
                        engine_debug!(
                            "[notify] seq={} record={} phase=spawn_local t={:.0}ms",
                            seq,
                            record_id_owned,
                            js_sys::Date::now()
                        );
                        if cb_clone.is_null() || cb_clone.is_undefined() {
                            engine_warn!("[notify] seq={} record={} null_callback=true", seq, record_id_owned);
                            return;
                        }
                        let func: js_sys::Function = cb_clone.unchecked_into();
                        let record_id_js = JsValue::from_str(&record_id_owned);
                        let json_js = JsValue::from_str(&json_str);
                        let seq_js = JsValue::from_f64(seq as f64);
                        if let Err(e) = func.call3(&JsValue::NULL, &record_id_js, &json_js, &seq_js) {
                            engine_error!("[notify] seq={} record={} callback_error={:?}", seq, record_id_owned, e);
                        }
                    });
                }
            }),
        );

        WasmSubscriptionHandle {
            handle: Some(handle),
            mirror_unsub: None,
        }
    }

    pub fn unsubscribe(&self, handle: &mut WasmSubscriptionHandle) -> Result<(), JsValue> {
        handle.cancel(self)
    }

    pub async fn rotate_keys(&self) -> Result<JsValue, JsValue> {
        let client = self.client.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        client.rotate_keys().await.map_err(|e| JsValue::from_str(&format!("Rotate keys failed: {:?}", e)))?;
        let active = client.active_key_version();
        Ok(JsValue::from_f64(active as f64))
    }

    pub fn active_key_version(&self) -> u64 {
        self.client.as_ref().map(|c| c.active_key_version()).unwrap_or(0)
    }

    pub fn list_key_versions(&self) -> Result<JsValue, JsValue> {
        let client = self.client.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let keys = client.list_key_versions();
        let active = client.active_key_version();
        let mut list = Vec::new();
        for key in keys {
            list.push(serde_json::json!({
                "version": key.version,
                "createdAt": key.created_at * 1000,
                "isActive": key.version == active,
            }));
        }
        let json_str = serde_json::to_string(&list)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn prune_key_versions(&self, keep_versions: u32) -> Result<(), JsValue> {
        if let Some(ref client) = self.client {
            client.prune_key_versions(keep_versions as u64);
        }
        Ok(())
    }

    pub async fn define_schema(&self, doc_id: &str, schema_json: &str) -> Result<(), JsValue> {
        let client = self.client.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let schema: vaultsync_core::schema::registry::DocumentSchema =
            serde_json::from_str(schema_json)
                .map_err(|e| JsValue::from_str(&format!("Invalid schema JSON: {:?}", e)))?;
        client
            .define_schema(doc_id, schema)
            .await
            .map_err(|e| JsValue::from_str(&format!("Define schema failed: {:?}", e)))?;
        Ok(())
    }

    /// Phase 4f: Check if mirror should promote to leader (leader heartbeat timeout).
    /// Returns true if leader is gone and JS should reinitialize with new_with_coordinator().
    #[wasm_bindgen(js_name = checkMirrorPromotion)]
    pub fn check_mirror_promotion(&self) -> bool {
        self.mirror.as_ref().map(|m| m.promote_to_leader()).unwrap_or(false)
    }

    /// Phase 5: Flush pending uploads through the UploadScheduler.
    /// Drains pending mutations from the debounced queue and processes them
    /// through VaultSyncClient. Returns JSON with processed count and status.
    /// Call this periodically (e.g., every 100ms via JS setInterval).
    #[wasm_bindgen(js_name = flushPendingUploads)]
    pub async fn flush_pending_uploads(&self) -> Result<String, JsValue> {
        let rt = match self.runtime.as_ref() {
            Some(rt) => rt,
            None => return Ok(r#"{"processed":0,"pending":0,"reason":"no_runtime"}"#.to_string()),
        };
        let batch = rt.upload_scheduler.flush();
        if batch.is_empty() {
            return Ok(format!(
                r#"{{"processed":0,"pending":{},"reason":"debounce"}}"#,
                rt.upload_scheduler.pending_count()
            ));
        }
        let mut processed = 0u64;
        let mut errors = 0u64;
        let client = match self.client.as_ref() {
            Some(c) => c,
            None => return Ok(r#"{"processed":0,"pending":0,"reason":"no_client"}"#.to_string()),
        };
        for upload in &batch {
            let result = match upload.action {
                UploadAction::Insert | UploadAction::Update => {
                    client.insert(&upload.doc_id, &upload.record_id, upload.fields.clone()).await
                }
                UploadAction::Delete => {
                    client.delete(&upload.doc_id, &upload.record_id).await
                }
            };
            match result {
                Ok(_) => processed += 1,
                Err(e) => {
                    errors += 1;
                    engine_warn!("[UploadScheduler] flush error doc={} record={} err={:?}", upload.doc_id, upload.record_id, e);
                }
            }
        }
        let pending = rt.upload_scheduler.pending_count();
        rt.metrics.page_writes.fetch_add(processed, std::sync::atomic::Ordering::Relaxed);
        Ok(format!(
            r#"{{"processed":{},"errors":{},"pending":{}}}"#,
            processed, errors, pending
        ))
    }

    pub fn is_leader(&self) -> bool {
        self.client.as_ref().map(|c| c.leader_election.is_leader()).unwrap_or(false)
    }

    /// Returns a clone of the PresenceManager if available.
    #[wasm_bindgen]
    pub fn presence(&self) -> Option<crate::presence::PresenceManager> {
        self.presence.clone()
    }

    /// Returns a JSON snapshot of all metrics counters.
    /// Phase 4: Returns mirror metrics in follower mode.
    #[wasm_bindgen(js_name = metricsSnapshot)]
    pub fn metrics_snapshot(&self) -> String {
        if let Some(ref mirror) = self.mirror {
            return mirror.metrics.snapshot_json();
        }
        if let Some(ref client) = self.client {
            let snapshot = client.metrics.snapshot();
            return serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string());
        }
        "{}".to_string()
    }

    /// Phase 6: Try auto-compaction via CompactionScheduler.
    /// Returns JSON with compaction stats, or null if no compaction was needed.
    /// JS should call this periodically (e.g., every 30s via setInterval).
    #[wasm_bindgen(js_name = tryCompact)]
    pub async fn try_compact(&self) -> Result<JsValue, JsValue> {
        let cs = self.vault_runtime.as_ref()
            .and_then(|vr| vr.compaction())
            .or_else(|| self.compaction_scheduler.as_ref());
        match cs {
            Some(cs) => {
                match cs.try_compact().await {
                    Some(json) => {
                        let val: serde_json::Value = serde_json::from_str(&json)
                            .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;
                        Ok(JsValue::from_str(&val.to_string()))
                    }
                    None => Ok(JsValue::NULL),
                }
            }
            None => Ok(JsValue::NULL),
        }
    }

    /// Phase 6: Returns last compaction duration in ms, or 0 if never run.
    #[wasm_bindgen(js_name = lastCompactionMs)]
    pub fn last_compaction_ms(&self) -> u64 {
        let cs = self.vault_runtime.as_ref()
            .and_then(|vr| vr.compaction())
            .or_else(|| self.compaction_scheduler.as_ref());
        cs.map(|cs| cs.last_compaction_ms())
            .unwrap_or(0)
    }

    /// Runs compaction on the given namespace, returns JSON stats.
    #[wasm_bindgen(js_name = compactNamespace)]
    pub async fn compact_namespace(&self, namespace: &str) -> Result<String, JsValue> {
        let sm = self.storage_manager.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let stats = sm
            .compact_namespace(namespace)
            .await
            .map_err(|e| JsValue::from_str(&format!("Compaction failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Runs lifecycle (tombstone cleanup) on the given namespace, returns JSON stats.
    #[wasm_bindgen(js_name = runLifecycle)]
    pub async fn run_lifecycle(&self, namespace: &str) -> Result<String, JsValue> {
        let sm = self.storage_manager.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let stats = sm
            .run_lifecycle(namespace)
            .await
            .map_err(|e| JsValue::from_str(&format!("Lifecycle failed: {:?}", e)))?;
        serde_json::to_string(&stats)
            .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))
    }

    /// Returns JSON with storage statistics (page count, segment state, etc.).
    #[wasm_bindgen(js_name = storageStats)]
    pub async fn storage_stats(&self) -> Result<String, JsValue> {
        let sm = self.storage_manager.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let stats = sm
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
        let sm = self.storage_manager.as_ref().ok_or_else(|| JsValue::from_str("not available in follower mode"))?;
        let stats = sm
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
            manager: self.workspace_manager.clone().unwrap_or_else(|| Arc::new(WorkspaceManager::new())),
        }
    }

    /// Returns the working sets namespace proxy for CRUD operations.
    #[wasm_bindgen(js_name = workingSets)]
    pub fn working_sets(&self) -> WorkingSetsNamespace {
        WorkingSetsNamespace {
            manager: self.working_set_manager.clone().unwrap_or_else(|| Arc::new(WorkingSetManager::new())),
        }
    }

    /// Returns the replication namespace proxy.
    #[wasm_bindgen(js_name = replication)]
    pub fn replication(&self) -> ReplicationNamespace {
        ReplicationNamespace {
            planner: self.replication_planner.clone().unwrap_or_else(|| Arc::new(ReplicationPlanner::new())),
        }
    }

    /// Returns the event bus for publishing/subscribing to engine events.
    #[wasm_bindgen(js_name = events)]
    pub fn events(&self) -> EventBusProxy {
        EventBusProxy {
            bus: self.event_bus.clone().unwrap_or_else(|| Arc::new(EventBus::new())),
        }
    }

    /// Run a single maintenance tick. Returns number of phases that ran.
    #[wasm_bindgen(js_name = maintenanceTick)]
    pub fn maintenance_tick(&self) -> u32 {
        if let Some(ref vr) = self.vault_runtime {
            if let Some(ref m) = vr.maintenance() {
                return m.tick();
            }
        }
        0
    }

    /// Check if cache eviction is needed.
    #[wasm_bindgen(js_name = cacheNeedsEviction)]
    pub fn cache_needs_eviction(&self) -> bool {
        if let Some(ref vr) = self.vault_runtime {
            return vr.cache().should_evict();
        }
        false
    }

    /// Get cache stats as JSON.
    #[wasm_bindgen(js_name = cacheStats)]
    pub fn cache_stats(&self) -> String {
        if let Some(ref vr) = self.vault_runtime {
            let cache = vr.cache();
            format!(
                r#"{{"budget":{},"usage":{},"watermark":{},"shouldEvict":{}}}"#,
                cache.budget(),
                cache.estimated_usage(),
                cache.watermark(),
                cache.should_evict(),
            )
        } else {
            r#"{"budget":0,"usage":0,"watermark":0,"shouldEvict":false}"#.to_string()
        }
    }

    /// List active namespaces as JSON array.
    #[wasm_bindgen(js_name = listNamespaces)]
    pub fn list_namespaces(&self) -> Vec<JsValue> {
        if let Some(ref vr) = self.vault_runtime {
            vr.workspace()
                .list()
                .into_iter()
                .map(|n| JsValue::from_str(&n))
                .collect()
        } else {
            Vec::new()
        }
    }
}

#[wasm_bindgen]
pub struct WasmSubscriptionHandle {
    handle: Option<vaultsync_core::subscription::engine::SubscriptionHandle>,
    /// Phase 4: MirrorRuntime subscription info (mirror, doc_id, sub_id)
    mirror_unsub: Option<(Arc<MirrorRuntime>, String, u64)>,
}

#[wasm_bindgen]
impl WasmSubscriptionHandle {
    pub fn cancel(&mut self, client: &VaultSyncRuntime) -> Result<(), JsValue> {
        if let Some(h) = self.handle.take() {
            if let Some(ref core_client) = client.client {
                core_client
                    .unsubscribe(h)
                    .map_err(|e| JsValue::from_str(&format!("Unsubscribe failed: {:?}", e)))?;
            }
        }
        if let Some((ref mirror, ref doc_id, sub_id)) = self.mirror_unsub.take() {
            mirror.unsubscribe(doc_id, sub_id);
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

/// Phase 3: Serialize fields to a JSON string for JS consumption.
pub(crate) fn fields_to_json_string(fields: &HashMap<String, CrdtValue>) -> Result<String, serde_json::Error> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        let json_val = match v {
            CrdtValue::String(s) => serde_json::Value::String(s.clone()),
            CrdtValue::Number(n) => serde_json::Value::Number(
                serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            CrdtValue::Boolean(b) => serde_json::Value::Bool(*b),
            CrdtValue::Null => serde_json::Value::Null,
            _ => serde_json::Value::Null,
        };
        map.insert(k.clone(), json_val);
    }
    serde_json::to_string(&serde_json::Value::Object(map))
}

/// Phase 3: Serialize fields to a JsValue for JS consumption.
fn fields_to_jsvalue(fields: &HashMap<String, CrdtValue>) -> Result<JsValue, JsValue> {
    let json_str = fields_to_json_string(fields)
        .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
    Ok(JsValue::from_str(&json_str))
}
