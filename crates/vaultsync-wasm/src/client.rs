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
use crate::session_protocol::BC_MSG_SEQ;
use crate::upload_scheduler::{PendingUpload, UploadAction};
use crate::compaction_scheduler::CompactionScheduler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

/// Monotonic counter for tracing mutations end-to-end.
/// Each broadcast_mutation call increments this and logs the id.
/// Follower logs the same (doc_id, record_id, payload_len) to correlate.
static TRACE_ID: AtomicU64 = AtomicU64::new(1);
/// Monotonic counter for tracing replay sessions end-to-end.
/// Included in SYNC_BEGIN/SYNC_DONE to match BEGIN↔DONE pairs.
static REPLAY_ID: AtomicU64 = AtomicU64::new(1);
use futures::channel::oneshot;
use std::time::Duration;
use vaultsync_core::runtime_state::RuntimeLifecycle;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::e2ee::keyring::KeyRing;
use vaultsync_core::storage::compaction::CompactionPolicy;
use vaultsync_core::storage::manager::{DefaultStorageManager, StorageManager};
use vaultsync_core::storage::traits::ReplayContext;
use vaultsync_core::storage::traits::ReplaySource;
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

/// SyncStateStore that reads/writes cursor+generation in memory AND persists
/// every write through the MetadataStore-backed Storage trait.
/// Legacy PersistentSyncStateStore is replaced by InMemorySyncStateStore.
/// Cursor and generation are persisted via MetadataRuntime on checkpoint,
/// not on every cursor update. This eliminates 2 OPFS writes per mutation.

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
    /// Phase 3: ReplayEngine — session replay from persisted storage via DocumentReader.
    /// Present on both leader and follower paths (leader uses it for SYNC responses).
    replay_engine: Option<Arc<crate::replay_engine::ReplayEngine>>,
    /// Phase 4: LeaderElection for follower tab — detects when lock becomes available.
    /// Used by promote_to_leader() and future in-Rust promote() method.
    leader_election: Option<Arc<vaultsync_core::ipc::leader_election::LeaderElection>>,
    /// Sprint D: Web Lock Lease held during leadership. Kept alive so the lock is
    /// never released. When the tab demotes, this field is set to None and the
    /// Lease is dropped, releasing the lock to the next waiting follower.
    leader_lease: Option<vaultsync_core::ipc::leader_election::Lease>,
    /// Phase 3: LifecycleManager — single background tick loop for maintenance.
    lifecycle_manager: Option<Arc<crate::lifecycle_manager::LifecycleManager>>,
    /// Step 4: oneshot receiver for event-driven promotion notification.
    /// Resolved when LeaderEvent::Acquired fires (Web Lock granted) or LEFT received.
    promotion_rx: Mutex<Option<oneshot::Receiver<()>>>,
    /// Saved config for demote follower re-creation.
    saved_namespace: Mutex<String>,
    saved_coordinator_url: Mutex<String>,
    saved_auth_token: Mutex<Option<String>>,
    saved_db_name: Mutex<Option<String>>,
    saved_storage_backend: Mutex<Option<String>>,
}

/// Step 8: Promotion pipeline context — each field populated by exactly one phase.
/// Keeps intermediate state between PromoteCtx flows without individual variables.
/// Defined outside #[wasm_bindgen] impl since wasm-bindgen doesn't support inner structs.
#[allow(dead_code)]
struct PromoteCtx {
    mirror: Arc<MirrorRuntime>,
    mirror_cursor: u64,
    doc_store: Arc<std::sync::Mutex<crate::runtime::DocumentStore>>,
    promote_lease: Option<vaultsync_core::ipc::leader_election::Lease>,
    vault_runtime: Option<Arc<VaultRuntime>>,
    storage_manager: Arc<dyn StorageManager>,
    runtime: Arc<crate::runtime::Runtime>,
    capability_manager: Arc<CapabilityManager>,
    persistence_engine: Option<Arc<dyn PersistenceEngine>>,
    broadcast_manager: Option<Arc<BroadcastManager>>,
    compaction_scheduler: Option<Arc<CompactionScheduler>>,
    replay_engine: Option<Arc<crate::replay_engine::ReplayEngine>>,
    runtime_coordinator: Option<Arc<crate::runtime_coordinator::RuntimeCoordinator>>,
    workspace_manager: Arc<WorkspaceManager>,
    working_set_manager: Arc<WorkingSetManager>,
    replication_planner: Arc<ReplicationPlanner>,
    event_bus: Arc<EventBus>,
    client: Arc<VaultSyncClient>,
    saved_auth: Option<String>,
    saved_db: Option<String>,
    saved_backend: Option<String>,
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
        // Phase 6: Register default namespace in WorkspaceRuntime
        let mut vault_runtime: Option<Arc<VaultRuntime>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            let pages = opfs.pages().clone();
            let metrics = Arc::new(crate::metrics::RuntimeMetrics::default());
            match StorageRuntime::new(pages, metrics.clone()).await {
                Ok(storage_runtime) => {
                    opfs.set_storage_runtime(storage_runtime.clone());
                    let sync_rt = crate::sync_runtime::SyncRuntime::new();
                    if let Some(mr) = opfs.try_load_metadata_runtime().await {
                        opfs.set_metadata_runtime(mr.clone());
                        storage_runtime.set_metadata_runtime(mr.clone());
                        // B3: Wire SyncRuntime → MetadataRuntime for checkpoint persistence
                        sync_rt.with_metadata(mr, namespace);
                    }
                    let runtime = Runtime::new(replica_id.to_string());
                    let scheduler = storage_runtime.scheduler.clone();
                    let vr = VaultRuntime::new(runtime, storage_runtime.clone(), scheduler, metrics);
                    let sub_index = crate::subscription_index::SubscriptionIndex::new();
                    let ns = crate::namespace_runtime::NamespaceRuntime::new(
                        "default".to_string(),
                        storage_runtime,
                        sync_rt,
                        sub_index,
                    );
                    vr.workspace().register(ns);
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

        // Phase 1: Create browser storage with runtime wiring
        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
        );

        // Phase 4.0: Attach MaintenanceRuntime to VaultRuntime (best-effort)
        if let Some(ref vr) = vault_runtime {
            let metadata_runtime = vr.storage().get_metadata_runtime();
            let mr = MaintenanceRuntime::new(
                vr.storage().clone(),
                storage_manager.clone(),
                vr.runtime().clone(),
                vr.metrics().clone(),
                metadata_runtime,
            );
            // Sprint B: run warm() in background (cleanup + deferred tasks)
            let mr_clone = mr.clone();
            vaultsync_core::time_utils::spawn(async move {
                if let Err(e) = mr_clone.warm().await {
                    engine_debug!("[maintenance] warm() completed with: {:?}", e);
                }
            });
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
            Arc::new(BroadcastManager::new(runtime.clone(), ipc, Arc::new(crate::metrics::RuntimeMetrics::default()), replica_id.to_string()))
        });

        // Wire CapabilityManager based on leader election
        let is_leader = client.leader_election.is_leader();
        if is_leader {
            capability_manager.set(RuntimeCapability::Leader);
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
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Mirroring);
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

        // Phase 3: Start lifecycle manager for maintenance ticks
        let lifecycle = crate::lifecycle_manager::LifecycleManager::new(5000);
        if let Some(ref mr) = vault_runtime.as_ref().and_then(|vr| vr.ctx.maintenance.clone()) {
            lifecycle.register_maintenance(mr);
        }
        lifecycle.start();

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
            replay_engine: None,
            leader_election: None,
            leader_lease: None,
            promotion_rx: Mutex::new(None),
            lifecycle_manager: Some(lifecycle),
            saved_namespace: Mutex::new(namespace.to_string()),
            saved_coordinator_url: Mutex::new(String::new()),
            saved_auth_token: Mutex::new(None),
            saved_db_name: Mutex::new(Some(final_db_name.clone())),
            saved_storage_backend: Mutex::new(None),
        };

        // Phase II: new leader announces via BC
        if is_leader {
            if let Some(ref bm) = broadcast_manager {
                bm.broadcast_leader_announcement(replica_id);
            }
        }

        Self::spawn_pending_listener(&result);

        debug_assert!(
            if is_leader {
                result.client.is_some() && result.mirror.is_none() && result.leader_election.is_none()
            } else {
                result.client.is_none()
            },
            "Constructor invariant violation in new()",
        );

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
        leader_election: Option<Arc<vaultsync_core::ipc::leader_election::LeaderElection>>,
        promotion_rx: Option<oneshot::Receiver<()>>,
    ) -> Result<VaultSyncRuntime, JsValue> {
        let event_callback: Arc<Mutex<Option<SendFunction>>> = Arc::new(Mutex::new(None));

        let capability_manager = Arc::new(CapabilityManager::new());
        capability_manager.set(RuntimeCapability::Follower);

        let bc_for_unload = cross_tab_channel.clone();
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
            mirror: Some(mirror.clone()),
            replay_engine: None,
            leader_election,
            leader_lease: None,
            promotion_rx: Mutex::new(promotion_rx),
            lifecycle_manager: None,
            saved_namespace: Mutex::new(mirror.promote_ns.lock().unwrap().clone()),
            saved_coordinator_url: Mutex::new(mirror.promote_url.lock().unwrap().clone()),
            saved_auth_token: Mutex::new(mirror.promote_auth.lock().unwrap().clone()),
            saved_db_name: Mutex::new(mirror.promote_db.lock().unwrap().clone()),
            saved_storage_backend: Mutex::new(mirror.promote_backend.lock().unwrap().clone()),
        };

        // Phase 5: Register beforeunload handler for follower tab
        let bc_unload = bc_for_unload;
        let tab_id_unload = replica_id.to_string();
        if let Some(window) = web_sys::window() {
            let onunload = Closure::wrap(Box::new(move || {
                // Best-effort: send LEFT via BC. OPFS checkpoint not possible here.
                let msg = format!("PROTO|LEFT|{}|", tab_id_unload);
                let _ = bc_unload.post_message(&JsValue::from_str(&msg));
                engine_info!("[lifecycle] follower beforeunload: sent LEFT, no metadata to flush");
            }) as Box<dyn FnMut()>);
            window.set_onbeforeunload(Some(onunload.as_ref().unchecked_ref()));
            onunload.forget();
        }

        result.log_state("init_follower");

        debug_assert!(
            result.mirror.is_some() && result.leader_election.is_some() && result.client.is_none(),
            "new_follower invariant: must have mirror + leader_election, no client",
        );

        Ok(result)
    }

    /// Phase 4: Handle BC messages for MirrorRuntime (follower) mode.
    /// Processes HEARTBEAT, MUTATION, SYNC_BEGIN, SYNC_DONE, LEADER_ELECTED messages.
    fn handle_mirror_bc_message(mirror: &MirrorRuntime, msg_str: &str) {
        // Phase 3: If paused for promotion, skip all mutations.
        // The paused flag stays true until promotion completes.
        if mirror.paused.load(Ordering::Acquire) {
            return;
        }

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
            if let Some((_from_tab, doc_id, record_id, fields_str)) =
                crate::session_protocol::SessionProtocol::parse_mutation(msg_str)
            {
                if let Ok(fields) = json_to_fields(&fields_str) {
                    let is_delete = fields.get("__deleted__").or_else(|| fields.get("_deleted"))
                        .and_then(|v| if let CrdtValue::Boolean(b) = v { Some(*b) } else { None })
                        .unwrap_or(false);
                    if is_delete {
                        let before = mirror.query_doc(&doc_id).len();
                        mirror.remove_record(&doc_id, &record_id);
                        let after = mirror.query_doc(&doc_id).len();
                        engine_info!("[mirror] DELETE doc={} record={} records={}->{}", doc_id, record_id, before, after);
                        let mut delete_fields = std::collections::HashMap::new();
                        delete_fields.insert("__deleted__".to_string(), CrdtValue::Boolean(true));
                        mirror.fire_subscription(&doc_id, &record_id, &delete_fields);
                    } else {
                        for (field, value) in &fields {
                            mirror.apply_mutation(&doc_id, &record_id, field, value.clone());
                        }
                        let fields_copy = fields;
                        mirror.fire_subscription(&doc_id, &record_id, &fields_copy);
                    }
                } else {
                    mirror.decode_fail_count.fetch_add(1, Ordering::Relaxed);
                    engine_trace!("[mirror] MUTATION decode fail: doc={} record={} len={}", doc_id, record_id, fields_str.len());
                }
                mirror.replay_mutation_count.fetch_add(1, Ordering::Relaxed);
                mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
                mirror.drain_seq.fetch_add(1, Ordering::Release);
            }
            return;
        }

        if msg_str.starts_with("SYNC_BEGIN|") {
            // SYNC_BEGIN|ver|ns|cursor|gen — follower receives this when leader
            // acknowledges SYNC request. Resets replay counter, treats as heartbeat.
            if let Some((_ver, _ns, gen, cursor)) =
                crate::session_protocol::SessionProtocol::parse_sync_begin(msg_str)
            {
                mirror.replay_mutation_count.store(0, Ordering::Relaxed);
                mirror.runtime_gen.store(gen, Ordering::Release);
                mirror.cursor.store(cursor, Ordering::Release);
                mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
                engine_info!("[Session] ← SYNC_BEGIN cursor={} gen={} — replay starting", cursor, gen);
            }
            return;
        }

        if msg_str.starts_with("SYNC_DONE|") {
            // SYNC_DONE|ver|cursor — leader finished streaming mutations
            if let Some((_ver, cursor)) =
                crate::session_protocol::SessionProtocol::parse_sync_done(msg_str)
            {
                let count = mirror.replay_mutation_count.swap(0, Ordering::Relaxed);
                let decode_fails = mirror.decode_fail_count.swap(0, Ordering::Relaxed);
                let doc_count = mirror.cached_count();
                mirror.cursor.store(cursor, Ordering::Release);
                mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
                engine_info!("[Session] ← SYNC_DONE cursor={} — total={} decode_fails={} applied={} docs_in_store={}",
                    cursor, count, decode_fails, count.saturating_sub(decode_fails), doc_count);
            }
            return;
        }

        if msg_str.starts_with("LEADER_ELECTED|") {
            let parts: Vec<&str> = msg_str.splitn(2, '|').collect();
            let other_tab = parts.get(1).unwrap_or(&"");
            if other_tab != &mirror.tab_id {
                engine_info!("[MirrorRuntime] leader elected: tab={}", other_tab);
                mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
            }
            return;
        }

        // PROTO|LEFT — leader or peer has left
        if msg_str.starts_with("PROTO|LEFT|") {
            let parts: Vec<&str> = msg_str.splitn(3, '|').collect();
            if parts.len() >= 2 {
                let leaver = parts[1];
                if leaver != mirror.tab_id {
                    engine_info!("[MirrorRuntime] tab LEFT: {} (follower fast path)", leaver);
                    mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
                    mirror.leader_left.store(true, Ordering::Relaxed);
                }
            }
            return;
        }

        // Legacy: PROTO|READY/DISCOVER — keep for backward compat
        if msg_str.starts_with("PROTO|READY|") || msg_str.starts_with("PROTO|DISCOVER|") {
            let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
            if parts.len() >= 2 {
                let sender = parts[1];
                if sender != mirror.tab_id {
                    mirror.last_heartbeat.store(js_sys::Date::now() as u64, Ordering::Release);
                }
            }
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

        // Step 1: Create BC channel (lightweight, no I/O)
        let channel_name = format!("vaultsync-ipc-{}", namespace);
        let cross_tab_channel = web_sys::BroadcastChannel::new(&channel_name).ok();

        // Step 2: Determine leadership via LeaderElection (instant, <5ms)
        // Uses acquire(AcquireMode::Immediate) with {ifAvailable: true} — no wait, no polling.
        // Web Lock is the sole authority for leadership — BC only announces state, never decides.
        // IMPORTANT: The Lease is NEVER dropped here. It is stored on VaultSyncRuntime
        // and kept alive for the entire leader session. Dropping it would release the Web Lock,
        // creating a race with waiting followers.
        let (is_leader, leader_lease) = if cross_tab_channel.is_some() {
            let le = Arc::new(
                vaultsync_core::ipc::leader_election::LeaderElection::new(
                    namespace,
                    &vaultsync_core::storage::traits::StorageConfig::Wasm,
                ),
            );
            match le.acquire(
                vaultsync_core::ipc::leader_election::AcquireMode::Immediate,
            ).await {
                Ok(vaultsync_core::ipc::leader_election::AcquireResult::Acquired(lease)) => {
                    engine_info!("[{:.0} ms] leader election: acquired (Immediate) — lease held", t());
                    (true, Some(lease))
                }
                Ok(vaultsync_core::ipc::leader_election::AcquireResult::Waiting) => {
                    engine_info!("[{:.0} ms] leader election: waiting (follower)", t());
                    (false, None)
                }
                _ => {
                    engine_debug!("[{:.0} ms] leader election: assuming leader (fallback)", t());
                    (true, None)
                }
            }
        } else {
            (true, None) // No BC → single tab, always leader
        };

        if is_leader {
            // ─────────────────────────────────────────────────────
            // LEADER PATH: build full engine
            // ─────────────────────────────────────────────────────
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

            // Phase 3: Wire StorageRuntime + MetadataRuntime into bootstrap (leader path)
            // Phase 6: Register default namespace in WorkspaceRuntime
            // B3: sync_store_for_client shared across Opfs/fallback paths
            let mut sync_store_for_client: Option<Arc<dyn vaultsync_core::sync::sync_state_store::SyncStateStore>> = None;
            let mut vault_runtime: Option<Arc<VaultRuntime>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
                let pages = opfs.pages().clone();
                let metrics = Arc::new(crate::metrics::RuntimeMetrics::default());
                match StorageRuntime::new(pages, metrics.clone()).await {
                    Ok(storage_runtime) => {
                        opfs.set_storage_runtime(storage_runtime.clone());
                        // Sprint B: Load MetadataRuntime — in-memory cache over MetadataStore
                        let sync_rt = crate::sync_runtime::SyncRuntime::new();
                        if let Some(mr) = opfs.try_load_metadata_runtime().await {
                            opfs.set_metadata_runtime(mr.clone());
                            storage_runtime.set_metadata_runtime(mr.clone());
                            // Wire SyncRuntime → MetadataRuntime for checkpoint persistence (B3)
                            sync_rt.with_metadata(mr, namespace);
                        }
                        let runtime = Runtime::new(replica_id.to_string());
                        let scheduler = storage_runtime.scheduler.clone();
                        let vr = VaultRuntime::new(runtime, storage_runtime.clone(), scheduler, metrics);
                        let sub_index = crate::subscription_index::SubscriptionIndex::new();
                        // Clone sync_rt Arc before moving it into NamespaceRuntime
                        // (B3: used as SyncStateStore for DownloadQueue → MetadataRuntime bridge)
                        sync_store_for_client = Some(sync_rt.clone());
                        let ns = crate::namespace_runtime::NamespaceRuntime::new(
                            "default".to_string(),
                            storage_runtime,
                            sync_rt,
                            sub_index,
                        );
                        vr.workspace().register(ns);
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

            // Sprint B: tombstoned page cleanup deferred to MaintenanceRuntime::warm()
            engine_debug!("[cleanup] deferred to warm phase");

            let storage_manager: Arc<dyn StorageManager> = Arc::new(
                DefaultStorageManager::new(storage.clone(), CompactionPolicy::default()),
            );
            phase_log("storage_manager ready");

            // Phase 4.0: Attach MaintenanceRuntime to VaultRuntime (best-effort)
            if let Some(ref vr) = vault_runtime {
                let metadata_runtime = vr.storage().get_metadata_runtime();
                let mr = MaintenanceRuntime::new(
                    vr.storage().clone(),
                    storage_manager.clone(),
                    vr.runtime().clone(),
                    vr.metrics().clone(),
                    metadata_runtime,
                );
                // Sprint B: run warm() in background (cleanup + deferred tasks)
                let mr_clone = mr.clone();
                vaultsync_core::time_utils::spawn(async move {
                    if let Err(e) = mr_clone.warm().await {
                        engine_debug!("[maintenance] warm() completed with: {:?}", e);
                    }
                });
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
            let saved_auth = auth_token.clone();
            let coordinator = Arc::new(crate::ws_coordinator::WasmWsCoordinator::new(
                coordinator_url,
                auth_token,
            ));
            phase_log("coordinator ready");

            // Create ConnectionManager and wire to coordinator (single source of truth for WS state)
            let (cm, _conn_events) = vaultsync_core::connection_manager::ConnectionManager::new();
            coordinator.set_connection_manager(cm.clone());
            phase_log("connection_manager ready");

            // Clone the Arc BEFORE coercing to dyn Coordinator, so we keep a concrete reference
            let coordinator_for_client: Arc<dyn vaultsync_core::coordinator::traits::Coordinator> =
                coordinator.clone();
            let keyring = Arc::new(KeyRing::generate());
            phase_log("keyring ready");

            // Sprint B3: SyncRuntime implements SyncStateStore — forwards cursor/gen writes
            // to MetadataRuntime for checkpoint persistence. No OPFS writes on cursor update.
            // Fallback to InMemorySyncStateStore when SyncRuntime is unavailable.
            let sync_store: Arc<dyn vaultsync_core::sync::sync_state_store::SyncStateStore> = sync_store_for_client.unwrap_or_else(|| {
                Arc::new(vaultsync_core::sync::sync_state_store::InMemorySyncStateStore::with_cursor(0))
            });
            let client = match VaultSyncClient::new_with_sync_state_store_skip_init(
                config,
                coordinator_for_client,
                keyring,
                storage,
                sync_store,
            ).await {
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

            // Wire ConnectionManager to UploadQueue (authoritative offline check before push)
            client.set_connection_manager(Some(cm));

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

            // Sprint D: Sync the outer Lease's leadership to VaultSyncClient's internal
            // LeaderElection. No try_acquire() needed — the Lease proves we hold the Web Lock.
            // A single blocking request from the outer acquire(Immediate) covers both sides.
            if leader_lease.is_some() {
                client.leader_election.set_is_leader(true);
                engine_info!("[{:.0} ms] leader election: synced from Lease", t());
            }

            engine_info!("[{:.0} ms] READY", t());

            let event_callback: Arc<Mutex<Option<SendFunction>>> = Arc::new(Mutex::new(None));

            // Engine V2: Runtime::new() already returns Arc<Runtime>
            let runtime = Runtime::new(replica_id.to_string());
            let capability_manager = Arc::new(CapabilityManager::new());

            // Engine V2/V3: create PersistenceEngine if OPFS backend
            let persistence_engine: Option<Arc<dyn PersistenceEngine>> = if let BrowserStorage::Opfs(opfs) = &*storage_for_engine {
                match Self::build_persistence_engine(opfs.clone().into(), runtime.clone()).await {
                    Ok(engine) => Some(engine),
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
                Arc::new(BroadcastManager::new(runtime.clone(), ipc, Arc::new(crate::metrics::RuntimeMetrics::default()), replica_id.to_string()))
            });

            // Wire CapabilityManager
            capability_manager.set(RuntimeCapability::Leader);

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
                coord.set_state(vaultsync_core::runtime_state::RuntimeState::Leading);
            }

            let compaction_scheduler = Some(Arc::new(CompactionScheduler::new(
                runtime.clone(),
                storage_manager.clone(),
                namespace,
            )));

            // Wire compaction_scheduler into VaultRuntime's EngineContext
            if let (Some(vr), Some(cs)) = (vault_runtime.clone(), compaction_scheduler.clone()) {
                vault_runtime = Some(vr.with_compaction(cs));
            }

            // Phase 3: Create ReplayEngine for session replay (routes through StorageRuntime)
            let replay_engine = vault_runtime.as_ref().and_then(|vr| {
                let storage_runtime = vr.storage().clone();
                let reader = Arc::new(crate::replay_engine::StorageDocumentReader::new(storage_runtime, "doc_data"));
                Some(Arc::new(crate::replay_engine::ReplayEngine::new(reader)))
            });

            // Phase 3: Start lifecycle manager for maintenance ticks
            let lifecycle = crate::lifecycle_manager::LifecycleManager::new(5000);
            if let Some(ref mr) = vault_runtime.as_ref().and_then(|vr| vr.ctx.maintenance.clone()) {
                lifecycle.register_maintenance(mr);
            }
            lifecycle.start();

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
                replay_engine,
                leader_election: None,
                leader_lease, // Held for entire leader session — never dropped until demote
                promotion_rx: Mutex::new(None),
                lifecycle_manager: Some(lifecycle),
                saved_namespace: Mutex::new(namespace.to_string()),
                saved_coordinator_url: Mutex::new(coordinator_url.to_string()),
                saved_auth_token: Mutex::new(saved_auth),
                saved_db_name: Mutex::new(Some(final_db_name.clone())),
                saved_storage_backend: Mutex::new(storage_backend),
            };

            // Announce READY via BC
            if let Some(ref bc) = cross_tab_channel {
                let cursor = runtime.metadata_store.lock().unwrap().cursor;
                let protocol = crate::session_protocol::SessionProtocol::new(
                    bc.clone(),
                    replica_id.to_string(),
                );
                protocol.send_sync_begin(namespace, 0, cursor);
                engine_debug!("[Leader] announced READY cursor={}", cursor);
            }

            Self::spawn_pending_listener(&result);
            Self::spawn_bc_message_handler(&result);

            // Register beforeunload handler
            if let Some(ref bc) = cross_tab_channel {
                let bc_unload = bc.clone();
                let tab_id_unload = replica_id.to_string();
                let window = web_sys::window().unwrap_throw();
                let onunload = Closure::wrap(Box::new(move || {
                    // Best-effort: send LEFT via BC. Periodic checkpoint (every 60s)
                    // is the real durability mechanism — beforeunload is not guaranteed
                    // to complete async operations.
                    let msg = format!("PROTO|LEFT|{}|", tab_id_unload);
                    let _ = bc_unload.post_message(&JsValue::from_str(&msg));
                    engine_info!("[lifecycle] leader beforeunload: sent LEFT — metadata persistence depends on last periodic checkpoint");
                }) as Box<dyn FnMut()>);
                window.set_onbeforeunload(Some(onunload.as_ref().unchecked_ref()));
                onunload.forget();
                engine_info!("[lifecycle] beforeunload handler registered (leader)");
            }

            debug_assert!(
                result.client.is_some() && result.mirror.is_none() && result.leader_election.is_none(),
                "new_with_coordinator leader invariant: must have client, no mirror or leader_election",
            );

            result.log_state("init_leader");
            Ok(result)
        } else {
            // ─────────────────────────────────────────────────────
            // FOLLOWER PATH: create MirrorRuntime, no storage/coordinator/recovery
            // ─────────────────────────────────────────────────────
            let bc = cross_tab_channel.as_ref().unwrap().clone();

            let doc_store = Arc::new(std::sync::Mutex::new(crate::runtime::DocumentStore::new()));
            let mirror = MirrorRuntime::new(replica_id, doc_store);

            // Phase 2: Store promote params on MirrorRuntime so the callback has them
            {
                *mirror.promote_ns.lock().unwrap() = namespace.to_string();
                *mirror.promote_url.lock().unwrap() = coordinator_url.to_string();
                *mirror.promote_auth.lock().unwrap() = auth_token.clone();
                *mirror.promote_db.lock().unwrap() = db_name.clone();
                *mirror.promote_backend.lock().unwrap() = storage_backend.clone();
            }

            // Set up BC handler via addEventListener (never overwrites, coexists with JS handler)
            let mirror_clone = mirror.clone();
            let mirror_handler = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Some(msg_str) = e.data().as_string() {
                    Self::handle_mirror_bc_message(&mirror_clone, &msg_str);
                }
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);
            {
                let func: &js_sys::Function = mirror_handler.as_ref().unchecked_ref();
                let _ = bc.add_event_listener_with_callback("message", func);
            }
            mirror_handler.forget();

            // Phase 4: Acquire Web Lock in Wait mode for promotion detection.
            // Subscribe to LeaderEvent::Acquired — fires when leader tab releases its lock
            // (tab closes or crashes). Notifies JS via oneshot channel (no polling).
            let le_follower = Arc::new(
                vaultsync_core::ipc::leader_election::LeaderElection::new(
                    namespace,
                    &vaultsync_core::storage::traits::StorageConfig::Wasm,
                ),
            );
            let le_events = le_follower.events();
            let mirror_sub = mirror.clone();
            let (promote_tx, promote_rx) = oneshot::channel::<()>();
            let promote_tx = Mutex::new(Some(promote_tx));
            let _sub = le_events.subscribe(move |_event: &vaultsync_core::runtime_bus::LeaderEvent| {
                mirror_sub.promotion_ready.store(true, Ordering::Release);
                if let Some(tx) = promote_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                engine_info!("[promotion] LeaderEvent::Acquired — lock granted, promotion ready");
            });
            let _ = le_follower.acquire(
                vaultsync_core::ipc::leader_election::AcquireMode::Wait,
            ).await;
            // Store leader_election for in-Rust promotion path.
            let leader_election = Some(le_follower);

            // Request initial SYNC from leader using SessionProtocol
            let session = crate::session_protocol::SessionProtocol::new(
                bc.clone(),
                replica_id.to_string(),
            );
            session.send_sync(namespace, 0, "");
            engine_info!("[Session] → SYNC cursor=0 — requesting full state from leader");

            phase_log("mirror runtime ready");
            let result = Self::new_follower(namespace, replica_id, bc, mirror, leader_election, Some(promote_rx)).await?;

            Ok(result)
        }
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

    fn broadcast_mutation(&self, doc_id: &str, record_id: &str, fields_json: &str) {
        let trace_id = TRACE_ID.fetch_add(1, Ordering::Relaxed);
        let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
        engine_debug!(
            "[BC→] trace={} bc_seq={} doc={} record={} payload_len={} hop=bc_send",
            trace_id, bc_seq, doc_id, record_id, fields_json.len(),
        );
        if let Some(ref bc) = self.cross_tab_channel {
            let msg = format!("MUTATION|{}|{}|{}|{}|{}", bc_seq, self.tab_id, doc_id, record_id, fields_json);
            let _ = bc.post_message(&JsValue::from_str(&msg));
        } else {
            engine_debug!("[BC→] trace={} bc_seq={} skipped (no channel)", trace_id, bc_seq);
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
                let fields_json = if json.is_empty() { "{\"__deleted__\":true}" } else { json };
                self.broadcast_mutation(doc_id, record_id, fields_json);
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

            // Sprint C: broadcast mutation in MUTATION| format for follower
            self.broadcast_mutation(doc_id, record_id, json);
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

            // Sprint C: broadcast mutation in MUTATION| format for follower
            self.broadcast_mutation(doc_id, record_id, json);
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
        if doc_id.is_empty() || record_id.is_empty() {
            return Err(JsValue::from_str(&format!(
                "delete: doc_id and record_id must be non-empty (got doc={:?} record={:?})",
                doc_id, record_id
            )));
        }
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

            // Sprint C: broadcast mutation in MUTATION| format for follower
            self.broadcast_mutation(doc_id, record_id, "{\"__deleted__\":true}");
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
        // Log which path is taken
        let path = if self.mirror.is_some() { "mirror" }
            else if self.client.is_some() { "client" }
            else if self.runtime.is_some() { "runtime_fallback" }
            else { "none" };
        engine_info!(
            "[FIND] doc={} path={} has_mirror={} has_client={} has_runtime={}",
            doc_id, path, self.mirror.is_some(), self.client.is_some(), self.runtime.is_some(),
        );

        // MirrorRuntime mode — read from in-memory store (no OPFS available)
        if let Some(ref mirror) = self.mirror {
            let records = mirror.query_doc(doc_id);
            let arr = js_sys::Array::new();
            for fields in records {
                let json_str = fields_to_json_string(&fields)
                    .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
                arr.push(&JsValue::from_str(&json_str));
            }
            engine_info!("[FIND] mirror_path doc={} records={}", doc_id, arr.length());
            return Ok(arr);
        }

        // Leader mode — OPFS is the authoritative source. Never prefer the Runtime
        // in-memory cache over OPFS, as the cache may be partially populated from BC
        // mutations during startup, returning incomplete results.
        let records = if let Some(ref c) = self.client {
            let results = c.find(doc_id, None).await;
            match &results {
                Ok(recs) => {
                    let total = recs.len();
                    let mut deleted = 0usize;
                    let mut ids: Vec<String> = Vec::new();
                    for r in recs {
                        let rid = r.get("record_id").or_else(|| r.get("id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        let is_del = r.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false);
                        if is_del { deleted += 1; }
                        ids.push(format!("{}(del={})", rid, is_del));
                    }
                    engine_info!("[FIND] client_path doc={} total={} deleted={} returned={} ids=[{}]",
                        doc_id, total, deleted, total - deleted, ids.join(","));
                }
                Err(e) => {
                    engine_info!("[FIND] client_path doc={} error={:?}", doc_id, e);
                }
            }
            results
        } else if let Some(ref rt) = self.runtime {
            // No client available (promotion in-flight) — fall back to Runtime cache
            let store = rt.document_store.lock().unwrap();
            let results: Vec<HashMap<String, CrdtValue>> = store.query_doc(doc_id)
                .into_iter().map(|r| r.clone()).collect();
            let total = results.len();
            let mut deleted = 0usize;
            let mut ids: Vec<String> = Vec::new();
            for r in &results {
                let rid = r.get("record_id").or_else(|| r.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let is_del = r.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false)
                    || r.get("__deleted__").and_then(|v| v.as_bool()).unwrap_or(false);
                if is_del { deleted += 1; }
                ids.push(format!("{}(del={})", rid, is_del));
            }
            engine_info!("[FIND] runtime_fallback doc={} total={} deleted={} returned={} ids=[{}]",
                doc_id, total, deleted, total - deleted, ids.join(","));
            Ok(results)
        } else {
            engine_info!("[FIND] no_path doc={} records=0", doc_id);
            Ok(Vec::new())
        }
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
            map.insert("mode".to_string(), serde_json::Value::String("Mirror".to_string()));
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

        let mode = self.capability_manager.as_ref().map(|cm| cm.get().mode_str()).unwrap_or("Offline");
        let mut map = serde_json::Map::new();
        map.insert("mode".to_string(), serde_json::Value::String(mode.to_string()));
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

        let mode = self.capability_manager.as_ref().map(|cm| cm.get().mode_str()).unwrap_or("Offline");
        map.insert("connectionState".to_string(), serde_json::Value::String(mode.to_string()));

        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| JsValue::from_str(&format!("Serialize error: {:?}", e)))?;
        Ok(JsValue::from_str(&json_str))
    }

    pub fn on_event(&self, callback: js_sys::Function) {
        *self.event_callback.lock().unwrap() = Some(SendFunction(JsValue::from(callback)));
    }

    /// Engine V2/V3: Listen for messages on BC — routes all messages through RuntimeCoordinator first.
    /// Uses addEventListener to coexist with JS-side handler (never overwrites it).
    fn spawn_bc_message_handler(this: &Self) {
        engine_info!("[BC] WASM BC handler spawned tab={}", this.tab_id);
        if let Some(ref bc) = this.cross_tab_channel {
            let rt = this.runtime.clone();
            let client = this.client.clone();
            let bm = this.broadcast_manager.clone();
            let tab_id = this.tab_id.clone();
            let cm = this.capability_manager.clone();
            let coord = this.runtime_coordinator.clone();
            let replay_engine = this.replay_engine.clone();

            let replay_in_progress = Arc::new(AtomicBool::new(false));
            let bc_clone = bc.clone();
            let onmsg = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Some(msg_str) = e.data().as_string() {
                    // Phase 4 v2: Route protocol messages through RuntimeCoordinator first
                    if let Some(ref c) = coord {
                        if c.handle_bc_message(&msg_str, &tab_id) {
                            return;
                        }
                    }
                    // Handle SYNC request from follower using SessionProtocol format
                    // SYNC|ver|caps|ns|cursor|gen
                    if msg_str.starts_with("SYNC|") {
                        let sync_info = crate::session_protocol::SessionProtocol::parse_sync(&msg_str);
                        if let Some((_ver, _caps, _ns, cursor, _gen)) = sync_info {
                            // Guard: only one active replay at a time
                            let replay_flag = replay_in_progress.clone();
                            if replay_flag.swap(true, Ordering::SeqCst) {
                                engine_info!("[Session] ← SYNC SKIPPED from tab={}: cursor={} — replay already in progress", tab_id, cursor);
                                return;
                            }
                            let replay_id = REPLAY_ID.fetch_add(1, Ordering::Relaxed);
                            let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
                            engine_info!("[Session] ← SYNC from tab={}: cursor={} — sending initial state (replay={} bc_seq={})", tab_id, cursor, replay_id, bc_seq);
                            let msg_begin = format!("SYNC_BEGIN|{}|{}|{}|{}|replay={}|bc_seq={}", 1u8, "default", cursor, cursor, replay_id, bc_seq);
                            let _ = bc_clone.post_message(&JsValue::from_str(&msg_begin));
                            // Phase 3: Replay via ReplayEngine — reads from persisted storage
                            if let Some(ref engine) = replay_engine {
                                let bc_for_replay = bc_clone.clone();
                                let tab_id_for_replay = tab_id.clone();
                                let rt_for_cursor = rt.clone();
                                let engine_clone = engine.clone();
                                let replay_flag = replay_flag.clone();
                                wasm_bindgen_futures::spawn_local(async move {
                                    let sink = BcMutationSink::new(bc_for_replay, tab_id_for_replay);
                                    let ctx = ReplayContext::new("default", cursor);
                                    let report = engine_clone.replay_since(&ctx, &sink).await;
                                    replay_flag.store(false, Ordering::SeqCst);
                                    // Read the authoritative cursor from metadata_store
                                    let current_cursor = match rt_for_cursor {
                                        Some(ref r) => r.metadata_store.lock().unwrap().cursor,
                                        None => report.requested_cursor,
                                    };
                                    // Send SYNC_DONE with the real cursor and replay_id
                                    let replay_id = REPLAY_ID.fetch_add(1, Ordering::Relaxed);
                                    let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
                                    let msg_done = format!("SYNC_DONE|{}|{}|replay={}|bc_seq={}", 1u8, current_cursor, replay_id, bc_seq);
                                    let _ = sink.bc().post_message(&JsValue::from_str(&msg_done));
                                    let (send_fails, serialize_fails) = sink.reset_fail_counts();
                                    engine_info!("[Session] → SYNC_DONE cursor={} replay={} bc_seq={} — sent={} send_fails={} serialize_fails={} report={}", current_cursor, replay_id, bc_seq, report.emitted, send_fails, serialize_fails, report.summary());
                                });
                            }
                        }
                        return;
                    }
                    // Engine V2 MUTATION: apply to Runtime cache via microtask
                    if msg_str.starts_with("MUTATION|") {
                        let mutation = crate::session_protocol::SessionProtocol::parse_mutation(&msg_str);
                        if let Some((from_tab, doc_id, record_id, fields_str)) = mutation {
                            engine_info!(
                                "[BC←] doc={} record={} from_tab={} payload_len={} hop=leader_receive",
                                doc_id, record_id, from_tab, fields_str.len(),
                            );
                            if from_tab != tab_id {
                                if let Some(ref rt) = rt {
                                    let rt_clone = rt.clone();
                                    let did = doc_id;
                                    let rid = record_id;
                                    let fstr = fields_str;
                                    wasm_bindgen_futures::spawn_local(async move {
                                        if let Ok(fields) = json_to_fields(&fstr) {
                                            for (field, value) in &fields {
                                                rt_clone.set_field(&did, &rid, field, value.clone());
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        return;
                    }

                    // PROTO|LEFT: another tab left (leader or follower)
                    if msg_str.starts_with("PROTO|LEFT|") {
                        let parts: Vec<&str> = msg_str.splitn(3, '|').collect();
                        if parts.len() >= 2 {
                            let leaver = parts[1];
                            if leaver != tab_id {
                                engine_info!("[BC] PROTO|LEFT tab={}", leaver);
                            }
                        }
                        return;
                    }

                    // LEADER_ELECTED: JS side handles demotion via async demote().
                    // We must NOT demote synchronously here — that would pre-empt the
                    // JS async path (this.inner.demote()) which does the full mirror
                    // recreation. Just attach to new leader's broadcast for mutation processing.
                    if msg_str.starts_with("LEADER_ELECTED|") {
                        let parts: Vec<&str> = msg_str.splitn(5, '|').collect();
                        let other_tab = parts.get(1).unwrap_or(&"");
                        if other_tab != &tab_id {
                            engine_info!("[BC] LEADER_ELECTED from tab={} — JS demote() handles mirror recreation", other_tab);
                            if let Some(ref bm) = bm {
                                let msg = format!("FOLLOWER_ATTACH|{}|{}|leader", tab_id, 1);
                                let _ = bm.channel.send(&msg);
                            }
                        }
                        return;
                    }

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

            let func: &js_sys::Function = onmsg.as_ref().unchecked_ref();
            let _ = bc.add_event_listener_with_callback("message", func);
            onmsg.forget();
        }
    }

    fn spawn_pending_listener(this: &Self) {
        let rx = this.client.as_ref().and_then(|c| c.take_pending_count_rx());
        match rx {
            Some(rx) => {
                let cb = this.event_callback.clone();
                let rt = this.runtime.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    futures::pin_mut!(rx);
                    while let Some(count) = rx.next().await {
                        engine_trace!("[event] pending_count={}", count);
                        // Update MetadataStore so heartbeats reflect the correct count
                        if let Some(ref runtime) = rt {
                            runtime.metadata_store.lock().unwrap().pending_count = count as usize;
                        }
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
            let sub_id = mirror.subscribe(doc_id, "", callback);
            return WasmSubscriptionHandle {
                handle: None,
                mirror_unsub: Some((mirror.clone(), sub_id)),
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

    /// Step 4: Wait for promotion signal via oneshot channel (event-driven, no polling).
    /// Resolves when LeaderEvent::Acquired fires (Web Lock granted to this follower).
    /// JS await this, then calls promote(). Returns immediately if already signaled.
    #[wasm_bindgen(js_name = waitForPromotion)]
    pub async fn wait_for_promotion(&self) -> Result<(), JsValue> {
        let rx = self.promotion_rx.lock().unwrap().take();
        match rx {
            Some(rx) => rx.await.map_err(|_| JsValue::from_str("promotion channel closed")),
            None => Ok(()), // already taken (promotion already signaled previously)
        }
    }

    /// Phase 3: Pause mirror mutation processing for deterministic promotion snapshot.
    /// After pause(), no BC mutations will be applied. Returns the current drain_seq
    /// value so JS can verify the queue is quiescent.
    #[wasm_bindgen(js_name = pauseMirror)]
    pub fn pause_mirror(&self) -> Result<u64, JsValue> {
        match self.mirror.as_ref() {
            Some(m) => {
                m.pause();
                Ok(m.current_drain_seq())
            }
            None => Err(JsValue::from_str("not in mirror mode")),
        }
    }

    /// Phase 3: Resume mirror mutation processing (abort promotion).
    #[wasm_bindgen(js_name = resumeMirror)]
    pub fn resume_mirror(&self) -> Result<(), JsValue> {
        match self.mirror.as_ref() {
            Some(m) => {
                m.resume();
                Ok(())
            }
            None => Err(JsValue::from_str("not in mirror mode")),
        }
    }

    /// Phase 3: Check mirror paused state and drain_seq for promotion diagnostics.
    #[wasm_bindgen(js_name = mirrorState)]
    pub fn mirror_state(&self) -> Result<JsValue, JsValue> {
        match self.mirror.as_ref() {
            Some(m) => {
                let state = serde_json::json!({
                    "paused": m.is_paused(),
                    "drain_seq": m.current_drain_seq(),
                    "doc_count": m.cached_count(),
                    "cursor": m.cursor.load(std::sync::atomic::Ordering::Acquire),
                    "pending_count": m.pending_count.load(std::sync::atomic::Ordering::Acquire),
                });
                let json_str = serde_json::to_string(&state).unwrap_or_else(|_| "{}".to_string());
                Ok(js_sys::JSON::parse(&json_str).unwrap_or(JsValue::NULL))
            }
            None => Ok(JsValue::NULL),
        }
    }

    /// Phase 4f: Check if mirror should promote to leader (leader heartbeat timeout).
    /// Returns true if leader is gone and JS should reinitialize with new_with_coordinator().
    /// Legacy — prefer waitForPromotion() for event-driven usage.
    #[wasm_bindgen(js_name = checkMirrorPromotion)]
    pub fn check_mirror_promotion(&self) -> bool {
        self.mirror.as_ref().map(|m| m.promote_to_leader()).unwrap_or(false)
    }

    /// Phase 4b: Promote this follower tab to leader in-process (6-phase pipeline).
    /// Called by JS when waitForPromotion() resolves.
    /// Phases: Acquire → Construct → Restore → Writable → Install → Announce.
    /// Each phase populates PromoteCtx fields; the pipeline guarantees ordering.
    #[wasm_bindgen(js_name = promote)]
    pub async fn promote(
        &self,
        namespace: &str,
        coordinator_url: &str,
        auth_token: Option<String>,
        db_name: Option<String>,
        storage_backend: Option<String>,
    ) -> Result<(), JsValue> {
        let t0 = js_sys::Date::now();
        let plog = |step: &str| {
            engine_info!("[promotion] step={} elapsed={:.0}ms", step, js_sys::Date::now() - t0);
        };

        let mirror = match self.mirror.as_ref() {
            Some(m) => m.clone(),
            None => return Err(JsValue::from_str("not in mirror mode")),
        };

        // ── Phase 1: Acquire — get Web Lock Lease + drain mirror docs ──
        let mirror_cursor = mirror.cursor.load(std::sync::atomic::Ordering::Acquire);
        engine_info!("[promotion] cursor_before: mirror_cursor={}", mirror_cursor);

        let doc_store = Arc::new(std::sync::Mutex::new(crate::runtime::DocumentStore::new()));
        mirror.drain_into(&doc_store);
        let doc_count = doc_store.lock().unwrap().documents.len();
        engine_info!("[promotion] step=drain docs={}", doc_count);
        plog("drain");

        let promote_lease = match self.leader_election.as_ref() {
            Some(le) => match le.acquire(
                vaultsync_core::ipc::leader_election::AcquireMode::Immediate,
            ).await {
                Ok(vaultsync_core::ipc::leader_election::AcquireResult::Acquired(lease)) => {
                    engine_info!("[promotion] acquired Lease from follower's LeaderElection");
                    debug_assert!(le.is_leader(), "promote() Lease must have is_leader=true");
                    Some(lease)
                }
                _ => {
                    engine_warn!("[promotion] could not acquire Lease — Web Lock may not be held");
                    None
                }
            },
            None => None,
        };

        // ── Phase 2: Construct — open storage, build VaultRuntime, build Client ──
        let saved_db = db_name.clone();
        let final_db_name = db_name.unwrap_or_else(|| format!("{}_db", namespace));
        let storage = Arc::new(
            crate::storage::BrowserStorage::new(&final_db_name, storage_backend.as_deref())
                .await
                .map_err(|e| JsValue::from_str(&format!("Storage failed: {:?}", e)))?,
        );
        plog("storage_open");

        let storage_for_engine = storage.clone();

        let mut config = vaultsync_core::VaultSyncConfig::default();
        config.namespace = namespace.to_string();
        config.replica_id = self.tab_id.clone();
        config.sync_interval = std::time::Duration::from_millis(200);
        config.retry.initial_delay = std::time::Duration::from_millis(50);
        config.storage = match &*storage {
            crate::storage::BrowserStorage::Opfs(_) | crate::storage::BrowserStorage::Idb(_) => {
                vaultsync_core::storage::traits::StorageConfig::Wasm
            }
        };

        // B3: SyncStateStore for DownloadQueue — share ref across Opfs/fallback paths
        let mut promote_sync_store: Option<Arc<dyn vaultsync_core::sync::sync_state_store::SyncStateStore>> = None;
        let mut vault_runtime: Option<Arc<VaultRuntime>> = if let crate::storage::BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            let pages = opfs.pages().clone();
            let metrics = Arc::new(crate::metrics::RuntimeMetrics::default());
            match crate::storage_runtime::StorageRuntime::new(pages, metrics.clone()).await {
                Ok(storage_runtime) => {
                    // Phase 1 invariant: compare mirror's in-memory state vs newly opened OPFS
                    {
                        let mirror_docs = mirror.cached_count();
                        let mirror_cursor_val = mirror.cursor.load(Ordering::Acquire);
                        let mirror_gen = mirror.runtime_gen.load(Ordering::Acquire);
                        let summary = storage_runtime.manifest_summary();
                        engine_info!("[promotion] mirror_vs_opfs: mirror_docs={} mirror_cursor={} mirror_gen={} | opfs=[{}]",
                            mirror_docs, mirror_cursor_val, mirror_gen, summary);
                    }
                    opfs.set_storage_runtime(storage_runtime.clone());
                    let sync_rt = crate::sync_runtime::SyncRuntime::new();
                    if let Some(mr) = opfs.try_load_metadata_runtime().await {
                        opfs.set_metadata_runtime(mr.clone());
                        storage_runtime.set_metadata_runtime(mr.clone());
                        sync_rt.with_metadata(mr, namespace);
                    }
                    promote_sync_store = Some(sync_rt.clone());
                    let rt = crate::runtime::Runtime::new(self.tab_id.clone());
                    let scheduler = storage_runtime.scheduler.clone();
                    let vr = VaultRuntime::new(rt, storage_runtime.clone(), scheduler, metrics);
                    let sub_index = crate::subscription_index::SubscriptionIndex::new();
                    let ns = crate::namespace_runtime::NamespaceRuntime::new(
                        "default".to_string(),
                        storage_runtime,
                        sync_rt,
                        sub_index,
                    );
                    vr.workspace().register(ns);
                    Some(vr)
                }
                Err(e) => {
                    engine_warn!("[promotion] StorageRuntime init deferred: {:?}", e);
                    None
                }
            }
        } else {
            None
        };
        plog("vault_runtime");

        let storage_manager: Arc<dyn StorageManager> = Arc::new(
            vaultsync_core::storage::manager::DefaultStorageManager::new(
                storage.clone(),
                vaultsync_core::storage::compaction::CompactionPolicy::default(),
            ),
        );

        if let Some(ref vr) = vault_runtime {
            let metadata_runtime = vr.storage().get_metadata_runtime();
            let mr = crate::maintenance_runtime::MaintenanceRuntime::new(
                vr.storage().clone(),
                storage_manager.clone(),
                vr.runtime().clone(),
                vr.metrics().clone(),
                metadata_runtime,
            );
            let mr_clone = mr.clone();
            vaultsync_core::time_utils::spawn(async move {
                if let Err(e) = mr_clone.warm().await {
                    engine_debug!("[promotion] maintenance warm() completed with: {:?}", e);
                }
            });
            vault_runtime = Some(vr.clone().with_maintenance(mr));
        }

        let saved_auth = auth_token.clone();
        let coordinator = Arc::new(crate::ws_coordinator::WasmWsCoordinator::new(
            coordinator_url,
            auth_token,
        ));
        plog("coordinator_created");

        let coordinator_for_client: Arc<dyn vaultsync_core::coordinator::traits::Coordinator> =
            coordinator.clone();
        let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());

        // B3: Use SyncRuntime-backed SyncStateStore when available (forwards to MetadataRuntime)
        // Falls back to InMemorySyncStateStore with mirror's cursor when SyncRuntime is unavailable.
        let promote_sync_store_ref = promote_sync_store.as_ref().map(|s| s.clone());
        let sync_store: Arc<dyn vaultsync_core::sync::sync_state_store::SyncStateStore> = promote_sync_store.unwrap_or_else(|| {
            Arc::new(vaultsync_core::sync::sync_state_store::InMemorySyncStateStore::with_cursor(mirror_cursor))
        });

        let client = match vaultsync_core::VaultSyncClient::new_with_sync_state_store_skip_init(
            config,
            coordinator_for_client,
            keyring,
            storage,
            sync_store,
        ).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                let msg = format!("[promotion] Client construction failed: {:?}", e);
                engine_error!("{}", msg);
                return Err(JsValue::from_str(&msg));
            }
        };
        plog("client_built");

        coordinator.set_download_notify(client.events.download_notify.clone());
        coordinator.set_upload_notify(client.events.upload_notify.clone());

        if let Err(e) = client.initialize().await {
            let msg = format!("[promotion] Initialize failed: {:?}", e);
            engine_error!("{}", msg);
            return Err(JsValue::from_str(&msg));
        }
        plog("client_initialized");
        {
            let sync_rt_info = promote_sync_store_ref.as_ref().map(|_| {
                let sr = vault_runtime.as_ref().and_then(|vr| vr.sync());
                match sr {
                    Some(s) => format!("SyncRuntime#{} cursor={}", s.id, s.cursor()),
                    None => "SyncRuntime not available".to_string(),
                }
            }).unwrap_or_else(|| "using InMemorySyncStateStore".to_string());
            engine_info!(
                "[promotion] post_init diagnostics: mirror_cursor={} sync_store={}",
                mirror_cursor, sync_rt_info,
            );
        }

        if promote_lease.is_some() {
            client.leader_election.set_is_leader(true);
            engine_info!("[promotion] leader election synced from Lease");
        } else {
            engine_warn!("[promotion] no Lease — leader election may not be held");
        }
        plog("leader_election");

        // ── Phase 3: Restore — drain documents, WAL checkpoint, cursor preservation ──
        let runtime = crate::runtime::Runtime::new(self.tab_id.clone());
        {
            let tmp_store = doc_store.lock().unwrap();
            let tmp_docs = tmp_store.documents.len();
            let mut rt_store = runtime.document_store.lock().unwrap();
            rt_store.documents = tmp_store.documents.clone();
            rt_store.access_counts = tmp_store.access_counts.clone();
            let rt_docs = rt_store.documents.len();
            engine_info!("[promotion] document_copy: tmp_docs={} rt_docs={} DocumentStore#{}=Runtime#{}", tmp_docs, rt_docs, rt_store.id, runtime.id);
        }
        plog("runtime_drained");

        let persistence_engine: Option<Arc<dyn PersistenceEngine>> = if let crate::storage::BrowserStorage::Opfs(opfs) = &*storage_for_engine {
            match Self::build_persistence_engine(opfs.clone().into(), runtime.clone()).await {
                Ok(engine) => Some(engine),
                Err(e) => {
                    engine_warn!("[promotion] persistence_engine skip: {:?}", e);
                    None
                }
            }
        } else {
            None
        };
        plog("persistence_engine");

        if let Some(ref pe) = persistence_engine {
            match pe.load_checkpoint().await {
                Ok((seq, count)) if seq > 0 => {
                    runtime.metadata_store.lock().unwrap().cursor = seq;
                    runtime.metadata_store.lock().unwrap().pending_count = count;
                    engine_info!("[promotion] WAL checkpoint restored: seq={} pending={} (mirror_cursor={})", seq, count, mirror_cursor);
                }
                Ok(_) => {
                    engine_info!("[promotion] WAL checkpoint: none (mirror_cursor={})", mirror_cursor);
                }
                Err(e) => engine_warn!("[promotion] load_checkpoint: {:?}", e),
            }
        }

        {
            let mut meta = runtime.metadata_store.lock().unwrap();
            if meta.cursor < mirror_cursor {
                engine_info!("[promotion] cursor advanced: {} → {} (mirror_cursor)", meta.cursor, mirror_cursor);
                meta.cursor = mirror_cursor;
            }
        }

        let broadcast_manager = self.cross_tab_channel.clone().map(|bc| {
            let ipc = Arc::new(crate::ipc::WasmIPC::new_with_channel(bc));
            Arc::new(crate::broadcast_manager::BroadcastManager::new(
                runtime.clone(),
                ipc,
                Arc::new(crate::metrics::RuntimeMetrics::default()),
                self.tab_id.clone(),
            ))
        });

        let compaction_scheduler = Some(Arc::new(
            crate::compaction_scheduler::CompactionScheduler::new(
                runtime.clone(),
                storage_manager.clone(),
                namespace,
            ),
        ));

        let capability_manager = Arc::new(crate::capability::CapabilityManager::new());

        if let (Some(vr), Some(cs)) = (vault_runtime.clone(), compaction_scheduler.clone()) {
            vault_runtime = Some(vr.with_compaction(cs));
        }

        let replay_engine = vault_runtime.as_ref().and_then(|vr| {
            let storage_runtime = vr.storage().clone();
            let reader = Arc::new(crate::replay_engine::StorageDocumentReader::new(storage_runtime, "doc_data"));
            Some(Arc::new(crate::replay_engine::ReplayEngine::new(reader)))
        });

        let runtime_coordinator = broadcast_manager.clone().map(|bm| {
            Arc::new(crate::runtime_coordinator::RuntimeCoordinator::new(
                runtime.clone(),
                bm,
                Arc::new(crate::metrics::RuntimeMetrics::default()),
                self.tab_id.clone(),
            ))
        });

        plog("engine_built");

        // ── Phase 4: Writable — set up managers (workspace, working set, event bus, planner) ──
        let workspace_manager = Arc::new(WorkspaceManager::new());
        let working_set_manager = Arc::new(WorkingSetManager::new());
        let replication_planner = Arc::new(ReplicationPlanner::new());
        let event_bus = Arc::new(EventBus::new());

        // Phase 3: Start lifecycle manager for maintenance ticks
        let lifecycle = crate::lifecycle_manager::LifecycleManager::new(5000);
        if let Some(ref vr) = vault_runtime {
            if let Some(ref mr) = vr.ctx.maintenance.clone() {
                lifecycle.register_maintenance(mr);
            }
        }
        lifecycle.start();

        // ── Phase 5: Install — atomically swap VaultSyncRuntime fields ──
        unsafe {
            self.transition_to_leader(
                capability_manager,
                client,
                storage_manager,
                workspace_manager,
                working_set_manager,
                replication_planner,
                event_bus,
                runtime.clone(),
                vault_runtime,
                broadcast_manager.clone(),
                runtime_coordinator,
                persistence_engine,
                compaction_scheduler,
                replay_engine,
                None, // presence — not carried over from follower
                promote_lease,
                Some(lifecycle),
                namespace.to_string(),
                coordinator_url.to_string(),
                saved_auth,
                saved_db,
                storage_backend,
            );
        }
        plog("install");

        // Immutable borrow from self for spawn calls
        engine_info!("[promotion] step=spawning_handlers");
        Self::spawn_pending_listener(self);
        Self::spawn_bc_message_handler(self);

        // ── Phase 6: Announce — broadcast LEADER_READY via BC ──
        // NOTE: handler registration (above) MUST precede broadcast to avoid a race
        // where the demoted tab's SYNC arrives before the WASM handler is installed.
        if let Some(ref bc) = self.cross_tab_channel {
            let cursor = runtime.metadata_store.lock().unwrap().cursor;
            let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
            engine_info!("[promotion] step=announce_readying cursor={} bc_seq={}", cursor, bc_seq);
            let announce = format!("LEADER_READY|{}|{}|bc_seq={}", self.tab_id, cursor, bc_seq);
            let _ = bc.post_message(&JsValue::from_str(&announce));
            engine_info!("[promotion] announced LEADER_READY cursor={}", cursor);
        }

        // Register beforeunload handler
        if let Some(ref bc) = self.cross_tab_channel {
            let bc_unload = bc.clone();
            let tab_id_unload = self.tab_id.clone();
            let window = web_sys::window().unwrap_throw();
            let onunload = Closure::wrap(Box::new(move || {
                let msg = format!("PROTO|LEFT|{}|", tab_id_unload);
                let _ = bc_unload.post_message(&JsValue::from_str(&msg));
                engine_info!("[lifecycle] promote beforeunload: sent LEFT — metadata persistence depends on last periodic checkpoint");
            }) as Box<dyn FnMut()>);
            window.set_onbeforeunload(Some(onunload.as_ref().unchecked_ref()));
            onunload.forget();
        }
        engine_info!("[lifecycle] beforeunload handler registered (promote)");
        plog("beforeunload");

        self.log_state("promote");
        let total_ms = (js_sys::Date::now() - t0) as u64;
        engine_info!("[promotion] step=complete — runtime is now Leader total={}ms", total_ms);
        Ok(())
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
        self.capability_manager.as_ref().map(|cm| cm.is_leader()).unwrap_or(false)
    }

    /// Force-demote this tab to Follower when LEADER_ELECTED is received from another tab.
    /// Creates a new MirrorRuntime + BC handler + LeaderElection so the tab can
    /// continue processing mutations and detect when to re-promote.
    /// Called from JS BC handler. Safe to call even if not currently leader (no-op in that case).
    #[wasm_bindgen(js_name = demote)]
    pub async fn demote(&self) -> Result<(), JsValue> {
        let cm = match self.capability_manager.as_ref() {
            Some(cm) => cm,
            None => return Ok(()),
        };
        if !cm.is_leader() {
            self.log_state("demote_skip");
            return Ok(());
        }

        // ── 1. Read saved config ──
        let ns = self.saved_namespace.lock().unwrap().clone();
        let url = self.saved_coordinator_url.lock().unwrap().clone();
        let auth = self.saved_auth_token.lock().unwrap().clone();
        let db = self.saved_db_name.lock().unwrap().clone();
        let backend = self.saved_storage_backend.lock().unwrap().clone();

        cm.set(crate::capability::RuntimeCapability::Follower);
        engine_info!("[demote] capability set to Follower — recreating mirror runtime");

        // ── 2. Create MirrorRuntime ──
        let doc_store = Arc::new(std::sync::Mutex::new(crate::runtime::DocumentStore::new()));
        let mirror = MirrorRuntime::new(&self.tab_id, doc_store);

        {
            let mut mn = mirror.promote_ns.lock().unwrap();
            *mn = ns.clone();
            let mut mu = mirror.promote_url.lock().unwrap();
            *mu = url.clone();
            let mut ma = mirror.promote_auth.lock().unwrap();
            *ma = auth.clone();
            let mut md = mirror.promote_db.lock().unwrap();
            *md = db.clone();
            let mut mb = mirror.promote_backend.lock().unwrap();
            *mb = backend.clone();
        }

        // ── 3. Attach BC message handler for MirrorRuntime ──
        if let Some(ref bc) = self.cross_tab_channel {
            let mirror_clone = mirror.clone();
            let mirror_handler = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                if let Some(msg_str) = e.data().as_string() {
                    Self::handle_mirror_bc_message(&mirror_clone, &msg_str);
                }
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);
            {
                let func: &js_sys::Function = mirror_handler.as_ref().unchecked_ref();
                let _ = bc.add_event_listener_with_callback("message", func);
            }
            mirror_handler.forget();
            engine_info!("[demote] BC mirror handler attached");

            // ── 4. Send SYNC to new leader ──
            let session = crate::session_protocol::SessionProtocol::new(
                bc.clone(),
                self.tab_id.clone(),
            );
            session.send_sync(&ns, 0, "");
            engine_info!("[demote] SYNC sent to new leader ns={}", ns);
        }

        // ── 5. Acquire Web Lock in Wait mode for future promotion ──
        let le = Arc::new(
            vaultsync_core::ipc::leader_election::LeaderElection::new(
                &ns,
                &vaultsync_core::storage::traits::StorageConfig::Wasm,
            ),
        );
        let le_events = le.events();
        let mirror_sub = mirror.clone();
        let _sub = le_events.subscribe(move |_event: &vaultsync_core::runtime_bus::LeaderEvent| {
            mirror_sub.promotion_ready.store(true, Ordering::Release);
            engine_info!("[demote] LeaderEvent::Acquired — lock granted, promotion ready");
        });
        let _ = le.acquire(
            vaultsync_core::ipc::leader_election::AcquireMode::Wait,
        ).await;
        engine_info!("[demote] Web Lock acquired in Wait mode");

        // ── 6. Swap fields via transition_to_follower ──
        unsafe {
            self.transition_to_follower(mirror, le);
        }

        Ok(())
    }

    /// Returns the current runtime status as a string.
    /// Sprint D: Enhanced runtime status with health, namespace, lag, and leader identity.
    /// Possible values for "mode": "Leader", "Mirror", "Promoting", "Recovering", "Connecting", "Offline"
    #[wasm_bindgen(js_name = runtimeStatus)]
    pub fn runtime_status(&self) -> String {
        // Build common fields
        let mut status = serde_json::json!({
            "mode": "Connecting",
            "health": "Unknown",
            "namespace": "default",
            "lag_ms": 0u64,
            "leader_id": "",
            "cursor": 0u64,
        });

        // Add StorageHealth if available
        if let Some(ref vr) = self.vault_runtime {
            let health = vr.storage().health();
            status["health"] = serde_json::Value::String(format!("{:?}", health));
            let ns = self.saved_namespace.lock().unwrap();
            status["namespace"] = serde_json::Value::String(ns.clone());
        } else if let Some(ref cm) = self.capability_manager {
            status["health"] = serde_json::Value::String("Starting".to_string());
        }

        // Follower/Mirror path
        if let Some(ref m) = self.mirror {
            let now = js_sys::Date::now() as u64;
            let leader_alive = !m.leader_left.load(Ordering::Acquire)
                && (now - m.last_heartbeat.load(Ordering::Acquire)) < 10_000;
            let cursor = m.cursor.load(Ordering::Acquire);
            status["mode"] = serde_json::Value::String(if m.promotion_ready.load(Ordering::Acquire) { "Promoting".to_string() } else { "Mirror".to_string() });
            status["cursor"] = serde_json::Value::Number(serde_json::Number::from(cursor));
            status["pending_count"] = serde_json::Value::Number(serde_json::Number::from(m.pending_count.load(Ordering::Acquire)));
            status["replay_mutation_count"] = serde_json::Value::Number(serde_json::Number::from(m.replay_mutation_count.load(Ordering::Acquire)));
            status["promotion_ready"] = serde_json::Value::Bool(m.promotion_ready.load(Ordering::Acquire));
            status["leader_left"] = serde_json::Value::Bool(m.leader_left.load(Ordering::Acquire));
            status["runtime_gen"] = serde_json::Value::Number(serde_json::Number::from(m.runtime_gen.load(Ordering::Acquire)));
            status["leader_alive"] = serde_json::Value::Bool(leader_alive);
            status["leader_id"] = serde_json::Value::String(m.tab_id.clone());
            // lag_ms: estimated time since last heartbeat
            status["lag_ms"] = serde_json::Value::Number(serde_json::Number::from(
                if leader_alive { 0u64 } else { now.saturating_sub(m.last_heartbeat.load(Ordering::Acquire)) }
            ));
            return serde_json::to_string(&status).unwrap_or_else(|_| r#"{"mode":"Mirror"}"#.to_string());
        }

        // Capability-based path (leader after engine build)
        if let Some(ref cm) = self.capability_manager {
            status["mode"] = serde_json::Value::String(cm.get().mode_str().to_string());
            if let Some(ref rt) = self.runtime {
                let meta = rt.metadata_store.lock().unwrap();
                status["cursor"] = serde_json::Value::Number(serde_json::Number::from(meta.cursor));
            }
            return serde_json::to_string(&status).unwrap_or_else(|_| r#"{"mode":"Leader"}"#.to_string());
        }

        // Fallback: has client/vault_runtime but no capability_manager yet
        if self.client.is_some() || self.vault_runtime.is_some() {
            status["mode"] = serde_json::Value::String("Leader".to_string());
            if let Some(ref rt) = self.runtime {
                let meta = rt.metadata_store.lock().unwrap();
                status["cursor"] = serde_json::Value::Number(serde_json::Number::from(meta.cursor));
            }
            return serde_json::to_string(&status).unwrap_or_else(|_| r#"{"mode":"Leader"}"#.to_string());
        }

        serde_json::to_string(&status).unwrap_or_else(|_| r#"{"mode":"Connecting"}"#.to_string())
    }

    /// Single-line state summary for debugging role disagreements.
    /// Logged at every role transition. Format:
    ///   ENGINE=<cap> LOCK=<bool> HEARTBEAT=<alive|none> MIRROR=<yes|no> BADGE=<leader|follower> CURSOR=<N>
    fn log_state(&self, tag: &str) {
        let cap = self.capability_manager.as_ref().map(|cm| cm.get().mode_str()).unwrap_or("none");
        let lock = self.leader_election.as_ref().map(|le| le.is_leader().to_string()).unwrap_or_else(|| "no_le".to_string());
        let heartbeat = match self.mirror.as_ref() {
            Some(m) => {
                let alive = !m.leader_left.load(Ordering::Acquire)
                    && (js_sys::Date::now() as u64 - m.last_heartbeat.load(Ordering::Acquire)) < 10_000;
                if alive { "alive".to_string() } else { "stale".to_string() }
            }
            None => "none".to_string(),
        };
        let mirror = if self.mirror.is_some() { "yes" } else { "no" };
        let badge = self.is_leader().to_string();
        let cursor = self.runtime.as_ref()
            .map(|r| r.metadata_store.lock().unwrap().cursor)
            .unwrap_or(0);
        engine_info!(
            "[state] {} ENGINE={} LOCK={} HEARTBEAT={} MIRROR={} BADGE={} CURSOR={}",
            tag, cap, lock, heartbeat, mirror, badge, cursor,
        );
    }

    /// Phase 6: Atomic transition to Leader role.
    /// Performs unsafe field swap + capability set + coordinator state + invariant assertion.
    /// Must only be called from promote() — never from constructors (which use safe struct init).
    unsafe fn transition_to_leader(
        &self,
        capability_manager: Arc<CapabilityManager>,
        client: Arc<VaultSyncClient>,
        storage_manager: Arc<dyn StorageManager>,
        workspace_manager: Arc<WorkspaceManager>,
        working_set_manager: Arc<WorkingSetManager>,
        replication_planner: Arc<ReplicationPlanner>,
        event_bus: Arc<EventBus>,
        runtime: Arc<Runtime>,
        vault_runtime: Option<Arc<VaultRuntime>>,
        broadcast_manager: Option<Arc<BroadcastManager>>,
        runtime_coordinator: Option<Arc<crate::runtime_coordinator::RuntimeCoordinator>>,
        persistence_engine: Option<Arc<dyn PersistenceEngine>>,
        compaction_scheduler: Option<Arc<CompactionScheduler>>,
        replay_engine: Option<Arc<crate::replay_engine::ReplayEngine>>,
        presence: Option<crate::presence::PresenceManager>,
        leader_lease: Option<vaultsync_core::ipc::leader_election::Lease>,
        lifecycle_manager: Option<Arc<crate::lifecycle_manager::LifecycleManager>>,
        saved_namespace: String,
        saved_coordinator_url: String,
        saved_auth_token: Option<String>,
        saved_db_name: Option<String>,
        saved_storage_backend: Option<String>,
    ) {
        let this_ptr = self as *const Self as *mut Self;
        (*this_ptr).capability_manager = Some(capability_manager);
        (*this_ptr).client = Some(client);
        (*this_ptr).storage_manager = Some(storage_manager);
        (*this_ptr).workspace_manager = Some(workspace_manager);
        (*this_ptr).working_set_manager = Some(working_set_manager);
        (*this_ptr).replication_planner = Some(replication_planner);
        (*this_ptr).event_bus = Some(event_bus);
        (*this_ptr).runtime = Some(runtime);
        (*this_ptr).vault_runtime = vault_runtime;
        (*this_ptr).broadcast_manager = broadcast_manager;
        (*this_ptr).runtime_coordinator = runtime_coordinator;
        (*this_ptr).persistence_engine = persistence_engine;
        (*this_ptr).compaction_scheduler = compaction_scheduler;
        (*this_ptr).replay_engine = replay_engine;
        (*this_ptr).presence = presence;
        (*this_ptr).mirror = None;
        (*this_ptr).leader_election = None;
        (*this_ptr).leader_lease = leader_lease;
        (*this_ptr).promotion_rx = Mutex::new(None);
        (*this_ptr).lifecycle_manager = lifecycle_manager;
        (*this_ptr).saved_namespace = Mutex::new(saved_namespace);
        (*this_ptr).saved_coordinator_url = Mutex::new(saved_coordinator_url);
        (*this_ptr).saved_auth_token = Mutex::new(saved_auth_token);
        (*this_ptr).saved_db_name = Mutex::new(saved_db_name);
        (*this_ptr).saved_storage_backend = Mutex::new(saved_storage_backend);

        if let Some(ref cm) = (*this_ptr).capability_manager {
            cm.set(RuntimeCapability::Leader);
        }
        if let Some(ref coord) = (*this_ptr).runtime_coordinator {
            coord.set_state(vaultsync_core::runtime_state::RuntimeState::Leading);
        }

        debug_assert!((*this_ptr).client.is_some(), "Leader invariant: client must be Some");
        debug_assert!((*this_ptr).mirror.is_none(), "Leader invariant: mirror must be None");
        debug_assert!((*this_ptr).leader_election.is_none(), "Leader invariant: leader_election must be None");
        debug_assert!(
            (*this_ptr).capability_manager.as_ref().map(|cm| cm.is_leader()).unwrap_or(false),
            "Leader invariant: capability must be Leader",
        );
        debug_assert!(
            (*this_ptr).client.as_ref().map(|c| c.leader_election.is_leader()).unwrap_or(false),
            "Leader invariant: VaultSyncClient must report is_leader=true",
        );

        self.log_state("transition_to_leader");
        engine_info!("[transition] → Leader — field swap complete");
    }

    /// Phase 6: Atomic transition to Follower role.
    /// Clears all leader resources, installs mirror + leader_election.
    /// Must only be called from demote().
    unsafe fn transition_to_follower(
        &self,
        mirror: Arc<MirrorRuntime>,
        leader_election: Arc<vaultsync_core::ipc::leader_election::LeaderElection>,
    ) {
        let this_ptr = self as *const Self as *mut Self;
        (*this_ptr).mirror = Some(mirror);
        (*this_ptr).leader_election = Some(leader_election);
        (*this_ptr).client = None;
        (*this_ptr).storage_manager = None;
        (*this_ptr).workspace_manager = None;
        (*this_ptr).working_set_manager = None;
        (*this_ptr).replication_planner = None;
        (*this_ptr).event_bus = None;
        (*this_ptr).presence = None;
        (*this_ptr).runtime = None;
        (*this_ptr).vault_runtime = None;
        (*this_ptr).broadcast_manager = None;
        (*this_ptr).runtime_coordinator = None;
        (*this_ptr).persistence_engine = None;
        (*this_ptr).compaction_scheduler = None;
        (*this_ptr).replay_engine = None;
        // Drop the Lease — this releases the Web Lock so the next waiting follower can acquire it
        (*this_ptr).leader_lease = None;
        (*this_ptr).promotion_rx = Mutex::new(None);

        if let Some(ref cm) = (*this_ptr).capability_manager {
            cm.set(RuntimeCapability::Follower);
        }

        debug_assert!((*this_ptr).mirror.is_some(), "Follower invariant: mirror must be Some");
        debug_assert!((*this_ptr).leader_election.is_some(), "Follower invariant: leader_election must be Some");
        debug_assert!((*this_ptr).client.is_none(), "Follower invariant: client must be None");
        debug_assert!(
            (*this_ptr).capability_manager.as_ref().map(|cm| cm.is_follower()).unwrap_or(false),
            "Follower invariant: capability must be Follower",
        );

        self.log_state("transition_to_follower");
        engine_info!("[transition] → Follower — field swap complete");
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
    /// Phase 4: MirrorRuntime subscription info (mirror, handle_id)
    mirror_unsub: Option<(Arc<MirrorRuntime>, u64)>,
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
        if let Some((ref mirror, handle_id)) = self.mirror_unsub.take() {
            mirror.unsubscribe(handle_id);
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

pub(crate) use crate::util::fields_to_json_string;

/// Sink that posts MUTATION/SYNC_DONE frames to the BroadcastChannel.
/// Used by ReplayEngine during session replay — never touches storage.
struct BcMutationSink {
    bc: web_sys::BroadcastChannel,
    tab_id: String,
    send_fails: std::sync::atomic::AtomicU64,
    serialize_fails: std::sync::atomic::AtomicU64,
}

impl BcMutationSink {
    fn new(bc: web_sys::BroadcastChannel, tab_id: String) -> Self {
        Self { bc, tab_id, send_fails: std::sync::atomic::AtomicU64::new(0), serialize_fails: std::sync::atomic::AtomicU64::new(0) }
    }

    fn bc(&self) -> &web_sys::BroadcastChannel {
        &self.bc
    }

    fn reset_fail_counts(&self) -> (u64, u64) {
        (self.send_fails.swap(0, Ordering::Relaxed), self.serialize_fails.swap(0, Ordering::Relaxed))
    }
}

impl vaultsync_core::storage::traits::MutationSink for BcMutationSink {
    fn send_mutation(
        &self,
        doc_id: &str,
        record_id: &str,
        fields: &std::collections::HashMap<String, CrdtValue>,
    ) {
        if let Ok(json) = fields_to_json_string(fields) {
            let trace_id = TRACE_ID.fetch_add(1, Ordering::Relaxed);
            let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
            engine_trace!(
                "[BC→] trace={} bc_seq={} doc={} record={} payload_len={} hop=replay_send",
                trace_id, bc_seq, doc_id, record_id, json.len(),
            );
            let mutation = format!("MUTATION|{}|{}|{}|{}|{}", bc_seq, self.tab_id, doc_id, record_id, json);
            if let Err(e) = self.bc.post_message(&JsValue::from_str(&mutation)) {
                self.send_fails.fetch_add(1, Ordering::Relaxed);
                engine_warn!("[BcMutationSink] post_message FAILED: {:?}", e);
            }
        } else {
            self.serialize_fails.fetch_add(1, Ordering::Relaxed);
            engine_warn!("[BcMutationSink] fields_to_json_string FAILED: doc={} record={}", doc_id, record_id);
        }
    }

    fn send_sync_done(&self, _cursor: u64, count: u64) {
        // Note: actual cursor value is passed through the ReplayReport,
        // but the BC protocol expects it here too. We use 0 as a placeholder
        // since the real cursor comes from ReplayReport.
        engine_warn!("[BcMutationSink] send_sync_done CALLED — expected dead code (emitted={})", count);
        let replay_id = REPLAY_ID.fetch_add(1, Ordering::Relaxed);
        let bc_seq = BC_MSG_SEQ.fetch_add(1, Ordering::Relaxed);
        let msg = format!("SYNC_DONE|{}|{}|replay={}|bc_seq={}", 1u8, 0, replay_id, bc_seq);
        let _ = self.bc.post_message(&JsValue::from_str(&msg));
        engine_warn!("[BcMutationSink] SYNC_DONE sent replay={} bc_seq={} (emitted={})", replay_id, bc_seq, count);
    }
}

/// Phase 3: Serialize fields to a JsValue for JS consumption.
fn fields_to_jsvalue(fields: &HashMap<String, CrdtValue>) -> Result<JsValue, JsValue> {
    let json_str = fields_to_json_string(fields)
        .map_err(|e| JsValue::from_str(&format!("Serialize failed: {:?}", e)))?;
    Ok(JsValue::from_str(&json_str))
}
