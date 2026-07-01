use crate::config::VaultSyncConfig;
#[cfg(feature = "async-runtime")]
use crate::coordinator::memory::InMemoryCoordinator;
use crate::coordinator::traits::{Coordinator, PendingMutation, ReplicaInfo};
use crate::crdt::document::CRDTDocument;
use crate::crdt::types::CrdtValue;
use crate::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor, KeyRing};
use crate::error::VaultSyncError;
use crate::oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus};
use crate::oplog::log::OpLog;
use crate::schema::migration::MigrationDefinition;
use crate::schema::registry::{DocumentSchema, SchemaRegistry};
use crate::storage::memory::InMemoryStorage;
#[cfg(feature = "storage-sqlite")]
use crate::storage::sqlite::SQLiteStorage;
use crate::storage::traits::{Storage, StorageConfig};
use crate::subscription::engine::{FireSource, SubscriptionEngine};
use crate::subscription::filter::Filter;
use crate::sync::download::DownloadQueue;
use crate::sync::reconciler::Reconciler;
use crate::sync::state::SyncState;
use crate::sync::upload::UploadQueue;
use crate::telemetry::tracing::VaultSyncTelemetry;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use yrs::updates::decoder::Decode;
use yrs::Update;
use sha2::{Digest, Sha256};

pub type Callback = Box<dyn Fn(&str, &str, &HashMap<String, CrdtValue>) + Send>;

pub struct VaultSyncClient {
    config: VaultSyncConfig,
    storage: Arc<dyn Storage>,
    coordinator: Arc<dyn Coordinator>,
    keyring: Arc<KeyRing>,
    encryptor: Arc<E2eeEncryptor>,
    _decryptor: Arc<E2eeDecryptor>,
    _oplog: Arc<OpLog>,
    schema: Arc<std::sync::Mutex<SchemaRegistry>>,
    subscriptions: Arc<std::sync::Mutex<SubscriptionEngine>>,
    upload_queue: Arc<UploadQueue>,
    download_queue: Arc<DownloadQueue>,
    _reconciler: Arc<Reconciler>,
    _telemetry: Arc<VaultSyncTelemetry>,
    #[cfg(target_arch = "wasm32")]
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(not(target_arch = "wasm32"))]
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    #[cfg(not(target_arch = "wasm32"))]
    p2p_handle: Option<vaultsync_transport_libp2p::LibP2pTransportHandle>,
    pub metrics: Arc<crate::telemetry::metrics::VaultSyncMetrics>,
    pub debug_api: Arc<crate::telemetry::debug::DebugApi>,
    pub leader_election: Arc<crate::ipc::leader_election::LeaderElection>,
    pub schema_version: u64,
    pub shared_memory: Arc<crate::ipc::shared_memory::SharedMemory>,
    pub events: crate::sync::events::SyncEvents,
    initialized: std::sync::atomic::AtomicBool,
    clock: crate::clock::HybridLogicalClock,
    pending_count_cache: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(target_arch = "wasm32")]
    pending_count_rx: std::sync::Mutex<Option<futures::channel::mpsc::UnboundedReceiver<usize>>>,
    /// Download channel receiver, held until `start_download_worker()` is called
    /// after subscribe completes in `initialize()`.
    download_worker_rx:
        std::sync::Mutex<Option<futures::channel::mpsc::UnboundedReceiver<Option<PendingMutation>>>>,
}

impl VaultSyncClient {
    pub async fn new(config: VaultSyncConfig) -> Result<Self, VaultSyncError> {
        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());
        Self::new_with_keyring(config, coordinator, keyring).await
    }

    /// Selects coordinator from `config.coordinator_endpoint` URL scheme:
    ///   "memory://"     → InMemoryCoordinator
    ///   "http(s)://"    → HttpCoordinator (speaks REST to any conforming HTTP endpoint)
    ///   <anything else> → InMemoryCoordinator (safe fallback)
    pub async fn connect(config: VaultSyncConfig) -> Result<Self, VaultSyncError> {
        let primary_coordinator: Arc<dyn Coordinator> =
            if config.coordinator_endpoint.starts_with("memory://") {
                Arc::new(InMemoryCoordinator::new())
            } else if config.coordinator_endpoint.starts_with("http://")
                || config.coordinator_endpoint.starts_with("https://")
            {
                #[cfg(feature = "coordinator-http")]
                {
                    Arc::new(crate::coordinator::http::HttpCoordinator::new(
                        crate::coordinator::http::HttpCoordinatorConfig {
                            url: config.coordinator_endpoint.clone(),
                            auth_token: config.auth_token.clone(),
                        },
                    ))
                }
                #[cfg(not(feature = "coordinator-http"))]
                {
                    return Err(VaultSyncError::Config(
                        "HTTP coordinator feature 'coordinator-http' is not enabled".into(),
                    ));
                }
            } else {
                Arc::new(InMemoryCoordinator::new())
            };

        let coordinator: Arc<dyn Coordinator> = if config.fallback_coordinator_urls.is_empty() {
            primary_coordinator
        } else {
            let mut candidates: Vec<Arc<dyn Coordinator>> = vec![primary_coordinator];
            for url in &config.fallback_coordinator_urls {
                let coord: Arc<dyn Coordinator> = if url.starts_with("memory://") {
                    Arc::new(InMemoryCoordinator::new())
                } else if url.starts_with("http://") || url.starts_with("https://") {
                    #[cfg(feature = "coordinator-http")]
                    {
                        Arc::new(crate::coordinator::http::HttpCoordinator::new(
                            crate::coordinator::http::HttpCoordinatorConfig {
                                url: url.clone(),
                                auth_token: config.auth_token.clone(),
                            },
                        ))
                    }
                    #[cfg(not(feature = "coordinator-http"))]
                    {
                        return Err(VaultSyncError::Config(
                            "HTTP coordinator feature is not enabled for fallbacks".into(),
                        ));
                    }
                } else {
                    Arc::new(InMemoryCoordinator::new())
                };
                candidates.push(coord);
            }
            Arc::new(crate::coordinator::failover::FailoverCoordinator::new(
                candidates,
            ))
        };

        let keyring = Arc::new(KeyRing::generate());
        Self::new_with_keyring(config, coordinator, keyring).await
    }

    pub async fn new_with_coordinator(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
    ) -> Result<Self, VaultSyncError> {
        let keyring = Arc::new(KeyRing::generate());
        Self::new_with_keyring(config, coordinator, keyring).await
    }

    pub async fn new_with_keyring(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
    ) -> Result<Self, VaultSyncError> {
        let storage: Arc<dyn Storage> = match &config.storage {
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::Sqlite { path } => Arc::new(SQLiteStorage::new(path)?),
            #[cfg(not(feature = "storage-sqlite"))]
            StorageConfig::Sqlite { .. } => {
                return Err(VaultSyncError::Config(
                    "sqlite storage not available".into(),
                ))
            }
            StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
            StorageConfig::Wasm => {
                return Err(VaultSyncError::Config(
                    "WASM storage not available in native build".into(),
                ))
            }
        };
        Self::new_with_storage(config, coordinator, keyring, storage).await
    }

    pub async fn new_with_storage(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
        storage: Arc<dyn Storage>,
    ) -> Result<Self, VaultSyncError> {
        Self::new_with_storage_and_migrations(
            config,
            coordinator,
            keyring,
            storage,
            crate::schema::migration::get_global_migrations(),
        )
        .await
    }

    /// Internal builder: shared implementation for all constructors.
    /// When `auto_initialize` is true, calls `initialize()` after spawning workers.
    /// When false, the caller must call `initialize()` and trigger startup catch-up.
    async fn build_client(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
        storage: Arc<dyn Storage>,
        migrations: Vec<Arc<crate::schema::migration::MigrationDefinition>>,
        auto_initialize: bool,
    ) -> Result<Self, VaultSyncError> {
        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] migration start");
        let runner = crate::schema::migration::MigrationRunner::new(storage.clone(), migrations);
        runner.validate_applied().await?;
        runner.run_pending().await?;
        let schema_version = runner.current_version().await;
        tracing::debug!("[client.new] migration done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] keys start");
        let mut stored_keys = storage.read_keys(&config.namespace).await?;
        if stored_keys.is_empty() {
            // Introduce a small random delay and re-read, to handle concurrent initialization races
            // in multi-tab environments.
            let delay_ms = 50 + (rand::Rng::gen_range(&mut rand::thread_rng(), 0..100));
            crate::time_utils::sleep(std::time::Duration::from_millis(delay_ms)).await;
            stored_keys = storage.read_keys(&config.namespace).await?;
        }

        if stored_keys.is_empty() {
            let active = keyring.active_key();
            let key_rec = crate::storage::traits::KeyRecord {
                namespace: config.namespace.clone(),
                key_bytes: active.private_key.to_vec(),
                version: active.version,
            };
            storage.write_key(&key_rec).await?;

            // Read-back verification to handle concurrent initialization races.
            let ver_keys = storage.read_keys(&config.namespace).await?;
            if let Some(first_key) = ver_keys.iter().find(|k| k.version == active.version) {
                if first_key.key_bytes != key_rec.key_bytes {
                    let mut loaded_keypairs = Vec::new();
                    for k_rec in ver_keys {
                        if k_rec.key_bytes.len() == 32 {
                            let mut private_key = [0u8; 32];
                            private_key.copy_from_slice(&k_rec.key_bytes);
                            let public_key = x25519_dalek::x25519(
                                private_key,
                                x25519_dalek::X25519_BASEPOINT_BYTES,
                            );
                            loaded_keypairs.push(crate::e2ee::keyring::NamespaceKeypair {
                                public_key,
                                private_key,
                                version: k_rec.version,
                                created_at: crate::time_utils::system_time_now_secs(),
                            });
                        }
                    }
                    if !loaded_keypairs.is_empty() {
                        keyring.load_keys(loaded_keypairs);
                    }
                }
            }
        } else {
            let mut loaded_keypairs = Vec::new();
            for key_rec in stored_keys {
                if key_rec.key_bytes.len() == 32 {
                    let mut private_key = [0u8; 32];
                    private_key.copy_from_slice(&key_rec.key_bytes);
                    let public_key =
                        x25519_dalek::x25519(private_key, x25519_dalek::X25519_BASEPOINT_BYTES);
                    loaded_keypairs.push(crate::e2ee::keyring::NamespaceKeypair {
                        public_key,
                        private_key,
                        version: key_rec.version,
                        created_at: crate::time_utils::system_time_now_secs(),
                    });
                }
            }
            if !loaded_keypairs.is_empty() {
                keyring.load_keys(loaded_keypairs);
            }
        }

        tracing::debug!("[client.new] keys done {} ms", crate::time_utils::system_time_now_ms() - _t);
        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] e2ee+subs+reconciler start");
        let encryptor = Arc::new(E2eeEncryptor::new(keyring.clone()));
        let decryptor = Arc::new(E2eeDecryptor::new(keyring.clone()));

        let subscriptions = Arc::new(std::sync::Mutex::new(SubscriptionEngine::new()));
        let reconciler = Arc::new(Reconciler::new(storage.clone(), subscriptions.clone()));
        tracing::debug!("[client.new] e2ee+subs+reconciler done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] oplog init start");
        let oplog = Arc::new(OpLog::new(storage.clone(), &config.namespace));
        tracing::debug!("[client.new] oplog init done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] read_sync_state start");
        let last_sequence = match storage.read_sync_state(&config.namespace).await? {
            Some(state) => {
                #[cfg(target_arch = "wasm32")]
                let backend = "opfs";
                #[cfg(not(target_arch = "wasm32"))]
                let backend = "sqlite";
                tracing::info!(
                    "[client.new] read_sync_state cursor={} gen={} backend={} namespace={} took {} ms",
                    state.last_synced_sequence,
                    state.generation_id,
                    backend,
                    config.namespace,
                    crate::time_utils::system_time_now_ms() - _t
                );
                state.last_synced_sequence
            }
            None => {
                tracing::debug!("[client.new] read_sync_state no state took {} ms", crate::time_utils::system_time_now_ms() - _t);
                0
            }
        };

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] queue init start");
        let metrics = Arc::new(crate::telemetry::metrics::VaultSyncMetrics::new());
        let debug_api = Arc::new(crate::telemetry::debug::DebugApi::new(
            storage.clone(),
            metrics.clone(),
        ));

        let upload_queue = Arc::new(UploadQueue::new(
            oplog.clone(),
            coordinator.clone(),
            config.upload.clone(),
            config.retry.clone(),
            metrics.clone(),
        ));

        let download_queue = Arc::new(DownloadQueue::new(
            coordinator.clone(),
            storage.clone(),
            &config.namespace,
            last_sequence,
            config.download.clone(),
            reconciler.clone(),
            decryptor.clone(),
            metrics.clone(),
            config.max_clock_skew,
            config.replica_id.clone(),
        ));
        tracing::debug!("[client.new] queue init done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let schema = Arc::new(std::sync::Mutex::new(SchemaRegistry::new()));
        let _telemetry = Arc::new(VaultSyncTelemetry::new());

        #[cfg(target_arch = "wasm32")]
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(not(target_arch = "wasm32"))]
        #[allow(unused_mut)]
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Perform crash recovery on startup before starting the sync loop
        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] crash_recovery start");
        let crash_recovery = crate::ipc::crash_recovery::CrashRecovery::new(
            storage.clone(),
            config.namespace.clone(),
        );
        if let Err(e) = crash_recovery.recover(keyring.clone()).await {
            tracing::error!(error = %e, "Crash recovery failed during startup");
        }
        tracing::debug!("[client.new] crash_recovery done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] leader_election start");
        let leader_election = Arc::new(crate::ipc::leader_election::LeaderElection::new(
            &config.namespace,
            &config.storage,
        ));
        tracing::debug!("[client.new] leader_election done {} ms", crate::time_utils::system_time_now_ms() - _t);

        #[cfg(not(target_arch = "wasm32"))]
        let mut p2p_handle = None;

        #[cfg(not(target_arch = "wasm32"))]
        if config.enable_p2p {
            let (incoming_tx, mut incoming_rx) = tokio::sync::mpsc::channel(100);
            let listen_addr = config.p2p_listen_addr.clone();
            let ns = config.namespace.clone();
            match vaultsync_transport_libp2p::LibP2pTransport::new(&ns, listen_addr, incoming_tx)
                .await
            {
                Ok((transport, handle)) => {
                    p2p_handle = Some(handle);
                    crate::time_utils::spawn(async move {
                        transport.run().await;
                    });
                    let dq = download_queue.clone();
                    crate::time_utils::spawn(async move {
                        while let Some(m) = incoming_rx.recv().await {
                            let core_mutation = crate::coordinator::traits::PendingMutation {
                                id: m.id,
                                namespace: m.namespace,
                                sequence: m.sequence,
                                doc_id: m.doc_id,
                                record_id: m.record_id,
                                encrypted_blob: m.encrypted_blob,
                                timestamp: m.timestamp,
                                key_version: m.key_version,
                                replica_id: m.replica_id,
                            };
                            if let Err(e) = dq.process_p2p_mutation(core_mutation).await {
                                tracing::error!(error = %e, "Failed to process P2P mutation");
                            }
                        }
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to initialize P2P transport");
                }
            }
        }

        let _t = crate::time_utils::system_time_now_ms();
        tracing::debug!("[client.new] shared_memory start");
        let shared_memory = Arc::new(
            crate::ipc::shared_memory::SharedMemory::create_with_namespace(
                &config.namespace,
                64 * 1024,
            )?,
        );
        tracing::debug!("[client.new] shared_memory done {} ms", crate::time_utils::system_time_now_ms() - _t);

        let _t = crate::time_utils::system_time_now_ms();
        #[cfg_attr(not(target_arch = "wasm32"), allow(unused_variables))]
        let (events, upload_event_rx, download_event_rx, pending_count_rx) =
            crate::sync::events::SyncEvents::new();

        // Clone before moving into Self (used by compaction worker below)
        let coord_for_comp = coordinator.clone();
        let pending_count_tx = events.pending_count.clone();
        let pending_cache = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let client = Self {
            config,
            storage,
            coordinator,
            keyring: keyring.clone(),
            encryptor,
            _decryptor: decryptor,
            _oplog: oplog,
            schema,
            subscriptions,
            upload_queue: upload_queue.clone(),
            download_queue: download_queue.clone(),
            _reconciler: reconciler,
            _telemetry,
            #[cfg(target_arch = "wasm32")]
            shutdown_flag: shutdown_flag.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            shutdown_tx,
            #[cfg(not(target_arch = "wasm32"))]
            p2p_handle,
            metrics,
            debug_api,
            leader_election: leader_election.clone(),
            schema_version,
            shared_memory,
            events,
            initialized: std::sync::atomic::AtomicBool::new(false),
            clock: crate::clock::HybridLogicalClock::new(),
            pending_count_cache: pending_cache.clone(),
            #[cfg(target_arch = "wasm32")]
            pending_count_rx: std::sync::Mutex::new(Some(pending_count_rx)),
            download_worker_rx: std::sync::Mutex::new(Some(download_event_rx)),
        };
        tracing::debug!("[client.new] construction done {} ms", crate::time_utils::system_time_now_ms() - _t);

        // Initialize pending count cache from storage
        let initial_pending = upload_queue.pending_count().await.unwrap_or(0);
        pending_cache.store(initial_pending, std::sync::atomic::Ordering::Relaxed);

        let namespace_clone = client.config.namespace.clone();
        let storage_clone = client.storage.clone();
        let upload_events_rx = upload_event_rx;

        #[cfg(not(target_arch = "wasm32"))]
        {
            let cleanup = crate::oplog::cleanup::OplogCleanup::new(storage_clone.clone());
            let ns = namespace_clone.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move {
                cleanup
                    .run_scheduled(
                        &ns,
                        std::time::Duration::from_secs(7 * 24 * 3600), // 7-day retention
                        std::time::Duration::from_secs(3600),          // run every hour
                        rx,
                    )
                    .await;
            });
        }

        // ── Upload worker (event-driven with offline backoff) ─────────────
        let uq = upload_queue.clone();
        let mut upload_rx = upload_events_rx;
        let pc_tx = pending_count_tx;
        let pc_cache = pending_cache.clone();
        crate::time_utils::spawn(async move {
            let mut offline = false;
            let mut backoff_ms: u64 = 0;
            loop {
                // Wait for a notification. While offline, skip the channel
                // and wait for the backoff timer directly.
                let wake_source: &str;
                if offline {
                    if backoff_ms > 0 {
                        crate::time_utils::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                    wake_source = "timer";
                    // Drain any stale notifications that piled up while we slept
                    while let Ok(()) = upload_rx.try_recv() {}
                } else {
                    wake_source = "notify";
                    match upload_rx.next().await {
                        Some(_) => {}
                        None => break,
                    }
                }

                tracing::debug!(
                    "[upload_worker] WAKE source={} offline={} backoff={}ms pending_cache={}",
                    wake_source, offline, backoff_ms, pc_cache.load(std::sync::atomic::Ordering::Relaxed),
                );

                // Process all pending batches
                loop {
                    match uq.process_batch().await {
                        Ok(0) => {
                            // Either no pending entries OR coordinator unavailable.
                            // Distinguish by checking pending count.
                            if let Ok(count) = uq.pending_count().await {
                                if count > 0 && !offline {
                                    tracing::warn!(
                                        "[upload_worker] OFFLINE_ENTER pending={} backoff=1s",
                                        count
                                    );
                                    offline = true;
                                    backoff_ms = 1000;
                                } else if count > 0 {
                                    // Still offline after retry; increase backoff
                                    backoff_ms = std::cmp::min(backoff_ms * 2, 30000);
                                    tracing::debug!(
                                        "[upload_worker] OFFLINE_RETRY pending={} backoff={}ms",
                                        count, backoff_ms,
                                    );
                                } else if count == 0 && offline {
                                    // All pending cleared — return to online
                                    tracing::info!("[upload_worker] ONLINE pending=0");
                                    offline = false;
                                    backoff_ms = 0;
                                }
                            }
                            break;
                        }
                        Ok(n) => {
                            // Successfully pushed — reset offline state
                            if offline {
                                tracing::info!("[upload_worker] RECONNECTED pushed={}", n);
                            }
                            offline = false;
                            backoff_ms = 0;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!("[upload_worker] BATCH_FAILED {:?}", e);
                            break;
                        }
                    }
                }

                // Update pending count cache
                if let Ok(count) = uq.pending_count().await {
                    pc_cache.store(count, std::sync::atomic::Ordering::Relaxed);
                    let _ = pc_tx.unbounded_send(count);
                }
            }
        });

        // ── Download worker (push-driven) — spawned in initialize() after subscribe ──

        // ── Leader election + compaction timer ────────────────────────────
        let le = leader_election.clone();
        let ns = namespace_clone.clone();
        let comp_storage = storage_clone.clone();
        let comp_coord = coord_for_comp;
        crate::time_utils::spawn(async move {
            let compaction_engine = crate::sync::compaction::CompactionEngine::new(
                comp_storage,
                crate::sync::compaction::CompactionConfig::default(),
            );
            let mut last_compaction = crate::time_utils::PlatformInstant::now();
            let mut last_snapshot = crate::time_utils::PlatformInstant::now();

            loop {
                crate::time_utils::sleep(std::time::Duration::from_secs(30)).await;
                // Age-based compaction and tombstone cleanup run on ALL tabs
                if last_compaction.elapsed() >= std::time::Duration::from_secs(300) {
                    let _ = compaction_engine.run_compaction(&ns).await;
                    last_compaction = crate::time_utils::PlatformInstant::now();
                }
                // Snapshot compaction runs only on the leader (re-writes docs)
                if le.try_acquire().unwrap_or(false)
                    && last_snapshot.elapsed() >= std::time::Duration::from_secs(900)
                {
                    match compaction_engine.run_snapshot_compaction(&ns).await {
                        Ok((_stats, snapshots)) => {
                            for snap in &snapshots {
                                if let Err(e) = comp_coord.store_snapshot(&ns, snap).await {
                                    tracing::warn!(
                                        "[compaction] failed to upload snapshot doc={} seq={}: {:?}",
                                        snap.doc_id, snap.sequence, e
                                    );
                                } else {
                                    tracing::debug!(
                                        "[compaction] uploaded snapshot doc={} seq={}",
                                        snap.doc_id, snap.sequence
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[compaction] snapshot run failed: {:?}", e);
                        }
                    }
                    last_snapshot = crate::time_utils::PlatformInstant::now();
                }
            }
        });

        if auto_initialize {
            client.initialize().await?;
        }

        #[cfg(feature = "telemetry")]
        if let Some(port) = client.config.debug_port {
            let rx = client.shutdown_tx.subscribe();
            client.debug_api.start(
                port,
                client.config.namespace.clone(),
                client.config.replica_id.clone(),
                client.keyring.clone(),
                client.coordinator.clone(),
                client.leader_election.clone(),
                client.upload_queue.clone(),
                client.download_queue.clone(),
                rx,
            );
        }

        Ok(client)
    }

    pub async fn new_with_storage_and_migrations(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
        storage: Arc<dyn Storage>,
        migrations: Vec<Arc<crate::schema::migration::MigrationDefinition>>,
    ) -> Result<Self, VaultSyncError> {
        Self::build_client(config, coordinator, keyring, storage, migrations, true).await
    }

    /// Like `new_with_storage`, but does NOT call `initialize()` internally.
    /// The caller MUST call `initialize()` before using the client.
    /// Use this when you need to wire event channels (e.g., download notification)
    /// before the WebSocket background processor starts.
    pub async fn new_with_storage_skip_init(
        config: VaultSyncConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
        storage: Arc<dyn Storage>,
    ) -> Result<Self, VaultSyncError> {
        Self::build_client(
            config,
            coordinator,
            keyring,
            storage,
            crate::schema::migration::get_global_migrations(),
            false,
        )
        .await
    }

    pub async fn initialize(&self) -> Result<(), VaultSyncError> {
        if self.initialized.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        tracing::info!(namespace = %self.config.namespace, mode = ?self.config.coordinator_mode, "VaultSync client initializing");

        if self.config.coordinator_mode == crate::config::CoordinatorMode::Offline {
            tracing::info!("Coordinator mode: offline, skipping register/generation check");
            return Ok(());
        }

        let replica_info = ReplicaInfo {
            replica_id: self.config.replica_id.clone(),
            namespace: self.config.namespace.clone(),
            public_key: self.keyring.active_key().public_key.to_vec(),
            schema_version: self.schema_version,
        };

        let cursor = self.download_queue.last_sequence();
        tracing::info!("[Coordinator] register cursor={}", cursor);
        let registered = self.coordinator
            .register(&self.config.namespace, replica_info.clone(), cursor)
            .await;

        match registered {
            Err(e) => {
                tracing::warn!("Coordinator unavailable, continuing offline: {:?}", e);
            }
            Ok(()) => {
                let cursor_before = self.download_queue.last_sequence();
                tracing::info!("[Coordinator] register returned cursor={}", cursor_before);

                // Check generation ID — reset cursor if server restarted
                let server_gen = self.coordinator.generation_id().await;
                if !server_gen.is_empty() {
                    tracing::info!("[Recovery] server gen={} cursor={}", server_gen, cursor_before);
                    if let Some(mut state) = self.storage.read_sync_state(&self.config.namespace).await? {
                        if state.generation_id != server_gen {
                            let cursor_was = cursor_before;

                            // Keep cursor on gen mismatch — only reset if server proves cursor invalid
                            state.generation_id = server_gen.clone();
                            let now_ms = crate::time_utils::system_time_now_ms();
                            state.last_sync_at = Some(now_ms);
                            // Preserve cursor in sync state (don't reset to 0)
                            self.storage.write_sync_state(&state).await?;
                            tracing::info!(
                                "[Recovery] gen mismatch: cursor={} gen={} (will validate)",
                                cursor_was,
                                state.generation_id
                            );

                            self.download_queue.set_replay_mode(true);
                            let _ = self.coordinator.disconnect().await;
                            match self.coordinator
                                .register(&self.config.namespace, replica_info.clone(), cursor_was)
                                .await
                            {
                                Ok(()) => {
                                    tracing::info!("[Recovery] re-registered cursor={}", cursor_was);
                                    // Evidence-based cursor validation
                                    let server_max = self.coordinator.max_sequence().await;
                                    if server_max == 0 {
                                        tracing::info!(
                                            "[Recovery] cursor validation unavailable (server_max=0), keeping cursor={}",
                                            cursor_was
                                        );
                                    } else if server_max >= cursor_was {
                                        tracing::info!(
                                            "[Recovery] cursor valid: server_max={} >= cursor={}",
                                            server_max, cursor_was
                                        );
                                    } else {
                                        tracing::info!(
                                            "[Recovery] cursor INVALID: server_max={} < cursor={} -> resetting to 0",
                                            server_max, cursor_was
                                        );
                                        state.last_synced_sequence = 0;
                                        self.storage.write_sync_state(&state).await?;
                                        self.download_queue.reset_cursor(0);
                                        let _ = self.coordinator.disconnect().await;
                                        match self.coordinator
                                            .register(&self.config.namespace, replica_info.clone(), 0)
                                            .await
                                        {
                                            Ok(()) => {
                                                tracing::info!("[Recovery] re-registered cursor=0 (full replay)");
                                            }
                                            Err(e) => {
                                                tracing::warn!("Re-register failed at cursor=0: {:?}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Re-register failed after gen change: {:?}", e);
                                }
                            }
                        } else {
                            tracing::info!("[Recovery] generation OK gen={}", server_gen);
                        }
                    }
                }
            }
        }

        // ── Subscribe for push notifications after recovery ──
        let sub_cursor = self.download_queue.last_sequence();
        if self.coordinator.subscribe(&self.config.namespace, sub_cursor).await.is_ok() {
            tracing::info!("[Subscribed] after={}", sub_cursor);
        } else {
            tracing::warn!("[Subscribed] failed (pull-only mode)");
        }

        // ── Start download worker now that initialization is complete ──
        self.start_download_worker();

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), VaultSyncError> {
        tracing::info!("VaultSync client shutting down");
        #[cfg(target_arch = "wasm32")]
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(target_arch = "wasm32"))]
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    /// Spawns the download worker thread/task.
    /// Called once from `initialize()` after subscribe completes,
    /// ensuring the worker never wakes before recovery is done.
    fn start_download_worker(&self) {
        let rx = self.download_worker_rx.lock().unwrap().take();
        let Some(mut download_rx) = rx else {
            tracing::warn!("[download_worker] already started or rx missing");
            return;
        };
        let dq = self.download_queue.clone();
        crate::time_utils::spawn(async move {
            // Drain stale wakeups accumulated before subscribe
            while let Ok(_) = download_rx.try_recv() {}

            tracing::debug!(
                "[download_worker] started cursor={}",
                dq.last_sequence()
            );

            loop {
                let mut needs_backfill = true;

                match download_rx.next().await {
                    None => {
                        tracing::debug!("[download_worker] channel closed, exiting");
                        break;
                    }
                    Some(Some(m)) => {
                        tracing::info!(
                            "[download_worker] push seq={} id={}",
                            m.sequence,
                            m.id
                        );
                        match dq.process_push_mutation(m).await {
                            Ok(outcome) => {
                                tracing::debug!(
                                    "[download_worker] push outcome={:?}",
                                    outcome
                                );
                                needs_backfill = outcome == crate::sync::download::PushOutcome::GapDetected;
                                if !needs_backfill && dq.is_replay_mode() {
                                    needs_backfill = true;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "[download_worker] process_push_mutation error: {:?}",
                                    e
                                );
                            }
                        }
                    }
                    Some(None) => {
                        // Bootstrap or wakeup: drain process_batch
                    }
                }

                if needs_backfill {
                    loop {
                        match dq.process_batch().await {
                            Ok(0) => {
                                tracing::debug!("[download_worker] process_batch -> 0");
                                break;
                            }
                            Ok(count) => {
                                tracing::info!(
                                    "[download_worker] process_batch -> {}",
                                    count
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "[download_worker] process_batch error {:?}",
                                    e
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Filter out engine-managed metadata fields from user-provided field maps.
    /// These fields are auto-set by the engine to prevent unnecessary CRDT mutations.
    fn filter_meta_fields(fields: &mut HashMap<String, CrdtValue>) {
        fields.remove("updatedAt");
        fields.remove("updated_at");
    }

    pub async fn insert(
        &self,
        doc_id: &str,
        record_id: &str,
        mut fields: HashMap<String, CrdtValue>,
    ) -> Result<(), VaultSyncError> {
        Self::filter_meta_fields(&mut fields);

        let span = tracing::info_span!(
            "vaultsync.write",
            doc_id = doc_id,
            record_id = record_id,
            namespace = self.config.namespace.as_str()
        );
        let _enter = span.enter();

        self.metrics
            .record_mutation_attempt(&self.config.namespace, "attempt");

        let mutation_id = uuid::Uuid::new_v4().to_string();
        let mut doc = CRDTDocument::new(doc_id, record_id, 0);

        // Auto-set updatedAt to HLC wall clock — it reflects when content was created
        let hlc = self.clock.now();
        fields.insert("updatedAt".to_string(), CrdtValue::Number(hlc.wall as f64));

        let sv_before = doc.state_vector();
        let sv_before_entries: Vec<_> = sv_before.iter().map(|(c, cl)| (c, cl)).collect();
        let snap_before = doc.to_snapshot();
        let snap_hash_before = hex::encode(&Sha256::digest(&snap_before)[..8]);
        let doc_ptr = &doc as *const CRDTDocument as usize;
        let inner_doc_ptr = doc.inner_doc() as *const yrs::Doc as usize;

        let update_bytes = doc.capture_incremental_update(|doc| {
            for (field, value) in &fields {
                doc.set_field(field, value.clone());
            }
        });

        let sv_after = doc.state_vector();
        let sv_after_entries: Vec<_> = sv_after.iter().map(|(c, cl)| (c, cl)).collect();
        let snap_after = doc.to_snapshot();
        let snap_hash_after = hex::encode(&Sha256::digest(&snap_after)[..8]);
        let doc_advanced = sv_before != sv_after;

        let content_hash = hex::encode(&Sha256::digest(&update_bytes)[..8]);
        if let Ok(decoded) = Update::decode_v1(&update_bytes) {
            let update_sv = decoded.state_vector();
            let update_sv_entries: Vec<_> = update_sv.iter().map(|(c, cl)| (c, cl)).collect();
            tracing::trace!(
                "[client.insert] id={} yrs_update source=client doc={} record={} sha256={} update_sv={:?} len={} doc_ptr=0x{:x} inner_doc_ptr=0x{:x} sv_before={:?} sv_after={:?} snap_hash_before={} snap_hash_after={} doc_advanced={}",
                mutation_id, doc_id, record_id, content_hash, update_sv_entries, update_bytes.len(),
                doc_ptr, inner_doc_ptr, sv_before_entries, sv_after_entries,
                snap_hash_before, snap_hash_after, doc_advanced,
            );
        }

        if !doc_advanced && !update_bytes.is_empty() {
            tracing::warn!(
                "[client.insert] MUTATION_DID_NOT_ADVANCE id={} doc={} record={} update_len={} sha256={} sv_before={:?}",
                mutation_id, doc_id, record_id, update_bytes.len(), content_hash, sv_before_entries,
            );
        }

        if update_bytes.len() > self.config.max_document_size {
            return Err(VaultSyncError::DocumentTooLarge(
                update_bytes.len(),
                self.config.max_document_size,
            ));
        }
        let snapshot = doc.to_snapshot();

        let encrypted_blob = self
            .encryptor
            .encrypt_symmetric(&update_bytes, &self.config.namespace)?;

        if self.clock.did_logical_wrap() {
            self.metrics.record_hlc_wrap();
            self.clock.clear_logical_wrap();
        }
        let entry = OplogEntry::new(
            mutation_id,
            self.config.replica_id.clone(),
            self.config.namespace.clone(),
            MutationType::CrdtInsert,
            doc_id.to_string(),
            record_id.to_string(),
            update_bytes,
            Some(encrypted_blob),
            hlc.wall,
            None,
            SyncStatus::Optimistic,
            None,
            hlc.wall,
            MutationOrigin::Insert,
            "",
        );

        let append_span = tracing::info_span!(
            "oplog.append",
            entry_id = entry.id.as_str(),
            namespace = self.config.namespace.as_str()
        );
        let append_guard = append_span.enter();
        let res = self
            .storage
            .write_document_and_oplog(doc_id, record_id, &snapshot, &entry)
            .await;
        drop(append_guard);

        match res {
            Ok(_) => {
                let _ = self.shared_memory.write_entry(&entry.id, &entry.yrs_update);
                let key_version = self.keyring.active_key().version;
                self.broadcast_p2p(&entry, key_version).await;
                self.events.notify_upload();
                self.pending_count_cache.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .record_optimistic_write();
                self.metrics
                    .record_mutation_attempt(&self.config.namespace, "success");
                let pending = self.pending_uploads().await.unwrap_or(0);
                self.metrics
                    .set_mutations_pending(&self.config.namespace, pending as i64);
                if let Ok(entries) = self
                    .storage
                    .read_pending_oplog(&self.config.namespace, 10000)
                    .await
                {
                    self.metrics
                        .set_oplog_size(&self.config.namespace, entries.len() as i64);
                }

                // Fire reactive subscriptions locally
                let state = doc.to_map();
                let sub_span = tracing::info_span!("subscription.fire", doc_id = doc_id);
                let _sub_guard = sub_span.enter();
                self.subscriptions
                    .lock()
                    .unwrap()
                    .fire(FireSource::LocalWrite, doc_id, record_id, &state);

                Ok(())
            }
            Err(e) => {
                self.metrics
                    .record_mutation_failed(&self.config.namespace, "storage_error");
                Err(e)
            }
        }
    }

    pub async fn update(
        &self,
        doc_id: &str,
        record_id: &str,
        mut fields: HashMap<String, CrdtValue>,
    ) -> Result<(), VaultSyncError> {
        Self::filter_meta_fields(&mut fields);

        let span = tracing::info_span!(
            "vaultsync.write",
            doc_id = doc_id,
            record_id = record_id,
            namespace = self.config.namespace.as_str()
        );
        let _enter = span.enter();

        self.metrics
            .record_mutation_attempt(&self.config.namespace, "attempt");

        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(doc_id, record_id, 0),
        };

        // ── Phase 0: skip no-op writes (no content field changed) ──
        let content_changed = fields.iter().any(|(key, value)| {
            doc.get_field(key).as_ref().map_or(true, |existing| existing != value)
        });
        if !content_changed {
            tracing::debug!(
                "[client.update] SKIP no-op mutation doc={} record={} — content unchanged",
                doc_id, record_id,
            );
            return Ok(());
        }

        let mutation_id = uuid::Uuid::new_v4().to_string();

        // Auto-set updatedAt to HLC wall clock (content changed, so timestamp advances)
        let hlc = self.clock.now();
        if self.clock.did_logical_wrap() {
            self.metrics.record_hlc_wrap();
            self.clock.clear_logical_wrap();
        }
        fields.insert("updatedAt".to_string(), CrdtValue::Number(hlc.wall as f64));

        // ── Phase 1: state vector + snapshot hash BEFORE mutation ──
        let sv_before = doc.state_vector();
        let sv_before_entries: Vec<_> = sv_before.iter().map(|(c, cl)| (c, cl)).collect();
        let snap_before = doc.to_snapshot();
        let snap_hash_before = hex::encode(&Sha256::digest(&snap_before)[..8]);
        let doc_ptr = &doc as *const CRDTDocument as usize;
        let inner_doc_ptr = doc.inner_doc() as *const yrs::Doc as usize;

        let update_bytes = doc.capture_incremental_update(|doc| {
            for (field, value) in &fields {
                doc.set_field(field, value.clone());
            }
        });

        // ── Phase 1: state vector + snapshot hash AFTER mutation ──
        let sv_after = doc.state_vector();
        let sv_after_entries: Vec<_> = sv_after.iter().map(|(c, cl)| (c, cl)).collect();
        let snap_after = doc.to_snapshot();
        let snap_hash_after = hex::encode(&Sha256::digest(&snap_after)[..8]);
        let doc_advanced = sv_before != sv_after;

        let content_hash = hex::encode(&Sha256::digest(&update_bytes)[..8]);
        if let Ok(decoded) = Update::decode_v1(&update_bytes) {
            let update_sv = decoded.state_vector();
            let update_sv_entries: Vec<_> = update_sv.iter().map(|(c, cl)| (c, cl)).collect();
            tracing::trace!(
                "[client.update] id={} yrs_update source=client doc={} record={} sha256={} update_sv={:?} len={} doc_ptr=0x{:x} inner_doc_ptr=0x{:x} sv_before={:?} sv_after={:?} snap_hash_before={} snap_hash_after={} doc_advanced={}",
                mutation_id, doc_id, record_id, content_hash, update_sv_entries, update_bytes.len(),
                doc_ptr, inner_doc_ptr, sv_before_entries, sv_after_entries,
                snap_hash_before, snap_hash_after, doc_advanced,
            );
        }

        if !doc_advanced && !update_bytes.is_empty() {
            tracing::warn!(
                "[client.update] MUTATION_DID_NOT_ADVANCE id={} doc={} record={} update_len={} sha256={} sv_before={:?}",
                mutation_id, doc_id, record_id, update_bytes.len(), content_hash, sv_before_entries,
            );
        }

        if update_bytes.len() > self.config.max_document_size {
            return Err(VaultSyncError::DocumentTooLarge(
                update_bytes.len(),
                self.config.max_document_size,
            ));
        }
        let snapshot = doc.to_snapshot();

        let encrypted_blob = self
            .encryptor
            .encrypt_symmetric(&update_bytes, &self.config.namespace)?;

        let entry = OplogEntry::new(
            mutation_id,
            self.config.replica_id.clone(),
            self.config.namespace.clone(),
            MutationType::CrdtUpdate,
            doc_id.to_string(),
            record_id.to_string(),
            update_bytes,
            Some(encrypted_blob),
            hlc.wall,
            None,
            SyncStatus::Optimistic,
            None,
            hlc.wall,
            MutationOrigin::Update,
            "",
        );

        let append_span = tracing::info_span!(
            "oplog.append",
            entry_id = entry.id.as_str(),
            namespace = self.config.namespace.as_str()
        );
        let append_guard = append_span.enter();
        let write_t0 = web_time::Instant::now();
        let res = self
            .storage
            .write_document_and_oplog(doc_id, record_id, &snapshot, &entry)
            .await;
        let write_elapsed = write_t0.elapsed();
        tracing::debug!(
            "[pipeline] write_document_and_oplog id={} doc={} record={} elapsed={}ms status={}",
            entry.id, doc_id, record_id, write_elapsed.as_millis(),
            if res.is_ok() { "ok" } else { "err" },
        );
        drop(append_guard);

        match res {
            Ok(_) => {
                let _ = self.shared_memory.write_entry(&entry.id, &entry.yrs_update);
                let key_version = self.keyring.active_key().version;
                self.broadcast_p2p(&entry, key_version).await;
                self.events.notify_upload();
                self.pending_count_cache.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .record_optimistic_write();
                self.metrics
                    .record_mutation_attempt(&self.config.namespace, "success");
                let pending = self.pending_uploads().await.unwrap_or(0);
                self.metrics
                    .set_mutations_pending(&self.config.namespace, pending as i64);
                if let Ok(entries) = self
                    .storage
                    .read_pending_oplog(&self.config.namespace, 10000)
                    .await
                {
                    self.metrics
                        .set_oplog_size(&self.config.namespace, entries.len() as i64);
                }

                // Fire reactive subscriptions locally
                let state = doc.to_map();
                let sub_span = tracing::info_span!("subscription.fire", doc_id = doc_id);
                let _sub_guard = sub_span.enter();
                self.subscriptions
                    .lock()
                    .unwrap()
                    .fire(FireSource::LocalWrite, doc_id, record_id, &state);

                Ok(())
            }
            Err(e) => {
                self.metrics
                    .record_mutation_failed(&self.config.namespace, "storage_error");
                Err(e)
            }
        }
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let span = tracing::info_span!(
            "vaultsync.write",
            doc_id = doc_id,
            record_id = record_id,
            namespace = self.config.namespace.as_str()
        );
        let _enter = span.enter();

        self.metrics
            .record_mutation_attempt(&self.config.namespace, "attempt");

        let existing = self.storage.get_document(doc_id, record_id).await?;
        if let Some(bytes) = existing {
            let mut doc = CRDTDocument::from_snapshot(&bytes)?;
            let mutation_id = uuid::Uuid::new_v4().to_string();

            let sv_before = doc.state_vector();
            let sv_before_entries: Vec<_> = sv_before.iter().map(|(c, cl)| (c, cl)).collect();
            let snap_before = doc.to_snapshot();
            let snap_hash_before = hex::encode(&Sha256::digest(&snap_before)[..8]);
            let doc_ptr = &doc as *const CRDTDocument as usize;
            let inner_doc_ptr = doc.inner_doc() as *const yrs::Doc as usize;

            let update_bytes = doc.set_field("_deleted", CrdtValue::Boolean(true));

            let sv_after = doc.state_vector();
            let sv_after_entries: Vec<_> = sv_after.iter().map(|(c, cl)| (c, cl)).collect();
            let snap_after = doc.to_snapshot();
            let snap_hash_after = hex::encode(&Sha256::digest(&snap_after)[..8]);
            let doc_advanced = sv_before != sv_after;

            let content_hash = hex::encode(&Sha256::digest(&update_bytes)[..8]);
            if let Ok(decoded) = Update::decode_v1(&update_bytes) {
                let update_sv = decoded.state_vector();
                let update_sv_entries: Vec<_> = update_sv.iter().map(|(c, cl)| (c, cl)).collect();
                    tracing::trace!(
                        "[client.delete] id={} yrs_update source=client doc={} record={} sha256={} update_sv={:?} len={} doc_ptr=0x{:x} inner_doc_ptr=0x{:x} sv_before={:?} sv_after={:?} snap_hash_before={} snap_hash_after={} doc_advanced={}",
                    mutation_id, doc_id, record_id, content_hash, update_sv_entries, update_bytes.len(),
                    doc_ptr, inner_doc_ptr, sv_before_entries, sv_after_entries,
                    snap_hash_before, snap_hash_after, doc_advanced,
                );
            }

            if !doc_advanced && !update_bytes.is_empty() {
                tracing::warn!(
                    "[client.delete] MUTATION_DID_NOT_ADVANCE id={} doc={} record={} update_len={} sha256={} sv_before={:?}",
                    mutation_id, doc_id, record_id, update_bytes.len(), content_hash, sv_before_entries,
                );
            }

            let snapshot = doc.to_snapshot();

            let encrypted_blob = self
                .encryptor
                .encrypt_symmetric(&update_bytes, &self.config.namespace)?;

            let hlc = self.clock.now();
            if self.clock.did_logical_wrap() {
                self.metrics.record_hlc_wrap();
                self.clock.clear_logical_wrap();
            }
            let entry = OplogEntry::new(
                mutation_id.clone(),
                self.config.replica_id.clone(),
                self.config.namespace.clone(),
                MutationType::CrdtDelete,
                doc_id.to_string(),
                record_id.to_string(),
                update_bytes,
                Some(encrypted_blob),
                hlc.wall,
                None,
                SyncStatus::Optimistic,
                None,
                hlc.wall,
                MutationOrigin::Delete,
                "",
            );

            let append_span = tracing::info_span!(
                "oplog.append",
                entry_id = entry.id.as_str(),
                namespace = self.config.namespace.as_str()
            );
            let append_guard = append_span.enter();
            let write_t0 = web_time::Instant::now();
            let res = self
                .storage
                .write_document_and_oplog(doc_id, record_id, &snapshot, &entry)
                .await;
            let write_elapsed = write_t0.elapsed();
            tracing::debug!(
                "[pipeline] write_document_and_oplog id={} doc={} record={} elapsed={}ms status={}",
                mutation_id, doc_id, record_id, write_elapsed.as_millis(),
                if res.is_ok() { "ok" } else { "err" },
            );
            drop(append_guard);

            match res {
            Ok(_) => {
                let _ = self.shared_memory.write_entry(&entry.id, &entry.yrs_update);
                let key_version = self.keyring.active_key().version;
                self.broadcast_p2p(&entry, key_version).await;
                self.events.notify_upload();
                self.pending_count_cache.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .record_optimistic_write();
                self.metrics
                    .record_mutation_attempt(&self.config.namespace, "success");
                let pending = self.pending_uploads().await.unwrap_or(0);
                self.metrics
                    .set_mutations_pending(&self.config.namespace, pending as i64);
                if let Ok(entries) = self
                    .storage
                    .read_pending_oplog(&self.config.namespace, 10000)
                    .await
                {
                    self.metrics
                        .set_oplog_size(&self.config.namespace, entries.len() as i64);
                }

                    // Fire reactive subscriptions locally
                    let state = doc.to_map();
                    let sub_span = tracing::info_span!("subscription.fire", doc_id = doc_id);
                    let _sub_guard = sub_span.enter();
                    self.subscriptions
                        .lock()
                        .unwrap()
                        .fire(FireSource::LocalWrite, doc_id, record_id, &state);
                }
                Err(e) => {
                    self.metrics
                        .record_mutation_failed(&self.config.namespace, "storage_error");
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub async fn get(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<HashMap<String, CrdtValue>>, VaultSyncError> {
        match self.storage.get_document(doc_id, record_id).await? {
            Some(bytes) => {
                let doc = CRDTDocument::from_snapshot(&bytes)?;
                let map = doc.to_map();
                if let Some(CrdtValue::Boolean(true)) = map.get("_deleted") {
                    Ok(None)
                } else {
                    Ok(Some(map))
                }
            }
            None => Ok(None),
        }
    }

    pub async fn find(
        &self,
        doc_id: &str,
        filter: Option<&Filter>,
    ) -> Result<Vec<HashMap<String, CrdtValue>>, VaultSyncError> {
        let docs = self.storage.list_documents(doc_id).await?;
        let mut results = Vec::new();
        for (_rid, bytes) in docs {
            let doc = CRDTDocument::from_snapshot(&bytes)?;
            let map = doc.to_map();
            if let Some(CrdtValue::Boolean(true)) = map.get("_deleted") {
                continue;
            }
            if filter.as_ref().is_none_or(|f| f.matches(&map)) {
                results.push(map);
            }
        }
        Ok(results)
    }

    pub fn subscribe(
        &self,
        doc_id: &str,
        callback: Callback,
    ) -> crate::subscription::engine::SubscriptionHandle {
        self.subscriptions
            .lock()
            .unwrap()
            .register(doc_id, callback)
    }

    pub fn unsubscribe(
        &self,
        handle: crate::subscription::engine::SubscriptionHandle,
    ) -> Result<(), VaultSyncError> {
        self.subscriptions.lock().unwrap().unregister(handle)
    }

    #[allow(clippy::type_complexity)]
    pub fn set_change_listener(&self, listener: Arc<dyn Fn(&str, &str) + Send + Sync>) {
        self.subscriptions
            .lock()
            .unwrap()
            .set_global_listener(listener);
    }

    pub fn fire_local_subscription(
        &self,
        doc_id: &str,
        record_id: &str,
        state: &HashMap<String, CrdtValue>,
    ) {
        self.subscriptions
            .lock()
            .unwrap()
            .fire_local(doc_id, record_id, state);
    }

    pub async fn force_sync(&self) -> Result<usize, VaultSyncError> {
        let uploaded = self.upload_queue.process_batch().await?;
        let downloaded = self.download_queue.process_batch().await?;
        Ok(uploaded + downloaded)
    }

    pub async fn sync_status(&self) -> Result<SyncState, VaultSyncError> {
        let is_leader = self.leader_election.is_leader();
        if let Some(mut state) = self.storage.read_sync_state(&self.config.namespace).await? {
            state.leader_status = Some(is_leader);
            Ok(state)
        } else {
            let last_seq = self.download_queue.last_sequence();
            Ok(SyncState {
                namespace: self.config.namespace.clone(),
                replica_id: self.config.replica_id.clone(),
                last_synced_sequence: last_seq,
                connection_status: crate::sync::state::ConnectionStatus::Connected,
                leader_status: Some(is_leader),
                last_connected_at: None,
                last_sync_at: None,
                schema_version: 0,
                generation_id: String::new(),
            })
        }
    }

    /// Returns the cached pending upload count. Updated by the upload worker
    /// after each batch and by write methods. Avoids OPFS reads on every call.
    pub async fn pending_uploads(&self) -> Result<usize, VaultSyncError> {
        Ok(self.pending_count_cache.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Take the pending-count receiver for event-driven status updates.
    /// Only used by the WASM bridge to push changes to JS.
    #[cfg(target_arch = "wasm32")]
    pub fn take_pending_count_rx(
        &self,
    ) -> Option<futures::channel::mpsc::UnboundedReceiver<usize>> {
        self.pending_count_rx.lock().unwrap().take()
    }

    pub async fn rotate_keys(&self) -> Result<(), VaultSyncError> {
        let span = tracing::info_span!(
            "vaultsync.rotate_keys",
            namespace = self.config.namespace.as_str()
        );
        let _enter = span.enter();

        let old_active_key = self.keyring.active_key();
        let new_pair = self.keyring.rotate();
        tracing::info!(key_version = new_pair.version, "Key rotated locally");

        let old_keyring = Arc::new(KeyRing::from_key(old_active_key));
        let old_decryptor = E2eeDecryptor::new(old_keyring);
        let new_encryptor = self.encryptor.clone();

        let pending = self
            .storage
            .read_pending_oplog(&self.config.namespace, 10000)
            .await?;
        let mut re_encrypted = 0usize;
        for entry in pending {
            if let Some(encrypted_blob) = entry.encrypted_blob {
                let plaintext =
                    old_decryptor.decrypt_symmetric(&encrypted_blob, &self.config.namespace)?;
                let new_encrypted_blob =
                    new_encryptor.encrypt_symmetric(&plaintext, &self.config.namespace)?;
                self.storage
                    .update_oplog_encrypted_blob(&entry.id, &new_encrypted_blob)
                    .await?;
                re_encrypted += 1;
            }
        }
        tracing::info!(re_encrypted, "Pending oplog entries re-encrypted");

        self.keyring.prune_old_versions(new_pair.version);

        let key_rec = crate::storage::traits::KeyRecord {
            namespace: self.config.namespace.clone(),
            key_bytes: new_pair.private_key.to_vec(),
            version: new_pair.version,
        };
        self.storage.write_key(&key_rec).await?;

        self.coordinator
            .update_replica_key(
                &self.config.namespace,
                &self.config.replica_id,
                new_pair.public_key.to_vec(),
                new_pair.version,
            )
            .await
            .map_err(|e| VaultSyncError::Coordinator(format!("key update failed: {e:?}")))?;

        self.coordinator
            .register(
                &self.config.namespace,
                ReplicaInfo {
                    replica_id: self.config.replica_id.clone(),
                    namespace: self.config.namespace.clone(),
                    public_key: new_pair.public_key.to_vec(),
                    schema_version: self.schema_version,
                },
                0,
            )
            .await
            .map_err(|e| VaultSyncError::Coordinator(format!("re-register failed: {e:?}")))?;

        self.metrics.record_key_rotation(&self.config.namespace);
        Ok(())
    }

    pub async fn export_public_key(&self) -> Result<Vec<u8>, VaultSyncError> {
        Ok(self.keyring.active_key().public_key.to_vec())
    }

    pub fn active_key_version(&self) -> u64 {
        self.keyring.active_version()
    }

    pub fn list_key_versions(&self) -> Vec<crate::e2ee::keyring::NamespaceKeypair> {
        self.keyring.all_keys()
    }

    pub fn prune_key_versions(&self, keep_versions: u64) {
        self.keyring.prune_to_keep(keep_versions);
    }

    pub async fn define_schema(
        &self,
        doc_id: &str,
        schema: DocumentSchema,
    ) -> Result<(), VaultSyncError> {
        self.schema.lock().unwrap().define(doc_id, schema)
    }

    pub async fn apply_migration(
        &self,
        migration: MigrationDefinition,
    ) -> Result<(), VaultSyncError> {
        if !migration.verify_checksum() {
            return Err(VaultSyncError::Schema(
                "Migration checksum verification failed".into(),
            ));
        }
        let applied = self.storage.read_migrations().await?;
        if let Some(existing) = applied.iter().find(|m| m.version == migration.version) {
            if existing.checksum != migration.checksum {
                return Err(VaultSyncError::Schema(format!(
                    "Migration checksum mismatch for version {}: expected {}, found {}",
                    migration.version, existing.checksum, migration.checksum
                )));
            }
            tracing::info!(version = %migration.version, "Migration already applied, skipping");
            return Ok(());
        }

        if let Err(e) = migration.apply() {
            let _ = migration.rollback();
            return Err(e);
        }

        let epoch = crate::time_utils::system_time_now_secs();
        let record = crate::storage::traits::MigrationRecord {
            version: migration.version.clone(),
            applied_at: epoch,
            checksum: migration.checksum.clone(),
        };
        self.storage.write_migration(&record).await?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn enable_p2p(&mut self) -> Result<(), VaultSyncError> {
        if self.p2p_handle.is_some() {
            return Ok(());
        }
        let (incoming_tx, mut incoming_rx) = tokio::sync::mpsc::channel(100);
        let listen_addr = self.config.p2p_listen_addr.clone();
        let ns = self.config.namespace.clone();
        match vaultsync_transport_libp2p::LibP2pTransport::new(&ns, listen_addr, incoming_tx).await
        {
            Ok((transport, handle)) => {
                self.p2p_handle = Some(handle);
                crate::time_utils::spawn(async move {
                    transport.run().await;
                });
                let dq = self.download_queue.clone();
                crate::time_utils::spawn(async move {
                    while let Some(m) = incoming_rx.recv().await {
                        let core_mutation = crate::coordinator::traits::PendingMutation {
                            id: m.id,
                            namespace: m.namespace,
                            sequence: m.sequence,
                            doc_id: m.doc_id,
                            record_id: m.record_id,
                            encrypted_blob: m.encrypted_blob,
                            timestamp: m.timestamp,
                            key_version: m.key_version,
                            replica_id: m.replica_id,
                        };
                        if let Err(e) = dq.process_p2p_mutation(core_mutation).await {
                            tracing::error!(error = %e, "Failed to process P2P mutation");
                        }
                    }
                });
                Ok(())
            }
            Err(e) => Err(VaultSyncError::Config(format!(
                "Failed to enable P2P: {}",
                e
            ))),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn enable_p2p(&mut self) -> Result<(), VaultSyncError> {
        Err(VaultSyncError::Config(
            "P2P transport is not supported on WebAssembly".into(),
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn disable_p2p(&mut self) {
        self.p2p_handle = None;
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn disable_p2p(&mut self) {}

    #[cfg(not(target_arch = "wasm32"))]
    async fn broadcast_p2p(&self, entry: &OplogEntry, key_version: u64) {
        if let Some(ref p2p) = self.p2p_handle {
            let m = vaultsync_transport_libp2p::PendingMutation {
                id: entry.id.clone(),
                namespace: entry.namespace.clone(),
                sequence: 0,
                doc_id: entry.doc_id.clone(),
                record_id: entry.record_id.clone(),
                encrypted_blob: entry.encrypted_blob.clone().unwrap_or_default(),
                timestamp: entry.timestamp,
                key_version,
                replica_id: entry.replica_id.clone(),
            };
            let _ = p2p.broadcast_mutation(m).await;
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn broadcast_p2p(&self, _entry: &OplogEntry, _key_version: u64) {}
}
