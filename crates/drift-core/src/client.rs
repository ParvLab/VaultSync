use std::collections::HashMap;
use std::sync::Arc;
use crate::config::DriftConfig;
use crate::error::DriftError;
use crate::crdt::document::CRDTDocument;
use crate::crdt::types::CrdtValue;
use crate::e2ee::keyring::{KeyRing, E2eeEncryptor, E2eeDecryptor};
use crate::storage::traits::{Storage, StorageConfig};
#[cfg(feature = "storage-sqlite")]
use crate::storage::sqlite::SQLiteStorage;
use crate::storage::memory::InMemoryStorage;
use crate::oplog::log::OpLog;
use crate::oplog::entry::{OplogEntry, MutationType, SyncStatus};
use crate::schema::registry::{SchemaRegistry, DocumentSchema};
use crate::schema::migration::MigrationDefinition;
use crate::subscription::engine::SubscriptionEngine;
use crate::subscription::filter::Filter;
use crate::sync::upload::UploadQueue;
use crate::sync::download::DownloadQueue;
use crate::sync::reconciler::Reconciler;
use crate::sync::state::SyncState;
use crate::coordinator::traits::{Coordinator, ReplicaInfo};
#[cfg(feature = "async-runtime")]
use crate::coordinator::memory::InMemoryCoordinator;
use crate::telemetry::tracing::DriftTelemetry;

pub type Callback = Box<dyn Fn(&str, &str, &HashMap<String, CrdtValue>) + Send>;

pub struct DriftClient {
    config: DriftConfig,
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
    _telemetry: Arc<DriftTelemetry>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl DriftClient {
    pub async fn new(config: DriftConfig) -> Result<Self, DriftError> {
        let coordinator = Arc::new(InMemoryCoordinator::new());
        let keyring = Arc::new(KeyRing::generate());
        Self::new_with_keyring(config, coordinator, keyring).await
    }

    pub async fn new_with_coordinator(config: DriftConfig, coordinator: Arc<dyn Coordinator>) -> Result<Self, DriftError> {
        let keyring = Arc::new(KeyRing::generate());
        Self::new_with_keyring(config, coordinator, keyring).await
    }

    pub async fn new_with_keyring(
        config: DriftConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
    ) -> Result<Self, DriftError> {
        let storage: Arc<dyn Storage> = match &config.storage {
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::Sqlite { path } => Arc::new(SQLiteStorage::new(path)?),
            #[cfg(not(feature = "storage-sqlite"))]
            StorageConfig::Sqlite { .. } => return Err(DriftError::Config("sqlite storage not available".into())),
            StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
            StorageConfig::Wasm => return Err(DriftError::Config("WASM storage not available in native build".into())),
        };
        Self::new_with_storage(config, coordinator, keyring, storage).await
    }

    pub async fn new_with_storage(
        config: DriftConfig,
        coordinator: Arc<dyn Coordinator>,
        keyring: Arc<KeyRing>,
        storage: Arc<dyn Storage>,
    ) -> Result<Self, DriftError> {
        let encryptor = Arc::new(E2eeEncryptor::new(keyring.clone()));
        let decryptor = Arc::new(E2eeDecryptor::new(keyring.clone()));

        let subscriptions = Arc::new(std::sync::Mutex::new(SubscriptionEngine::new()));
        let reconciler = Arc::new(Reconciler::new(storage.clone(), subscriptions.clone()));

        let oplog = Arc::new(OpLog::new(storage.clone(), &config.namespace));

        let last_sequence = match storage.read_sync_state(&config.namespace).await? {
            Some(state) => state.last_synced_sequence,
            None => 0,
        };

        let upload_queue = Arc::new(UploadQueue::new(
            oplog.clone(),
            coordinator.clone(),
            config.upload.clone(),
            config.retry.clone(),
        ));

        let download_queue = Arc::new(DownloadQueue::new(
            coordinator.clone(),
            storage.clone(),
            &config.namespace,
            last_sequence,
            config.download.clone(),
            reconciler.clone(),
            decryptor.clone(),
        ));

        let schema = Arc::new(std::sync::Mutex::new(SchemaRegistry::new()));
        let _telemetry = Arc::new(DriftTelemetry::new());

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let client = Self {
            config,
            storage,
            coordinator,
            keyring,
            encryptor,
            _decryptor: decryptor,
            _oplog: oplog,
            schema,
            subscriptions,
            upload_queue: upload_queue.clone(),
            download_queue: download_queue.clone(),
            _reconciler: reconciler,
            _telemetry,
            shutdown_tx,
        };

        let upload_queue_clone = upload_queue.clone();
        let download_queue_clone = download_queue.clone();
        let sync_interval = client.config.sync_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + sync_interval, sync_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let _ = upload_queue_clone.process_batch().await;
                        let _ = download_queue_clone.process_batch().await;
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        });

        client.initialize().await?;
        Ok(client)
    }

    pub async fn initialize(&self) -> Result<(), DriftError> {
        tracing::info!(namespace = %self.config.namespace, "Drift client initializing");
        self.coordinator.register(&self.config.namespace, ReplicaInfo {
            replica_id: self.config.replica_id.clone(),
            namespace: self.config.namespace.clone(),
            public_key: self.keyring.active_key().public_key.to_vec(),
            schema_version: 0,
        }).await.map_err(|e| DriftError::Coordinator(format!("register failed: {e:?}")))?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), DriftError> {
        tracing::info!("Drift client shutting down");
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<(), DriftError> {
        let mut doc = CRDTDocument::new(doc_id, record_id, 0);
        for (field, value) in &fields {
            doc.set_field(field, value.clone());
        }
        let update_bytes = doc.to_snapshot();
        let snapshot = update_bytes.clone();

        let encrypted_blob = self.encryptor.encrypt_symmetric(&update_bytes, &self.config.namespace)?;

        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let entry = OplogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: self.config.replica_id.clone(),
            namespace: self.config.namespace.clone(),
            mutation_type: MutationType::CrdtInsert,
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            yrs_update: update_bytes,
            encrypted_blob: Some(encrypted_blob),
            timestamp: epoch,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: epoch,
        };
        
        self.storage.write_document_and_oplog(doc_id, record_id, &snapshot, &entry).await?;

        // Fire reactive subscriptions locally
        let state = doc.to_map();
        self.subscriptions.lock().unwrap().fire(doc_id, record_id, &state);

        Ok(())
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<(), DriftError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(doc_id, record_id, 0),
        };
        for (field, value) in &fields {
            doc.set_field(field, value.clone());
        }
        let update_bytes = doc.to_snapshot();
        let snapshot = update_bytes.clone();

        let encrypted_blob = self.encryptor.encrypt_symmetric(&update_bytes, &self.config.namespace)?;

        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let entry = OplogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: self.config.replica_id.clone(),
            namespace: self.config.namespace.clone(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            yrs_update: update_bytes,
            encrypted_blob: Some(encrypted_blob),
            timestamp: epoch,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: epoch,
        };

        self.storage.write_document_and_oplog(doc_id, record_id, &snapshot, &entry).await?;

        // Fire reactive subscriptions locally
        let state = doc.to_map();
        self.subscriptions.lock().unwrap().fire(doc_id, record_id, &state);

        Ok(())
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        if let Some(bytes) = existing {
            let mut doc = CRDTDocument::from_snapshot(&bytes)?;
            doc.set_field("_deleted", CrdtValue::Boolean(true));
            let update_bytes = doc.to_snapshot();
            let snapshot = update_bytes.clone();

            let encrypted_blob = self.encryptor.encrypt_symmetric(&update_bytes, &self.config.namespace)?;

            let epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            let entry = OplogEntry {
                id: uuid::Uuid::new_v4().to_string(),
                replica_id: self.config.replica_id.clone(),
                namespace: self.config.namespace.clone(),
                mutation_type: MutationType::CrdtDelete,
                doc_id: doc_id.to_string(),
                record_id: record_id.to_string(),
                yrs_update: update_bytes,
                encrypted_blob: Some(encrypted_blob),
                timestamp: epoch,
                sequence: None,
                sync_status: SyncStatus::Pending,
                synced_at: None,
                created_at: epoch,
            };

            self.storage.write_document_and_oplog(doc_id, record_id, &snapshot, &entry).await?;

            // Fire reactive subscriptions locally
            let state = doc.to_map();
            self.subscriptions.lock().unwrap().fire(doc_id, record_id, &state);
        }
        Ok(())
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<Option<HashMap<String, CrdtValue>>, DriftError> {
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

    pub async fn find(&self, doc_id: &str, filter: Option<&Filter>) -> Result<Vec<HashMap<String, CrdtValue>>, DriftError> {
        let docs = self.storage.list_documents(doc_id).await?;
        let mut results = Vec::new();
        for (_rid, bytes) in docs {
            let doc = CRDTDocument::from_snapshot(&bytes)?;
            let map = doc.to_map();
            if let Some(CrdtValue::Boolean(true)) = map.get("_deleted") {
                continue;
            }
            if filter.as_ref().map_or(true, |f| f.matches(&map)) {
                results.push(map);
            }
        }
        Ok(results)
    }

    pub fn subscribe(&self, doc_id: &str, callback: Callback) -> crate::subscription::engine::SubscriptionHandle {
        self.subscriptions.lock().unwrap().register(doc_id, callback)
    }

    pub fn unsubscribe(&self, handle: crate::subscription::engine::SubscriptionHandle) -> Result<(), DriftError> {
        self.subscriptions.lock().unwrap().unregister(handle)
    }

    pub async fn force_sync(&self) -> Result<usize, DriftError> {
        let uploaded = self.upload_queue.process_batch().await?;
        let downloaded = self.download_queue.process_batch().await?;
        Ok(uploaded + downloaded)
    }

    pub async fn sync_status(&self) -> Result<SyncState, DriftError> {
        if let Some(state) = self.storage.read_sync_state(&self.config.namespace).await? {
            Ok(state)
        } else {
            let last_seq = self.download_queue.last_sequence();
            Ok(SyncState {
                namespace: self.config.namespace.clone(),
                replica_id: self.config.replica_id.clone(),
                last_synced_sequence: last_seq,
                connection_status: crate::sync::state::ConnectionStatus::Connected,
                leader_status: Some(true),
                last_connected_at: None,
                last_sync_at: None,
                schema_version: 0,
            })
        }
    }

    pub async fn pending_uploads(&self) -> Result<usize, DriftError> {
        self.upload_queue.pending_count().await
    }

    pub async fn rotate_keys(&self) -> Result<(), DriftError> {
        Ok(())
    }

    pub async fn export_public_key(&self) -> Result<Vec<u8>, DriftError> {
        Ok(self.keyring.active_key().public_key.to_vec())
    }

    pub async fn define_schema(&self, doc_id: &str, schema: DocumentSchema) -> Result<(), DriftError> {
        self.schema.lock().unwrap().define(doc_id, schema)
    }

    pub async fn apply_migration(&self, migration: MigrationDefinition) -> Result<(), DriftError> {
        if !migration.verify_checksum() {
            return Err(DriftError::Schema("Migration checksum verification failed".into()));
        }
        let applied = self.storage.read_migrations().await?;
        if let Some(existing) = applied.iter().find(|m| m.version == migration.version) {
            if existing.checksum != migration.checksum {
                return Err(DriftError::Schema(format!(
                    "Migration checksum mismatch for version {}: expected {}, found {}",
                    migration.version, existing.checksum, migration.checksum
                )));
            }
            tracing::info!(version = %migration.version, "Migration already applied, skipping");
            return Ok(());
        }
        
        migration.apply()?;

        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let record = crate::storage::traits::MigrationRecord {
            version: migration.version.clone(),
            applied_at: epoch,
            checksum: migration.checksum.clone(),
        };
        self.storage.write_migration(&record).await?;
        Ok(())
    }
}
