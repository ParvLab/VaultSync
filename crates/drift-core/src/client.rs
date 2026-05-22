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
    decryptor: Arc<E2eeDecryptor>,
    oplog: Arc<OpLog>,
    schema: Arc<std::sync::Mutex<SchemaRegistry>>,
    subscriptions: Arc<std::sync::Mutex<SubscriptionEngine>>,
    upload_queue: Arc<UploadQueue>,
    download_queue: Arc<DownloadQueue>,
    reconciler: Arc<Reconciler>,
    _telemetry: Arc<DriftTelemetry>,
}

impl DriftClient {
    pub async fn new(config: DriftConfig) -> Result<Self, DriftError> {
        let storage: Arc<dyn Storage> = match &config.storage {
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::Sqlite { path } => Arc::new(SQLiteStorage::new(path)?),
            #[cfg(not(feature = "storage-sqlite"))]
            StorageConfig::Sqlite { .. } => return Err(DriftError::Config("sqlite storage not available".into())),
            StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
            StorageConfig::Wasm => return Err(DriftError::Config("WASM storage not available in native build".into())),
        };

        let coordinator: Arc<dyn Coordinator> = Arc::new(InMemoryCoordinator::new());

        let keyring = Arc::new(KeyRing::generate());
        let encryptor = Arc::new(E2eeEncryptor::new(KeyRing::generate()));
        let decryptor = Arc::new(E2eeDecryptor::new(KeyRing::generate()));

        let subscriptions = Arc::new(std::sync::Mutex::new(SubscriptionEngine::new()));
        let reconciler = Arc::new(Reconciler::new(storage.clone(), subscriptions.clone()));

        let oplog = Arc::new(OpLog::new(storage.clone(), &config.namespace));

        let upload_queue = Arc::new(UploadQueue::new(
            oplog.clone(),
            coordinator.clone(),
            config.upload.clone(),
        ));

        let download_queue = Arc::new(DownloadQueue::new(
            coordinator.clone(),
            &config.namespace,
            0,
            config.download.clone(),
        ));

        let schema = Arc::new(std::sync::Mutex::new(SchemaRegistry::new()));
        let _telemetry = Arc::new(DriftTelemetry::new());

        let client = Self {
            config,
            storage,
            coordinator,
            keyring,
            encryptor,
            decryptor,
            oplog,
            schema,
            subscriptions,
            upload_queue,
            download_queue,
            reconciler,
            _telemetry,
        };

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
        Ok(())
    }

    pub async fn insert(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<(), DriftError> {
        let mut doc = CRDTDocument::new(doc_id, record_id, 0);
        let mut update_bytes = Vec::new();
        for (field, value) in &fields {
            let update = doc.set_field(field, value.clone());
            update_bytes = update;
        }
        let snapshot = doc.to_snapshot();
        self.storage.insert_document(doc_id, record_id, &snapshot).await?;

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
            encrypted_blob: None,
            timestamp: epoch,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: epoch,
        };
        self.oplog.append(entry).await?;
        Ok(())
    }

    pub async fn update(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<(), DriftError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(doc_id, record_id, 0),
        };
        let mut update_bytes = Vec::new();
        for (field, value) in &fields {
            let update = doc.set_field(field, value.clone());
            update_bytes = update;
        }
        let snapshot = doc.to_snapshot();
        self.storage.insert_document(doc_id, record_id, &snapshot).await?;

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
            encrypted_blob: None,
            timestamp: epoch,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: epoch,
        };
        self.oplog.append(entry).await?;
        Ok(())
    }

    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        if let Some(bytes) = existing {
            let mut doc = CRDTDocument::from_snapshot(&bytes)?;
            let update_bytes = doc.delete_field("doc_id");
            self.storage.delete_document(doc_id, record_id).await?;

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
                encrypted_blob: None,
                timestamp: epoch,
                sequence: None,
                sync_status: SyncStatus::Pending,
                synced_at: None,
                created_at: epoch,
            };
            self.oplog.append(entry).await?;
        }
        Ok(())
    }

    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<Option<HashMap<String, CrdtValue>>, DriftError> {
        match self.storage.get_document(doc_id, record_id).await? {
            Some(bytes) => {
                let doc = CRDTDocument::from_snapshot(&bytes)?;
                Ok(Some(doc.to_map()))
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
        self.upload_queue.process_batch().await
    }

    pub async fn sync_status(&self) -> Result<SyncState, DriftError> {
        Ok(SyncState {
            namespace: self.config.namespace.clone(),
            replica_id: self.config.replica_id.clone(),
            last_synced_sequence: 0,
            connection_status: crate::sync::state::ConnectionStatus::Connected,
            leader_status: Some(true),
            last_connected_at: None,
            last_sync_at: None,
            schema_version: 0,
        })
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

    pub async fn apply_migration(&self, _migration: MigrationDefinition) -> Result<(), DriftError> {
        Ok(())
    }
}
