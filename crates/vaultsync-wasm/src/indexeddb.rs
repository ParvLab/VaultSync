use async_trait::async_trait;
use js_sys::{Promise, Uint8Array};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;
use wasm_bindgen::{prelude::*, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::*;

struct SendJsFuture<T = JsValue>(JsFuture<T>);

unsafe impl<T> Send for SendJsFuture<T> {}

impl<T> Future for SendJsFuture<T> {
    type Output = Result<T, JsValue>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
    }
}

impl<T: wasm_bindgen::convert::FromWasmAbi + 'static> From<Promise<T>> for SendJsFuture<T> {
    fn from(p: Promise<T>) -> Self {
        Self(JsFuture::from(p))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdbIndex {
    pub version: u32,
    pub doc_listing: HashMap<String, Vec<String>>,
    pub oplog: Vec<OplogEntry>,
    pub sync_states: HashMap<String, SyncState>,
    pub schemas: HashMap<String, SchemaMeta>,
    pub migrations: Vec<MigrationRecord>,
    pub keys: Vec<KeyRecord>,
}

impl Default for IdbIndex {
    fn default() -> Self {
        Self {
            version: 1,
            doc_listing: HashMap::new(),
            oplog: Vec::new(),
            sync_states: HashMap::new(),
            schemas: HashMap::new(),
            migrations: Vec::new(),
            keys: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct IndexedDbStorage {
    db: IdbDatabase,
    index: Mutex<IdbIndex>,
}

unsafe impl Send for IndexedDbStorage {}
unsafe impl Sync for IndexedDbStorage {}

fn request_to_future(req: &IdbRequest) -> SendJsFuture {
    let promise = Promise::new(&mut |resolve, reject| {
        let onsuccess = Closure::wrap(Box::new(move |event: Event| {
            let target = event.target().unwrap();
            let req = target.dyn_into::<IdbRequest>().unwrap();
            let result = req.result().unwrap_or(JsValue::NULL);
            resolve.call1(&JsValue::UNDEFINED, &result).unwrap();
        }) as Box<dyn FnMut(Event)>);

        let onerror = Closure::wrap(Box::new(move |_event: Event| {
            reject
                .call1(
                    &JsValue::UNDEFINED,
                    &JsValue::from_str("IndexedDB request failed"),
                )
                .unwrap();
        }) as Box<dyn FnMut(Event)>);

        req.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        req.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        onsuccess.forget();
        onerror.forget();
    });
    SendJsFuture::from(promise)
}

impl IndexedDbStorage {
    pub async fn new(db_name: &str) -> Result<Self, VaultSyncError> {
        let window = window().ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
        let idb_factory = window
            .indexed_db()
            .map_err(|e| VaultSyncError::Storage(format!("indexedDB not available: {:?}", e)))?
            .ok_or_else(|| VaultSyncError::Storage("indexedDB is None".into()))?;

        let req = idb_factory
            .open_with_u32(db_name, 1)
            .map_err(|e| VaultSyncError::Storage(format!("open db request failed: {:?}", e)))?;

        let promise = Promise::new(&mut |resolve, reject| {
            let onupgradeneeded = Closure::wrap(Box::new(move |event: Event| {
                let target = event.target().unwrap();
                let req = target.dyn_into::<IdbOpenDbRequest>().unwrap();
                let db: IdbDatabase = req.result().unwrap().into();

                db.create_object_store("documents").unwrap();
                db.create_object_store("system").unwrap();
            }) as Box<dyn FnMut(Event)>);

            let onsuccess = Closure::wrap(Box::new(move |event: Event| {
                let target = event.target().unwrap();
                let req = target.dyn_into::<IdbOpenDbRequest>().unwrap();
                let db = req.result().unwrap();
                resolve.call1(&JsValue::UNDEFINED, &db).unwrap();
            }) as Box<dyn FnMut(Event)>);

            let onerror = Closure::wrap(Box::new(move |_event: Event| {
                reject
                    .call1(
                        &JsValue::UNDEFINED,
                        &JsValue::from_str("IndexedDB open failed"),
                    )
                    .unwrap();
            }) as Box<dyn FnMut(Event)>);

            req.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
            req.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
            req.set_onerror(Some(onerror.as_ref().unchecked_ref()));

            onupgradeneeded.forget();
            onsuccess.forget();
            onerror.forget();
        });

        let db_val = SendJsFuture::from(promise)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("open_db promise failed: {:?}", e)))?;
        let db: IdbDatabase = db_val.into();

        // Load index from system store
        let index = Self::load_index_from_db(&db).await?.unwrap_or_default();

        Ok(Self {
            db,
            index: Mutex::new(index),
        })
    }

    async fn load_index_from_db(db: &IdbDatabase) -> Result<Option<IdbIndex>, VaultSyncError> {
        let tx = db
            .transaction_with_str_and_mode("system", IdbTransactionMode::Readonly)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("system")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;
        let req = store
            .get(&JsValue::from_str("index"))
            .map_err(|e| VaultSyncError::Storage(format!("get failed: {:?}", e)))?;

        let val = request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("request failed: {:?}", e)))?;

        if val.is_null() || val.is_undefined() {
            return Ok(None);
        }

        let uint8 = Uint8Array::new(&val);
        let mut bytes = vec![0u8; uint8.length() as usize];
        uint8.copy_to(&mut bytes);

        let index: IdbIndex = serde_json::from_slice(&bytes)
            .map_err(|e| VaultSyncError::Storage(format!("parse failed: {:?}", e)))?;
        Ok(Some(index))
    }

    async fn flush_index(&self, index: &IdbIndex) -> Result<(), VaultSyncError> {
        let bytes = serde_json::to_vec(index)
            .map_err(|e| VaultSyncError::Storage(format!("serialize failed: {:?}", e)))?;
        let tx = self
            .db
            .transaction_with_str_and_mode("system", IdbTransactionMode::Readwrite)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("system")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let uint8 = Uint8Array::from(bytes.as_slice());
        let req = store
            .put_with_key(&uint8, &JsValue::from_str("index"))
            .map_err(|e| VaultSyncError::Storage(format!("put failed: {:?}", e)))?;

        request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("flush failed: {:?}", e)))?;
        Ok(())
    }

    async fn get_fresh_index(&self) -> Result<IdbIndex, VaultSyncError> {
        let index = Self::load_index_from_db(&self.db)
            .await?
            .unwrap_or_default();
        Ok(index)
    }
}

#[async_trait]
impl Storage for IndexedDbStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        let tx = self
            .db
            .transaction_with_str_and_mode("documents", IdbTransactionMode::Readwrite)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("documents")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let uint8 = Uint8Array::from(bytes);
        let key = format!("{}/{}", doc_id, record_id);
        let req = store
            .put_with_key(&uint8, &JsValue::from_str(&key))
            .map_err(|e| VaultSyncError::Storage(format!("put failed: {:?}", e)))?;

        request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("insert doc failed: {:?}", e)))?;

        // Update doc listing index
        let mut index = self.get_fresh_index().await?;
        let listing = index.doc_listing.entry(doc_id.to_string()).or_default();
        if !listing.contains(&record_id.to_string()) {
            listing.push(record_id.to_string());
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;

        Ok(())
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let tx = self
            .db
            .transaction_with_str_and_mode("documents", IdbTransactionMode::Readonly)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("documents")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let key = format!("{}/{}", doc_id, record_id);
        let req = store
            .get(&JsValue::from_str(&key))
            .map_err(|e| VaultSyncError::Storage(format!("get failed: {:?}", e)))?;

        let val = request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("get doc failed: {:?}", e)))?;

        if val.is_null() || val.is_undefined() {
            return Ok(None);
        }

        let uint8 = Uint8Array::new(&val);
        let mut bytes = vec![0u8; uint8.length() as usize];
        uint8.copy_to(&mut bytes);
        Ok(Some(bytes))
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        let tx = self
            .db
            .transaction_with_str_and_mode("documents", IdbTransactionMode::Readwrite)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("documents")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let key = format!("{}/{}", doc_id, record_id);
        let req = store
            .delete(&JsValue::from_str(&key))
            .map_err(|e| VaultSyncError::Storage(format!("delete failed: {:?}", e)))?;

        request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("delete doc failed: {:?}", e)))?;

        // Update doc listing index
        let mut index = self.get_fresh_index().await?;
        if let Some(listing) = index.doc_listing.get_mut(doc_id) {
            listing.retain(|r| r != record_id);
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;

        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        let listing = index.doc_listing.get(doc_id).cloned().unwrap_or_default();

        let mut results = Vec::new();
        for record_id in &listing {
            if let Some(bytes) = self.get_document(doc_id, record_id).await? {
                results.push((record_id.clone(), bytes));
            }
        }
        Ok(results)
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let tx = self
            .db
            .transaction_with_str_and_mode("documents", IdbTransactionMode::Readwrite)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("documents")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let uint8 = Uint8Array::from(bytes);
        let key = format!("{}/{}", doc_id, record_id);
        let req = store
            .put_with_key(&uint8, &JsValue::from_str(&key))
            .map_err(|e| VaultSyncError::Storage(format!("put failed: {:?}", e)))?;

        request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("write doc failed: {:?}", e)))?;

        // Update doc listing index and oplog
        let mut index = self.get_fresh_index().await?;
        let listing = index.doc_listing.entry(doc_id.to_string()).or_default();
        if !listing.contains(&record_id.to_string()) {
            listing.push(record_id.to_string());
        }
        index.oplog.push(entry.clone());
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;

        Ok(())
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let tx = self
            .db
            .transaction_with_str_and_mode("documents", IdbTransactionMode::Readwrite)
            .map_err(|e| VaultSyncError::Storage(format!("tx failed: {:?}", e)))?;
        let store = tx
            .object_store("documents")
            .map_err(|e| VaultSyncError::Storage(format!("store failed: {:?}", e)))?;

        let key = format!("{}/{}", doc_id, record_id);
        let req = store
            .delete(&JsValue::from_str(&key))
            .map_err(|e| VaultSyncError::Storage(format!("delete failed: {:?}", e)))?;

        request_to_future(&req)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("delete doc failed: {:?}", e)))?;

        // Update doc listing index and oplog
        let mut index = self.get_fresh_index().await?;
        if let Some(listing) = index.doc_listing.get_mut(doc_id) {
            listing.retain(|r| r != record_id);
        }
        index.oplog.push(entry.clone());
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;

        Ok(())
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        index.oplog.push(entry.clone());
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        let pending: Vec<OplogEntry> = index
            .oplog
            .iter()
            .filter(|e| {
                e.namespace == namespace
                    && matches!(
                        e.sync_status,
                        vaultsync_core::oplog::entry::SyncStatus::Pending
                    )
            })
            .take(limit)
            .cloned()
            .collect();
        Ok(pending)
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
            entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Synced;
            entry.sequence = Some(sequence);
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
            entry.sync_status = vaultsync_core::oplog::entry::SyncStatus::Failed;
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        let entries: Vec<OplogEntry> = index
            .oplog
            .iter()
            .filter(|e| e.namespace == namespace && e.sequence.unwrap_or(0) > seq)
            .cloned()
            .collect();
        Ok(entries)
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        Ok(index.sync_states.get(namespace).cloned())
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        index
            .sync_states
            .insert(state.namespace.clone(), state.clone());
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        Ok(index.schemas.get(doc_id).cloned())
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        index.schemas.insert(meta.doc_id.clone(), meta.clone());
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        Ok(index.migrations.clone())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        let pos = index
            .migrations
            .iter()
            .position(|m| m.version == record.version);
        if let Some(i) = pos {
            index.migrations[i] = record.clone();
        } else {
            index.migrations.push(record.clone());
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        let index = self.get_fresh_index().await?;
        let results: Vec<KeyRecord> = index
            .keys
            .iter()
            .filter(|k| k.namespace == namespace)
            .cloned()
            .collect();
        Ok(results)
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        let pos = index
            .keys
            .iter()
            .position(|k| k.namespace == key.namespace && k.version == key.version);
        if let Some(i) = pos {
            index.keys[i] = key.clone();
        } else {
            index.keys.push(key.clone());
        }
        self.flush_index(&index).await?;
        *self.index.lock().unwrap() = index;
        Ok(())
    }

    async fn reset_stale_pending(
        &self,
        _namespace: &str,
        _older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        Ok(0)
    }

    async fn delete_synced_oplog_older_than(
        &self,
        _namespace: &str,
        _older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        Ok(0)
    }

    async fn list_tombstoned_documents(
        &self,
        _namespace: &str,
        _older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        Ok(Vec::new())
    }

    async fn update_oplog_encrypted_blob(
        &self,
        _id: &str,
        _new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        Ok(())
    }

    async fn list_active_documents(
        &self,
        _namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        Ok(Vec::new())
    }

    async fn read_synced_oplog_for_document(
        &self,
        _namespace: &str,
        _doc_id: &str,
        _record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        Ok(Vec::new())
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        _namespace: &str,
        _doc_id: &str,
        _record_id: &str,
        _timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        Ok(0)
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let mut index = self.get_fresh_index().await?;
        let before = index.oplog.len();
        index.oplog.retain(|entry| {
            !(entry.namespace == namespace
                && entry.sync_status == vaultsync_core::oplog::entry::SyncStatus::Synced
                && entry.created_at < cutoff_ms)
        });
        let removed = before - index.oplog.len();
        if removed > 0 {
            self.flush_index(&index).await?;
            *self.index.lock().unwrap() = index;
        }
        Ok(removed)
    }
}
