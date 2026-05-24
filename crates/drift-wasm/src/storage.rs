use std::collections::HashMap;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::*;
use js_sys::{Promise, Uint8Array};
use drift_core::DriftError;
use drift_core::oplog::entry::{OplogEntry, SyncStatus};
use drift_core::sync::state::SyncState;
use drift_core::storage::traits::{Storage, SchemaMeta, MigrationRecord, KeyRecord};

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
struct OpfsIndex {
    version: u32,
    doc_listing: HashMap<String, Vec<String>>,
    oplog: Vec<OplogEntry>,
    sync_states: HashMap<String, SyncState>,
    schemas: HashMap<String, SchemaMeta>,
    migrations: Vec<MigrationRecord>,
    keys: Vec<KeyRecord>,
}

impl Default for OpfsIndex {
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
pub struct OpfsStorage {
    inner: Mutex<OpfsInner>,
}

#[derive(Debug)]
struct OpfsInner {
    root: JsValue,
    index: OpfsIndex,
}

impl OpfsInner {
    fn root_handle(&self) -> FileSystemDirectoryHandle {
        self.root.clone().dyn_into().unwrap()
    }
}

impl OpfsStorage {
    pub async fn new() -> Result<Self, DriftError> {
        let window = web_sys::window().ok_or_else(|| DriftError::Storage("no window".into()))?;
        let navigator = window.navigator();
        let storage: StorageManager = navigator.storage();

        let root_val = SendJsFuture::from(storage.get_directory()).await
            .map_err(|e| DriftError::Storage(format!("get_directory failed: {:?}", e)))?;

        let root_handle: FileSystemDirectoryHandle = root_val.clone().into();
        Self::ensure_dir(&root_handle, "_system").await?;
        Self::ensure_dir(&root_handle, "docs").await?;

        let index = Self::load_index(&root_handle).await.unwrap_or_default();

        Ok(Self {
            inner: Mutex::new(OpfsInner { root: root_val, index }),
        })
    }

    async fn ensure_dir(root: &FileSystemDirectoryHandle, name: &str) -> Result<FileSystemDirectoryHandle, DriftError> {
        let opts = FileSystemGetDirectoryOptions::new();
        opts.set_create(true);
        let promise = root.get_directory_handle_with_options(name, &opts);
        let val = SendJsFuture::from(promise).await
            .map_err(|e| DriftError::Storage(format!("ensure_dir failed: {:?}", e)))?;
        Ok(val.into())
    }

    async fn get_dir(root: &FileSystemDirectoryHandle, path: &[&str]) -> Result<FileSystemDirectoryHandle, DriftError> {
        let mut dir = root.clone();
        for p in path {
            let opts = FileSystemGetDirectoryOptions::new();
            opts.set_create(true);
            let promise = dir.get_directory_handle_with_options(p, &opts);
            let val = SendJsFuture::from(promise).await
                .map_err(|e| DriftError::Storage(format!("get_dir failed: {:?}", e)))?;
            dir = val.into();
        }
        Ok(dir)
    }

    async fn write_doc_file(root: &FileSystemDirectoryHandle, doc_id: &str, record_id: &str, data: &[u8]) -> Result<(), DriftError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);
        let promise = dir.get_file_handle_with_options(record_id, &opts);
        let file_val = SendJsFuture::from(promise).await
            .map_err(|e| DriftError::Storage(format!("get_file_handle failed: {:?}", e)))?;
        let file_handle: FileSystemFileHandle = file_val.into();

        let writable_promise = file_handle.create_writable();
        let writable_val = SendJsFuture::from(writable_promise).await
            .map_err(|e| DriftError::Storage(format!("create_writable failed: {:?}", e)))?;
        let writable: FileSystemWritableFileStream = writable_val.into();

        let write_promise = writable.write_with_u8_array(data)
            .map_err(|e| DriftError::Storage(format!("write error: {:?}", e)))?;
        SendJsFuture::from(write_promise).await
            .map_err(|e| DriftError::Storage(format!("write failed: {:?}", e)))?;

        let close: WritableStream = writable.into();
        SendJsFuture::from(close.close()).await
            .map_err(|e| DriftError::Storage(format!("close failed: {:?}", e)))?;

        Ok(())
    }

    async fn read_doc_file(root: &FileSystemDirectoryHandle, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, DriftError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;

        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);
        let file_val = match SendJsFuture::from(dir.get_file_handle_with_options(record_id, &opts)).await {
            Ok(val) => val,
            Err(_) => return Ok(None),
        };
        let file_handle: FileSystemFileHandle = file_val.into();

        let file_val = SendJsFuture::from(file_handle.get_file()).await
            .map_err(|e| DriftError::Storage(format!("get_file failed: {:?}", e)))?;
        let file: File = file_val.into();

        let blob: &Blob = file.as_ref();
        let buf_promise = blob.array_buffer();
        let buf_val = SendJsFuture::from(buf_promise).await
            .map_err(|e| DriftError::Storage(format!("array_buffer failed: {:?}", e)))?;

        let uint8 = Uint8Array::new(&buf_val);
        let mut bytes = vec![0u8; uint8.length() as usize];
        uint8.copy_to(&mut bytes);
        Ok(Some(bytes))
    }

    async fn delete_doc_file(root: &FileSystemDirectoryHandle, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;
        let promise = dir.remove_entry(record_id);
        let _ = SendJsFuture::from(promise).await;
        Ok(())
    }

    async fn load_index(root: &FileSystemDirectoryHandle) -> Result<OpfsIndex, DriftError> {
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);
        let dir = Self::get_dir(root, &["_system"]).await?;
        let file_val = match SendJsFuture::from(dir.get_file_handle_with_options("index.json", &opts)).await {
            Ok(val) => val,
            Err(_) => return Ok(OpfsIndex::default()),
        };
        let file_handle: FileSystemFileHandle = file_val.into();
        let file_val = SendJsFuture::from(file_handle.get_file()).await
            .map_err(|e| DriftError::Storage(format!("get_file: {:?}", e)))?;
        let file: File = file_val.into();
        let blob: &Blob = file.as_ref();
        let buf_promise = blob.array_buffer();
        let buf_val = SendJsFuture::from(buf_promise).await
            .map_err(|e| DriftError::Storage(format!("array_buffer: {:?}", e)))?;
        let uint8 = Uint8Array::new(&buf_val);
        let mut bytes = vec![0u8; uint8.length() as usize];
        uint8.copy_to(&mut bytes);

        let index: OpfsIndex = serde_json::from_slice(&bytes)
            .map_err(|e| DriftError::Storage(format!("json parse: {:?}", e)))?;
        Ok(index)
    }

    async fn flush_index(root: &FileSystemDirectoryHandle, index: &OpfsIndex) -> Result<(), DriftError> {
        let json = serde_json::to_vec(index)
            .map_err(|e| DriftError::Storage(format!("json: {:?}", e)))?;
        let sys_dir = Self::get_dir(root, &["_system"]).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);
        let promise = sys_dir.get_file_handle_with_options("index.json", &opts);
        let file_val = SendJsFuture::from(promise).await
            .map_err(|e| DriftError::Storage(format!("get_file_handle: {:?}", e)))?;
        let file_handle: FileSystemFileHandle = file_val.into();
        let writable_promise = file_handle.create_writable();
        let writable_val = SendJsFuture::from(writable_promise).await
            .map_err(|e| DriftError::Storage(format!("create_writable: {:?}", e)))?;
        let writable: FileSystemWritableFileStream = writable_val.into();
        let write_promise = writable.write_with_u8_array(&json)
            .map_err(|e| DriftError::Storage(format!("write: {:?}", e)))?;
        SendJsFuture::from(write_promise).await
            .map_err(|e| DriftError::Storage(format!("write done: {:?}", e)))?;
        let close: WritableStream = writable.into();
        SendJsFuture::from(close.close()).await
            .map_err(|e| DriftError::Storage(format!("close: {:?}", e)))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for OpfsStorage {
    async fn insert_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        Self::write_doc_file(&root, doc_id, record_id, bytes).await?;
        index.doc_listing.entry(doc_id.to_string()).or_default().push(record_id.to_string());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn get_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, DriftError> {
        let root = { let inner = self.inner.lock().unwrap(); inner.root_handle() };
        Self::read_doc_file(&root, doc_id, record_id).await
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        Self::delete_doc_file(&root, doc_id, record_id).await?;
        if let Some(listing) = index.doc_listing.get_mut(doc_id) {
            listing.retain(|r| r != record_id);
        }
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn write_document_and_oplog(&self, doc_id: &str, record_id: &str, bytes: &[u8], entry: &OplogEntry) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        Self::write_doc_file(&root, doc_id, record_id, bytes).await?;
        index.doc_listing.entry(doc_id.to_string()).or_default().push(record_id.to_string());
        index.oplog.push(entry.clone());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn delete_document_and_oplog(&self, doc_id: &str, record_id: &str, entry: &OplogEntry) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        Self::delete_doc_file(&root, doc_id, record_id).await?;
        if let Some(listing) = index.doc_listing.get_mut(doc_id) {
            listing.retain(|r| r != record_id);
        }
        index.oplog.push(entry.clone());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, DriftError> {
        let (root, record_ids) = {
            let inner = self.inner.lock().unwrap();
            let ids = inner.index.doc_listing.get(doc_id).cloned().unwrap_or_default();
            (inner.root_handle(), ids)
        };
        let mut results = Vec::new();
        for rid in &record_ids {
            if let Some(data) = Self::read_doc_file(&root, doc_id, rid).await? {
                results.push((rid.clone(), data));
            }
        }
        Ok(results)
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        index.oplog.push(entry.clone());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn read_pending_oplog(&self, namespace: &str, limit: usize) -> Result<Vec<OplogEntry>, DriftError> {
        let inner = self.inner.lock().unwrap();
        let results: Vec<OplogEntry> = inner.index.oplog.iter()
            .filter(|e| e.namespace == namespace && matches!(e.sync_status, SyncStatus::Pending))
            .take(limit)
            .cloned()
            .collect();
        Ok(results)
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
            entry.sync_status = SyncStatus::Synced;
            entry.sequence = Some(sequence);
        }
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
            entry.sync_status = SyncStatus::Failed;
        }
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn read_oplog_after_sequence(&self, namespace: &str, seq: u64) -> Result<Vec<OplogEntry>, DriftError> {
        let inner = self.inner.lock().unwrap();
        let results: Vec<OplogEntry> = inner.index.oplog.iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .cloned()
            .collect();
        Ok(results)
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, DriftError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.index.sync_states.get(namespace).cloned())
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        index.sync_states.insert(state.namespace.clone(), state.clone());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, DriftError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.index.schemas.get(doc_id).cloned())
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        index.schemas.insert(meta.doc_id.clone(), meta.clone());
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, DriftError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.index.migrations.clone())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        let pos = index.migrations.iter().position(|m| m.version == record.version);
        if let Some(i) = pos {
            index.migrations[i] = record.clone();
        } else {
            index.migrations.push(record.clone());
        }
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, DriftError> {
        let inner = self.inner.lock().unwrap();
        let results: Vec<KeyRecord> = inner.index.keys.iter()
            .filter(|k| k.namespace == namespace)
            .cloned()
            .collect();
        Ok(results)
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), DriftError> {
        let (root, mut index) = {
            let inner = self.inner.lock().unwrap();
            (inner.root_handle(), inner.index.clone())
        };
        let pos = index.keys.iter().position(|k| k.namespace == key.namespace && k.version == key.version);
        if let Some(i) = pos {
            index.keys[i] = key.clone();
        } else {
            index.keys.push(key.clone());
        }
        Self::flush_index(&root, &index).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.index = index;
        Ok(())
    }
}
