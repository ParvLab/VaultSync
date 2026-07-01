use async_trait::async_trait;
use js_sys::Uint8Array;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use vaultsync_core::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::*;

use vaultsync_core::time_utils::SendJsFuture;



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

#[derive(Debug, Clone)]
pub struct OpfsStorage {
    inner: Arc<Mutex<OpfsInner>>,
    tx_lock: Arc<Mutex<()>>,
    lock_name: Arc<String>,
}

#[derive(Debug, Clone)]
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
    pub async fn new(db_name: &str) -> Result<Self, VaultSyncError> {
        let window =
            web_sys::window().ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
        let navigator = window.navigator();
        let storage: StorageManager = navigator.storage();

        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] get_directory start"));
        let root_val = SendJsFuture::from(storage.get_directory())
            .await
            .map_err(|e| VaultSyncError::Storage(format!("get_directory failed: {:?}", e)))?;
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] get_directory done"));

        let root_handle: FileSystemDirectoryHandle = root_val.clone().into();
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!("[opfs] ensure_dir '{}' start", db_name)));
        let db_dir = Self::ensure_dir(&root_handle, db_name).await?;
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!("[opfs] ensure_dir '{}' done", db_name)));
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] ensure_dir '_system' start"));
        Self::ensure_dir(&db_dir, "_system").await?;
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] ensure_dir '_system' done"));
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] ensure_dir 'docs' start"));
        Self::ensure_dir(&db_dir, "docs").await?;
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] ensure_dir 'docs' done"));

        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] load_index start"));
        let index = Self::load_index(&db_dir).await?;
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
            "[opfs] load_index done: {} sync_states, {} oplog entries",
            index.sync_states.len(),
            index.oplog.len()
        )));

        let lock_name = format!("vaultsync-opfs-index-{}", db_name);

        Ok(Self {
            inner: Arc::new(Mutex::new(OpfsInner {
                root: JsValue::from(db_dir),
                index,
            })),
            tx_lock: Arc::new(Mutex::new(())),
            lock_name: Arc::new(lock_name),
        })
    }

    async fn ensure_dir(
        root: &FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
        let opts = FileSystemGetDirectoryOptions::new();
        opts.set_create(true);
        let promise = root.get_directory_handle_with_options(name, &opts);
        let val = SendJsFuture::from(promise)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("ensure_dir failed: {:?}", e)))?;
        Ok(val.into())
    }

    async fn get_dir(
        root: &FileSystemDirectoryHandle,
        path: &[&str],
    ) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
        let mut dir = root.clone();
        for p in path {
            let opts = FileSystemGetDirectoryOptions::new();
            opts.set_create(true);
            let promise = dir.get_directory_handle_with_options(p, &opts);
            let val = SendJsFuture::from(promise)
                .await
                .map_err(|e| VaultSyncError::Storage(format!("get_dir failed: {:?}", e)))?;
            dir = val.into();
        }
        Ok(dir)
    }

    async fn sleep(ms: u64) {
        vaultsync_core::time_utils::sleep(std::time::Duration::from_millis(ms)).await;
    }

    async fn write_doc_file(
        root: &FileSystemDirectoryHandle,
        doc_id: &str,
        record_id: &str,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);

        let mut attempts = 0;
        let max_attempts = 15;
        loop {
            let res = async {
                let promise = dir.get_file_handle_with_options(record_id, &opts);
                let file_val = SendJsFuture::from(promise).await.map_err(|e| {
                    VaultSyncError::Storage(format!("get_file_handle failed: {:?}", e))
                })?;
                let file_handle: FileSystemFileHandle = file_val.into();

                let writable_promise = file_handle.create_writable();
                let writable_val = SendJsFuture::from(writable_promise).await.map_err(|e| {
                    VaultSyncError::Storage(format!("create_writable failed: {:?}", e))
                })?;
                let writable: FileSystemWritableFileStream = writable_val.into();

                let js_array = js_sys::Uint8Array::new_with_length(data.len() as u32);
                js_array.copy_from(data);
                let write_promise = writable
                    .write_with_buffer_source(&js_array)
                    .map_err(|e| VaultSyncError::Storage(format!("write error: {:?}", e)))?;
                SendJsFuture::from(write_promise)
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("write failed: {:?}", e)))?;

                let close: WritableStream = writable.into();
                SendJsFuture::from(close.close())
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("close failed: {:?}", e)))?;

                Ok(())
            }
            .await;

            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = 15 + (js_sys::Math::random() * 15.0) as u64;
                    Self::sleep(delay).await;
                }
            }
        }
    }

    async fn read_doc_file(
        root: &FileSystemDirectoryHandle,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);

        let mut attempts = 0;
        let max_attempts = 15;
        loop {
            let res = async {
                let file_val =
                    match SendJsFuture::from(dir.get_file_handle_with_options(record_id, &opts))
                        .await
                    {
                        Ok(val) => val,
                        Err(e) => {
                            let is_not_found = js_sys::Reflect::get(&e, &JsValue::from_str("name"))
                                .ok()
                                .and_then(|v| v.as_string())
                                .map_or(false, |s| s == "NotFoundError");
                            if is_not_found {
                                return Ok(None);
                            } else {
                                return Err(VaultSyncError::Storage(format!(
                                    "get_file_handle failed: {:?}",
                                    e
                                )));
                            }
                        }
                    };
                let file_handle: FileSystemFileHandle = file_val.into();

                let file_val = SendJsFuture::from(file_handle.get_file())
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("get_file failed: {:?}", e)))?;
                let file: File = file_val.into();

                let blob: &Blob = file.as_ref();
                let buf_promise = blob.array_buffer();
                let buf_val = SendJsFuture::from(buf_promise).await.map_err(|e| {
                    VaultSyncError::Storage(format!("array_buffer failed: {:?}", e))
                })?;

                let uint8 = Uint8Array::new(&buf_val);
                let mut bytes = vec![0u8; uint8.length() as usize];
                uint8.copy_to(&mut bytes);
                Ok(Some(bytes))
            }
            .await;

            match res {
                Ok(data) => return Ok(data),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = 15 + (js_sys::Math::random() * 15.0) as u64;
                    Self::sleep(delay).await;
                }
            }
        }
    }

    async fn delete_doc_file(
        root: &FileSystemDirectoryHandle,
        doc_id: &str,
        record_id: &str,
    ) -> Result<(), VaultSyncError> {
        let dir = Self::get_dir(root, &["docs", doc_id]).await?;
        let mut attempts = 0;
        let max_attempts = 15;
        loop {
            let promise = dir.remove_entry(record_id);
            match SendJsFuture::from(promise).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(VaultSyncError::Storage(format!(
                            "remove_entry failed: {:?}",
                            e
                        )));
                    }
                    let delay = 15 + (js_sys::Math::random() * 15.0) as u64;
                    Self::sleep(delay).await;
                }
            }
        }
    }

    /// Snapshot the index state for diagnostic logging.
    fn index_state(label: &str, index: &OpfsIndex) {
        let pending: Vec<&str> = index
            .oplog
            .iter()
            .filter(|e| e.sync_status.is_uploadable())
            .map(|e| e.id.as_str())
            .collect();
        let synced: Vec<&str> = index
            .oplog
            .iter()
            .filter(|e| matches!(e.sync_status, SyncStatus::Synced))
            .map(|e| e.id.as_str())
            .collect();
        let mut hasher = DefaultHasher::new();
        index.oplog.len().hash(&mut hasher);
        pending.len().hash(&mut hasher);
        for id in &pending {
            id.hash(&mut hasher);
        }
        let hash = hasher.finish();
        web_sys::console::debug_1(&wasm_bindgen::JsValue::from_str(&format!(
            "[INDEX] {} hash={:#x} oplog={} pending={} synced={} pending_ids={:?}",
            label,
            hash,
            index.oplog.len(),
            pending.len(),
            synced.len(),
            pending,
        )));
    }

    async fn load_index(root: &FileSystemDirectoryHandle) -> Result<OpfsIndex, VaultSyncError> {
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);
        let dir = Self::get_dir(root, &["_system"]).await?;

        let mut attempts = 0;
        let max_attempts = 15;
        loop {
            if attempts > 0 {
                web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!("[opfs] load_index retry attempt {}", attempts)));
            }
            let res = async {
                let file_val =
                    match SendJsFuture::from(dir.get_file_handle_with_options("index.json", &opts))
                        .await
                    {
                        Ok(val) => val,
                        Err(e) => {
                            let is_not_found = js_sys::Reflect::get(&e, &JsValue::from_str("name"))
                                .ok()
                                .and_then(|v| v.as_string())
                                .map_or(false, |s| s == "NotFoundError");
                            if is_not_found {
                                web_sys::console::log_1(&wasm_bindgen::JsValue::from_str("[opfs] load_index default empty (no index.json)"));
                                return Ok(OpfsIndex::default());
                            } else {
                                return Err(VaultSyncError::Storage(format!(
                                    "get_file_handle: {:?}",
                                    e
                                )));
                            }
                        }
                    };
                let file_handle: FileSystemFileHandle = file_val.into();
                let file_val = SendJsFuture::from(file_handle.get_file())
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
                let file: File = file_val.into();
                let blob: &Blob = file.as_ref();
                let buf_promise = blob.array_buffer();
                let buf_val = SendJsFuture::from(buf_promise)
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
                let uint8 = Uint8Array::new(&buf_val);
                let mut bytes = vec![0u8; uint8.length() as usize];
                uint8.copy_to(&mut bytes);

                if bytes.is_empty() {
                    return Err(VaultSyncError::Storage("empty index file".into()));
                }

                let index: OpfsIndex = serde_json::from_slice(&bytes)
                    .map_err(|e| VaultSyncError::Storage(format!("json parse: {:?}", e)))?;
                Ok(index)
            }
            .await;

            match res {
                Ok(index) => {
                    Self::index_state("LOAD", &index);
                    return Ok(index);
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = 15 + (js_sys::Math::random() * 15.0) as u64;
                    Self::sleep(delay).await;
                }
            }
        }
    }

    async fn flush_index(
        root: &FileSystemDirectoryHandle,
        index: &OpfsIndex,
    ) -> Result<(), VaultSyncError> {
        Self::index_state("FLUSH_BEFORE", index);
        // ── Race detection: log all uploadable entries at flush time ──
        let pending: Vec<&OplogEntry> = index.oplog.iter().filter(|e| e.sync_status.is_uploadable()).collect();
        if !pending.is_empty() {
            let ids: Vec<String> = pending.iter().map(|e| format!("{}={:?}", &e.id[..e.id.len().min(12)], e.sync_status)).collect();
            let op_count: usize = index.doc_listing.values().map(|v| v.len()).sum();
            web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
                "[opfs_flush] pending={} entries=[{}] docs={} version={}",
                pending.len(),
                ids.join(", "),
                op_count,
                index.version,
            )));
        }
        let json = serde_json::to_vec(index)
            .map_err(|e| VaultSyncError::Storage(format!("json: {:?}", e)))?;
        let sys_dir = Self::get_dir(root, &["_system"]).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);

        let mut attempts = 0;
        let max_attempts = 15;
        loop {
            let res = async {
                let promise = sys_dir.get_file_handle_with_options("index.json", &opts);
                let file_val = SendJsFuture::from(promise)
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
                let file_handle: FileSystemFileHandle = file_val.into();
                let writable_promise = file_handle.create_writable();
                let writable_val = SendJsFuture::from(writable_promise)
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("create_writable: {:?}", e)))?;
                let writable: FileSystemWritableFileStream = writable_val.into();
                let js_array = js_sys::Uint8Array::new_with_length(json.len() as u32);
                js_array.copy_from(&json);
                let write_promise = writable
                    .write_with_buffer_source(&js_array)
                    .map_err(|e| VaultSyncError::Storage(format!("write: {:?}", e)))?;
                SendJsFuture::from(write_promise)
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("write done: {:?}", e)))?;
                let close: WritableStream = writable.into();
                SendJsFuture::from(close.close())
                    .await
                    .map_err(|e| VaultSyncError::Storage(format!("close: {:?}", e)))?;
                Ok(())
            }
            .await;

            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    let delay = 15 + (js_sys::Math::random() * 15.0) as u64;
                    Self::sleep(delay).await;
                }
            }
        }
    }

    fn get_cached_index(&self) -> OpfsIndex {
        self.inner.lock().unwrap().index.clone()
    }

    fn get_cached_root_and_index(&self) -> (FileSystemDirectoryHandle, OpfsIndex) {
        let inner = self.inner.lock().unwrap();
        (inner.root_handle(), inner.index.clone())
    }

    async fn load_index_from_disk(
        &self,
    ) -> Result<(FileSystemDirectoryHandle, OpfsIndex), VaultSyncError> {
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        let index = Self::load_index(&root).await?;
        Ok((root, index))
    }

    /// Execute an index mutation under both cross-tab (navigator.locks) and
    /// intra-tab (tx_lock) mutual exclusion.
    ///
    /// Lock order: navigator.locks → tx_lock (never reversed).
    /// The entire read-modify-write cycle is inside the navigator.locks callback,
    /// ensuring the cross-tab lock is held for the full transaction.
    async fn index_transaction<F, T>(&self, f: F) -> Result<T, VaultSyncError>
    where
        F: FnOnce(&mut OpfsIndex) -> Result<T, VaultSyncError> + 'static,
        T: 'static,
    {
        let this = self.clone();
        let lock_name = self.lock_name.clone();

        const LOCK_NAME_PREFIX: &str = "vaultsync-opfs-index";

        let result: Arc<Mutex<Option<Result<T, VaultSyncError>>>> =
            Arc::new(Mutex::new(None));
        let result_clone = result.clone();

        // Step 1: The navigator.locks callback runs when the cross-tab lock is acquired.
        // The lock is held for the duration of the returned Promise.
        // The `_lock` parameter is the Lock object (or null/undefined) from the browser.
        let cb = Closure::once_into_js(move |_lock: JsValue| {
            wasm_bindgen_futures::future_to_promise(async move {
                // Step 2: Acquire intra-tab lock (ensures single-task entry within this tab)
                let _guard = this.tx_lock.lock().unwrap();

                // Step 3: Load latest index from disk (sees all tabs' committed writes)
                let (root, mut index) = match this.load_index_from_disk().await {
                    Ok(v) => v,
                    Err(e) => {
                        *result_clone.lock().unwrap() = Some(Err(e));
                        return Err(JsValue::undefined());
                    }
                };

                // Step 4: Apply mutation (pure in-memory — no async I/O in f)
                let r = match f(&mut index) {
                    Ok(v) => v,
                    Err(e) => {
                        *result_clone.lock().unwrap() = Some(Err(e));
                        return Err(JsValue::undefined());
                    }
                };

                // Step 5: Flush modified index back to OPFS
                if let Err(e) = Self::flush_index(&root, &index).await {
                    *result_clone.lock().unwrap() = Some(Err(e));
                    return Err(JsValue::undefined());
                }

                // Step 6: Update in-memory cache
                // Use root from step 3 (no re-lock needed — we hold tx_lock)
                *this.inner.lock().unwrap() = OpfsInner {
                    root: root.clone().into(),
                    index,
                };

                // Steps 7-8: tx_lock and navigator.locks released on scope exit
                *result_clone.lock().unwrap() = Some(Ok(r));
                Ok(JsValue::undefined())
            })
        });

        let func: &js_sys::Function<fn(js_sys::JsOption<web_sys::Lock>) -> js_sys::Promise> = cb.as_ref().unchecked_ref();

        let lock_manager = Self::get_lock_manager()?;
        let full_lock_name = format!("{}-{}", LOCK_NAME_PREFIX, &*lock_name);
        let promise = lock_manager
            .request_with_callback(&full_lock_name, func);

        // Drop the Closure before the await, since Closure is !Send on WASM
        // and it's no longer needed (the JS side has its own reference).
        drop(cb);

        // Await the navigator.locks promise — resolves when the callback's
        // promise settles, meaning the transaction is complete and locks released.
        // Use SendJsFuture instead of JsFuture because JsFuture is !Send
        // and the Storage trait requires Send futures.
        SendJsFuture::from(promise)
            .await
            .map_err(|e| VaultSyncError::Storage(format!("lock wait: {:?}", e)))?;

        let mut guard = result.lock().unwrap();
        guard
            .take()
            .unwrap_or(Err(VaultSyncError::Storage("index_transaction failed".into())))
    }

    fn get_lock_manager() -> Result<LockManager, VaultSyncError> {
        let window = web_sys::window()
            .ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
        let navigator = window.navigator();
        Ok(navigator.locks())
    }
}

#[async_trait]
impl Storage for OpfsStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        // Write doc file first (no cross-tab lock needed — files are per-record)
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        Self::write_doc_file(&root, doc_id, record_id, bytes).await?;

        // Update doc_listing under cross-tab lock
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        self.index_transaction(move |index| {
            let listing = index.doc_listing.entry(did).or_default();
            if !listing.contains(&rid) {
                listing.push(rid);
            }
            Ok(())
        })
        .await
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        Self::read_doc_file(&root, doc_id, record_id).await
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        // Delete doc file first (no cross-tab lock needed — files are per-record)
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        Self::delete_doc_file(&root, doc_id, record_id).await?;

        // Update doc_listing under cross-tab lock
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        self.index_transaction(move |index| {
            if let Some(listing) = index.doc_listing.get_mut(&did) {
                listing.retain(|r| r != &rid);
            }
            Ok(())
        })
        .await
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        // Write doc file first (no cross-tab lock needed)
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        Self::write_doc_file(&root, doc_id, record_id, bytes).await?;

        // Update doc_listing + append oplog under cross-tab lock
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        let entry = entry.clone();
        self.index_transaction(move |index| {
            let listing = index.doc_listing.entry(did).or_default();
            if !listing.contains(&rid) {
                listing.push(rid);
            }
            index.oplog.push(entry);
            Ok(())
        })
        .await
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        // Delete doc file first (no cross-tab lock needed)
        let root = {
            let inner = self.inner.lock().unwrap();
            inner.root_handle()
        };
        Self::delete_doc_file(&root, doc_id, record_id).await?;

        // Update doc_listing + append oplog under cross-tab lock
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        let entry = entry.clone();
        self.index_transaction(move |index| {
            if let Some(listing) = index.doc_listing.get_mut(&did) {
                listing.retain(|r| r != &rid);
            }
            index.oplog.push(entry);
            Ok(())
        })
        .await
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let (root, index) = self.get_cached_root_and_index();
        let record_ids = index.doc_listing.get(doc_id).cloned().unwrap_or_default();
        let mut results = Vec::new();
        for rid in &record_ids {
            if let Some(data) = Self::read_doc_file(&root, doc_id, rid).await? {
                results.push((rid.clone(), data));
            }
        }
        Ok(results)
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        let entry = entry.clone();
        self.index_transaction(move |index| {
            index.oplog.push(entry);
            Ok(())
        })
        .await
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let ns = namespace.to_string();
        self.index_transaction(move |index| {
            let results: Vec<OplogEntry> = index
                .oplog
                .iter()
                .filter(|e| e.namespace == ns && e.sync_status.is_uploadable())
                .take(limit)
                .cloned()
                .collect();
            Ok(results)
        })
        .await
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        let id = id.to_string();
        self.index_transaction(move |index| {
            if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
                entry.sync_status = SyncStatus::Synced;
                entry.sequence = Some(sequence);
            }
            Ok(())
        })
        .await
    }

    async fn mark_failed(&self, id: &str, _error: &str) -> Result<(), VaultSyncError> {
        let id = id.to_string();
        self.index_transaction(move |index| {
            if let Some(entry) = index.oplog.iter_mut().find(|e| e.id == id) {
                entry.sync_status = SyncStatus::Failed;
            }
            Ok(())
        })
        .await
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let index = self.get_cached_index();
        let results: Vec<OplogEntry> = index
            .oplog
            .iter()
            .filter(|e| e.namespace == namespace && e.sequence.map_or(false, |s| s > seq))
            .cloned()
            .collect();
        Ok(results)
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        Ok(self.get_cached_index().sync_states.get(namespace).cloned())
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        let state = state.clone();
        self.index_transaction(move |index| {
            index.sync_states.insert(state.namespace.clone(), state);
            Ok(())
        })
        .await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        Ok(self.get_cached_index().schemas.get(doc_id).cloned())
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        let meta = meta.clone();
        self.index_transaction(move |index| {
            index.schemas.insert(meta.doc_id.clone(), meta);
            Ok(())
        })
        .await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        Ok(self.get_cached_index().migrations.clone())
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        let record = record.clone();
        self.index_transaction(move |index| {
            let pos = index.migrations.iter().position(|m| m.version == record.version);
            if let Some(i) = pos {
                index.migrations[i] = record;
            } else {
                index.migrations.push(record);
            }
            Ok(())
        })
        .await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        let index = self.get_cached_index();
        let results: Vec<KeyRecord> = index
            .keys
            .iter()
            .filter(|k| k.namespace == namespace)
            .cloned()
            .collect();
        Ok(results)
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        let key = key.clone();
        self.index_transaction(move |index| {
            let pos = index
                .keys
                .iter()
                .position(|k| k.namespace == key.namespace && k.version == key.version);
            if let Some(i) = pos {
                index.keys[i] = key;
            } else {
                index.keys.push(key);
            }
            Ok(())
        })
        .await
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
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        let ns = namespace.to_string();
        let cutoff = (js_sys::Date::now() as u64)
            .saturating_sub(older_than_secs * 1000);
        self.index_transaction(move |index| {
            let before = index.oplog.len();
            index.oplog.retain(|entry| {
                !(entry.namespace == ns
                    && entry.sync_status == SyncStatus::Synced
                    && entry.created_at < cutoff)
            });
            let removed = before - index.oplog.len();
            Ok(removed)
        })
        .await
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let ns = namespace.to_string();
        let index = self.get_cached_index();
        let now_ms = js_sys::Date::now() as u64;
        let threshold = now_ms.saturating_sub(older_than_secs * 1000);
        let results: Vec<(String, String)> = index
            .oplog
            .iter()
            .filter(|e| {
                e.namespace == ns
                    && e.mutation_type == MutationType::CrdtDelete
                    && e.sync_status == SyncStatus::Synced
                    && e.created_at < threshold
            })
            .map(|e| (e.doc_id.clone(), e.record_id.clone()))
            .collect();
        Ok(results)
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
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        let ns = namespace.to_string();
        let index = self.get_cached_index();
        let mut seen = std::collections::HashSet::new();
        for entry in &index.oplog {
            if entry.namespace == ns && !seen.contains(&(entry.doc_id.clone(), entry.record_id.clone())) {
                seen.insert((entry.doc_id.clone(), entry.record_id.clone()));
            }
        }
        Ok(seen.into_iter().collect())
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let ns = namespace.to_string();
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        let index = self.get_cached_index();
        let results: Vec<OplogEntry> = index
            .oplog
            .iter()
            .filter(|e| {
                e.namespace == ns
                    && e.doc_id == did
                    && e.record_id == rid
                    && e.sync_status == SyncStatus::Synced
            })
            .cloned()
            .collect();
        Ok(results)
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        let ns = namespace.to_string();
        let did = doc_id.to_string();
        let rid = record_id.to_string();
        self.index_transaction(move |index| {
            let before = index.oplog.len();
            index.oplog.retain(|entry| {
                !(entry.namespace == ns
                    && entry.doc_id == did
                    && entry.record_id == rid
                    && entry.sync_status == SyncStatus::Synced
                    && entry.created_at < timestamp)
            });
            let removed = before - index.oplog.len();
            Ok(removed)
        })
        .await
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        let ns = namespace.to_string();
        self.index_transaction(move |index| {
            let before = index.oplog.len();
            index.oplog.retain(|entry| {
                !(entry.namespace == ns
                    && entry.sync_status == SyncStatus::Synced
                    && entry.created_at < cutoff_ms)
            });
            let removed = before - index.oplog.len();
            Ok(removed)
        })
        .await
    }
}

use crate::indexeddb::IndexedDbStorage;

#[derive(Debug)]
pub enum BrowserStorage {
    Opfs(OpfsStorage),
    Idb(IndexedDbStorage),
}

impl BrowserStorage {
    pub async fn new(db_name: &str, backend: Option<&str>) -> Result<Self, VaultSyncError> {
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
            "[Rust] BrowserStorage::new: db_name={}, requested_backend={:?}",
            db_name, backend
        )));

        match backend {
            Some("opfs") => {
                let opfs = OpfsStorage::new(db_name).await?;
                web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(
                    "[Rust] Using forced OPFS storage backend"
                ));
                Ok(Self::Opfs(opfs))
            }
            Some("indexeddb") => {
                let idb = IndexedDbStorage::new(db_name).await?;
                web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(
                    "[Rust] Using forced IndexedDB storage backend"
                ));
                Ok(Self::Idb(idb))
            }
            _ => {
                // OPFS is the preferred default: it is faster, quota-exempt, and
                // not shared across browser profiles (avoids key-mismatch issues).
                // Fall back to IndexedDB only when OPFS is unavailable (e.g. non-secure
                // context or older browsers).
                match OpfsStorage::new(db_name).await {
                    Ok(opfs) => {
                        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(
                            "[Rust] Using default OPFS storage backend",
                        ));
                        Ok(Self::Opfs(opfs))
                    }
                    Err(e) => {
                        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
                            "[Rust] OPFS unavailable, falling back to IndexedDB: {:?}",
                            e
                        )));
                        let idb = IndexedDbStorage::new(db_name).await?;
                        Ok(Self::Idb(idb))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Storage for BrowserStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.insert_document(doc_id, record_id, bytes).await,
            Self::Idb(s) => s.insert_document(doc_id, record_id, bytes).await,
        }
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.get_document(doc_id, record_id).await,
            Self::Idb(s) => s.get_document(doc_id, record_id).await,
        }
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_document(doc_id, record_id).await,
            Self::Idb(s) => s.delete_document(doc_id, record_id).await,
        }
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.list_documents(doc_id).await,
            Self::Idb(s) => s.list_documents(doc_id).await,
        }
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.write_document_and_oplog(doc_id, record_id, bytes, entry)
                    .await
            }
            Self::Idb(s) => {
                s.write_document_and_oplog(doc_id, record_id, bytes, entry)
                    .await
            }
        }
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_document_and_oplog(doc_id, record_id, entry).await,
            Self::Idb(s) => s.delete_document_and_oplog(doc_id, record_id, entry).await,
        }
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.append_oplog(entry).await,
            Self::Idb(s) => s.append_oplog(entry).await,
        }
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_pending_oplog(namespace, limit).await,
            Self::Idb(s) => s.read_pending_oplog(namespace, limit).await,
        }
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.mark_synced(id, sequence).await,
            Self::Idb(s) => s.mark_synced(id, sequence).await,
        }
    }

    async fn mark_failed(&self, id: &str, error: &str) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.mark_failed(id, error).await,
            Self::Idb(s) => s.mark_failed(id, error).await,
        }
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_oplog_after_sequence(namespace, seq).await,
            Self::Idb(s) => s.read_oplog_after_sequence(namespace, seq).await,
        }
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_sync_state(namespace).await,
            Self::Idb(s) => s.read_sync_state(namespace).await,
        }
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_sync_state(state).await,
            Self::Idb(s) => s.write_sync_state(state).await,
        }
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_schema(doc_id).await,
            Self::Idb(s) => s.read_schema(doc_id).await,
        }
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_schema(meta).await,
            Self::Idb(s) => s.write_schema(meta).await,
        }
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_migrations().await,
            Self::Idb(s) => s.read_migrations().await,
        }
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_migration(record).await,
            Self::Idb(s) => s.write_migration(record).await,
        }
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.read_keys(namespace).await,
            Self::Idb(s) => s.read_keys(namespace).await,
        }
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.write_key(key).await,
            Self::Idb(s) => s.write_key(key).await,
        }
    }

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.reset_stale_pending(namespace, older_than_ms).await,
            Self::Idb(s) => s.reset_stale_pending(namespace, older_than_ms).await,
        }
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.delete_synced_oplog_older_than(namespace, older_than_secs)
                    .await
            }
            Self::Idb(s) => {
                s.delete_synced_oplog_older_than(namespace, older_than_secs)
                    .await
            }
        }
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.list_tombstoned_documents(namespace, older_than_secs)
                    .await
            }
            Self::Idb(s) => {
                s.list_tombstoned_documents(namespace, older_than_secs)
                    .await
            }
        }
    }

    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        match self {
            Self::Opfs(s) => s.update_oplog_encrypted_blob(id, new_blob).await,
            Self::Idb(s) => s.update_oplog_encrypted_blob(id, new_blob).await,
        }
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.list_active_documents(namespace).await,
            Self::Idb(s) => s.list_active_documents(namespace).await,
        }
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.read_synced_oplog_for_document(namespace, doc_id, record_id)
                    .await
            }
            Self::Idb(s) => {
                s.read_synced_oplog_for_document(namespace, doc_id, record_id)
                    .await
            }
        }
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => {
                s.delete_synced_oplog_before_timestamp(namespace, doc_id, record_id, timestamp)
                    .await
            }
            Self::Idb(s) => {
                s.delete_synced_oplog_before_timestamp(namespace, doc_id, record_id, timestamp)
                    .await
            }
        }
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        match self {
            Self::Opfs(s) => s.delete_synced_before(namespace, cutoff_ms).await,
            Self::Idb(s) => s.delete_synced_before(namespace, cutoff_ms).await,
        }
    }
}
