use js_sys::Uint8Array;
use std::sync::{Arc, Mutex};
use vaultsync_core::oplog::entry::OplogEntry;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{FileSystemDirectoryHandle, FileSystemFileHandle};

/// OPFS-backed mutation store.
///
/// Persists pending mutations as individual files under a `_mutations/`
/// directory. Each file is named `<sequence>-<uuid>.json`.
/// Provides a queue-like interface for the upload pipeline.
pub struct MutationStore {
    root: Arc<FileSystemDirectoryHandle>,
    pending: Mutex<Vec<String>>, // in-memory index of pending mutation filenames
}

impl MutationStore {
    /// Open or create the mutation store under the given root directory.
    pub async fn new(root: &FileSystemDirectoryHandle) -> Result<Self, JsValue> {
        let dir = ensure_mutations_dir(root).await?;

        // Scan existing mutation files
        let mut pending = Vec::new();
        let iter = dir.entries();
        loop {
            let entry = js_sys::Reflect::get(&iter, &JsValue::from_str("next"))?
                .dyn_into::<js_sys::Function>()?
                .call0(&iter)?;
            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))?
                .as_bool()
                .unwrap_or(true);
            if done {
                break;
            }
            if let Some(name) = js_sys::Reflect::get(&entry, &JsValue::from_str("value"))?
                .as_string()
            {
                if name.ends_with(".json") {
                    pending.push(name);
                }
            }
        }

        Ok(Self {
            root: Arc::new(dir),
            pending: Mutex::new(pending),
        })
    }

    /// Store a pending mutation to OPFS.
    pub async fn push(&self, entry: &OplogEntry) -> Result<(), JsValue> {
        let filename = format!("{}-{}.json", entry.sequence.unwrap_or(0), entry.id);
        let json = serde_json::to_string(entry).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let bytes = json.into_bytes();

        let file_handle = self.root.get_file_handle(&filename).await?;
        let file_handle: FileSystemFileHandle = file_handle.dyn_into()?;
        let writable = file_handle.create_writable().await?;
        let writable: web_sys::FileSystemWritableFileStream = writable.dyn_into()?;
        let chunk = Uint8Array::from(&bytes[..]);
        let _ = writable.write_with_buffer_source(&chunk)?;
        writable.close().await?;

        self.pending.lock().unwrap().push(filename);
        Ok(())
    }

    /// Load up to `limit` pending mutations for upload.
    pub async fn pop_batch(&self, limit: usize) -> Result<Vec<OplogEntry>, JsValue> {
        let mut batch = Vec::new();
        let mut remaining = Vec::new();
        {
            let pending = self.pending.lock().unwrap();
            for (i, name) in pending.iter().enumerate() {
                if i >= limit {
                    remaining.extend(pending[i..].iter().cloned());
                    break;
                }
                match self.read_entry(name).await {
                    Ok(Some(entry)) => batch.push(entry),
                    Ok(None) => {} // skip missing files
                    Err(_) => {}
                }
            }
        }
        // Retain unprocessed files in the pending list
        *self.pending.lock().unwrap() = remaining;
        Ok(batch)
    }

    /// Mark a mutation as synced — remove from OPFS.
    pub async fn ack(&self, id: &str) -> Result<(), JsValue> {
        let filename = self.find_file_by_id(id).await;
        if let Some(name) = filename {
            self.root.remove_entry(&name).await?;
            let mut pending = self.pending.lock().unwrap();
            pending.retain(|f| f != &name);
        }
        Ok(())
    }

    /// Mark a mutation as failed — keep in store for retry.
    pub async fn nack(&self, _id: &str) -> Result<(), JsValue> {
        // For now, just leave the file in place (retry on next batch)
        Ok(())
    }

    /// Number of pending mutations.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    async fn read_entry(&self, filename: &str) -> Result<Option<OplogEntry>, JsValue> {
        let file_handle = match self.root.get_file_handle(filename).await {
            Ok(fh) => fh,
            Err(_) => return Ok(None),
        };
        let file_handle: FileSystemFileHandle = file_handle.dyn_into()?;
        let file = file_handle.get_file().await?;
        let file: web_sys::File = file.dyn_into()?;
        let buf = file.array_buffer().await?;
        let bytes = Uint8Array::new(&buf).to_vec();
        let json = String::from_utf8(bytes).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let entry: OplogEntry =
            serde_json::from_str(&json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Some(entry))
    }

    async fn find_file_by_id(&self, id: &str) -> Option<String> {
        let pending = self.pending.lock().unwrap();
        for name in pending.iter() {
            if name.contains(id) {
                return Some(name.clone());
            }
        }
        None
    }
}

async fn ensure_mutations_dir(root: &FileSystemDirectoryHandle) -> Result<FileSystemDirectoryHandle, JsValue> {
    let dir = root.get_directory_handle("_mutations").await?;
    dir.dyn_into()
}
