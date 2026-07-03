use crate::page_store::PageStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta};
use vaultsync_core::sync::state::SyncState;
use vaultsync_core::VaultSyncError;
use web_sys::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1Index {
    version: u32,
    doc_listing: HashMap<String, Vec<String>>,
    oplog: Vec<OplogEntry>,
    sync_states: HashMap<String, SyncState>,
    schemas: HashMap<String, SchemaMeta>,
    migrations: Vec<MigrationRecord>,
    keys: Vec<KeyRecord>,
}

pub async fn try_migrate_from_v1(
    db_name: &str,
) -> Result<bool, VaultSyncError> {
    let window = web_sys::window()
        .ok_or_else(|| VaultSyncError::Storage("no window".into()))?;
    let storage_mgr = window.navigator().storage();
    let root_val = vaultsync_core::time_utils::SendJsFuture::from(storage_mgr.get_directory())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_directory: {:?}", e)))?;
    let root: FileSystemDirectoryHandle = root_val.into();

    let db_dir = get_dir_v1(&root, db_name).await?;
    match load_v1_index(&db_dir).await {
        Ok(Some(index)) => {
            migrate_index_to_pages(&db_dir, &index).await?;
            rename_v1_system(&db_dir).await?;
            Ok(true)
        }
        Ok(None) => Ok(false),
        Err(e) => {
            web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
                "[migrate] v1 index found but error: {:?}",
                e
            )));
            Ok(false)
        }
    }
}

async fn load_v1_index(db_dir: &FileSystemDirectoryHandle) -> Result<Option<V1Index>, VaultSyncError> {
    let sys_dir = match get_dir_v1(db_dir, "_system").await {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    let file_val = match vaultsync_core::time_utils::SendJsFuture::from(
        sys_dir.get_file_handle_with_options("index.json", &opts),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let file_handle: FileSystemFileHandle = file_val.into();
    let file_val = vaultsync_core::time_utils::SendJsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
    let file: File = file_val.into();
    let blob: &Blob = file.as_ref();
    let buf_val = vaultsync_core::time_utils::SendJsFuture::from(blob.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = js_sys::Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    if bytes.is_empty() {
        return Ok(None);
    }
    let index: V1Index = serde_json::from_slice(&bytes)
        .map_err(|e| VaultSyncError::Storage(format!("json parse: {:?}", e)))?;
    Ok(Some(index))
}

async fn migrate_index_to_pages(
    db_dir: &FileSystemDirectoryHandle,
    index: &V1Index,
) -> Result<(), VaultSyncError> {
    web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
        "[migrate] migrating {} oplog entries, {} docs, {} sync_states, {} schemas, {} migrations, {} keys",
        index.oplog.len(),
        index.doc_listing.values().map(|v| v.len()).sum::<usize>(),
        index.sync_states.len(),
        index.schemas.len(),
        index.migrations.len(),
        index.keys.len(),
    )));

    let stores = PagesDir::open(db_dir).await?;

    for (doc_id, record_ids) in &index.doc_listing {
        for record_id in record_ids {
            if let Ok(Some(bytes)) = read_v1_doc(db_dir, doc_id, record_id).await {
                let entry = DocEntry {
                    doc_id: doc_id.clone(),
                    record_id: record_id.clone(),
                    bytes,
                };
                let page_id = stores.doc_data.allocate_page_id().await?;
                let encoded = postcard::to_allocvec(&entry)
                    .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
                stores.doc_data.write_page(page_id, &encoded).await?;
            }
        }
    }

    let mut oplog_batch = index.oplog.clone();
    while !oplog_batch.is_empty() {
        let chunk_len = oplog_batch.len().min(50);
        let chunk: Vec<OplogEntry> = oplog_batch.drain(..chunk_len).collect();
        let page_id = stores.oplog.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(&chunk)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        stores.oplog.write_page(page_id, &encoded).await?;
    }

    for (ns, state) in &index.sync_states {
        let page_id = stores.sync_states.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(&(ns.clone(), state.clone()))
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        stores.sync_states.write_page(page_id, &encoded).await?;
    }

    for (doc_id, schema) in &index.schemas {
        let page_id = stores.schemas.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(&(doc_id.clone(), schema.clone()))
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        stores.schemas.write_page(page_id, &encoded).await?;
    }

    for migration in &index.migrations {
        let page_id = stores.migrations.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(migration)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        stores.migrations.write_page(page_id, &encoded).await?;
    }

    for key in &index.keys {
        let page_id = stores.keys.allocate_page_id().await?;
        let encoded = postcard::to_allocvec(key)
            .map_err(|e| VaultSyncError::Storage(format!("encode: {:?}", e)))?;
        stores.keys.write_page(page_id, &encoded).await?;
    }

    Ok(())
}

async fn rename_v1_system(db_dir: &FileSystemDirectoryHandle) -> Result<(), VaultSyncError> {
    let sys_dir = get_dir_v1(db_dir, "_system").await?;
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    if let Ok(handle) = vaultsync_core::time_utils::SendJsFuture::from(
        sys_dir.get_file_handle_with_options("index.json", &opts),
    )
    .await
    {
        let _: FileSystemFileHandle = handle.into();
        let new_name = format!("index.json.v1-migrated-{}", js_sys::Date::now() as u64);
        let rename_opts = FileSystemGetFileOptions::new();
        rename_opts.set_create(true);
        let data = vaultsync_core::time_utils::SendJsFuture::from(
            sys_dir.get_file_handle_with_options(&new_name, &rename_opts),
        )
        .await;
        if data.is_ok() {
            let _ = vaultsync_core::time_utils::SendJsFuture::from(sys_dir.remove_entry("index.json")).await;
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PagesDir {
    pub doc_data: PageStore,
    pub oplog: PageStore,
    pub sync_states: PageStore,
    pub schemas: PageStore,
    pub migrations: PageStore,
    pub keys: PageStore,
}

impl PagesDir {
    pub async fn open(db_dir: &FileSystemDirectoryHandle) -> Result<Self, VaultSyncError> {
        let pages_root = ensure_dir(db_dir, "_pages").await?;
        Ok(Self {
            doc_data: PageStore::open(&pages_root, "doc_data").await?,
            oplog: PageStore::open(&pages_root, "oplog").await?,
            sync_states: PageStore::open(&pages_root, "sync_states").await?,
            schemas: PageStore::open(&pages_root, "schemas").await?,
            migrations: PageStore::open(&pages_root, "migrations").await?,
            keys: PageStore::open(&pages_root, "keys").await?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocEntry {
    pub doc_id: String,
    pub record_id: String,
    pub bytes: Vec<u8>,
}

async fn get_dir_v1(
    root: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(false);
    let promise = root.get_directory_handle_with_options(name, &opts);
    let val = vaultsync_core::time_utils::SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_dir failed: {:?}", e)))?;
    Ok(val.into())
}

async fn ensure_dir(
    root: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, VaultSyncError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(true);
    let promise = root.get_directory_handle_with_options(name, &opts);
    let val = vaultsync_core::time_utils::SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("ensure_dir failed: {:?}", e)))?;
    Ok(val.into())
}

async fn read_v1_doc(
    db_dir: &FileSystemDirectoryHandle,
    doc_id: &str,
    record_id: &str,
) -> Result<Option<Vec<u8>>, VaultSyncError> {
    let docs_dir = get_dir_v1(db_dir, "docs").await?;
    let doc_dir = match get_dir_v1(&docs_dir, doc_id).await {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    let file_val = match vaultsync_core::time_utils::SendJsFuture::from(
        doc_dir.get_file_handle_with_options(record_id, &opts),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let file_handle: FileSystemFileHandle = file_val.into();
    let file_val = vaultsync_core::time_utils::SendJsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
    let file: File = file_val.into();
    let blob: &Blob = file.as_ref();
    let buf_val = vaultsync_core::time_utils::SendJsFuture::from(blob.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = js_sys::Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    Ok(Some(bytes))
}
