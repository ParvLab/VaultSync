use js_sys::Uint8Array;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use vaultsync_core::time_utils::SendJsFuture;
use vaultsync_core::VaultSyncError;
use web_sys::*;

pub type PageId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub version: u8,
    pub checksum: u32,
    pub flags: u8,
    pub data_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreMeta {
    next_page_id: PageId,
    version: u8,
}

#[derive(Debug, Clone)]
pub struct PageStore {
    dir: Arc<FileSystemDirectoryHandle>,
    store_name: Arc<String>,
}

impl PageStore {
    pub async fn open(
        root: &FileSystemDirectoryHandle,
        store_name: &str,
    ) -> Result<Self, VaultSyncError> {
        let dir = ensure_dir(root, store_name).await?;
        let meta = read_meta(&dir).await.unwrap_or(StoreMeta {
            next_page_id: 0,
            version: 1,
        });
        if meta.version == 0 {
            write_meta(&dir, &StoreMeta {
                next_page_id: meta.next_page_id,
                version: 1,
            })
            .await?;
        }
        Ok(Self {
            dir: Arc::new(dir),
            store_name: Arc::new(store_name.to_string()),
        })
    }

    pub async fn allocate_page_id(&self) -> Result<PageId, VaultSyncError> {
        let meta = read_meta(&self.dir).await.unwrap_or(StoreMeta {
            next_page_id: 0,
            version: 1,
        });
        let page_id = meta.next_page_id;
        write_meta(
            &self.dir,
            &StoreMeta {
                next_page_id: page_id + 1,
                version: 1,
            },
        )
        .await?;
        Ok(page_id)
    }

    pub async fn write_page(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        let checksum = crc32fast::hash(data);
        let header = PageHeader {
            version: 1,
            checksum,
            flags: 0,
            data_len: data.len() as u32,
        };
        let header_bytes = postcard::to_allocvec(&header)
            .map_err(|e| VaultSyncError::Storage(format!("header encode: {:?}", e)))?;

        let mut buf = Vec::with_capacity(header_bytes.len() + data.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(data);

        write_file(&self.dir, &page_filename(page_id), &buf).await
    }

    pub async fn write_page_verify(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        let tmp_id = page_id.wrapping_add(0x8000_0000_0000_0000);
        let checksum = crc32fast::hash(data);
        let header = PageHeader {
            version: 1,
            checksum,
            flags: 0,
            data_len: data.len() as u32,
        };
        let header_bytes = postcard::to_allocvec(&header)
            .map_err(|e| VaultSyncError::Storage(format!("header encode: {:?}", e)))?;
        let mut buf = Vec::with_capacity(header_bytes.len() + data.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(data);

        write_file(&self.dir, &page_filename(tmp_id), &buf).await?;

        let read_back = read_page_file(&self.dir, &page_filename(tmp_id)).await?;
        match read_back {
            Some((_, read_data)) if read_data == data => {}
            Some(_) => {
                let _ = delete_file(&self.dir, &page_filename(tmp_id)).await;
                return Err(VaultSyncError::Storage("verify mismatch on write".into()));
            }
            None => {
                return Err(VaultSyncError::Storage("verify read failed".into()));
            }
        }

        rename_file(&self.dir, &page_filename(tmp_id), &page_filename(page_id)).await
    }

    pub async fn read_page(&self, page_id: PageId) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let (header, data) = match read_page_file(&self.dir, &page_filename(page_id)).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let actual_checksum = crc32fast::hash(&data);
        if actual_checksum != header.checksum {
            return Err(VaultSyncError::Storage(format!(
                "checksum mismatch page {}: expected {} got {}",
                page_id, header.checksum, actual_checksum
            )));
        }
        if header.flags & 0x02 != 0 {
            return Ok(None);
        }
        Ok(Some(data))
    }

    pub async fn delete_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        delete_file(&self.dir, &page_filename(page_id)).await
    }

    pub async fn tombstone_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let (mut header, _data) =
            match read_page_file_raw(&self.dir, &page_filename(page_id)).await? {
                Some(pair) => pair,
                None => return Err(VaultSyncError::Storage(format!("page {} not found", page_id))),
            };
        header.flags |= 0x02;
        let header_bytes = postcard::to_allocvec(&header)
            .map_err(|e| VaultSyncError::Storage(format!("header encode: {:?}", e)))?;
        let existing = read_all_bytes(&self.dir, &page_filename(page_id)).await?;
        let data_start = header_bytes.len();
        if data_start <= existing.len() {
            let mut buf = Vec::with_capacity(existing.len());
            buf.extend_from_slice(&header_bytes);
            buf.extend_from_slice(&existing[data_start..]);
        write_file(&self.dir, &page_filename(page_id), &buf).await
        } else {
            Err(VaultSyncError::Storage("page header size mismatch".into()))
        }
    }

    pub async fn list_page_ids(&self) -> Result<Vec<PageId>, VaultSyncError> {
        let meta = read_meta(&self.dir).await.unwrap_or(StoreMeta {
            next_page_id: 0,
            version: 1,
        });
        let mut ids = Vec::new();
        for id in 0..meta.next_page_id {
            if page_exists(&self.dir, &page_filename(id)).await? {
                let (header, _) = match read_page_file_raw(&self.dir, &page_filename(id)).await? {
                    Some(pair) => pair,
                    None => continue,
                };
                if header.flags & 0x02 == 0 {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }
}

fn page_filename(page_id: PageId) -> String {
    format!("{}.page", page_id)
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

async fn read_meta(dir: &FileSystemDirectoryHandle) -> Option<StoreMeta> {
    read_all_bytes(dir, "_meta").await.ok().and_then(|bytes| {
        serde_json::from_slice::<StoreMeta>(&bytes).ok()
    })
}

async fn write_meta(dir: &FileSystemDirectoryHandle, meta: &StoreMeta) -> Result<(), VaultSyncError> {
    let json = serde_json::to_vec(meta)
        .map_err(|e| VaultSyncError::Storage(format!("meta json: {:?}", e)))?;
    write_file(dir, "_meta", &json).await
}

async fn read_all_bytes(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Vec<u8>, VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    let file_val = SendJsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
    let file_handle: FileSystemFileHandle = file_val.into();
    let file_val = SendJsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file: {:?}", e)))?;
    let file: File = file_val.into();
    let blob: &Blob = file.as_ref();
    let buf_val = SendJsFuture::from(blob.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    Ok(bytes)
}

async fn write_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
    data: &[u8],
) -> Result<(), VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let file_val = SendJsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await
        .map_err(|e| VaultSyncError::Storage(format!("get_file_handle: {:?}", e)))?;
    let file_handle: FileSystemFileHandle = file_val.into();
    let writable_val = SendJsFuture::from(file_handle.create_writable())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("create_writable: {:?}", e)))?;
    let writable: FileSystemWritableFileStream = writable_val.into();
    let js_array = Uint8Array::new_with_length(data.len() as u32);
    js_array.copy_from(data);
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

async fn rename_file(
    dir: &FileSystemDirectoryHandle,
    from: &str,
    to: &str,
) -> Result<(), VaultSyncError> {
    if from == to {
        return Ok(());
    }
    let data = read_all_bytes(dir, from).await?;
    let create_opts = FileSystemGetFileOptions::new();
    create_opts.set_create(true);
    let dst_handle: FileSystemFileHandle = SendJsFuture::from(
        dir.get_file_handle_with_options(to, &create_opts),
    )
    .await
    .map_err(|e| VaultSyncError::Storage(format!("get_file_handle dst: {:?}", e)))?
    .into();
    let writable_val = SendJsFuture::from(dst_handle.create_writable())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("create_writable dst: {:?}", e)))?;
    let writable: FileSystemWritableFileStream = writable_val.into();
    let js_array = Uint8Array::new_with_length(data.len() as u32);
    js_array.copy_from(&data);
    let write_promise = writable
        .write_with_buffer_source(&js_array)
        .map_err(|e| VaultSyncError::Storage(format!("write dst: {:?}", e)))?;
    SendJsFuture::from(write_promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("write dst done: {:?}", e)))?;
    let close: WritableStream = writable.into();
    SendJsFuture::from(close.close())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("close dst: {:?}", e)))?;
    delete_file(dir, from).await?;
    Ok(())
}

async fn delete_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<(), VaultSyncError> {
    let promise = dir.remove_entry(name);
    SendJsFuture::from(promise)
        .await
        .map_err(|e| VaultSyncError::Storage(format!("remove_entry: {:?}", e)))?;
    Ok(())
}

async fn page_exists(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<bool, VaultSyncError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(false);
    match SendJsFuture::from(dir.get_file_handle_with_options(name, &opts)).await {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

async fn read_page_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    match read_page_file_raw(dir, name).await? {
        Some((header, data)) => {
            if data.len() as u32 != header.data_len {
                return Err(VaultSyncError::Storage(format!(
                    "page data len mismatch: header {} actual {}",
                    header.data_len,
                    data.len()
                )));
            }
            Ok(Some((header, data)))
        }
        None => Ok(None),
    }
}

async fn read_page_file_raw(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    let bytes = match read_all_bytes(dir, name).await {
        Ok(b) => b,
        Err(e) => {
            let err_str = format!("{:?}", e);
            if err_str.contains("NotFoundError") || err_str.contains("NotFound") {
                return Ok(None);
            }
            return Err(e);
        }
    };
    if bytes.len() < 14 {
        return Err(VaultSyncError::Storage(format!(
            "corrupt page {}: too short ({} bytes)",
            name,
            bytes.len()
        )));
    }
    let header: PageHeader = postcard::from_bytes(&bytes[..14])
        .map_err(|e| VaultSyncError::Storage(format!("header decode: {:?}", e)))?;
    if header.version != 1 {
        return Err(VaultSyncError::Storage(format!(
            "page {}: unknown version {}",
            name, header.version
        )));
    }
    let data = bytes[14..].to_vec();
    Ok(Some((header, data)))
}
