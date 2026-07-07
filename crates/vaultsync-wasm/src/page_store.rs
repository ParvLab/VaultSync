use js_sys::Uint8Array;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use vaultsync_core::time_utils::SendJsFuture;
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::VaultSyncError;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::*;

pub type PageId = u64;

/// V3 format: [0x56, 0x53] + [postcard PageHeader] + [data]
const PAGE_MAGIC: [u8; 2] = [0x56, 0x53];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub version: u8,
    pub checksum: u32,
    pub flags: u8,
    pub data_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreManifest {
    version: u8,
    highest_page_id: PageId,
    current_page: PageId,
    last_sequence: u64,
    pending_count: usize,
    live_pages: usize,
    tombstoned_pages: usize,
    created_at: u64,
    updated_at: u64,
    checksum: u32,
}

impl StoreManifest {
    fn new() -> Self {
        let now = js_sys::Date::now() as u64;
        let mut m = Self {
            version: 1,
            highest_page_id: 0,
            current_page: 0,
            last_sequence: 0,
            pending_count: 0,
            live_pages: 0,
            tombstoned_pages: 0,
            created_at: now,
            updated_at: now,
            checksum: 0,
        };
        m.checksum = m.compute_checksum();
        m
    }

    fn compute_checksum(&self) -> u32 {
        let buf = postcard::to_allocvec(self).unwrap_or_default();
        crc32fast::hash(&buf)
    }

    fn verify(&self) -> bool {
        let expected = self.checksum;
        let mut copy = self.clone();
        copy.checksum = 0;
        copy.compute_checksum() == expected
    }
}

#[derive(Debug)]
pub struct PageStore {
    dir: Arc<FileSystemDirectoryHandle>,
    store_name: Arc<String>,
    gc_needed: Arc<AtomicBool>,
    split_pending: Arc<AtomicBool>,
}

impl Clone for PageStore {
    fn clone(&self) -> Self {
        Self {
            dir: Arc::clone(&self.dir),
            store_name: Arc::clone(&self.store_name),
            gc_needed: Arc::clone(&self.gc_needed),
            split_pending: Arc::clone(&self.split_pending),
        }
    }
}

impl PageStore {
    pub async fn open(
        root: &FileSystemDirectoryHandle,
        store_name: &str,
    ) -> Result<Self, VaultSyncError> {
        let dir = ensure_dir(root, store_name).await?;
        let manifest = match read_manifest(&dir).await {
            Some(m) if m.verify() => m,
            _ => {
                let rebuilt = rebuild_manifest(&dir).await?;
                write_manifest(&dir, &rebuilt).await?;
                rebuilt
            }
        };
        Ok(Self {
            dir: Arc::new(dir),
            store_name: Arc::new(store_name.to_string()),
            gc_needed: Arc::new(AtomicBool::new(false)),
            split_pending: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn allocate_page_id(&self) -> Result<PageId, VaultSyncError> {
        let manifest = read_manifest(&self.dir).await.unwrap_or_else(|| {
            // Fallback: should not happen after open() rebuilds, but be safe
            let mut m = StoreManifest::new();
            m.checksum = m.compute_checksum();
            m
        });
        let page_id = manifest.highest_page_id;
        let mut updated = manifest.clone();
        updated.highest_page_id = page_id + 1;
        updated.live_pages += 1;
        updated.updated_at = js_sys::Date::now() as u64;
        write_manifest(&self.dir, &updated).await?;
        Ok(page_id)
    }

    /// Write raw page data without any split checks. Used by split_page internally.
    async fn write_page_raw(&self, page_id: PageId, data: &[u8]) -> Result<(), VaultSyncError> {
        let buf = encode_v3_page(data, 0)?;
        write_file(&self.dir, &page_filename(page_id), &buf).await?;
        self.bump_manifest_live_page(page_id).await?;
        Ok(())
    }

    pub async fn write_page(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        // Phase 2: Bounded Pages — check multi-metric thresholds.
        let entry_count = Self::estimate_entry_count(data);

        // Hard threshold: synchronous emergency split before write.
        if data.len() > Self::HARD_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE * 2 {
            engine_warn!("[page_store] emergency split page {}: {} bytes, ~{} entries", page_id, data.len(), entry_count);
            // Write to a temporary page, split it, and return instead of writing the oversized page.
            let tmp_id = self.allocate_page_id().await?;
            self.write_page_raw(tmp_id, data).await?;
            return self.split_page(tmp_id).await;
        }

        // Soft threshold: schedule background split.
        if data.len() > Self::SOFT_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE {
            engine_debug!("[page_store] scheduling background split for page {}: {} bytes, ~{} entries", page_id, data.len(), entry_count);
            self.split_pending.store(true, Ordering::Release);
        }

        self.write_page_raw(page_id, data).await
    }

    pub async fn write_page_verify(
        &self,
        page_id: PageId,
        data: &[u8],
    ) -> Result<(), VaultSyncError> {
        let tmp_id = page_id.wrapping_add(0x8000_0000_0000_0000);
        let buf = encode_v3_page(data, 0)?;

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

        rename_file(&self.dir, &page_filename(tmp_id), &page_filename(page_id)).await?;
        self.bump_manifest_live_page(page_id).await?;
        Ok(())
    }

    pub async fn read_page(&self, page_id: PageId) -> Result<Option<Vec<u8>>, VaultSyncError> {
        let (header, data) = match read_page_file(&self.dir, &page_filename(page_id)).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let actual_checksum = crc32fast::hash(&data);
        if header.version >= 1 && actual_checksum != header.checksum {
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
        delete_file(&self.dir, &page_filename(page_id)).await?;
        let manifest = read_manifest(&self.dir).await;
        if let Some(mut m) = manifest {
            if m.live_pages > 0 {
                m.live_pages = m.live_pages.saturating_sub(1);
            } else if m.tombstoned_pages > 0 {
                m.tombstoned_pages = m.tombstoned_pages.saturating_sub(1);
            }
            m.updated_at = js_sys::Date::now() as u64;
            write_manifest(&self.dir, &m).await?;
        }
        Ok(())
    }

    pub async fn tombstone_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let name = page_filename(page_id);
        let existing = match read_all_bytes(&self.dir, &name).await {
            Ok(b) => b,
            Err(e) if is_not_found(&e) => {
                return Err(VaultSyncError::Storage(format!("page {} not found", page_id)));
            }
            Err(e) => return Err(e),
        };

        let data = extract_page_data(&existing)?;
        let buf = encode_v3_page(&data, 0x02)?;
        write_file(&self.dir, &name, &buf).await?;

        // Update manifest: move from live to tombstoned
        let manifest = read_manifest(&self.dir).await;
        if let Some(mut m) = manifest {
            if m.live_pages > 0 {
                m.live_pages = m.live_pages.saturating_sub(1);
            }
            m.tombstoned_pages += 1;
            m.updated_at = js_sys::Date::now() as u64;
            write_manifest(&self.dir, &m).await?;
        }
        Ok(())
    }

    /// Update manifest after writing a new or existing live page.
    async fn bump_manifest_live_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let manifest = read_manifest(&self.dir).await;
        if let Some(mut m) = manifest {
            if page_id >= m.highest_page_id {
                m.highest_page_id = page_id + 1;
            }
            // Check if this is a new page (not already counted as live or tombstoned)
            if page_id >= m.highest_page_id.saturating_sub(1) {
                // Likely a new allocation — already counted in allocate_page_id
            }
            m.updated_at = js_sys::Date::now() as u64;
            write_manifest(&self.dir, &m).await?;
        }
        Ok(())
    }

    /// Remove pages that have the tombstone flag (0x02) set.
    /// Returns the number of pages cleaned up.
    pub async fn cleanup_tombstoned_pages(&self) -> Result<usize, VaultSyncError> {
        let mut cleaned = 0usize;
        let iter = self.dir.entries();

        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };

            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };

            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };

            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if done {
                break;
            }

            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                })
            {
                Some(n) => n,
                None => continue,
            };

            if let Some(id) = parse_page_id(&name) {
                match read_page_header(&self.dir, &name).await? {
                    Some(header) if header.flags & 0x02 != 0 => {
                        delete_file(&self.dir, &name).await?;
                        cleaned += 1;
                    }
                    _ => {}
                }
            }
        }

        if cleaned > 0 {
            // After cleanup, rebuild manifest to get accurate counts
            let rebuilt = rebuild_manifest(&self.dir).await?;
            write_manifest(&self.dir, &rebuilt).await?;
        }

        Ok(cleaned)
    }

    pub async fn list_page_ids(&self) -> Result<Vec<PageId>, VaultSyncError> {
        let mut ids = Vec::new();

        let iter = self.dir.entries();

        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };

            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };

            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };

            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if done {
                break;
            }

            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                })
            {
                Some(n) => n,
                None => continue,
            };

            if let Some(id) = parse_page_id(&name) {
                match read_page_header(&self.dir, &name).await? {
                    Some(header) if header.flags & 0x02 == 0 => {
                        ids.push(id);
                    }
                    _ => {}
                }
            }
        }

        Ok(ids)
    }

    /// Set the current full-state page ID in the manifest.
    /// Signals that this page represents the complete store state.
    pub async fn set_current_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let manifest = read_manifest(&self.dir).await;
        let mut m = match manifest {
            Some(m) if m.verify() => m,
            _ => StoreManifest::new(),
        };
        m.current_page = page_id;
        m.updated_at = js_sys::Date::now() as u64;
        write_manifest(&self.dir, &m).await
    }

    /// Get the current full-state page ID from the manifest.
    /// Returns 0 if no current page is set (legacy store).
    pub async fn current_page(&self) -> PageId {
        read_manifest(&self.dir)
            .await
            .map(|m| m.current_page)
            .unwrap_or(0)
    }

    /// Track last sequence number and pending count in the manifest.
    pub async fn set_manifest_meta(
        &self,
        last_sequence: u64,
        pending_count: usize,
    ) -> Result<(), VaultSyncError> {
        let manifest = read_manifest(&self.dir).await;
        let mut m = match manifest {
            Some(m) if m.verify() => m,
            _ => StoreManifest::new(),
        };
        m.last_sequence = last_sequence;
        m.pending_count = pending_count;
        m.updated_at = js_sys::Date::now() as u64;
        write_manifest(&self.dir, &m).await
    }

    /// Mark that GC is needed (called after a full-state rewrite).
    pub fn schedule_gc(&self) {
        self.gc_needed.store(true, Ordering::Release);
    }

    // ── Page splitting (Phase 2: Bounded Pages) ──────────────────────

    /// Thresholds for page splitting.
    const TARGET_PAGE_BYTES: usize = 4_000_000;
    const SOFT_SPLIT_BYTES: usize = 8_000_000;
    const HARD_SPLIT_BYTES: usize = 16_000_000;
    const MAX_ENTRIES_PER_PAGE: usize = 4096;

    /// Check whether a page should be split based on multiple metrics.
    pub fn should_split(&self, data: &[u8], entry_count_hint: usize) -> bool {
        data.len() > Self::HARD_SPLIT_BYTES
            || entry_count_hint > Self::MAX_ENTRIES_PER_PAGE
            || data.len() > Self::SOFT_SPLIT_BYTES
    }

    /// Estimate the number of entries in a postcard-encoded page.
    /// Tries to decode as Vec<T> first; if that fails, estimates from byte length.
    fn estimate_entry_count(data: &[u8]) -> usize {
        // Try to decode as Vec<OplogEntry> (the most common case)
        if let Ok(vec) = postcard::from_bytes::<Vec<vaultsync_core::oplog::entry::OplogEntry>>(data) {
            return vec.len();
        }
        // Fallback: assume average entry size of ~256 bytes
        if data.is_empty() {
            return 0;
        }
        std::cmp::max(1, data.len() / 256)
    }

    /// Read a page, split its entries into two balanced pages, write both, and GC the original.
    pub async fn split_page(&self, page_id: PageId) -> Result<(), VaultSyncError> {
        let data = match self.read_page(page_id).await? {
            Some(d) => d,
            None => return Ok(()),
        };

        // Try to split entries evenly
        let entries: Vec<Vec<u8>> = if let Ok(vec) = postcard::from_bytes::<Vec<vaultsync_core::oplog::entry::OplogEntry>>(&data) {
            let mid = vec.len() / 2;
            let left: Vec<_> = vec[..mid].to_vec();
            let right: Vec<_> = vec[mid..].to_vec();
            vec![
                postcard::to_allocvec(&left).map_err(|e| VaultSyncError::Storage(format!("split encode left: {:?}", e)))?,
                postcard::to_allocvec(&right).map_err(|e| VaultSyncError::Storage(format!("split encode right: {:?}", e)))?,
            ]
        } else {
            // Cannot decode — skip splitting this page
            engine_warn!("[page_store] cannot split page {}: not a Vec<OplogEntry>", page_id);
            return Ok(());
        };

        if entries.len() != 2 {
            return Ok(());
        }

        let left_id = self.allocate_page_id().await?;
        let right_id = self.allocate_page_id().await?;

        self.write_page_raw(left_id, &entries[0]).await?;
        self.write_page_raw(right_id, &entries[1]).await?;

        self.tombstone_page(page_id).await?;

        engine_debug!("[page_store] split page {} -> {} and {}", page_id, left_id, right_id);
        Ok(())
    }

    /// Run pending page splits (triggered by write_page for oversized data).
    pub async fn run_pending_split(&self) -> Result<usize, VaultSyncError> {
        if !self.is_split_pending() {
            return Ok(0);
        }

        let page_ids = self.list_page_ids().await?;
        let mut split_count = 0usize;

        for id in &page_ids {
            if let Some(data) = self.read_page(*id).await? {
                let entry_count = Self::estimate_entry_count(&data);
                if data.len() > Self::SOFT_SPLIT_BYTES || entry_count > Self::MAX_ENTRIES_PER_PAGE {
                    if let Err(e) = self.split_page(*id).await {
                        engine_warn!("[page_store] split page {} failed: {:?}", id, e);
                    } else {
                        split_count += 1;
                    }
                }
            }
        }

        Ok(split_count)
    }

    /// Schedule a background page split for an oversized page.
    pub async fn schedule_split(&self, page_id: &PageId) {
        self.split_pending.store(true, Ordering::Release);
    }

    /// Check and clear the split-pending flag.
    pub fn is_split_pending(&self) -> bool {
        self.split_pending.swap(false, Ordering::Acquire)
    }

    /// Get pending count from manifest.
    pub async fn pending_count(&self) -> usize {
        let count = read_manifest(&self.dir)
            .await
            .map(|m| m.pending_count)
            .unwrap_or(0);
        engine_trace!("[pending] manifest pending_count={}", count);
        count
    }

    /// Set pending count in manifest atomically.
    pub async fn set_pending_count(&self, count: usize) -> Result<(), VaultSyncError> {
        let manifest = read_manifest(&self.dir).await;
        let mut m = match manifest {
            Some(m) if m.verify() => m,
            _ => StoreManifest::new(),
        };
        m.pending_count = count;
        m.updated_at = js_sys::Date::now() as u64;
        write_manifest(&self.dir, &m).await
    }

    /// Adjust pending count by a delta (positive or negative).
    pub async fn adjust_pending_count(&self, delta: i32) -> Result<(), VaultSyncError> {
        let manifest = read_manifest(&self.dir).await;
        let before = manifest.as_ref().map(|m| m.pending_count).unwrap_or(0);
        let mut m = match manifest {
            Some(m) if m.verify() => m,
            _ => StoreManifest::new(),
        };
        m.pending_count = (m.pending_count as i32).saturating_add(delta) as usize;
        m.updated_at = js_sys::Date::now() as u64;
        write_manifest(&self.dir, &m).await?;
        engine_trace!("[pending] manifest_write before={} delta={} after={}", before, delta, m.pending_count);
        Ok(())
    }

    /// Run pending GC if any — deletes pages older than the current full-state page.
    /// Returns the number of pages deleted.
    pub async fn run_pending_gc(&self) -> Result<usize, VaultSyncError> {
        if !self.gc_needed.load(Ordering::Acquire) {
            return Ok(0);
        }

        let manifest = match read_manifest(&self.dir).await {
            Some(m) if m.verify() && m.current_page > 0 => m,
            _ => return Ok(0),
        };

        let current = manifest.current_page;
        let mut deleted = 0usize;

        let iter = self.dir.entries();
        loop {
            let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
            {
                Some(f) => f,
                None => break,
            };
            let result = match next_fn.call0(&iter) {
                Ok(r) => r,
                Err(_) => break,
            };
            let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
                match SendJsFuture::from(promise.clone()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            } else {
                result
            };
            let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if done { break; }
            let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                }) {
                Some(n) => n,
                None => continue,
            };
            if let Some(id) = parse_page_id(&name) {
                if id < current {
                    if let Err(e) = delete_file(&self.dir, &name).await {
                        if !is_not_found(&e) {
                            return Err(e);
                        }
                    }
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            // Rebuild manifest to reflect deleted pages
            let rebuilt = rebuild_manifest(&self.dir).await?;
            let mut final_manifest = rebuilt;
            final_manifest.current_page = current;
            final_manifest.updated_at = js_sys::Date::now() as u64;
            write_manifest(&self.dir, &final_manifest).await?;
        }

        self.gc_needed.store(false, Ordering::Release);
        Ok(deleted)
    }
}

fn page_filename(page_id: PageId) -> String {
    format!("{}.page", page_id)
}

fn parse_page_id(name: &str) -> Option<PageId> {
    if let Some(stripped) = name.strip_suffix(".page") {
        stripped.parse::<PageId>().ok()
    } else {
        None
    }
}

fn encode_v3_page(data: &[u8], flags: u8) -> Result<Vec<u8>, VaultSyncError> {
    let checksum = crc32fast::hash(data);
    let header = PageHeader {
        version: 1,
        checksum,
        flags,
        data_len: data.len() as u32,
    };
    let header_bytes = postcard::to_allocvec(&header)
        .map_err(|e| VaultSyncError::Storage(format!("header encode: {:?}", e)))?;

    let mut buf = Vec::with_capacity(PAGE_MAGIC.len() + header_bytes.len() + data.len());
    buf.extend_from_slice(&PAGE_MAGIC);
    buf.extend_from_slice(&header_bytes);
    buf.extend_from_slice(data);
    Ok(buf)
}

fn encoded_header_len(header: &PageHeader) -> usize {
    postcard::to_allocvec(header)
        .expect("PageHeader serialization cannot fail")
        .len()
}

fn is_not_found(err: &VaultSyncError) -> bool {
    let s = format!("{:?}", err);
    s.contains("NotFoundError") || s.contains("NotFound")
}

/// Maximum bytes to read when only the header is needed.
/// Covers V1 (14 bytes), V2/V3 (~12 bytes), and future expansion.
const MAX_HEADER_SIZE: u32 = 128;

/// Read only the first `MAX_HEADER_SIZE` bytes of a page file using Blob.slice().
/// Avoids reading the entire file payload from OPFS when only metadata is needed.
async fn read_header_bytes(
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

    // Slice only the header region instead of reading the entire file.
    // Browser clamps end to file size, so this is safe for small files too.
    let sliced = blob
        .slice_with_i32_and_i32(0, MAX_HEADER_SIZE as i32)
        .map_err(|e| VaultSyncError::Storage(format!("blob.slice: {:?}", e)))?;

    let buf_val = SendJsFuture::from(sliced.array_buffer())
        .await
        .map_err(|e| VaultSyncError::Storage(format!("array_buffer: {:?}", e)))?;
    let uint8 = Uint8Array::new(&buf_val);
    let mut bytes = vec![0u8; uint8.length() as usize];
    uint8.copy_to(&mut bytes);
    Ok(bytes)
}

/// Read a page header (flags, checksum, etc.) without extracting the data payload.
/// Uses Blob.slice() to read only MAX_HEADER_SIZE bytes instead of the full file.
/// Returns None if the page doesn't exist.
async fn read_page_header(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<PageHeader>, VaultSyncError> {
    let bytes = match read_header_bytes(dir, name).await {
        Ok(b) => b,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };

    if bytes.is_empty() {
        return Err(VaultSyncError::Storage(format!("empty page {}", name)));
    }

    let header = decode_page_header(&bytes)?;
    Ok(Some(header))
}

/// Decode a page header from raw file bytes, detecting V3/V2/V1 format.
fn decode_page_header(bytes: &[u8]) -> Result<PageHeader, VaultSyncError> {
    // V3: magic + postcard header
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage(format!(
                "unknown V3 version {}", header.version
            )));
        }
        return Ok(header);
    }

    // V2: postcard from byte 0 (brief period after initial fix)
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            return Ok(header);
        }
    }

    // V1: 14-byte fixed header (bug-era legacy)
    if bytes.len() >= 14 {
        let flags = u16::from_le_bytes(
            bytes[8..10].try_into().unwrap_or([0, 0]),
        ) as u8;
        let data_len = u32::from_le_bytes(
            bytes[10..14].try_into().unwrap_or([0, 0, 0, 0]),
        );
        return Ok(PageHeader {
            version: 0,
            checksum: 0,
            flags,
            data_len,
        });
    }

    Err(VaultSyncError::Storage(format!(
        "unrecognized page format ({} bytes)",
        bytes.len()
    )))
}

/// Extract the data payload from raw page bytes (any format).
/// Used by tombstone_page to re-encode pages in V3 format.
fn extract_page_data(bytes: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
    if bytes.is_empty() {
        return Err(VaultSyncError::Storage("empty page".into()));
    }

    // V3: magic + postcard header + data
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage("unknown V3 version".into()));
        }
        let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
        if header_end > bytes.len() {
            return Err(VaultSyncError::Storage("truncated V3 page".into()));
        }
        let data_end = header_end + (header.data_len as usize).min(bytes.len() - header_end);
        return Ok(bytes[header_end..data_end].to_vec());
    }

    // V2: postcard from byte 0
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            let header_end = encoded_header_len(&header);
            if header_end > bytes.len() {
                return Err(VaultSyncError::Storage("truncated V2 page".into()));
            }
            let data_end = header_end + (header.data_len as usize).min(bytes.len() - header_end);
            return Ok(bytes[header_end..data_end].to_vec());
        }
    }

    // V1: 14-byte fixed header
    if bytes.len() >= 14 {
        let data_len = u32::from_le_bytes(
            bytes[10..14].try_into().unwrap_or([0, 0, 0, 0]),
        ) as usize;
        let start = 14usize;
        let end = start + data_len.min(bytes.len().saturating_sub(start));
        return Ok(bytes[start..end].to_vec());
    }

    Err(VaultSyncError::Storage("unrecognized page format".into()))
}

/// Read a page file and return (header, data).
/// Supports V3, V2, and V1 formats.
async fn read_page_file_raw(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    let bytes = match read_all_bytes(dir, name).await {
        Ok(b) => b,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };

    if bytes.is_empty() {
        return Err(VaultSyncError::Storage(format!("empty page {}", name)));
    }

    // V3: magic + postcard header + data
    if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
        let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
            .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
        if header.version != 1 {
            return Err(VaultSyncError::Storage(format!(
                "unknown V3 version {}", header.version
            )));
        }
        let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
        if header_end > bytes.len() {
            return Err(VaultSyncError::Storage(format!(
                "truncated V3 page {}", name
            )));
        }
        let data = bytes[header_end..].to_vec();
        return Ok(Some((header, data)));
    }

    // V2: postcard from byte 0
    if let Ok(header) = postcard::from_bytes::<PageHeader>(&bytes) {
        if header.version == 1 {
            let header_end = encoded_header_len(&header);
            if header_end > bytes.len() {
                return Err(VaultSyncError::Storage(format!(
                    "truncated V2 page {}", name
                )));
            }
            let data = bytes[header_end..].to_vec();
            return Ok(Some((header, data)));
        }
    }

    // V1: 14-byte fixed header
    if bytes.len() >= 14 {
        let flags = u16::from_le_bytes(bytes[8..10].try_into().unwrap_or([0, 0])) as u8;
        let data_len = u32::from_le_bytes(bytes[10..14].try_into().unwrap_or([0, 0, 0, 0])) as usize;
        let start = 14usize;
        let end = start + data_len.min(bytes.len().saturating_sub(start));
        let data = bytes[start..end].to_vec();
        return Ok(Some((
            PageHeader {
                version: 0,
                checksum: 0,
                flags,
                data_len: data.len() as u32,
            },
            data,
        )));
    }

    Err(VaultSyncError::Storage(format!(
        "corrupt page {}: unrecognized format",
        name
    )))
}

/// Read a page file and validate data_len matches extracted data.
async fn read_page_file(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<(PageHeader, Vec<u8>)>, VaultSyncError> {
    match read_page_file_raw(dir, name).await? {
        Some((header, data)) => {
            if header.version >= 1 && data.len() as u32 != header.data_len {
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

async fn read_manifest(dir: &FileSystemDirectoryHandle) -> Option<StoreManifest> {
    read_all_bytes(dir, "_manifest").await.ok().and_then(|bytes| {
        let m: StoreManifest = postcard::from_bytes(&bytes).ok()?;
        if m.verify() { Some(m) } else { None }
    })
}

async fn write_manifest(dir: &FileSystemDirectoryHandle, m: &StoreManifest) -> Result<(), VaultSyncError> {
    let mut copy = m.clone();
    copy.checksum = 0;
    copy.checksum = copy.compute_checksum();
    let bytes = postcard::to_allocvec(&copy)
        .map_err(|e| VaultSyncError::Storage(format!("manifest encode: {:?}", e)))?;
    write_file(dir, "_manifest", &bytes).await
}

/// Scan all pages to rebuild the manifest.
async fn rebuild_manifest(dir: &FileSystemDirectoryHandle) -> Result<StoreManifest, VaultSyncError> {
    let now = js_sys::Date::now() as u64;
    let mut highest_page_id: PageId = 0;
    let mut live_pages = 0usize;
    let mut tombstoned_pages = 0usize;

    let iter = dir.entries();
    loop {
        let next_fn = match js_sys::Reflect::get(&iter, &JsValue::from_str("next"))
            .ok()
            .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
        {
            Some(f) => f,
            None => break,
        };
        let result = match next_fn.call0(&iter) {
            Ok(r) => r,
            Err(_) => break,
        };
        let entry = if let Some(promise) = result.dyn_ref::<js_sys::Promise>() {
            match SendJsFuture::from(promise.clone()).await {
                Ok(v) => v,
                Err(_) => continue,
            }
        } else {
            result
        };
        let done = js_sys::Reflect::get(&entry, &JsValue::from_str("done"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if done { break; }
        let name = match js_sys::Reflect::get(&entry, &JsValue::from_str("value"))
            .ok()
            .and_then(|v| {
                v.as_string().or_else(|| {
                    js_sys::Reflect::get(&v, &JsValue::from_f64(0.0))
                        .ok()
                        .and_then(|n| n.as_string())
                })
            }) {
            Some(n) => n,
            None => continue,
        };
        if let Some(id) = parse_page_id(&name) {
            if id > highest_page_id {
                highest_page_id = id;
            }
            if let Some(header) = read_page_header(dir, &name).await? {
                if header.flags & 0x02 != 0 {
                    tombstoned_pages += 1;
                } else {
                    live_pages += 1;
                }
            }
        }
    }

    let mut m = StoreManifest {
        version: 1,
        highest_page_id,
        current_page: 0,
        last_sequence: 0,
        pending_count: 0,
        live_pages,
        tombstoned_pages,
        created_at: now,
        updated_at: now,
        checksum: 0,
    };
    m.checksum = m.compute_checksum();
    Ok(m)
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    fn roundtrip_v3(payload: &[u8]) {
        let encoded = encode_v3_page(payload, 0).unwrap();
        let decoded_header = decode_page_header(&encoded).unwrap();
        assert_eq!(decoded_header.version, 1);
        assert_eq!(decoded_header.flags, 0);
        assert_eq!(decoded_header.data_len, payload.len() as u32);

        let decoded_data = extract_page_data(&encoded).unwrap();
        assert_eq!(decoded_data, payload);

        let (_, raw_data) = decode_v3_raw(&encoded).unwrap();
        assert_eq!(raw_data, payload);
    }

    /// Test helper: inline version of read_page_file_raw for pre-encoded bytes.
    /// Test helper: decode V3 page from raw bytes.
    fn decode_v3_raw(bytes: &[u8]) -> Result<(PageHeader, Vec<u8>), VaultSyncError> {
        if bytes.len() >= 2 && bytes[0] == PAGE_MAGIC[0] && bytes[1] == PAGE_MAGIC[1] {
            let header: PageHeader = postcard::from_bytes(&bytes[PAGE_MAGIC.len()..])
                .map_err(|e| VaultSyncError::Storage(format!("V3 decode: {:?}", e)))?;
            let header_end = PAGE_MAGIC.len() + encoded_header_len(&header);
            Ok((header, bytes[header_end..].to_vec()))
        } else {
            Err(VaultSyncError::Storage("not V3".into()))
        }
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_empty() {
        roundtrip_v3(&[]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_small() {
        roundtrip_v3(b"hello world");
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_45() {
        roundtrip_v3(&vec![0xAB; 45]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_128() {
        roundtrip_v3(&vec![0xCD; 128]);
    }

    #[wasm_bindgen_test]
    fn v3_roundtrip_16384() {
        roundtrip_v3(&vec![0xEF; 16384]);
    }

    #[wasm_bindgen_test]
    fn v3_checksum_validation() {
        let payload = b"test data";
        let mut encoded = encode_v3_page(payload, 0).unwrap();
        // Corrupt payload
        let data_start = encoded.len() - payload.len();
        encoded[data_start] ^= 0xFF;
        // header still decodes (postcard from start)
        let header = decode_page_header(&encoded).unwrap();
        assert_eq!(header.version, 1);
        let extracted = extract_page_data(&encoded).unwrap();
        // corrupt payload returns corrupt data
        assert_ne!(extracted, payload);
        // checksum mismatch: crc32 of corrupt data != stored checksum
        let actual = crc32fast::hash(&extracted);
        assert_ne!(actual, header.checksum);
    }

    #[wasm_bindgen_test]
    fn v3_tombstone_flag() {
        let payload = b"test data";
        let encoded = encode_v3_page(payload, 0x02).unwrap();
        let header = decode_page_header(&encoded).unwrap();
        assert!(header.flags & 0x02 != 0);
    }

    #[wasm_bindgen_test]
    fn encode_v3_consistency() {
        for len in [0, 1, 10, 100, 1000] {
            let data: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
            let encoded = encode_v3_page(&data, 0).unwrap();
            let decoded = extract_page_data(&encoded).unwrap();
            assert_eq!(decoded, data, "encode/decode mismatch at len={}", len);
        }
    }
}
