use std::marker::PhantomData;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use vaultsync_core::VaultSyncError;

use crate::page_store::PageStore;

/// MetadataStore<T> — generic KV store over a single PageStore.
///
/// Unlike the append-only pattern used by legacy sync_states/schemas/keys/migrations,
/// this store keeps exactly **one live page** by:
///   1. Reading `current_page` from the manifest before writing
///   2. Tombstoning the old current_page (if any)
///   3. Allocating a new page
///   4. Writing the data
///   5. Setting `current_page` to the new page ID
///
/// `read()` reads only the current_page — no OPFS directory scan.
#[derive(Debug, Clone)]
pub struct MetadataStore<T: Serialize + DeserializeOwned> {
    store: PageStore,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> MetadataStore<T> {
    pub fn new(store: PageStore) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    /// Write a value, replacing any previous page for this key.
    ///
    /// Uses a single atomic manifest operation (allocate + set_current + tombstone index)
    /// instead of N separate manifest read-modify-write cycles. This eliminates the
    /// WASM task interleaving window where concurrent operations can corrupt manifest state.
    pub async fn write(&self, value: &T) -> Result<(), VaultSyncError> {
        let encoded = postcard::to_allocvec(value)
            .map_err(|e| VaultSyncError::Storage(format!("metadata encode: {:?}", e)))?;

        let (new_page, old_page) = self.store.allocate_and_rotate("metadata_write").await?;

        // Write page data to OPFS (no manifest update — already in ContentIndex)
        self.store.write_page_raw_data(new_page, &encoded).await?;

        // Best-effort tombstone old page file in OPFS (ContentIndex already updated)
        if old_page > 0 && old_page != new_page {
            let _ = self.store.tombstone_page_file_only(old_page).await;
        }

        Ok(())
    }

    /// Read the current value.
    ///
    /// Only reads `current_page` from the manifest — no OPFS scan of old pages.
    /// Structured logging helps diagnose why promotion sometimes sees sync_state=false.
    pub async fn read(&self) -> Result<Option<T>, VaultSyncError> {
        let current = self.store.current_page().await;
        let store_name = self.store.store_name().to_string();
        let this_addr = self as *const _ as u64;
        let store_addr = &self.store as *const _ as u64;
        let manifest_arc_addr = Arc::as_ptr(&self.store.manifest) as u64;
        if current == 0 {
            engine_info!(
                "[MetadataStore::read] store={} MetadataStore=0x{:x} PageStore=0x{:x} manifest_arc=0x{:x} current_page=0 — returning None (no persisted data)",
                store_name, this_addr, store_addr, manifest_arc_addr,
            );

            // Debug-only fallback: scan OPFS for any .page files as diagnostic
            #[cfg(debug_assertions)]
            {
                let fallback = self.debug_fallback_scan(&store_name).await;
                if fallback.is_some() {
                    engine_warn!(
                        "[MetadataStore::read] store={} current_page=0 BUT debug fallback found data — manifest pointer is stale or lost",
                        store_name,
                    );
                }
            }

            return Ok(None);
        }

        match self.store.read_page(current).await {
            Ok(Some(data)) => {
                let data_len = data.len();
                match postcard::from_bytes::<T>(&data) {
                    Ok(val) => {
                        engine_info!(
                            "[MetadataStore::read] store={} MetadataStore=0x{:x} PageStore=0x{:x} current_page={} bytes={} — decoded OK",
                            store_name, this_addr, store_addr, current, data_len,
                        );
                        Ok(Some(val))
                    }
                    Err(e) => {
                        engine_warn!(
                            "[MetadataStore::read] store={} MetadataStore=0x{:x} PageStore=0x{:x} current_page={} bytes={} — decode FAILED: {:?}",
                            store_name, this_addr, store_addr, current, data_len, e,
                        );
                        Ok(None)
                    }
                }
            }
            Ok(None) => {
                engine_warn!(
                    "[MetadataStore::read] store={} MetadataStore=0x{:x} PageStore=0x{:x} current_page={} — page file exists but read returned None (tombstoned?)",
                    store_name, this_addr, store_addr, current,
                );
                Ok(None)
            }
            Err(e) => {
                engine_warn!(
                    "[MetadataStore::read] store={} MetadataStore=0x{:x} PageStore=0x{:x} current_page={} — read ERROR: {:?}",
                    store_name, this_addr, store_addr, current, e,
                );
                Ok(None)
            }
        }
    }

    /// Debug-only fallback: scan OPFS directory for .page files and try the highest one.
    /// This helps diagnose why current_page=0 despite data existing on disk.
    /// NEVER enabled in release builds — it masks the real bug.
    #[cfg(debug_assertions)]
    async fn debug_fallback_scan(&self, store_name: &str) -> Option<T> {
        let dir = self.store.dir();
        use wasm_bindgen::JsCast;
        use wasm_bindgen::JsValue;
        use js_sys::Reflect;
        use vaultsync_core::time_utils::SendJsFuture;

        let iter = dir.entries();

        let mut page_ids: Vec<u64> = Vec::new();
        loop {
            let next_fn = match Reflect::get(&iter, &JsValue::from_str("next"))
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
            let done = Reflect::get(&entry, &JsValue::from_str("done"))
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if done { break; }
            let name = Reflect::get(&entry, &JsValue::from_str("value"))
                .ok()
                .and_then(|v| {
                    v.as_string().or_else(|| {
                        Reflect::get(&v, &JsValue::from_f64(0.0))
                            .ok()
                            .and_then(|n| n.as_string())
                    })
                });
            if let Some(n) = name {
                if let Some(id) = n.strip_suffix(".page").and_then(|s| s.parse::<u64>().ok()) {
                    if id > 0 {
                        page_ids.push(id);
                    }
                }
            }
        }

        if page_ids.is_empty() {
            engine_warn!(
                "[MetadataStore::debug_fallback_scan] store={} — no .page files found",
                store_name,
            );
            return None;
        }

        page_ids.sort_unstable();
        let best = page_ids[page_ids.len() - 1];
        engine_warn!(
            "[MetadataStore::debug_fallback_scan] store={} — found {} .page files, trying highest page_id={}",
            store_name, page_ids.len(), best,
        );

        match self.store.read_page(best).await {
            Ok(Some(data)) => match postcard::from_bytes::<T>(&data) {
                Ok(val) => {
                    engine_warn!(
                        "[MetadataStore::debug_fallback_scan] store={} page_id={} bytes={} — DECODED OK (current_page was 0!)",
                        store_name, best, data.len(),
                    );
                    Some(val)
                }
                Err(e) => {
                    engine_warn!(
                        "[MetadataStore::debug_fallback_scan] store={} page_id={} bytes={} — decode FAILED: {:?}",
                        store_name, best, data.len(), e,
                    );
                    None
                }
            },
            Ok(None) => {
                engine_warn!(
                    "[MetadataStore::debug_fallback_scan] store={} page_id={} — read returned None",
                    store_name, best,
                );
                None
            }
            Err(e) => {
                engine_warn!(
                    "[MetadataStore::debug_fallback_scan] store={} page_id={} — read ERROR: {:?}",
                    store_name, best, e,
                );
                None
            }
        }
    }

    /// Delete the current value by tombstoning the current page.
    /// Uses a single atomic manifest operation instead of two separate writes.
    pub async fn delete(&self) -> Result<(), VaultSyncError> {
        let current = self.store.current_page().await;
        if current > 0 {
            // Mark the page as tombstoned in ContentIndex via manifest (atomic)
            {
                let cloned = {
                    let mut guard = self.store.manifest.lock().unwrap();
                    if let Some(ref mut m) = *guard {
                        m.current_page = 0;
                        m.content_index.remove_by_page_id(current);
                        m.live_pages = m.content_index.live_pages.len();
                        m.tombstoned_pages = m.content_index.tombstoned_pages.len();
                        m.updated_at = js_sys::Date::now() as u64;
                        Some(m.clone())
                    } else {
                        None
                    }
                };
                if let Some(ref m) = cloned {
                    crate::page_store::write_manifest(self.store.dir(), self.store.store_name(), "MetadataStore::delete", m).await?;
                }
            }
            // Best-effort OPFS tombstone (ContentIndex already updated)
            let _ = self.store.tombstone_page_file_only(current).await;
        }
        Ok(())
    }

    /// Return the underlying PageStore reference.
    pub fn store(&self) -> &PageStore {
        &self.store
    }

    /// Return the current page ID (0 if none).
    pub async fn current_page(&self) -> u64 {
        self.store.current_page().await
    }
}
