use std::marker::PhantomData;

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
    /// The old page is tombstoned to keep exactly one live page.
    pub async fn write(&self, value: &T) -> Result<(), VaultSyncError> {
        let old_page = self.store.current_page().await;

        let encoded = postcard::to_allocvec(value)
            .map_err(|e| VaultSyncError::Storage(format!("metadata encode: {:?}", e)))?;

        let new_page = self.store.allocate_page_id("metadata_write").await?;
        self.store.write_page(new_page, &encoded).await?;
        self.store.set_current_page(new_page).await?;

        // Tombstone old page after new one is committed
        if old_page > 0 && old_page != new_page {
            let _ = self.store.tombstone_page(old_page).await;
        }

        Ok(())
    }

    /// Read the current value.
    ///
    /// Only reads `current_page` from the manifest — no OPFS scan of old pages.
    pub async fn read(&self) -> Result<Option<T>, VaultSyncError> {
        let current = self.store.current_page().await;
        if current == 0 {
            return Ok(None);
        }

        match self.store.read_page(current).await {
            Ok(Some(data)) => match postcard::from_bytes::<T>(&data) {
                Ok(val) => Ok(Some(val)),
                Err(e) => {
                    engine_warn!(
                        "[MetadataStore] decode error on page {}: {:?}",
                        current,
                        e
                    );
                    Ok(None)
                }
            },
            Ok(None) => Ok(None),
            Err(e) => {
                engine_warn!(
                    "[MetadataStore] read error on page {}: {:?}",
                    current,
                    e
                );
                Ok(None)
            }
        }
    }

    /// Delete the current value by tombstoning the current page.
    pub async fn delete(&self) -> Result<(), VaultSyncError> {
        let current = self.store.current_page().await;
        if current > 0 {
            self.store.tombstone_page(current).await?;
            self.store.set_current_page(0).await?;
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
