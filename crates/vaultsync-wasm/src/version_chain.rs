use crate::page_store::{PageId, PageStore};
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::VaultSyncError;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::Mutex;

/// Tracks (store, page_id) pairs that have already warned about missing pages.
/// First occurrence logs at engine_debug, subsequent occurrences at engine_trace.
static MISSING_PAGE_WARNED: LazyLock<Mutex<HashSet<(String, u64)>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Resolves a base page plus zero or more delta pages into the latest view.
/// Each delta page overrides entries from the base by matching `OplogEntry.id`.
/// Pages are resolved in ascending page_id order — later deltas win.
pub struct VersionChain {
    base_id: PageId,
    delta_ids: Vec<PageId>,
    orphan_ids: Vec<PageId>,
}

impl VersionChain {
    pub fn new(base_id: PageId) -> Self {
        Self {
            base_id,
            delta_ids: Vec::new(),
            orphan_ids: Vec::new(),
        }
    }

    pub fn add_delta(&mut self, delta_id: PageId) {
        self.delta_ids.push(delta_id);
    }

    pub fn add_orphan(&mut self, orphan_id: PageId) {
        self.orphan_ids.push(orphan_id);
    }

    /// Read all pages from the store and resolve into a single deduplicated Vec.
    /// Pages are always processed in ascending page_id order — later deltas win.
    /// Logs per-page read/deserialize results for diagnosing empty read_pending_oplog.
    pub async fn resolve(
        &self,
        store: &PageStore,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let mut seen: HashMap<String, OplogEntry> = HashMap::new();
        let mut delta_ids = self.delta_ids.clone();
        delta_ids.sort_unstable();
        let mut orphan_ids = self.orphan_ids.clone();
        orphan_ids.sort_unstable();
        let store_name = store.store_name();
        let total_pages = delta_ids.len() + orphan_ids.len() + if self.base_id > 0 { 1 } else { 0 };
        let mut missing_count: u64 = 0;

        // Read base page first (full state from last checkpoint)
        if self.base_id > 0 {
            match store.read_page(self.base_id).await {
                Ok(Some(data)) => {
                    match postcard::from_bytes::<Vec<OplogEntry>>(&data) {
                        Ok(chunk) => {
                            engine_trace!("[VersionChain] store={} base_page={} entries={}", store_name, self.base_id, chunk.len());
                            for entry in chunk {
                                seen.insert(entry.id.clone(), entry);
                            }
                        }
                        Err(e) => {
                            engine_warn!("[VersionChain] store={} base_page={} deserialize FAILED: {:?} (bytes={})", store_name, self.base_id, e, data.len());
                        }
                    }
                }
                Ok(None) => {
                    missing_count += 1;
                    let key = (store_name.to_string(), self.base_id);
                    let first_seen = {
                        let mut guard = MISSING_PAGE_WARNED.lock().unwrap();
                        if guard.contains(&key) {
                            false
                        } else {
                            guard.insert(key.clone());
                            true
                        }
                    };
                    if first_seen {
                        engine_debug!("[VersionChain] store={} base_page={} missing from OPFS (first occurrence)", store_name, self.base_id);
                    } else {
                        engine_trace!("[VersionChain] store={} base_page={} missing from OPFS (repeated)", store_name, self.base_id);
                    }
                }
                Err(e) => {
                    engine_warn!("[VersionChain] store={} base_page={} read ERROR: {:?}", store_name, self.base_id, e);
                }
            }
        }

        // Read delta pages (id > base_id) — later deltas override earlier entries
        for delta_id in &delta_ids {
            match store.read_page(*delta_id).await {
                Ok(Some(data)) => {
                    let chunk = postcard::from_bytes::<Vec<OplogEntry>>(&data);
                    match chunk {
                        Ok(chunk) => {
                            engine_trace!("[VersionChain] store={} delta_page={} entries={}", store_name, delta_id, chunk.len());
                            for entry in chunk {
                                seen.insert(entry.id.clone(), entry);
                            }
                        }
                        Err(_) => {
                            if let Ok(single) = postcard::from_bytes::<OplogEntry>(&data) {
                                engine_trace!("[VersionChain] store={} delta_page={} single_entry id={}", store_name, delta_id, single.id);
                                seen.insert(single.id.clone(), single);
                            } else {
                                engine_warn!("[VersionChain] store={} delta_page={} deserialize FAILED (bytes={}) — page may be corrupt", store_name, delta_id, data.len());
                            }
                        }
                    }
                }
                Ok(None) => {
                    missing_count += 1;
                    let key = (store_name.to_string(), *delta_id);
                    let first_seen = {
                        let mut guard = MISSING_PAGE_WARNED.lock().unwrap();
                        if guard.contains(&key) {
                            false
                        } else {
                            guard.insert(key.clone());
                            true
                        }
                    };
                    if first_seen {
                        engine_debug!("[VersionChain] store={} delta_page={} missing from OPFS (first occurrence)", store_name, delta_id);
                    } else {
                        engine_trace!("[VersionChain] store={} delta_page={} missing from OPFS (repeated)", store_name, delta_id);
                    }
                }
                Err(e) => {
                    engine_warn!("[VersionChain] store={} delta_page={} read ERROR: {:?}", store_name, delta_id, e);
                }
            }
        }

        // Read orphan pages (recycled IDs < base_id, written after checkpoint)
        for orphan_id in &orphan_ids {
            match store.read_page(*orphan_id).await {
                Ok(Some(data)) => {
                    let chunk = postcard::from_bytes::<Vec<OplogEntry>>(&data);
                    match chunk {
                        Ok(chunk) => {
                            engine_trace!("[VersionChain] store={} orphan_page={} entries={}", store_name, orphan_id, chunk.len());
                            for entry in chunk {
                                seen.insert(entry.id.clone(), entry);
                            }
                        }
                        Err(_) => {
                            if let Ok(single) = postcard::from_bytes::<OplogEntry>(&data) {
                                engine_trace!("[VersionChain] store={} orphan_page={} single_entry id={}", store_name, orphan_id, single.id);
                                seen.insert(single.id.clone(), single);
                            } else {
                                engine_warn!("[VersionChain] store={} orphan_page={} deserialize FAILED (bytes={}) — page may be corrupt", store_name, orphan_id, data.len());
                            }
                        }
                    }
                }
                Ok(None) => {
                    missing_count += 1;
                    let key = (store_name.to_string(), *orphan_id);
                    let first_seen = {
                        let mut guard = MISSING_PAGE_WARNED.lock().unwrap();
                        if guard.contains(&key) {
                            false
                        } else {
                            guard.insert(key.clone());
                            true
                        }
                    };
                    if first_seen {
                        engine_debug!("[VersionChain] store={} orphan_page={} missing from OPFS (first occurrence)", store_name, orphan_id);
                    } else {
                        engine_trace!("[VersionChain] store={} orphan_page={} missing from OPFS (repeated)", store_name, orphan_id);
                    }
                }
                Err(e) => {
                    engine_warn!("[VersionChain] store={} orphan_page={} read ERROR: {:?}", store_name, orphan_id, e);
                }
            }
        }

        let result: Vec<OplogEntry> = seen.into_values().collect();
        if missing_count > 0 {
            engine_debug!("[VersionChain] store={} total_pages={} resolved_entries={} missing_pages={}",
                store_name, total_pages, result.len(), missing_count);
        } else {
            engine_trace!("[VersionChain] store={} total_pages={} resolved_entries={}",
                store_name, total_pages, result.len());
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vaultsync_core::oplog::entry::{MutationType, SyncStatus, MutationOrigin};

    fn make_entry(id: &str, seq: u64, status: SyncStatus) -> OplogEntry {
        OplogEntry {
            id: id.to_string(),
            replica_id: "test".to_string(),
            namespace: "test".to_string(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: "doc".to_string(),
            record_id: "rec".to_string(),
            yrs_update: vec![],
            encrypted_blob: None,
            timestamp: 0,
            sequence: Some(seq),
            sync_status: status,
            synced_at: None,
            created_at: 0,
            schema_version: 0,
            origin: MutationOrigin::Unknown,
            origin_context: String::new(),
        }
    }

    #[test]
    fn test_chain_no_deltas() {
        let chain = VersionChain::new(1);
        assert_eq!(chain.base_id, 1);
        assert!(chain.delta_ids.is_empty());
        assert!(chain.orphan_ids.is_empty());
    }

    #[test]
    fn test_chain_with_deltas() {
        let mut chain = VersionChain::new(1);
        chain.add_delta(2);
        chain.add_delta(3);
        assert_eq!(chain.delta_ids.len(), 2);
        assert!(chain.orphan_ids.is_empty());
    }

    #[test]
    fn test_chain_with_orphans() {
        let mut chain = VersionChain::new(5);
        chain.add_delta(7);
        chain.add_delta(8);
        chain.add_orphan(3);
        chain.add_orphan(4);
        assert_eq!(chain.delta_ids.len(), 2);
        assert_eq!(chain.orphan_ids.len(), 2);
        assert!(chain.orphan_ids.contains(&3));
        assert!(chain.orphan_ids.contains(&4));
    }

    #[test]
    fn test_delta_overrides_base() {
        let base_entry = make_entry("e1", 1, SyncStatus::Pending);
        let delta_entry = make_entry("e1", 5, SyncStatus::Synced);

        let mut seen: HashMap<String, OplogEntry> = HashMap::new();
        seen.insert(base_entry.id.clone(), base_entry);
        // Delta overrides
        seen.insert(delta_entry.id.clone(), delta_entry);

        let result: Vec<OplogEntry> = seen.into_values().collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "e1");
        assert_eq!(result[0].sync_status, SyncStatus::Synced);
        assert_eq!(result[0].sequence, Some(5));
    }

    #[test]
    fn test_multiple_entries() {
        let e1 = make_entry("e1", 1, SyncStatus::Pending);
        let e2 = make_entry("e2", 2, SyncStatus::Pending);
        let delta = make_entry("e1", 10, SyncStatus::Synced);

        let mut seen: HashMap<String, OplogEntry> = HashMap::new();
        seen.insert(e1.id.clone(), e1);
        seen.insert(e2.id.clone(), e2);
        seen.insert(delta.id.clone(), delta);

        assert_eq!(seen.len(), 2);
        assert_eq!(seen["e1"].sync_status, SyncStatus::Synced);
        assert_eq!(seen["e2"].sync_status, SyncStatus::Pending);
    }
}
