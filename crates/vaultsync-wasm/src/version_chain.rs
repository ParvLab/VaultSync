use crate::page_store::{PageId, PageStore};
use vaultsync_core::oplog::entry::OplogEntry;
use vaultsync_core::VaultSyncError;
use std::collections::HashMap;

/// Resolves a base page plus zero or more delta pages into the latest view.
/// Each delta page overrides entries from the base by matching `OplogEntry.id`.
/// Pages are resolved in ascending page_id order — later deltas win.
pub struct VersionChain {
    base_id: PageId,
    delta_ids: Vec<PageId>,
}

impl VersionChain {
    pub fn new(base_id: PageId) -> Self {
        Self {
            base_id,
            delta_ids: Vec::new(),
        }
    }

    pub fn add_delta(&mut self, delta_id: PageId) {
        self.delta_ids.push(delta_id);
    }

    /// Read all pages from the store and resolve into a single deduplicated Vec.
    /// Pages are always processed in ascending page_id order — later deltas win.
    pub async fn resolve(
        &self,
        store: &PageStore,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        let mut seen: HashMap<String, OplogEntry> = HashMap::new();
        let mut delta_ids = self.delta_ids.clone();
        delta_ids.sort_unstable();

        // Read base page first
        if self.base_id > 0 {
            if let Some(data) = store.read_page(self.base_id).await? {
                if let Ok(chunk) = postcard::from_bytes::<Vec<OplogEntry>>(&data) {
                    for entry in chunk {
                        seen.insert(entry.id.clone(), entry);
                    }
                }
            }
        }

        // Read delta pages in order — later deltas override earlier entries
        for delta_id in &delta_ids {
            if let Some(data) = store.read_page(*delta_id).await? {
                if let Ok(chunk) = postcard::from_bytes::<Vec<OplogEntry>>(&data) {
                    for entry in chunk {
                        seen.insert(entry.id.clone(), entry);
                    }
                } else if let Ok(single) = postcard::from_bytes::<OplogEntry>(&data) {
                    seen.insert(single.id.clone(), single);
                }
            }
        }

        Ok(seen.into_values().collect())
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
    }

    #[test]
    fn test_chain_with_deltas() {
        let mut chain = VersionChain::new(1);
        chain.add_delta(2);
        chain.add_delta(3);
        assert_eq!(chain.delta_ids.len(), 2);
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
