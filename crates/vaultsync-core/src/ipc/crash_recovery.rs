use crate::crdt::document::CRDTDocument;
use crate::error::VaultSyncError;
use crate::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use crate::storage::traits::Storage;
use std::sync::Arc;

pub struct CrashRecovery {
    storage: Arc<dyn Storage>,
    namespace: String,
}

impl CrashRecovery {
    pub fn new(storage: Arc<dyn Storage>, namespace: String) -> Self {
        Self { storage, namespace }
    }

    pub async fn recover(
        &self,
        keyring: Arc<crate::e2ee::keyring::KeyRing>,
    ) -> Result<usize, VaultSyncError> {
        // 1. Reset any stale non-synced entries older than 60 seconds back to Pending.
        let mut recovered_count = self
            .storage
            .reset_stale_pending(&self.namespace, 60_000)
            .await?;

        // 2. Open shared memory ring buffer and read uncommitted entries
        if let Ok(shm) = crate::ipc::shared_memory::SharedMemory::open(&self.namespace) {
            if let Ok(uncommitted_entries) = shm.read_uncommitted() {
                let encryptor = crate::e2ee::keyring::E2eeEncryptor::new(keyring);
                for entry in uncommitted_entries {
                    if let Ok(doc) = CRDTDocument::from_snapshot(&entry.yrs_update) {
                        let doc_id = doc.doc_id.clone();
                        let record_id = doc.record_id.clone();

                        let encrypted_blob =
                            encryptor.encrypt_symmetric(&entry.yrs_update, &self.namespace)?;
                        let epoch = entry.timestamp;
                        let oplog_entry = OplogEntry {
                            id: entry.id.clone(),
                            replica_id: "recovered-replica".to_string(),
                            namespace: self.namespace.clone(),
                            mutation_type: MutationType::CrdtUpdate,
                            doc_id: doc_id.clone(),
                            record_id: record_id.clone(),
                            yrs_update: entry.yrs_update.clone(),
                            encrypted_blob: Some(encrypted_blob),
                            timestamp: epoch,
                            sequence: None,
                            sync_status: SyncStatus::Pending,
                            synced_at: None,
                            created_at: epoch,
                        };

                        match self
                            .storage
                            .write_document_and_oplog(
                                &doc_id,
                                &record_id,
                                &entry.yrs_update,
                                &oplog_entry,
                            )
                            .await
                        {
                            Ok(_) => {
                                recovered_count += 1;
                            }
                            Err(_) => {
                                // Ignore duplicate key errors or constraint errors
                            }
                        }
                    }
                }
            }
        }

        Ok(recovered_count)
    }
}
