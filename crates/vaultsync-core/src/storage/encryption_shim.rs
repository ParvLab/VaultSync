use super::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use crate::error::VaultSyncError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use aead::{Aead, KeyInit};
use async_trait::async_trait;
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

const ENC_NONCE: &[u8; 12] = b"vaultsync_00";

#[derive(Debug)]
pub struct EncryptedStorage {
    inner: Box<dyn Storage>,
    device_key: [u8; 32],
}

impl EncryptedStorage {
    pub fn new(inner: Box<dyn Storage>, device_key: [u8; 32]) -> Self {
        Self { inner, device_key }
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.device_key));
        let nonce = Nonce::from_slice(ENC_NONCE);
        cipher
            .encrypt(nonce, data)
            .map_err(|e| VaultSyncError::Encryption(format!("storage encrypt failed: {e}")))
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.device_key));
        let nonce = Nonce::from_slice(ENC_NONCE);
        cipher
            .decrypt(nonce, data)
            .map_err(|e| VaultSyncError::Encryption(format!("storage decrypt failed: {e}")))
    }
}

#[async_trait]
impl Storage for EncryptedStorage {
    async fn insert_document(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
    ) -> Result<(), VaultSyncError> {
        let encrypted = self.encrypt(bytes)?;
        self.inner
            .insert_document(doc_id, record_id, &encrypted)
            .await
    }

    async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Option<Vec<u8>>, VaultSyncError> {
        match self.inner.get_document(doc_id, record_id).await? {
            Some(data) => Ok(Some(self.decrypt(&data)?)),
            None => Ok(None),
        }
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), VaultSyncError> {
        self.inner.delete_document(doc_id, record_id).await
    }

    async fn write_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        bytes: &[u8],
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        let encrypted = self.encrypt(bytes)?;
        self.inner
            .write_document_and_oplog(doc_id, record_id, &encrypted, entry)
            .await
    }

    async fn delete_document_and_oplog(
        &self,
        doc_id: &str,
        record_id: &str,
        entry: &OplogEntry,
    ) -> Result<(), VaultSyncError> {
        self.inner
            .delete_document_and_oplog(doc_id, record_id, entry)
            .await
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, VaultSyncError> {
        let docs = self.inner.list_documents(doc_id).await?;
        docs.into_iter()
            .map(|(rid, data)| Ok((rid, self.decrypt(&data)?)))
            .collect()
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), VaultSyncError> {
        self.inner.append_oplog(entry).await
    }

    async fn read_pending_oplog(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        self.inner.read_pending_oplog(namespace, limit).await
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), VaultSyncError> {
        self.inner.mark_synced(id, sequence).await
    }

    async fn mark_failed(&self, id: &str, error_msg: &str) -> Result<(), VaultSyncError> {
        self.inner.mark_failed(id, error_msg).await
    }

    async fn read_oplog_after_sequence(
        &self,
        namespace: &str,
        seq: u64,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        self.inner.read_oplog_after_sequence(namespace, seq).await
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, VaultSyncError> {
        self.inner.read_sync_state(namespace).await
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), VaultSyncError> {
        self.inner.write_sync_state(state).await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, VaultSyncError> {
        self.inner.read_schema(doc_id).await
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), VaultSyncError> {
        self.inner.write_schema(meta).await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, VaultSyncError> {
        self.inner.read_migrations().await
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), VaultSyncError> {
        self.inner.write_migration(record).await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, VaultSyncError> {
        self.inner.read_keys(namespace).await
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), VaultSyncError> {
        self.inner.write_key(key).await
    }

    async fn reset_stale_pending(
        &self,
        namespace: &str,
        older_than_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        self.inner
            .reset_stale_pending(namespace, older_than_ms)
            .await
    }

    async fn delete_synced_oplog_older_than(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<usize, VaultSyncError> {
        self.inner
            .delete_synced_oplog_older_than(namespace, older_than_secs)
            .await
    }

    async fn list_tombstoned_documents(
        &self,
        namespace: &str,
        older_than_secs: u64,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        self.inner
            .list_tombstoned_documents(namespace, older_than_secs)
            .await
    }

    async fn update_oplog_encrypted_blob(
        &self,
        id: &str,
        new_blob: &[u8],
    ) -> Result<(), VaultSyncError> {
        self.inner.update_oplog_encrypted_blob(id, new_blob).await
    }

    async fn list_active_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, String)>, VaultSyncError> {
        self.inner.list_active_documents(namespace).await
    }

    async fn read_synced_oplog_for_document(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
    ) -> Result<Vec<OplogEntry>, VaultSyncError> {
        self.inner
            .read_synced_oplog_for_document(namespace, doc_id, record_id)
            .await
    }

    async fn delete_synced_oplog_before_timestamp(
        &self,
        namespace: &str,
        doc_id: &str,
        record_id: &str,
        timestamp: u64,
    ) -> Result<usize, VaultSyncError> {
        self.inner
            .delete_synced_oplog_before_timestamp(namespace, doc_id, record_id, timestamp)
            .await
    }

    async fn delete_synced_before(
        &self,
        namespace: &str,
        cutoff_ms: u64,
    ) -> Result<usize, VaultSyncError> {
        self.inner.delete_synced_before(namespace, cutoff_ms).await
    }

    async fn write_batch_reconciliation(
        &self,
        documents: Vec<(String, String, Vec<u8>)>,
    ) -> Result<(), VaultSyncError> {
        let mut encrypted_documents = Vec::with_capacity(documents.len());
        for (doc_id, record_id, bytes) in documents {
            let encrypted = self.encrypt(&bytes)?;
            encrypted_documents.push((doc_id, record_id, encrypted));
        }
        self.inner
            .write_batch_reconciliation(encrypted_documents)
            .await
    }
}
