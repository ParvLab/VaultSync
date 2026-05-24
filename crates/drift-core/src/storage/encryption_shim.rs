use async_trait::async_trait;
use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crate::error::DriftError;
use crate::oplog::entry::OplogEntry;
use crate::sync::state::SyncState;
use super::traits::{Storage, SchemaMeta, MigrationRecord, KeyRecord};

const ENC_NONCE: &[u8; 12] = b"driftstore00";

#[derive(Debug)]
pub struct EncryptedStorage {
    inner: Box<dyn Storage>,
    device_key: [u8; 32],
}

impl EncryptedStorage {
    pub fn new(inner: Box<dyn Storage>, device_key: [u8; 32]) -> Self {
        Self { inner, device_key }
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, DriftError> {
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.device_key));
        let nonce = Nonce::from_slice(ENC_NONCE);
        cipher.encrypt(nonce, data)
            .map_err(|e| DriftError::Encryption(format!("storage encrypt failed: {e}")))
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, DriftError> {
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.device_key));
        let nonce = Nonce::from_slice(ENC_NONCE);
        cipher.decrypt(nonce, data)
            .map_err(|e| DriftError::Encryption(format!("storage decrypt failed: {e}")))
    }
}

#[async_trait]
impl Storage for EncryptedStorage {
    async fn insert_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<(), DriftError> {
        let encrypted = self.encrypt(bytes)?;
        self.inner.insert_document(doc_id, record_id, &encrypted).await
    }

    async fn get_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>, DriftError> {
        match self.inner.get_document(doc_id, record_id).await? {
            Some(data) => Ok(Some(self.decrypt(&data)?)),
            None => Ok(None),
        }
    }

    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<(), DriftError> {
        self.inner.delete_document(doc_id, record_id).await
    }

    async fn write_document_and_oplog(&self, doc_id: &str, record_id: &str, bytes: &[u8], entry: &OplogEntry) -> Result<(), DriftError> {
        let encrypted = self.encrypt(bytes)?;
        self.inner.write_document_and_oplog(doc_id, record_id, &encrypted, entry).await
    }

    async fn delete_document_and_oplog(&self, doc_id: &str, record_id: &str, entry: &OplogEntry) -> Result<(), DriftError> {
        self.inner.delete_document_and_oplog(doc_id, record_id, entry).await
    }

    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>, DriftError> {
        let docs = self.inner.list_documents(doc_id).await?;
        docs.into_iter().map(|(rid, data)| {
            Ok((rid, self.decrypt(&data)?))
        }).collect()
    }

    async fn append_oplog(&self, entry: &OplogEntry) -> Result<(), DriftError> {
        self.inner.append_oplog(entry).await
    }

    async fn read_pending_oplog(&self, namespace: &str, limit: usize) -> Result<Vec<OplogEntry>, DriftError> {
        self.inner.read_pending_oplog(namespace, limit).await
    }

    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<(), DriftError> {
        self.inner.mark_synced(id, sequence).await
    }

    async fn mark_failed(&self, id: &str, error_msg: &str) -> Result<(), DriftError> {
        self.inner.mark_failed(id, error_msg).await
    }

    async fn read_oplog_after_sequence(&self, namespace: &str, seq: u64) -> Result<Vec<OplogEntry>, DriftError> {
        self.inner.read_oplog_after_sequence(namespace, seq).await
    }

    async fn read_sync_state(&self, namespace: &str) -> Result<Option<SyncState>, DriftError> {
        self.inner.read_sync_state(namespace).await
    }

    async fn write_sync_state(&self, state: &SyncState) -> Result<(), DriftError> {
        self.inner.write_sync_state(state).await
    }

    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>, DriftError> {
        self.inner.read_schema(doc_id).await
    }

    async fn write_schema(&self, meta: &SchemaMeta) -> Result<(), DriftError> {
        self.inner.write_schema(meta).await
    }

    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>, DriftError> {
        self.inner.read_migrations().await
    }

    async fn write_migration(&self, record: &MigrationRecord) -> Result<(), DriftError> {
        self.inner.write_migration(record).await
    }

    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>, DriftError> {
        self.inner.read_keys(namespace).await
    }

    async fn write_key(&self, key: &KeyRecord) -> Result<(), DriftError> {
        self.inner.write_key(key).await
    }
}
