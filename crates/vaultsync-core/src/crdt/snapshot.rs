use serde::{Deserialize, Serialize};
use yrs::{ReadTxn, Transact};
use crate::crdt::document::CRDTDocument;
use crate::error::VaultSyncError;
use crc32fast::Hasher;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub doc_id: String,
    pub record_id: String,
    pub schema_version: u64,
    pub sequence: u64,          // last included oplog sequence
    pub created_at: u64,        // unix timestamp ms
    pub bytes: Vec<u8>,         // yrs full-state encoding
    pub checksum: u32,          // CRC32 of bytes for corruption detection
}

impl Snapshot {
    pub fn from_document(doc: &CRDTDocument, sequence: u64) -> Self {
        let bytes = doc.to_snapshot();
        let mut hasher = Hasher::new();
        hasher.update(&bytes);
        let checksum = hasher.finalize();
        let created_at = crate::time_utils::system_time_now_ms();
        Self {
            doc_id: doc.doc_id.clone(),
            record_id: doc.record_id.clone(),
            schema_version: doc.schema_version,
            sequence,
            created_at,
            bytes,
            checksum,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, VaultSyncError> {
        bincode::serialize(self)
            .map_err(|e| VaultSyncError::Crdt(format!("failed to serialize snapshot: {e}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, VaultSyncError> {
        let snap: Self = bincode::deserialize(bytes)
            .map_err(|e| VaultSyncError::Crdt(format!("failed to deserialize snapshot: {e}")))?;
        Ok(snap)
    }

    pub fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.bytes);
        hasher.finalize() == self.checksum
    }
}

pub fn create_snapshot(doc: &CRDTDocument) -> Vec<u8> {
    let snap = Snapshot::from_document(doc, 0);
    snap.encode().unwrap_or_default()
}

pub fn create_snapshot_with_meta(doc: &CRDTDocument, sequence: u64) -> Result<Vec<u8>, VaultSyncError> {
    let snap = Snapshot::from_document(doc, sequence);
    snap.encode()
}

pub fn load_snapshot(bytes: &[u8]) -> Result<CRDTDocument, VaultSyncError> {
    let snap = Snapshot::decode(bytes)?;
    if !snap.verify_checksum() {
        return Err(VaultSyncError::Crdt("snapshot checksum verification failed".to_string()));
    }
    CRDTDocument::from_snapshot(&snap.bytes)
}

pub fn load_and_verify_snapshot(bytes: &[u8]) -> Result<(CRDTDocument, Snapshot), VaultSyncError> {
    let snap = Snapshot::decode(bytes)?;
    if !snap.verify_checksum() {
        return Err(VaultSyncError::Crdt("snapshot checksum verification failed".to_string()));
    }
    let doc = CRDTDocument::from_snapshot(&snap.bytes)?;
    Ok((doc, snap))
}

pub fn snapshot_size(bytes: &[u8]) -> u64 {
    bytes.len() as u64
}

pub fn snapshot_diff(from: &CRDTDocument, to: &CRDTDocument) -> Result<Vec<u8>, VaultSyncError> {
    let txn_from = from.inner_doc().transact();
    let sv = txn_from.state_vector();
    drop(txn_from);
    let txn_to = to.inner_doc().transact();
    let update = txn_to.encode_diff_v1(&sv);
    Ok(update)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_roundtrip() {
        let mut doc = CRDTDocument::new("doc_1", "rec_1", 2);
        doc.set_field("name", crate::crdt::types::CrdtValue::String("vaultsync".to_string()));
        
        let snap_bytes = create_snapshot_with_meta(&doc, 42).unwrap();
        let (loaded_doc, snap) = load_and_verify_snapshot(&snap_bytes).unwrap();
        
        assert_eq!(loaded_doc.doc_id, "doc_1");
        assert_eq!(loaded_doc.record_id, "rec_1");
        assert_eq!(loaded_doc.schema_version, 2);
        assert_eq!(snap.sequence, 42);
        assert_eq!(
            loaded_doc.get_field("name"),
            Some(crate::crdt::types::CrdtValue::String("vaultsync".to_string()))
        );
        assert!(snap.verify_checksum());
    }

    #[test]
    fn test_snapshot_checksum_failure() {
        let mut doc = CRDTDocument::new("doc_1", "rec_1", 2);
        doc.set_field("name", crate::crdt::types::CrdtValue::String("vaultsync".to_string()));
        
        let snap_bytes = create_snapshot_with_meta(&doc, 42).unwrap();
        let mut snap = Snapshot::decode(&snap_bytes).unwrap();
        // Corrupt the data
        snap.bytes[0] ^= 0xFF;
        
        let corrupted_bytes = snap.encode().unwrap();
        let res = load_and_verify_snapshot(&corrupted_bytes);
        assert!(res.is_err());
    }
}
