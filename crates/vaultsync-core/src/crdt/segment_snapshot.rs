use serde::{Deserialize, Serialize};
use crc32fast::Hasher;

use super::snapshot::Snapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentSnapshot {
    pub segment_id: String,
    pub namespace: String,
    pub documents: Vec<Snapshot>,
    pub checksum: u32,
    pub created_at: u64,
    pub byte_len: u64,
}

impl SegmentSnapshot {
    pub fn new(segment_id: &str, namespace: &str, now_ms: u64) -> Self {
        Self {
            segment_id: segment_id.to_string(),
            namespace: namespace.to_string(),
            documents: Vec::new(),
            checksum: 0,
            created_at: now_ms,
            byte_len: 0,
        }
    }

    pub fn from_documents(
        segment_id: &str,
        namespace: &str,
        documents: Vec<Snapshot>,
        now_ms: u64,
    ) -> Self {
        let byte_len: u64 = documents.iter().map(|d| d.bytes.len() as u64).sum();
        let mut segment = Self {
            segment_id: segment_id.to_string(),
            namespace: namespace.to_string(),
            documents,
            checksum: 0,
            created_at: now_ms,
            byte_len,
        };
        segment.recompute_checksum();
        segment
    }

    pub fn add_document(&mut self, snapshot: Snapshot) {
        self.byte_len += snapshot.bytes.len() as u64;
        self.documents.push(snapshot);
        self.recompute_checksum();
    }

    pub fn recompute_checksum(&mut self) {
        let mut hasher = Hasher::new();
        for doc in &self.documents {
            hasher.update(&doc.checksum.to_le_bytes());
        }
        self.checksum = hasher.finalize();
    }

    pub fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        for doc in &self.documents {
            hasher.update(&doc.checksum.to_le_bytes());
        }
        hasher.finalize() == self.checksum
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.byte_len
    }

    pub fn document_ids(&self) -> Vec<(&str, &str)> {
        self.documents
            .iter()
            .map(|d| (d.doc_id.as_str(), d.record_id.as_str()))
            .collect()
    }

    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn max_sequence(&self) -> u64 {
        self.documents.iter().map(|d| d.sequence).max().unwrap_or(0)
    }

    pub fn min_sequence(&self) -> u64 {
        self.documents.iter().map(|d| d.sequence).min().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::snapshot::Snapshot;

    fn test_snapshot(doc_id: &str, record_id: &str, sequence: u64) -> Snapshot {
        Snapshot {
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            schema_version: 1,
            sequence,
            created_at: 1000,
            bytes: vec![1, 2, 3, 4, 5],
            checksum: 0,
        }
    }

    #[test]
    fn test_create_segment_snapshot() {
        let segment = SegmentSnapshot::new("seg1", "finance", 1000);
        assert_eq!(segment.segment_id, "seg1");
        assert_eq!(segment.namespace, "finance");
        assert!(segment.is_empty());
        assert_eq!(segment.document_count(), 0);
    }

    #[test]
    fn test_from_documents() {
        let docs = vec![
            test_snapshot("d1", "r1", 1),
            test_snapshot("d2", "r2", 2),
        ];
        let segment = SegmentSnapshot::from_documents("seg1", "finance", docs, 1000);

        assert_eq!(segment.document_count(), 2);
        assert!(!segment.is_empty());
        assert!(segment.verify_checksum());
    }

    #[test]
    fn test_add_document() {
        let mut segment = SegmentSnapshot::new("seg1", "finance", 1000);
        segment.add_document(test_snapshot("d1", "r1", 1));
        segment.add_document(test_snapshot("d2", "r2", 2));

        assert_eq!(segment.document_count(), 2);
        assert!(segment.verify_checksum());
    }

    #[test]
    fn test_checksum_changes_on_add() {
        let mut segment = SegmentSnapshot::new("seg1", "finance", 1000);
        segment.add_document(test_snapshot("d1", "r1", 1));
        let checksum1 = segment.checksum;

        segment.add_document(test_snapshot("d2", "r2", 2));
        let checksum2 = segment.checksum;

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_encode_decode() {
        let docs = vec![
            test_snapshot("d1", "r1", 1),
            test_snapshot("d2", "r2", 2),
        ];
        let segment = SegmentSnapshot::from_documents("seg1", "finance", docs, 1000);

        let encoded = segment.encode().unwrap();
        let decoded = SegmentSnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded.segment_id, "seg1");
        assert_eq!(decoded.document_count(), 2);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_document_ids() {
        let docs = vec![
            test_snapshot("d1", "r1", 1),
            test_snapshot("d2", "r2", 2),
        ];
        let segment = SegmentSnapshot::from_documents("seg1", "finance", docs, 1000);

        let ids = segment.document_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&("d1", "r1")));
        assert!(ids.contains(&("d2", "r2")));
    }

    #[test]
    fn test_max_min_sequence() {
        let docs = vec![
            test_snapshot("d1", "r1", 5),
            test_snapshot("d2", "r2", 10),
            test_snapshot("d3", "r3", 3),
        ];
        let segment = SegmentSnapshot::from_documents("seg1", "finance", docs, 1000);

        assert_eq!(segment.max_sequence(), 10);
        assert_eq!(segment.min_sequence(), 3);
    }

    #[test]
    fn test_total_bytes() {
        let docs = vec![
            test_snapshot("d1", "r1", 1),
            test_snapshot("d2", "r2", 2),
        ];
        let segment = SegmentSnapshot::from_documents("seg1", "finance", docs, 1000);

        assert_eq!(segment.total_bytes(), 10);
    }

    #[test]
    fn test_empty_segment() {
        let segment = SegmentSnapshot::new("seg1", "finance", 1000);
        assert!(segment.is_empty());
        assert_eq!(segment.max_sequence(), 0);
        assert_eq!(segment.min_sequence(), 0);
        assert_eq!(segment.total_bytes(), 0);
    }
}
