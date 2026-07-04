use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use super::snapshot::Snapshot;
use super::segment_snapshot::SegmentSnapshot;

#[async_trait]
pub trait SnapshotStore: Send + Sync + std::fmt::Debug {
    async fn store_snapshot(&self, namespace: &str, snapshot: Snapshot) -> Result<(), String>;
    async fn get_snapshot(&self, namespace: &str, doc_id: &str, record_id: &str) -> Result<Option<Snapshot>, String>;
    async fn list_snapshots(&self, namespace: &str) -> Result<Vec<Snapshot>, String>;
    async fn store_segment(&self, segment: SegmentSnapshot) -> Result<(), String>;
    async fn get_segment(&self, namespace: &str, segment_id: &str) -> Result<Option<SegmentSnapshot>, String>;
    async fn list_segments(&self, namespace: &str) -> Result<Vec<SegmentSnapshot>, String>;
}

#[derive(Debug)]
pub struct InMemorySnapshotStore {
    snapshots: RwLock<HashMap<(String, String, String), Snapshot>>,
    segments: RwLock<HashMap<(String, String), SegmentSnapshot>>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
            segments: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotStore for InMemorySnapshotStore {
    async fn store_snapshot(&self, namespace: &str, snapshot: Snapshot) -> Result<(), String> {
        if let Ok(mut snaps) = self.snapshots.write() {
            let key = (
                namespace.to_string(),
                snapshot.doc_id.clone(),
                snapshot.record_id.clone(),
            );
            snaps.insert(key, snapshot);
            Ok(())
        } else {
            Err("Failed to acquire lock".into())
        }
    }

    async fn get_snapshot(&self, namespace: &str, doc_id: &str, record_id: &str) -> Result<Option<Snapshot>, String> {
        let snaps = self.snapshots.read().map_err(|e| e.to_string())?;
        let key = (namespace.to_string(), doc_id.to_string(), record_id.to_string());
        Ok(snaps.get(&key).cloned())
    }

    async fn list_snapshots(&self, namespace: &str) -> Result<Vec<Snapshot>, String> {
        let snaps = self.snapshots.read().map_err(|e| e.to_string())?;
        Ok(snaps
            .iter()
            .filter(|((ns, _, _), _)| ns == namespace)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn store_segment(&self, segment: SegmentSnapshot) -> Result<(), String> {
        if let Ok(mut segs) = self.segments.write() {
            let key = (segment.namespace.clone(), segment.segment_id.clone());
            segs.insert(key, segment);
            Ok(())
        } else {
            Err("Failed to acquire lock".into())
        }
    }

    async fn get_segment(&self, namespace: &str, segment_id: &str) -> Result<Option<SegmentSnapshot>, String> {
        let segs = self.segments.read().map_err(|e| e.to_string())?;
        let key = (namespace.to_string(), segment_id.to_string());
        Ok(segs.get(&key).cloned())
    }

    async fn list_segments(&self, namespace: &str) -> Result<Vec<SegmentSnapshot>, String> {
        let segs = self.segments.read().map_err(|e| e.to_string())?;
        Ok(segs
            .iter()
            .filter(|((ns, _), _)| ns == namespace)
            .map(|(_, v)| v.clone())
            .collect())
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

    #[tokio::test]
    async fn test_store_and_get_snapshot() {
        let store = InMemorySnapshotStore::new();
        let snap = test_snapshot("d1", "r1", 1);

        store.store_snapshot("ns1", snap.clone()).await.unwrap();
        let retrieved = store.get_snapshot("ns1", "d1", "r1").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().doc_id, "d1");
    }

    #[tokio::test]
    async fn test_list_snapshots() {
        let store = InMemorySnapshotStore::new();
        store.store_snapshot("ns1", test_snapshot("d1", "r1", 1)).await.unwrap();
        store.store_snapshot("ns1", test_snapshot("d2", "r2", 2)).await.unwrap();
        store.store_snapshot("ns2", test_snapshot("d3", "r3", 3)).await.unwrap();

        let ns1_snaps = store.list_snapshots("ns1").await.unwrap();
        assert_eq!(ns1_snaps.len(), 2);
    }

    #[tokio::test]
    async fn test_store_and_get_segment() {
        let store = InMemorySnapshotStore::new();
        let mut seg = SegmentSnapshot::new("seg1", "ns1", 1000);
        seg.add_document(test_snapshot("d1", "r1", 1));

        store.store_segment(seg).await.unwrap();
        let retrieved = store.get_segment("ns1", "seg1").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().document_count(), 1);
    }

    #[tokio::test]
    async fn test_list_segments() {
        let store = InMemorySnapshotStore::new();
        let seg1 = SegmentSnapshot::new("seg1", "ns1", 1000);
        let seg2 = SegmentSnapshot::new("seg2", "ns1", 2000);
        store.store_segment(seg1).await.unwrap();
        store.store_segment(seg2).await.unwrap();

        let segments = store.list_segments("ns1").await.unwrap();
        assert_eq!(segments.len(), 2);
    }
}
