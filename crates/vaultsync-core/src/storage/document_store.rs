use crate::VaultSyncError;
use crate::storage::traits::Storage;
use std::sync::Arc;

/// Maximum documents per segment before split.
const MAX_DOCS_PER_SEGMENT: usize = 256;

/// Minimum docs per segment (below this, try to merge with sibling).
#[allow(dead_code)]
const MIN_DOCS_PER_SEGMENT: usize = 32;

/// A segment holds 32-256 documents identified by their `(namespace, doc_id)`.
#[derive(Debug, Clone)]
pub struct Segment {
    pub segment_id: u64,
    pub namespace: String,
    pub doc_count: usize,
    pub byte_size: usize,
    pub page_key: String,
}

/// Map of `(namespace, doc_id)` to `segment_id`.
#[derive(Debug, Clone, Default)]
pub struct DocIndex {
    entries: Vec<DocIndexEntry>,
}

#[derive(Debug, Clone)]
struct DocIndexEntry {
    namespace: String,
    doc_id: String,
    segment_id: u64,
}

impl DocIndex {
    pub fn lookup(&self, namespace: &str, doc_id: &str) -> Option<u64> {
        self.entries.iter()
            .find(|e| e.namespace == namespace && e.doc_id == doc_id)
            .map(|e| e.segment_id)
    }

    pub fn insert(&mut self, namespace: &str, doc_id: &str, segment_id: u64) {
        // Remove old entry if exists
        self.entries.retain(|e| !(e.namespace == namespace && e.doc_id == doc_id));
        self.entries.push(DocIndexEntry {
            namespace: namespace.to_string(),
            doc_id: doc_id.to_string(),
            segment_id,
        });
    }

    pub fn remove(&mut self, namespace: &str, doc_id: &str) {
        self.entries.retain(|e| !(e.namespace == namespace && e.doc_id == doc_id));
    }

    pub fn count(&self) -> usize {
        self.entries.len()
    }
}

/// Bounded mutation queue backed by VersionChain.
#[derive(Debug)]
pub struct MutationQueue {
    max_pending: usize,
}

impl MutationQueue {
    pub fn new(max_pending: usize) -> Self {
        Self { max_pending }
    }

    /// Returns true if the queue is full and should be flushed/compacted.
    pub fn needs_flush(&self, pending_count: usize) -> bool {
        pending_count >= self.max_pending
    }

    pub fn max_pending(&self) -> usize {
        self.max_pending
    }
}

impl Default for MutationQueue {
    fn default() -> Self {
        Self::new(100)
    }
}

/// Segment-based document store.
#[allow(dead_code)]
pub struct DocumentStore<S: Storage> {
    storage: Arc<S>,
    index: DocIndex,
    segments: Vec<Segment>,
    namespace: String,
}

impl<S: Storage> DocumentStore<S> {
    pub fn new(storage: Arc<S>, namespace: String) -> Self {
        Self {
            storage,
            index: DocIndex::default(),
            segments: Vec::new(),
            namespace,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn index(&self) -> &DocIndex {
        &self.index
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn segment_for(&self, doc_id: &str) -> Option<&Segment> {
        let sid = self.index.lookup(&self.namespace, doc_id)?;
        self.segments.iter().find(|s| s.segment_id == sid)
    }

    pub fn add_to_segment(&mut self, doc_id: &str, page_key: &str) -> Result<(), VaultSyncError> {
        // Find a segment with room
        let seg_id = if let Some(seg) = self.segments.iter_mut().find(|s| s.doc_count < MAX_DOCS_PER_SEGMENT) {
            seg.doc_count += 1;
            seg.byte_size += 256; // rough estimate
            seg.segment_id
        } else {
            // Create new segment
            let seg_id = (self.segments.len() as u64) + 1;
            self.segments.push(Segment {
                segment_id: seg_id,
                namespace: self.namespace.clone(),
                doc_count: 1,
                byte_size: 256,
                page_key: page_key.to_string(),
            });
            seg_id
        };

        self.index.insert(&self.namespace, doc_id, seg_id);
        Ok(())
    }

    pub fn remove_document(&mut self, doc_id: &str) {
        if let Some(sid) = self.index.lookup(&self.namespace, doc_id) {
            if let Some(seg) = self.segments.iter_mut().find(|s| s.segment_id == sid) {
                seg.doc_count = seg.doc_count.saturating_sub(1);
            }
            self.index.remove(&self.namespace, doc_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::InMemoryStorage;

    fn make_storage() -> Arc<InMemoryStorage> {
        Arc::new(InMemoryStorage::new())
    }

    #[test]
    fn test_add_and_lookup_document() {
        let storage = make_storage();
        let mut store = DocumentStore::new(storage, "test_ns".to_string());

        store.add_to_segment("doc_1", "page_1").unwrap();
        let seg = store.segment_for("doc_1").expect("should find segment");
        assert_eq!(seg.doc_count, 1);

        assert_eq!(store.segment_count(), 1);
    }

    #[test]
    fn test_segment_reaches_max() {
        let storage = make_storage();
        let mut store = DocumentStore::new(storage, "test_ns".to_string());

        // Fill up one segment
        for i in 0..MAX_DOCS_PER_SEGMENT {
            store.add_to_segment(&format!("doc_{}", i), "page_1").unwrap();
        }

        // One segment with MAX docs
        assert_eq!(store.segment_count(), 1);

        // Add one more — should create new segment
        store.add_to_segment("doc_extra", "page_2").unwrap();
        assert_eq!(store.segment_count(), 2);
    }

    #[test]
    fn test_remove_document_updates_count() {
        let storage = make_storage();
        let mut store = DocumentStore::new(storage, "test_ns".to_string());

        store.add_to_segment("doc_1", "page_1").unwrap();
        assert_eq!(store.segment_for("doc_1").unwrap().doc_count, 1);

        store.remove_document("doc_1");
        assert!(store.segment_for("doc_1").is_none());

        // Count should be decremented
        let seg = &store.segments[0];
        assert_eq!(seg.doc_count, 0);
    }

    #[test]
    fn test_mutation_queue_needs_flush() {
        let mq = MutationQueue::new(100);
        assert!(!mq.needs_flush(50));
        assert!(mq.needs_flush(100));
        assert!(mq.needs_flush(150));
    }
}
