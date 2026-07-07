use std::collections::HashMap;

/// Access statistics for a document.
#[derive(Debug, Clone)]
pub struct DocAccessStats {
    pub read_count: u64,
    pub write_count: u64,
    pub last_access_ms: u64,
    pub byte_size: u64,
}

/// Metadata index maps (namespace, doc_id) to segment_id and access stats.
#[derive(Debug, Clone)]
pub struct MetadataIndex {
    entries: HashMap<(String, String), MetadataEntry>,
}

#[derive(Debug, Clone)]
struct MetadataEntry {
    pub segment_id: u64,
    pub schema_version: u64,
    pub pinned: bool,
    pub stats: DocAccessStats,
}

impl MetadataIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn lookup(&self, namespace: &str, doc_id: &str) -> Option<u64> {
        self.entries.get(&(namespace.to_string(), doc_id.to_string())).map(|e| e.segment_id)
    }

    pub fn insert(
        &mut self,
        namespace: &str,
        doc_id: &str,
        segment_id: u64,
        schema_version: u64,
    ) {
        let entry = self.entries.entry((namespace.to_string(), doc_id.to_string())).or_insert_with(|| MetadataEntry {
            segment_id,
            schema_version,
            pinned: false,
            stats: DocAccessStats {
                read_count: 0,
                write_count: 0,
                last_access_ms: 0,
                byte_size: 0,
            },
        });
        entry.segment_id = segment_id;
        entry.schema_version = schema_version;
    }

    pub fn remove(&mut self, namespace: &str, doc_id: &str) {
        self.entries.remove(&(namespace.to_string(), doc_id.to_string()));
    }

    pub fn record_read(&mut self, namespace: &str, doc_id: &str, now_ms: u64) {
        if let Some(entry) = self.entries.get_mut(&(namespace.to_string(), doc_id.to_string())) {
            entry.stats.read_count += 1;
            entry.stats.last_access_ms = now_ms;
        }
    }

    pub fn record_write(&mut self, namespace: &str, doc_id: &str, now_ms: u64) {
        if let Some(entry) = self.entries.get_mut(&(namespace.to_string(), doc_id.to_string())) {
            entry.stats.write_count += 1;
            entry.stats.last_access_ms = now_ms;
        }
    }

    pub fn pin(&mut self, namespace: &str, doc_id: &str) {
        if let Some(entry) = self.entries.get_mut(&(namespace.to_string(), doc_id.to_string())) {
            entry.pinned = true;
        }
    }

    pub fn unpin(&mut self, namespace: &str, doc_id: &str) {
        if let Some(entry) = self.entries.get_mut(&(namespace.to_string(), doc_id.to_string())) {
            entry.pinned = false;
        }
    }

    pub fn is_pinned(&self, namespace: &str, doc_id: &str) -> bool {
        self.entries.get(&(namespace.to_string(), doc_id.to_string())).map_or(false, |e| e.pinned)
    }

    pub fn stats(&self, namespace: &str, doc_id: &str) -> Option<DocAccessStats> {
        self.entries.get(&(namespace.to_string(), doc_id.to_string())).map(|e| e.stats.clone())
    }

    pub fn hot_documents(&self, threshold: u64) -> Vec<(String, String, u64)> {
        self.entries.iter()
            .filter(|(_, e)| e.stats.read_count >= threshold)
            .map(|((ns, id), _)| (ns.clone(), id.clone(), 0))
            .collect()
    }

    pub fn cold_documents(&self, threshold: u64, now_ms: u64) -> Vec<(String, String, u64)> {
        self.entries.iter()
            .filter(|(_, e)| !e.pinned && now_ms.saturating_sub(e.stats.last_access_ms) > threshold)
            .map(|((ns, id), e)| (ns.clone(), id.clone(), e.stats.last_access_ms))
            .collect()
    }

    pub fn count(&self) -> usize {
        self.entries.len()
    }

    pub fn all_schema_versions(&self) -> HashMap<(String, String), u64> {
        self.entries.iter()
            .map(|((ns, id), e)| ((ns.clone(), id.clone()), e.schema_version))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup() {
        let mut idx = MetadataIndex::new();
        idx.insert("ns1", "doc_a", 1, 1);
        assert_eq!(idx.lookup("ns1", "doc_a"), Some(1));
        assert!(idx.lookup("ns1", "doc_b").is_none());
    }

    #[test]
    fn test_remove() {
        let mut idx = MetadataIndex::new();
        idx.insert("ns1", "doc_a", 1, 1);
        idx.remove("ns1", "doc_a");
        assert!(idx.lookup("ns1", "doc_a").is_none());
    }

    #[test]
    fn test_record_read_write() {
        let mut idx = MetadataIndex::new();
        idx.insert("ns1", "doc_a", 1, 1);
        idx.record_read("ns1", "doc_a", 1000);
        idx.record_write("ns1", "doc_a", 2000);

        let stats = idx.stats("ns1", "doc_a").unwrap();
        assert_eq!(stats.read_count, 1);
        assert_eq!(stats.write_count, 1);
        assert_eq!(stats.last_access_ms, 2000);
    }

    #[test]
    fn test_pin_unpin() {
        let mut idx = MetadataIndex::new();
        idx.insert("ns1", "doc_a", 1, 1);
        assert!(!idx.is_pinned("ns1", "doc_a"));

        idx.pin("ns1", "doc_a");
        assert!(idx.is_pinned("ns1", "doc_a"));

        idx.unpin("ns1", "doc_a");
        assert!(!idx.is_pinned("ns1", "doc_a"));
    }

    #[test]
    fn test_hot_cold_classification() {
        let mut idx = MetadataIndex::new();
        idx.insert("ns1", "hot_doc", 1, 1);
        idx.record_read("ns1", "hot_doc", 4000);
        idx.record_read("ns1", "hot_doc", 4000);

        idx.insert("ns1", "cold_doc", 1, 1);
        // cold_doc has 0 reads, last_access stays 0

        let hot = idx.hot_documents(2);
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].1, "hot_doc");

        // threshold=4000, now=5000: hot_doc last_access=4000 → 1000ms ago → not cold
        // cold_doc last_access=0 → 5000ms ago → cold
        let cold = idx.cold_documents(4000, 5000);
        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].1, "cold_doc");
    }
}
