use crate::crdt::document::CRDTDocument;
use crate::crdt::types::CrdtValue;
use crate::error::VaultSyncError;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub tombstone_ttl: Duration,
    pub snapshot_interval: usize,
    pub max_oplog_age: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            tombstone_ttl: Duration::from_secs(86400),
            snapshot_interval: 1000,
            max_oplog_age: Duration::from_secs(604800),
        }
    }
}

pub fn gc_tombstones(
    doc: &mut CRDTDocument,
    config: &CompactionConfig,
) -> Result<Vec<u8>, VaultSyncError> {
    let now = crate::time_utils::system_time_now_ms();
    let ttl_ms = config.tombstone_ttl.as_millis() as u64;

    // Find all fields in the root map that are tombstones to clean up
    let fields = doc.to_map();
    let mut keys_to_delete = Vec::new();
    for (key, val) in fields {
        if let CrdtValue::Map(inner_map) = val {
            if let Some(CrdtValue::Boolean(true)) = inner_map.get("_deleted") {
                if let Some(CrdtValue::Number(deleted_at)) = inner_map.get("_deleted_at") {
                    if now >= (*deleted_at as u64) + ttl_ms {
                        keys_to_delete.push(key);
                    }
                }
            }
        }
    }

    let mut combined_update = Vec::new();
    for key in keys_to_delete {
        let update = doc.delete_field(&key);
        combined_update.extend(update);
    }

    Ok(combined_update)
}

pub fn should_compact(oplog_len: usize, config: &CompactionConfig) -> bool {
    oplog_len >= config.snapshot_interval
}

pub fn compact(
    doc: &mut CRDTDocument,
    oplog_len: &mut usize,
    _config: &CompactionConfig,
) -> Result<Vec<u8>, VaultSyncError> {
    let snapshot = crate::crdt::snapshot::Snapshot::from_document(doc, 0);
    let bytes = snapshot.encode()?;
    *oplog_len = 0;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_gc_tombstones_removes_expired() {
        let mut doc = CRDTDocument::new("doc_1", "rec_1", 1);

        let mut expired_tombstone = HashMap::new();
        expired_tombstone.insert("_deleted".to_string(), CrdtValue::Boolean(true));
        // Expired (100 seconds ago)
        let now = crate::time_utils::system_time_now_ms();
        expired_tombstone.insert(
            "_deleted_at".to_string(),
            CrdtValue::Number((now - 100000) as f64),
        );

        let mut active_tombstone = HashMap::new();
        active_tombstone.insert("_deleted".to_string(), CrdtValue::Boolean(true));
        // Active (deleted just now)
        active_tombstone.insert("_deleted_at".to_string(), CrdtValue::Number(now as f64));

        doc.set_field("expired_key", CrdtValue::Map(expired_tombstone));
        doc.set_field("active_key", CrdtValue::Map(active_tombstone));
        doc.set_field("normal_key", CrdtValue::String("hello".to_string()));

        let config = CompactionConfig {
            tombstone_ttl: Duration::from_secs(50),
            snapshot_interval: 10,
            max_oplog_age: Duration::from_secs(100),
        };

        let update = gc_tombstones(&mut doc, &config).unwrap();
        assert!(!update.is_empty());

        let final_map = doc.to_map();
        assert!(final_map.contains_key("normal_key"));
        assert!(final_map.contains_key("active_key"));
        assert!(!final_map.contains_key("expired_key"));
    }
}
