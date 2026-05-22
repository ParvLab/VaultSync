use std::time::Duration;
use yrs::Transact;
use crate::crdt::document::CRDTDocument;
use crate::error::DriftError;

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

pub fn gc_tombstones(doc: &mut CRDTDocument, _config: &CompactionConfig) -> Result<(), DriftError> {
    let txn = doc.inner_doc().transact();
    drop(txn);
    Ok(())
}

pub fn should_compact(oplog_len: usize, config: &CompactionConfig) -> bool {
    oplog_len >= config.snapshot_interval
}

pub fn compact(_doc: &mut CRDTDocument, _oplog_len: &mut usize, _config: &CompactionConfig) -> Result<(), DriftError> {
    *_oplog_len = 0;
    Ok(())
}
