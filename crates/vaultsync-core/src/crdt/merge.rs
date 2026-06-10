use yrs::{Transact, Update};
use yrs::updates::decoder::Decode;
use crate::crdt::document::CRDTDocument;
use crate::error::VaultSyncError;

pub fn merge_update_into_document(doc: &mut CRDTDocument, update: &[u8]) -> Result<(), VaultSyncError> {
    doc.apply_update(update)
}

pub fn merge_batch(doc: &mut CRDTDocument, updates: &[Vec<u8>]) -> Result<(), VaultSyncError> {
    for update_bytes in updates {
        let decoded = Update::decode_v1(update_bytes)
            .map_err(|e| VaultSyncError::Crdt(format!("failed to decode batch update: {e}")))?;
        let mut txn = doc.inner_doc().transact_mut();
        txn.apply_update(decoded);
    }
    Ok(())
}

pub fn assert_deterministic(a: &CRDTDocument, b: &CRDTDocument) {
    let sv_a = a.state_vector();
    let sv_b = b.state_vector();
    assert_eq!(sv_a, sv_b, "CRDT state vectors must match for deterministic merge");
    let map_a = a.to_map();
    let map_b = b.to_map();
    assert_eq!(map_a, map_b, "CRDT document maps must be identical after convergent merge");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::types::CrdtValue;

    #[test]
    fn test_concurrent_create_and_delete() {
        let mut base = CRDTDocument::new("doc1", "rec1", 1);
        base.set_field("item", CrdtValue::Number(10.0));
        let base_snapshot = base.to_snapshot();

        let mut replica_a = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
        let mut replica_b = CRDTDocument::from_snapshot(&base_snapshot).unwrap();

        // Replica A updates the field
        let update_a = replica_a.set_field("item", CrdtValue::Number(20.0));
        // Replica B deletes the field
        let update_b = replica_b.delete_field("item");

        // Merge updates
        replica_a.apply_update(&update_b).unwrap();
        replica_b.apply_update(&update_a).unwrap();

        assert_deterministic(&replica_a, &replica_b);
        // Under Yrs CRDT map semantics, a concurrent set creates a new block which is not affected by
        // a concurrent delete of the previous block version, so the concurrent set wins.
        assert_eq!(replica_a.get_field("item"), Some(CrdtValue::Number(20.0)));
    }

    #[test]
    fn test_concurrent_create_and_update() {
        let base = CRDTDocument::new("doc1", "rec1", 1);
        let base_snapshot = base.to_snapshot();

        let mut replica_a = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
        let mut replica_b = CRDTDocument::from_snapshot(&base_snapshot).unwrap();

        // Replica A sets field A
        let update_a = replica_a.set_field("field_a", CrdtValue::String("val_a".to_string()));
        // Replica B sets field B
        let update_b = replica_b.set_field("field_b", CrdtValue::Number(100.0));

        replica_a.apply_update(&update_b).unwrap();
        replica_b.apply_update(&update_a).unwrap();

        assert_deterministic(&replica_a, &replica_b);
        assert_eq!(replica_a.get_field("field_a"), Some(CrdtValue::String("val_a".to_string())));
        assert_eq!(replica_a.get_field("field_b"), Some(CrdtValue::Number(100.0)));
    }

    #[test]
    fn test_three_way_concurrent_edit() {
        let base = CRDTDocument::new("doc1", "rec1", 1);
        let base_snapshot = base.to_snapshot();

        let mut replica_a = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
        let mut replica_b = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
        let mut replica_c = CRDTDocument::from_snapshot(&base_snapshot).unwrap();

        let update_a = replica_a.set_field("field_a", CrdtValue::String("a".to_string()));
        let update_b = replica_b.set_field("field_b", CrdtValue::String("b".to_string()));
        let update_c = replica_c.set_field("field_c", CrdtValue::String("c".to_string()));

        // replica A merges B and C
        replica_a.apply_update(&update_b).unwrap();
        replica_a.apply_update(&update_c).unwrap();

        // replica B merges A and C
        replica_b.apply_update(&update_a).unwrap();
        replica_b.apply_update(&update_c).unwrap();

        // replica C merges A and B
        replica_c.apply_update(&update_a).unwrap();
        replica_c.apply_update(&update_b).unwrap();

        assert_deterministic(&replica_a, &replica_b);
        assert_deterministic(&replica_b, &replica_c);
        assert_eq!(replica_a.get_field("field_a"), Some(CrdtValue::String("a".to_string())));
        assert_eq!(replica_a.get_field("field_b"), Some(CrdtValue::String("b".to_string())));
        assert_eq!(replica_a.get_field("field_c"), Some(CrdtValue::String("c".to_string())));
    }
}
