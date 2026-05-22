use yrs::{Transact, Update};
use yrs::updates::decoder::Decode;
use crate::crdt::document::CRDTDocument;
use crate::error::DriftError;

pub fn merge_update_into_document(doc: &mut CRDTDocument, update: &[u8]) -> Result<(), DriftError> {
    doc.apply_update(update)
}

pub fn merge_batch(doc: &mut CRDTDocument, updates: &[Vec<u8>]) -> Result<(), DriftError> {
    for update_bytes in updates {
        let decoded = Update::decode_v1(update_bytes)
            .map_err(|e| DriftError::Crdt(format!("failed to decode batch update: {e}")))?;
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
