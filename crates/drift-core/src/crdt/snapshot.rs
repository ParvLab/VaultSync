use yrs::{ReadTxn, Transact};
use crate::crdt::document::CRDTDocument;
use crate::error::DriftError;

pub fn create_snapshot(doc: &CRDTDocument) -> Vec<u8> {
    doc.to_snapshot()
}

pub fn load_snapshot(bytes: &[u8]) -> Result<CRDTDocument, DriftError> {
    CRDTDocument::from_snapshot(bytes)
}

pub fn snapshot_size(bytes: &[u8]) -> u64 {
    bytes.len() as u64
}

pub fn snapshot_diff(from: &CRDTDocument, to: &CRDTDocument) -> Result<Vec<u8>, DriftError> {
    let txn_from = from.inner_doc().transact();
    let sv = txn_from.state_vector();
    drop(txn_from);
    let txn_to = to.inner_doc().transact();
    let update = txn_to.encode_diff_v1(&sv);
    Ok(update)
}
