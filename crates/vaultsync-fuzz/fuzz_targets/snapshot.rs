#![no_main]
use libfuzzer_sys::fuzz_target;
use vaultsync_core::crdt::document::CRDTDocument;

fuzz_target!(|data: &[u8]| {
    let _ = CRDTDocument::from_snapshot(data);
});
