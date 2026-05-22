#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(doc) = drift_core::crdt::document::CRDTDocument::from_snapshot(data) {
        let snapshot = doc.to_snapshot();
        assert_eq!(snapshot.is_empty(), data.is_empty());
    }
});
