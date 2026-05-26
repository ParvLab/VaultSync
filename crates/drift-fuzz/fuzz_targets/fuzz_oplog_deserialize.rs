#![no_main]
use libfuzzer_sys::fuzz_target;
use drift_core::oplog::entry::OplogEntry;

fuzz_target!(|data: &[u8]| {
    if let Ok(json_str) = std::str::from_utf8(data) {
        let _: Result<OplogEntry, _> = serde_json::from_str(json_str);
    }
});
