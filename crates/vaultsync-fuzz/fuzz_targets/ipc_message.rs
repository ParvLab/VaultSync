#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg_str) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<serde_json::Value>(msg_str);
    }
});
