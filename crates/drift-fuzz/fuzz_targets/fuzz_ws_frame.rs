#![no_main]
use libfuzzer_sys::fuzz_target;
use drift_core::coordinator::ws_proto::decode_frame;

fuzz_target!(|data: &[u8]| {
    let _ = decode_frame(data);
});
