#![no_main]
use libfuzzer_sys::fuzz_target;
use drift_core::sync::state::SyncState;

fuzz_target!(|data: &[u8]| {
    let _: Result<SyncState, _> = bincode::deserialize(data);
});
