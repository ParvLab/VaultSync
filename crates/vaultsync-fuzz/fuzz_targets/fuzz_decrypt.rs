#![no_main]
use libfuzzer_sys::fuzz_target;
use vaultsync_core::e2ee::keyring::{E2eeDecryptor, KeyRing};

fuzz_target!(|data: &[u8]| {
    let keyring = std::sync::Arc::new(KeyRing::generate());
    let decryptor = E2eeDecryptor::new(keyring);
    let _ = decryptor.decrypt_symmetric(data, "fuzz-ns");
});
