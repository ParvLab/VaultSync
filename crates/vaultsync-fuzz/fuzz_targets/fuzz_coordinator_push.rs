#![no_main]
use libfuzzer_sys::fuzz_target;
use vaultsync_core::coordinator::memory::InMemoryCoordinator;
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation};

fuzz_target!(|data: &[u8]| {
    if let Ok(mutations) = serde_json::from_slice::<Vec<EncryptedMutation>>(data) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let coord = InMemoryCoordinator::new();
        let _ = rt.block_on(async {
            let _ = coord.push("fuzz-ns", mutations).await;
        });
    }
});
