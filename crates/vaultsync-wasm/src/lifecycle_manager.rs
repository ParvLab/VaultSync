use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::maintenance_runtime::MaintenanceRuntime;

/// LifecycleManager — single timer that drives maintenance ticks.
///
/// Created and owned by the client (VaultSyncRuntime). The timer is
/// started explicitly via `start()` and stopped via `stop()`.  No
/// runtime registers its own timer — the LifecycleManager is the
/// sole heartbeat for deferred maintenance work (checkpoint, GC,
/// compaction, health).
///
/// Architecture:
///   Client
///     └── LifecycleManager.spawn()
///           └── every 5s:
///                 ├── MaintenanceRuntime.tick()
///                 └── MaintenanceRuntime.run_deferred()
pub struct LifecycleManager {
    tick_interval_ms: u64,
    running: Arc<AtomicBool>,
    maintenance: std::sync::Mutex<Option<Arc<MaintenanceRuntime>>>,
}

impl LifecycleManager {
    pub fn new(tick_interval_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            tick_interval_ms,
            running: Arc::new(AtomicBool::new(false)),
            maintenance: std::sync::Mutex::new(None),
        })
    }

    /// Register a MaintenanceRuntime to be ticked.
    /// Register a MaintenanceRuntime to be ticked.
    pub fn register_maintenance(self: &Arc<Self>, mr: &Arc<MaintenanceRuntime>) {
        *self.maintenance.lock().unwrap() = Some(mr.clone());
    }

    /// Start the background tick loop.  Spawns a single `wasm_bindgen_futures::spawn_local`
    /// loop that calls `tick()` then `run_deferred()` on the registered runtimes.
    /// Calling `start()` multiple times is a no-op (gated by `running`).
    pub fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // already running
        }
        let this = self.clone();
        let interval_ms = self.tick_interval_ms;
        engine_info!("[LifecycleManager] starting tick loop interval={}ms", interval_ms);
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                if !this.running.load(Ordering::Acquire) {
                    engine_info!("[LifecycleManager] tick loop stopped");
                    break;
                }
                vaultsync_core::time_utils::sleep(Duration::from_millis(interval_ms)).await;
                if !this.running.load(Ordering::Acquire) {
                    break;
                }
                // Tick all registered runtimes
                if let Some(ref mr) = *this.maintenance.lock().unwrap() {
                    mr.tick();
                    mr.run_deferred().await;
                }
            }
        });
    }

    /// Stop the tick loop.  The loop exits on the next sleep completion.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        engine_info!("[LifecycleManager] stop requested");
    }
}

impl Drop for LifecycleManager {
    fn drop(&mut self) {
        self.stop();
    }
}
