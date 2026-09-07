use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;

use crate::metrics::RuntimeMetrics;
use crate::storage_runtime::StorageRuntime;
use crate::subscription_index::SubscriptionIndex;
use crate::sync_runtime::SyncRuntime;
use vaultsync_core::runtime_state::RuntimeLifecycle;

/// NamespaceRuntime — isolated per-namespace runtime.
///
/// Each namespace has its own:
/// - StorageRuntime (manifest, ContentIndex, pages/)
/// - SyncRuntime (cursor, generation, pending queue)
/// - Active flag (lazy-loaded, not loaded at startup)
pub struct NamespaceRuntime {
    pub id: u64,
    pub name: String,
    pub storage: Arc<StorageRuntime>,
    pub sync: Arc<SyncRuntime>,
    pub subscription_index: Arc<SubscriptionIndex>,
    active: AtomicBool,
    created_at: AtomicU64,
}

impl std::fmt::Debug for NamespaceRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceRuntime")
            .field("name", &self.name)
            .field("active", &self.active)
            .finish()
    }
}

impl NamespaceRuntime {
    pub fn new(
        name: String,
        storage: Arc<StorageRuntime>,
        sync: Arc<SyncRuntime>,
        subscription_index: Arc<SubscriptionIndex>,
    ) -> Arc<Self> {
        let id = crate::runtime::next_runtime_id();
        engine_info!("[NamespaceRuntime#{}] new ns={} StorageRuntime#{} SyncRuntime#{}", id, name, storage.id, sync.id);
        Arc::new(Self {
            id,
            name,
            storage,
            sync,
            subscription_index,
            active: AtomicBool::new(true),
            created_at: AtomicU64::new(js_sys::Date::now() as u64),
        })
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::Release);
    }

    pub fn age_ms(&self) -> u64 {
        (js_sys::Date::now() as u64).saturating_sub(self.created_at.load(Ordering::Acquire))
    }
}

#[async_trait]
impl RuntimeLifecycle for NamespaceRuntime {
    async fn boot(&self) -> Result<(), String> {
        // Namespace is lazy-loaded — no boot-time work
        Ok(())
    }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> { Ok(()) }
    async fn idle(&self) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> {
        self.set_active(false);
        Ok(())
    }
}

/// WorkspaceRuntime — manages lazy-loaded NamespaceRuntimes.
///
/// Namespaces are opened on user click, not at startup.
/// Unused namespaces can be garbage-collected by MaintenanceRuntime.
pub struct WorkspaceRuntime {
    namespaces: Mutex<HashMap<String, Arc<NamespaceRuntime>>>,
    metrics: Arc<RuntimeMetrics>,
}

impl std::fmt::Debug for WorkspaceRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.namespaces.lock().unwrap().len();
        f.debug_struct("WorkspaceRuntime").field("namespace_count", &count).finish()
    }
}

impl WorkspaceRuntime {
    pub fn new(metrics: Arc<RuntimeMetrics>) -> Arc<Self> {
        Arc::new(Self {
            namespaces: Mutex::new(HashMap::new()),
            metrics,
        })
    }

    pub fn register(&self, ns: Arc<NamespaceRuntime>) {
        self.namespaces.lock().unwrap().insert(ns.name.clone(), ns);
    }

    pub fn get(&self, name: &str) -> Option<Arc<NamespaceRuntime>> {
        self.namespaces.lock().unwrap().get(name).cloned()
    }

    pub fn list(&self) -> Vec<String> {
        self.namespaces.lock().unwrap().keys().cloned().collect()
    }

    pub fn active_count(&self) -> usize {
        self.namespaces.lock().unwrap().values().filter(|ns| ns.is_active()).count()
    }

    pub fn remove(&self, name: &str) {
        self.namespaces.lock().unwrap().remove(name);
    }

    pub fn clear(&self) {
        self.namespaces.lock().unwrap().clear();
    }
}

#[async_trait]
impl RuntimeLifecycle for WorkspaceRuntime {
    async fn boot(&self) -> Result<(), String> {
        engine_trace!("[workspace] boot: no namespaces loaded yet");
        Ok(())
    }
    async fn ready(&self) -> Result<(), String> { Ok(()) }
    async fn warm(&self) -> Result<(), String> { Ok(()) }
    async fn idle(&self) -> Result<(), String> {
        // GC: deactivate namespaces that haven't been touched
        let names: Vec<String> = self.list();
        for name in names {
            if let Some(ns) = self.get(&name) {
                if ns.age_ms() > 300_000 && !ns.is_active() {
                    engine_trace!("[workspace] idle GC: deactivating namespace {}", name);
                    ns.set_active(false);
                }
            }
        }
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), String> {
        self.clear();
        Ok(())
    }
}
