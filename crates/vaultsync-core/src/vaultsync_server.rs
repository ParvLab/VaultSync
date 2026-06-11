use crate::coordinator::http::HttpCoordinatorConfig;
use crate::coordinator::mux_coordinator::{MuxCoordinator, NamespacedCoordinator};
use crate::ipc::leader_election::LeaderElection;
use crate::storage::traits::Storage;
use std::sync::{Arc, Mutex};

pub struct VaultSyncServer {
    pub storage: Arc<dyn Storage>,
    pub leader_election: Arc<LeaderElection>,
    pub namespaces: Mutex<Vec<String>>,
    pub mux_coordinator: Arc<MuxCoordinator>,
}

impl std::fmt::Debug for VaultSyncServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultSyncServer")
            .field("namespaces", &self.namespaces)
            .field("mux_coordinator", &self.mux_coordinator)
            .finish()
    }
}

impl VaultSyncServer {
    pub fn new(storage: Arc<dyn Storage>, config: HttpCoordinatorConfig) -> Self {
        // Shared leader election across all namespaces (spec §27)
        let leader_election = Arc::new(LeaderElection::new(
            "__global__",
            &crate::storage::traits::StorageConfig::InMemory,
        ));
        Self {
            storage,
            leader_election,
            namespaces: Mutex::new(Vec::new()),
            mux_coordinator: Arc::new(MuxCoordinator::new(config)),
        }
    }

    pub fn connect(config: HttpCoordinatorConfig) -> Arc<MuxCoordinator> {
        Arc::new(MuxCoordinator::new(config))
    }

    pub async fn join_namespace(&self, namespace: &str) -> Arc<NamespacedCoordinator> {
        let mut ns_guard = self.namespaces.lock().unwrap();
        if !ns_guard.contains(&namespace.to_string()) {
            ns_guard.push(namespace.to_string());
        }
        self.mux_coordinator.join_namespace(namespace.to_string())
    }

    pub fn is_leader(&self) -> bool {
        self.leader_election.is_leader()
    }
}
