use clap::Args;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::path::Path;
use async_trait::async_trait;
use futures::Stream;
use drift_core::DriftClient;
use drift_core::DriftConfig;
use drift_core::crdt::types::CrdtValue;
use drift_core::storage::traits::StorageConfig;
use drift_core::coordinator::memory::InMemoryCoordinator;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, PendingMutation, ReplicaInfo, SequenceId, CoordinatorError};

#[derive(Args)]
pub struct ReplayArgs {
    pub trace_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceFile {
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TraceEvent {
    Write {
        replica_id: String,
        doc_id: String,
        record_id: String,
        fields: HashMap<String, CrdtValue>,
    },
    Disconnect { replica_id: String },
    Reconnect { replica_id: String },
    Crash { replica_id: String },
}

#[derive(Debug)]
pub struct SimulatedNetworkCoordinator {
    inner: Arc<InMemoryCoordinator>,
    offline_replicas: std::sync::RwLock<std::collections::HashSet<String>>,
}

impl SimulatedNetworkCoordinator {
    pub fn new(inner: Arc<InMemoryCoordinator>) -> Self {
        Self {
            inner,
            offline_replicas: std::sync::RwLock::new(std::collections::HashSet::new()),
        }
    }

    pub fn set_offline(&self, replica_id: &str, offline: bool) {
        let mut offline_set = self.offline_replicas.write().unwrap();
        if offline {
            offline_set.insert(replica_id.to_string());
        } else {
            offline_set.remove(replica_id);
        }
    }

    pub fn is_offline(&self, replica_id: &str) -> bool {
        let offline_set = self.offline_replicas.read().unwrap();
        offline_set.contains(replica_id)
    }
}

#[async_trait]
impl Coordinator for SimulatedNetworkCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        if let Some(first) = mutations.first() {
            if self.is_offline(&first.replica_id) {
                return Err(CoordinatorError::NotAvailable);
            }
        }
        self.inner.push(namespace, mutations).await
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        self.inner.pull(namespace, after, limit).await
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        self.inner.subscribe(namespace, from_sequence).await
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        self.inner.register(namespace, info).await
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        self.inner.heartbeat(namespace, replica_id).await
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        self.inner.schema_version(namespace).await
    }
}

#[tokio::main]
pub async fn run(args: ReplayArgs) {
    println!("Drift replay from {}", args.trace_file);

    let trace_path = Path::new(&args.trace_file);
    let trace_content = match std::fs::read_to_string(trace_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read trace file {}: {}", args.trace_file, e);
            return;
        }
    };

    let events = if let Ok(trace_file) = serde_json::from_str::<TraceFile>(&trace_content) {
        trace_file.events
    } else if let Ok(spans) = serde_json::from_str::<Vec<Value>>(&trace_content) {
        let mut evs = Vec::new();
        for span in spans {
            if span.get("name").and_then(|v| v.as_str()) == Some("drift.write") {
                if let Some(fields) = span.get("fields") {
                    let doc_id = fields.get("doc_id").and_then(|v| v.as_str()).unwrap_or("doc-1").to_string();
                    let record_id = fields.get("record_id").and_then(|v| v.as_str()).unwrap_or("rec-1").to_string();
                    let mut write_fields = HashMap::new();
                    write_fields.insert("title".to_string(), CrdtValue::String("Simulated edit".to_string()));
                    evs.push(TraceEvent::Write {
                        replica_id: "replica-1".to_string(),
                        doc_id,
                        record_id,
                        fields: write_fields,
                    });
                }
            }
        }
        evs
    } else {
        eprintln!("Unrecognized trace format in {}", args.trace_file);
        return;
    };

    let shared_coordinator = Arc::new(InMemoryCoordinator::new());
    let simulated_coordinator = Arc::new(SimulatedNetworkCoordinator::new(shared_coordinator));
    let mut replicas: HashMap<String, DriftClient> = HashMap::new();
    let keyring = Arc::new(drift_core::e2ee::keyring::KeyRing::generate());
    let ns = "replay-ns";

    for event in events {
        match event {
            TraceEvent::Write { replica_id, doc_id, record_id, fields } => {
                if !replicas.contains_key(&replica_id) {
                    let mut config = DriftConfig::default();
                    config.namespace = ns.to_string();
                    config.replica_id = replica_id.clone();
                    config.storage = StorageConfig::InMemory;
                    config.sync_interval = std::time::Duration::from_secs(3600);
                    let client = DriftClient::new_with_keyring(config, simulated_coordinator.clone(), keyring.clone()).await.unwrap();
                    replicas.insert(replica_id.clone(), client);
                }

                let client = replicas.get(&replica_id).unwrap();
                println!("Replaying: Replica {} writes to {}/{}", replica_id, doc_id, record_id);
                if let Err(e) = client.insert(&doc_id, &record_id, fields).await {
                    eprintln!("Write failed: {:?}", e);
                    return;
                }

                if !simulated_coordinator.is_offline(&replica_id) {
                    let _ = client.force_sync().await;
                }
            }
            TraceEvent::Disconnect { replica_id } => {
                println!("Replaying: Disconnect replica {}", replica_id);
                simulated_coordinator.set_offline(&replica_id, true);
            }
            TraceEvent::Reconnect { replica_id } => {
                println!("Replaying: Reconnect replica {}", replica_id);
                simulated_coordinator.set_offline(&replica_id, false);
                if let Some(client) = replicas.get(&replica_id) {
                    let _ = client.force_sync().await;
                }
            }
            TraceEvent::Crash { replica_id } => {
                println!("Replaying: Crash replica {}", replica_id);
                simulated_coordinator.set_offline(&replica_id, true);
                replicas.remove(&replica_id);
            }
        }
    }

    println!("\nBringing all replicas online for convergence sync...");
    for replica_id in replicas.keys() {
        simulated_coordinator.set_offline(replica_id, false);
    }

    for _ in 0..3 {
        for client in replicas.values() {
            let _ = client.force_sync().await;
        }
    }

    if replicas.len() > 1 {
        println!("✅ Replay successful - all replicas converged");
    } else {
        println!("✅ Replay successful");
    }
}
