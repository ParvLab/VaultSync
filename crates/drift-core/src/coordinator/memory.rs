use async_trait::async_trait;
use futures::Stream;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use super::traits::*;

#[derive(Debug)]
pub struct InMemoryCoordinator {
    next_seq: std::sync::atomic::AtomicU64,
    ops: Arc<RwLock<BTreeMap<(String, u64), PendingMutation>>>,
    schema_versions: Arc<RwLock<std::collections::HashMap<String, u64>>>,
    replicas: Arc<RwLock<std::collections::HashMap<(String, String), ReplicaInfo>>>,
    tx: broadcast::Sender<PendingMutation>,
}

impl InMemoryCoordinator {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            next_seq: std::sync::atomic::AtomicU64::new(1),
            ops: Arc::new(RwLock::new(BTreeMap::new())),
            schema_versions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            replicas: Arc::new(RwLock::new(std::collections::HashMap::new())),
            tx,
        }
    }
}

#[async_trait]
impl Coordinator for InMemoryCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let mut seqs = Vec::new();
        let mut to_broadcast = Vec::new();
        let mut ops = self.ops.write().map_err(|_| CoordinatorError::NotAvailable)?;
        for m in mutations {
            let seq = self.next_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let pm = PendingMutation {
                id: m.id,
                namespace: namespace.to_string(),
                sequence: seq,
                doc_id: m.doc_id,
                record_id: m.record_id,
                encrypted_blob: m.encrypted_blob,
                timestamp: m.timestamp,
            };
            ops.insert((namespace.to_string(), seq), pm.clone());
            seqs.push(seq);
            to_broadcast.push(pm);
        }
        drop(ops);

        for pm in to_broadcast {
            let _ = self.tx.send(pm);
        }

        Ok(seqs)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, _limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let ops = self.ops.read().map_err(|_| CoordinatorError::NotAvailable)?;
        let range = ops.range((namespace.to_string(), after + 1)..);
        Ok(range.take(_limit).map(|(_, v)| v.clone()).collect())
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx_mpsc, rx_mpsc) = tokio::sync::mpsc::channel(4096);
        let mut rx_broadcast = self.tx.subscribe();
        
        let ops = self.ops.clone();
        let namespace_str = namespace.to_string();
        
        crate::time_utils::spawn(async move {
            let mut last_sent = from_sequence;
            
            // 1. Collect all existing mutations from the in-memory store while holding the lock
            let mut existing = Vec::new();
            if let Ok(ops_guard) = ops.read() {
                let range = ops_guard.range((namespace_str.clone(), from_sequence + 1)..);
                for ((_, _), v) in range {
                    existing.push(v.clone());
                }
            } // lock is released here
            
            // Send existing mutations
            for v in existing {
                last_sent = last_sent.max(v.sequence);
                if tx_mpsc.send(v).await.is_err() {
                    return;
                }
            }
            
            // 2. Stream new mutations from the broadcast channel
            while let Ok(m) = rx_broadcast.recv().await {
                if m.namespace == namespace_str && m.sequence > last_sent {
                    last_sent = m.sequence;
                    if tx_mpsc.send(m).await.is_err() {
                        return;
                    }
                }
            }
        });
        
        Ok(Box::new(InMemorySubscription { rx: Some(rx_mpsc) }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let mut reps = self.replicas.write().map_err(|_| CoordinatorError::NotAvailable)?;
        reps.insert((namespace.to_string(), info.replica_id.clone()), info);
        Ok(())
    }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> { Ok(()) }

    async fn list_replicas(&self, namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        let reps = self.replicas.read().map_err(|_| CoordinatorError::NotAvailable)?;
        Ok(reps.iter()
            .filter(|((ns, _), _)| ns == namespace)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(self.schema_versions.read().map_err(|_| CoordinatorError::NotAvailable)?
            .get(namespace).copied().unwrap_or(0))
    }
}

struct InMemorySubscription {
    rx: Option<tokio::sync::mpsc::Receiver<PendingMutation>>,
}

impl Stream for InMemorySubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.rx {
            Some(rx) => rx.poll_recv(cx),
            None => Poll::Ready(None),
        }
    }
}
