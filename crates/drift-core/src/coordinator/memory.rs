use async_trait::async_trait;
use futures::Stream;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use super::traits::*;

#[derive(Debug)]
pub struct InMemoryCoordinator {
    next_seq: std::sync::atomic::AtomicU64,
    ops: Arc<RwLock<BTreeMap<(String, u64), PendingMutation>>>,
    schema_versions: Arc<RwLock<std::collections::HashMap<String, u64>>>,
}

impl InMemoryCoordinator {
    pub fn new() -> Self {
        Self {
            next_seq: std::sync::atomic::AtomicU64::new(1),
            ops: Arc::new(RwLock::new(BTreeMap::new())),
            schema_versions: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl Coordinator for InMemoryCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let mut seqs = Vec::new();
        let mut ops = self.ops.write().map_err(|_| CoordinatorError::NotAvailable)?;
        for m in mutations {
            let seq = self.next_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ops.insert((namespace.to_string(), seq), PendingMutation {
                id: m.id,
                namespace: namespace.to_string(),
                sequence: seq,
                doc_id: m.doc_id,
                record_id: m.record_id,
                encrypted_blob: m.encrypted_blob,
                timestamp: m.timestamp,
            });
            seqs.push(seq);
        }
        Ok(seqs)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, _limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let ops = self.ops.read().map_err(|_| CoordinatorError::NotAvailable)?;
        let range = ops.range((namespace.to_string(), after + 1)..);
        Ok(range.take(_limit).map(|(_, v)| v.clone()).collect())
    }

    async fn subscribe(&self, _namespace: &str, _from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let ops = self.ops.read().map_err(|_| CoordinatorError::NotAvailable)?;
        let range = ops.range((_namespace.to_string(), _from_sequence + 1)..);
        for (_, v) in range.take(100) {
            let _ = tx.send(v.clone());
        }
        drop(ops);
        Ok(Box::new(InMemorySubscription { rx: Some(rx) }))
    }

    async fn register(&self, _namespace: &str, _info: ReplicaInfo) -> Result<(), CoordinatorError> { Ok(()) }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> { Ok(()) }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(self.schema_versions.read().map_err(|_| CoordinatorError::NotAvailable)?
            .get(namespace).copied().unwrap_or(0))
    }
}

struct InMemorySubscription {
    rx: Option<tokio::sync::mpsc::UnboundedReceiver<PendingMutation>>,
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
