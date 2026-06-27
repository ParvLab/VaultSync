use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub type SequenceId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedMutation {
    pub id: String,
    pub namespace: String,
    pub replica_id: String,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub schema_version: u64,
    pub key_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub sequence: SequenceId,
    pub doc_id: String,
    pub record_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub key_version: u64,
    #[serde(default)]
    pub replica_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub namespace: String,
    pub public_key: Vec<u8>,
    pub schema_version: u64,
}

#[derive(Debug, Clone)]
pub enum CoordinatorError {
    NotAvailable,
    AuthFailed,
    SchemaMismatch,
    Timeout,
    NotSupported(String),
    Internal(String),
}

#[async_trait]
pub trait Coordinator: Send + Sync + std::fmt::Debug {
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError>;
    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError>;
    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError>;
    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
        last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError>;
    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError>;
    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError>;
    /// Forcefully disconnect from the coordinator.
    /// After disconnect, `register()` can be called again to reconnect.
    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        Ok(())
    }
    /// Returns the server's generation ID (UUID) if the coordinator supports it.
    /// Empty string means "generation tracking not supported" — cursor reset is skipped.
    async fn generation_id(&self) -> String {
        String::new()
    }
    async fn update_replica_key(
        &self,
        _namespace: &str,
        _replica_id: &str,
        _public_key: Vec<u8>,
        _key_version: u64,
    ) -> Result<(), CoordinatorError> {
        Ok(())
    }
    async fn get_replica_key(
        &self,
        _namespace: &str,
        _replica_id: &str,
    ) -> Result<Option<(Vec<u8>, u64)>, CoordinatorError> {
        Ok(None)
    }
    async fn list_replicas(&self, _namespace: &str) -> Result<Vec<ReplicaInfo>, CoordinatorError> {
        Ok(vec![])
    }
    async fn get_snapshot(
        &self,
        _namespace: &str,
        _doc_id: &str,
        _record_id: &str,
    ) -> Result<Option<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        Ok(None)
    }
    async fn store_snapshot(
        &self,
        _namespace: &str,
        _snapshot: &crate::crdt::snapshot::Snapshot,
    ) -> Result<(), CoordinatorError> {
        Ok(())
    }
    async fn compact_oplog(
        &self,
        _namespace: &str,
    ) -> Result<crate::sync::compaction::CompactionStats, CoordinatorError> {
        Ok(crate::sync::compaction::CompactionStats::default())
    }
    async fn list_snapshots(
        &self,
        _namespace: &str,
    ) -> Result<Vec<crate::crdt::snapshot::Snapshot>, CoordinatorError> {
        Ok(vec![])
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct LibP2pSubscription {
    rx: tokio::sync::broadcast::Receiver<vaultsync_transport_libp2p::PendingMutation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Stream for LibP2pSubscription {
    type Item = PendingMutation;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::future::Future;
        let fut = self.rx.recv();
        tokio::pin!(fut);
        match fut.poll(cx) {
            std::task::Poll::Ready(Ok(m)) => std::task::Poll::Ready(Some(PendingMutation {
                id: m.id,
                namespace: m.namespace,
                sequence: m.sequence,
                doc_id: m.doc_id,
                record_id: m.record_id,
                encrypted_blob: m.encrypted_blob,
                timestamp: m.timestamp,
                key_version: m.key_version,
                replica_id: m.replica_id,
            })),
            std::task::Poll::Ready(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Ready(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Coordinator for vaultsync_transport_libp2p::LibP2pTransportHandle {
    async fn push(
        &self,
        _namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        for m in mutations {
            let pm = vaultsync_transport_libp2p::PendingMutation {
                id: m.id,
                namespace: m.namespace,
                sequence: 0,
                doc_id: m.doc_id,
                record_id: m.record_id,
                encrypted_blob: m.encrypted_blob,
                timestamp: m.timestamp,
                key_version: m.key_version,
                replica_id: m.replica_id,
            };
            self.broadcast_mutation(pm)
                .await
                .map_err(CoordinatorError::Internal)?;
        }
        Ok(vec![])
    }

    async fn pull(
        &self,
        _namespace: &str,
        _after: SequenceId,
        _limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        Ok(vec![])
    }

    async fn subscribe(
        &self,
        _namespace: &str,
        _from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        Ok(Box::new(LibP2pSubscription {
            rx: self.incoming_broadcast(),
        }))
    }

    async fn register(
        &self,
        _namespace: &str,
        _info: ReplicaInfo,
        _last_sequence: SequenceId,
    ) -> Result<(), CoordinatorError> {
        Ok(())
    }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> {
        Ok(())
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(0)
    }
}
