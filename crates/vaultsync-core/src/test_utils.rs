//! Test utilities: VaultSyncFixture, SimulatedNetwork, DeterministicClock.
//! Available under #[cfg(any(test, feature = "test-utils"))]

use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Duration;

use crate::{
    coordinator::traits::{Coordinator, EncryptedMutation},
    crdt::{document::CRDTDocument, types::CrdtValue},
    error::VaultSyncError,
    oplog::entry::{OplogEntry, SyncStatus, MutationType},
    storage::traits::Storage,
    sync::state::SyncState,
};

// ─────────────────────────────────────────────────────────────────────────────
// DeterministicClock
// ─────────────────────────────────────────────────────────────────────────────

/// A clock whose time only advances when you call `advance()`.
/// Prevents flaky timing-dependent tests.
#[derive(Clone, Debug)]
pub struct DeterministicClock {
    time_ms: Arc<AtomicU64>,
}

impl DeterministicClock {
    pub fn new() -> Self {
        Self { time_ms: Arc::new(AtomicU64::new(1_000_000_000)) }
    }

    /// Advance the clock by `ms` milliseconds.
    pub fn advance(&self, ms: u64) {
        self.time_ms.fetch_add(ms, Ordering::SeqCst);
    }

    /// Return current time in milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.time_ms.load(Ordering::SeqCst)
    }
}

impl Default for DeterministicClock {
    fn default() -> Self { Self::new() }
}

// ─────────────────────────────────────────────────────────────────────────────
// VaultSyncFixture
// ─────────────────────────────────────────────────────────────────────────────

/// Pre-configured VaultSync test fixture with isolated storage, mock coordinator,
/// and deterministic clock.
#[derive(Debug)]
pub struct VaultSyncFixture {
    pub storage: Arc<dyn Storage>,
    pub coordinator: Arc<dyn Coordinator>,
    pub clock: DeterministicClock,
    pub namespace: String,
    pub replica_id: String,
}

impl VaultSyncFixture {
    pub fn new(storage: Arc<dyn Storage>, coordinator: Arc<dyn Coordinator>) -> Self {
        Self {
            storage,
            coordinator,
            clock: DeterministicClock::new(),
            namespace: "test-ns".to_string(),
            replica_id: format!("test-replica-{}", uuid::Uuid::new_v4()),
        }
    }

    pub fn with_namespace(mut self, ns: &str) -> Self {
        self.namespace = ns.to_string();
        self
    }

    pub fn with_replica_id(mut self, id: &str) -> Self {
        self.replica_id = id.to_string();
        self
    }

    /// Write a CRDT field update into local storage and oplog.
    pub async fn write(
        &self,
        doc_id: &str,
        record_id: &str,
        fields: HashMap<String, CrdtValue>,
    ) -> Result<(), VaultSyncError> {
        let existing = self.storage.get_document(doc_id, record_id).await?;
        let mut doc = match existing {
            Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
            None => CRDTDocument::new(doc_id, record_id, 0),
        };

        for (k, v) in &fields {
            doc.set_field(k, v.clone());
        }
        let snapshot = doc.to_snapshot();

        let update = doc.encode_update();
        let entry = OplogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: self.replica_id.clone(),
            namespace: self.namespace.clone(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: doc_id.to_string(),
            record_id: record_id.to_string(),
            yrs_update: update,
            encrypted_blob: None,
            timestamp: self.clock.now_ms(),
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: self.clock.now_ms(),
        };
        self.storage.write_document_and_oplog(doc_id, record_id, &snapshot, &entry).await?;
        Ok(())
    }

    /// Flush pending ops to coordinator.
    pub async fn upload(&self) -> Result<usize, VaultSyncError> {
        let pending = self.storage
            .read_pending_oplog(&self.namespace, 1000).await?;
        let count = pending.len();
        for entry in &pending {
            let mutation = EncryptedMutation {
                id: entry.id.clone(),
                namespace: entry.namespace.clone(),
                replica_id: self.replica_id.clone(),
                doc_id: entry.doc_id.clone(),
                record_id: entry.record_id.clone(),
                encrypted_blob: entry.yrs_update.clone(),
                timestamp: entry.timestamp,
                schema_version: 0,
                key_version: 0,
            };
            if let Ok(seqs) = self.coordinator
                .push(&self.namespace, vec![mutation]).await
            {
                if let Some(&seq) = seqs.first() {
                    let _ = self.storage.mark_synced(&entry.id, seq).await;
                }
            }
        }
        Ok(count)
    }

    /// Pull from coordinator and merge into local storage.
    pub async fn download(&self) -> Result<usize, VaultSyncError> {
        let sync_state = self.storage.read_sync_state(&self.namespace).await?
            .unwrap_or_else(|| SyncState {
                namespace: self.namespace.clone(),
                replica_id: self.replica_id.clone(),
                last_synced_sequence: 0,
                connection_status: crate::sync::state::ConnectionStatus::Connected,
                leader_status: Some(true),
                last_connected_at: None,
                last_sync_at: None,
                schema_version: 0,
            });
        let mutations = self.coordinator
            .pull(&self.namespace, sync_state.last_synced_sequence, 1000)
            .await
            .map_err(|e| VaultSyncError::Coordinator(format!("{e:?}")))?;
        let count = mutations.len();
        let mut max_seq = sync_state.last_synced_sequence;
        for m in &mutations {
            let doc_id = &m.doc_id;
            let record_id = &m.record_id;
            let existing = self.storage.get_document(doc_id, record_id).await?;
            let mut doc = match existing {
                Some(bytes) => CRDTDocument::from_snapshot(&bytes)?,
                None => CRDTDocument::new(doc_id, record_id, 0),
            };
            doc.apply_update(&m.encrypted_blob)?;
            let snapshot = doc.to_snapshot();
            let entry = OplogEntry {
                id: m.id.clone(),
                replica_id: self.replica_id.clone(), // or mutation replica id
                namespace: self.namespace.clone(),
                mutation_type: MutationType::CrdtUpdate,
                doc_id: doc_id.clone(),
                record_id: record_id.clone(),
                yrs_update: m.encrypted_blob.clone(),
                encrypted_blob: None,
                timestamp: m.timestamp,
                sequence: Some(m.sequence),
                sync_status: SyncStatus::Synced,
                synced_at: Some(self.clock.now_ms() / 1000),
                created_at: self.clock.now_ms(),
            };
            self.storage.write_document_and_oplog(doc_id, record_id, &snapshot, &entry).await?;
            if m.sequence > max_seq {
                max_seq = m.sequence;
            }
        }
        if max_seq > sync_state.last_synced_sequence {
            let mut new_state = sync_state;
            new_state.last_synced_sequence = max_seq;
            self.storage.write_sync_state(&new_state).await?;
        }
        Ok(count)
    }

    /// Full sync cycle: upload then download.
    pub async fn sync(&self) -> Result<(), VaultSyncError> {
        self.upload().await?;
        self.download().await?;
        Ok(())
    }

    /// Get current document state.
    pub async fn get_document(
        &self,
        doc_id: &str,
        record_id: &str,
    ) -> Option<CRDTDocument> {
        if let Ok(Some(bytes)) = self.storage.get_document(doc_id, record_id).await {
            CRDTDocument::from_snapshot(&bytes).ok()
        } else {
            None
        }
    }

    /// Assert this fixture's state matches another's for a given document.
    pub async fn assert_converges_with(
        &self,
        other: &VaultSyncFixture,
        doc_id: &str,
        record_id: &str,
    ) {
        let a = self.get_document(doc_id, record_id).await;
        let b = other.get_document(doc_id, record_id).await;
        match (a, b) {
            (Some(da), Some(db)) => assert_eq!(
                da.to_map(), db.to_map(),
                "CRDT state diverged between fixtures"
            ),
            (None, None) => {}
            _ => panic!("One fixture has the document, the other does not"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fault trait and built-in fault types
// ─────────────────────────────────────────────────────────────────────────────

pub trait Fault: Send + Sync {
    fn apply(&self, replica: &mut SimulatedReplica);
    fn remove(&self, replica: &mut SimulatedReplica);
}

pub struct NetworkPartitionFault {
    pub target_id: String,
}

impl Fault for NetworkPartitionFault {
    fn apply(&self, replica: &mut SimulatedReplica) {
        if replica.id == self.target_id {
            replica.connected = false;
        }
    }
    fn remove(&self, replica: &mut SimulatedReplica) {
        if replica.id == self.target_id {
            replica.connected = true;
        }
    }
}

pub struct AllPartitionFault;

impl Fault for AllPartitionFault {
    fn apply(&self, replica: &mut SimulatedReplica) { replica.connected = false; }
    fn remove(&self, replica: &mut SimulatedReplica) { replica.connected = true; }
}

// ─────────────────────────────────────────────────────────────────────────────
// SimulatedReplica
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SimulatedReplica {
    pub id: String,
    pub fixture: VaultSyncFixture,
    pub connected: bool,
}

impl SimulatedReplica {
    pub fn disconnect(&mut self) { self.connected = false; }

    pub async fn reconnect(&mut self) {
        self.connected = true;
        let _ = self.flush().await;
    }

    pub async fn write(
        &self,
        doc_id: &str,
        record_id: &str,
        fields: HashMap<String, CrdtValue>,
    ) -> Result<(), VaultSyncError> {
        self.fixture.write(doc_id, record_id, fields).await?;
        if self.connected {
            let _ = self.fixture.upload().await;
        }
        Ok(())
    }

    pub async fn get_document(&self, doc_id: &str, record_id: &str) -> Option<CRDTDocument> {
        self.fixture.get_document(doc_id, record_id).await
    }

    pub async fn flush(&self) -> Result<usize, VaultSyncError> {
        if !self.connected { return Ok(0); }
        self.fixture.upload().await
    }

    pub async fn pull(&self) -> Result<usize, VaultSyncError> {
        if !self.connected { return Ok(0); }
        self.fixture.download().await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SimulatedNetwork
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SimulatedNetwork {
    pub coordinator: Arc<dyn Coordinator>,
    pub replicas: Vec<SimulatedReplica>,
}

impl SimulatedNetwork {
    pub fn new(coordinator: Arc<dyn Coordinator>) -> Self {
        Self {
            coordinator,
            replicas: Vec::new(),
        }
    }

    pub fn add_replica(
        &mut self,
        id: &str,
        storage: Arc<dyn Storage>,
    ) {
        let fixture = VaultSyncFixture::new(storage, self.coordinator.clone())
            .with_replica_id(id);

        self.replicas.push(SimulatedReplica {
            id: id.to_string(),
            fixture,
            connected: true,
        });
    }

    pub fn inject_fault(&mut self, fault: &dyn Fault) {
        for replica in &mut self.replicas {
            fault.apply(replica);
        }
    }

    pub fn remove_fault(&mut self, fault: &dyn Fault) {
        for replica in &mut self.replicas {
            fault.remove(replica);
        }
    }

    /// All connected replicas upload then download.
    pub async fn sync_all(&mut self) {
        for replica in self.replicas.iter() {
            if replica.connected {
                let _ = replica.flush().await;
            }
        }
        for replica in self.replicas.iter() {
            if replica.connected {
                let _ = replica.pull().await;
            }
        }
    }

    /// Sync with timeout.
    pub async fn sync_all_with_timeout(&mut self, timeout: Duration) -> Result<(), VaultSyncError> {
        tokio::time::timeout(timeout, async { self.sync_all().await })
            .await
            .map_err(|_| VaultSyncError::Coordinator("sync_all timed out".to_string()))
    }

    /// Assert all replicas have identical CRDT state for doc/record.
    pub async fn assert_all_converge(&self, doc_id: &str, record_id: &str) {
        let mut states = Vec::new();
        for r in &self.replicas {
            states.push(r.get_document(doc_id, record_id).await);
        }

        let snapshots: Vec<_> = states.iter()
            .map(|d| d.as_ref().map(|doc| doc.to_map()))
            .collect();

        for (i, s) in snapshots[1..].iter().enumerate() {
            assert_eq!(
                &snapshots[0], s,
                "Replica {} (id: {}) diverged from replica 0 (id: {})",
                i + 1, self.replicas[i + 1].id, self.replicas[0].id
            );
        }
    }
}
