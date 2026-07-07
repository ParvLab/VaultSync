use crate::VaultSyncError;
use crate::storage::traits::Storage;
use crate::storage::document_store::{DocumentStore, MutationQueue};
use crate::storage::metadata_index::MetadataIndex;
use crate::storage::planner::{StoragePlanner, StorageMetrics, PlanAction};
use crate::storage::scheduler::CompactionScheduler;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A composite engine per workspace: DocumentStore + MutationQueue + MetadataIndex + Scheduler.
pub struct WorkspaceEngine<S: Storage> {
    pub namespace: String,
    pub document_store: DocumentStore<S>,
    pub mutation_queue: MutationQueue,
    pub metadata_index: MetadataIndex,
    pub scheduler: CompactionScheduler,
    pub storage: Arc<S>,
    planner: StoragePlanner,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

impl<S: Storage> WorkspaceEngine<S> {
    pub fn new(storage: Arc<S>, namespace: String) -> Self {
        Self {
            document_store: DocumentStore::new(storage.clone(), namespace.clone()),
            mutation_queue: MutationQueue::default(),
            metadata_index: MetadataIndex::new(),
            scheduler: CompactionScheduler::new(),
            storage,
            planner: StoragePlanner,
            namespace,
        }
    }

    pub fn record_read(&mut self, doc_id: &str, now: u64) {
        self.metadata_index.record_read(&self.namespace, doc_id, now);
    }

    pub fn record_write(&mut self, doc_id: &str, now: u64) {
        self.metadata_index.record_write(&self.namespace, doc_id, now);
    }

    /// Build StorageMetrics from current state.
    pub fn build_metrics(&self, total_bytes: u64, idle_ms: u64) -> StorageMetrics {
        StorageMetrics {
            pages: self.document_store.segments().iter().map(|seg| {
                use crate::storage::planner::PageMetrics;
                PageMetrics {
                    page_id: seg.segment_id,
                    byte_size: seg.byte_size,
                    entry_count: seg.doc_count,
                    is_base: true,
                }
            }).collect(),
            total_bytes,
            delta_page_count: 0,
            base_page_count: self.document_store.segment_count(),
            pending_mutation_count: self.mutation_queue.max_pending(), // approximate
            last_compaction_at: 0,
            idle_time_ms: idle_ms,
        }
    }

    /// Run the planner and execute scheduled actions.
    pub async fn tick(&mut self, total_bytes: u64, idle_ms: u64) -> Result<Vec<PlanAction>, VaultSyncError> {
        let metrics = self.build_metrics(total_bytes, idle_ms);
        let planned = self.planner.evaluate(&metrics);
        let now = now_ms();
        let scheduled = self.scheduler.schedule(&planned, &metrics, now);

        let mut executed = Vec::new();
        for sa in scheduled {
            // Execute the action
            self.execute_action(&sa.action).await?;
            self.scheduler.record_execution(&sa.action, now);
            executed.push(sa.action);
        }

        Ok(executed)
    }

    async fn execute_action(&self, action: &PlanAction) -> Result<(), VaultSyncError> {
        match action {
            PlanAction::SplitPage { page_id } => {
                tracing::debug!("[workspace_engine] split page {}", page_id);
                Ok(())
            }
            PlanAction::MergeDeltas { base_id, .. } => {
                tracing::debug!("[workspace_engine] merge deltas for base {}", base_id);
                Ok(())
            }
            PlanAction::GarbageCollect { .. } => {
                tracing::debug!("[workspace_engine] garbage collect");
                Ok(())
            }
            PlanAction::CreateSnapshot { .. } => {
                tracing::debug!("[workspace_engine] create snapshot");
                Ok(())
            }
            PlanAction::FlushManifest => {
                tracing::debug!("[workspace_engine] flush manifest");
                Ok(())
            }
            PlanAction::EvictPages { count } => {
                tracing::debug!("[workspace_engine] evict {} pages", count);
                Ok(())
            }
        }
    }
}

/// Manages multiple WorkspaceEngines.
pub struct WorkspaceEngineManager<S: Storage> {
    engines: Vec<WorkspaceEngine<S>>,
}

impl<S: Storage> WorkspaceEngineManager<S> {
    pub fn new() -> Self {
        Self {
            engines: Vec::new(),
        }
    }

    pub fn get_or_create(&mut self, storage: Arc<S>, namespace: &str) -> &mut WorkspaceEngine<S> {
        if let Some(pos) = self.engines.iter().position(|e| e.namespace == namespace) {
            &mut self.engines[pos]
        } else {
            self.engines.push(WorkspaceEngine::new(storage, namespace.to_string()));
            self.engines.last_mut().unwrap()
        }
    }

    pub fn get(&mut self, namespace: &str) -> Option<&mut WorkspaceEngine<S>> {
        self.engines.iter_mut().find(|e| e.namespace == namespace)
    }

    pub fn remove(&mut self, namespace: &str) {
        self.engines.retain(|e| e.namespace != namespace);
    }

    pub fn engine_count(&self) -> usize {
        self.engines.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::InMemoryStorage;

    fn make_storage() -> Arc<InMemoryStorage> {
        Arc::new(InMemoryStorage::new())
    }

    #[test]
    fn test_engine_creation_and_tick() {
        let storage = make_storage();
        let mut engine = WorkspaceEngine::new(storage, "test_ns".to_string());
        assert_eq!(engine.namespace, "test_ns");
        assert_eq!(engine.document_store.segment_count(), 0);
    }

    #[tokio::test]
    async fn test_tick_with_empty_store() {
        let storage = make_storage();
        let mut engine = WorkspaceEngine::new(storage, "test_ns".to_string());
        let executed = engine.tick(0, 0).await.unwrap();
        assert!(executed.is_empty());
    }

    #[test]
    fn test_engine_manager() {
        let storage = make_storage();
        let mut mgr = WorkspaceEngineManager::<InMemoryStorage>::new();
        assert_eq!(mgr.engine_count(), 0);

        mgr.get_or_create(storage.clone(), "ns1");
        assert_eq!(mgr.engine_count(), 1);

        // Same namespace returns existing
        mgr.get_or_create(storage, "ns1");
        assert_eq!(mgr.engine_count(), 1);

        // Remove works
        mgr.remove("ns1");
        assert_eq!(mgr.engine_count(), 0);
    }
}
