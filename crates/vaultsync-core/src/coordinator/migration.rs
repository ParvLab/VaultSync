use crate::coordinator::traits::{Coordinator, CoordinatorError, EncryptedMutation};
use std::sync::Arc;

pub struct CoordinatorMigrationManager;

impl CoordinatorMigrationManager {
    /// Migrate all namespace data from one coordinator to another.
    /// This includes schema version, replica registrations, replica public keys, snapshots, and mutations.
    pub async fn migrate<F>(
        from: Arc<dyn Coordinator>,
        to: Arc<dyn Coordinator>,
        namespace: &str,
        chunk_size: usize,
        progress_callback: F,
    ) -> Result<usize, CoordinatorError>
    where
        F: Fn(usize) + Send + Sync + 'static,
    {
        // 1. Migrate Replicas and their Encryption Keys
        let replicas = from.list_replicas(namespace).await?;
        for r in &replicas {
            to.register(namespace, r.clone()).await?;
            if let Some((pub_key, key_ver)) = from.get_replica_key(namespace, &r.replica_id).await?
            {
                to.update_replica_key(namespace, &r.replica_id, pub_key, key_ver)
                    .await?;
            }
        }

        // 2. Migrate Snapshots
        let snapshots = from.list_snapshots(namespace).await?;
        for s in &snapshots {
            to.store_snapshot(namespace, s).await?;
        }

        // 3. Migrate Schema Version
        let schema_version = from.schema_version(namespace).await?;

        // 4. Migrate Mutations in Chunks
        let mut after = 0;
        let mut total_migrated = 0;

        loop {
            let pending_muts = from.pull(namespace, after, chunk_size).await?;
            if pending_muts.is_empty() {
                break;
            }

            let last_seq = pending_muts.last().unwrap().sequence;

            let encrypted_muts: Vec<EncryptedMutation> = pending_muts
                .into_iter()
                .map(|pm| EncryptedMutation {
                    id: pm.id,
                    namespace: pm.namespace,
                    replica_id: "migrated".to_string(),
                    doc_id: pm.doc_id,
                    record_id: pm.record_id,
                    encrypted_blob: pm.encrypted_blob,
                    timestamp: pm.timestamp,
                    schema_version,
                    key_version: pm.key_version,
                })
                .collect();

            let len = encrypted_muts.len();
            to.push(namespace, encrypted_muts).await?;
            total_migrated += len;
            progress_callback(total_migrated);

            after = last_seq;
        }

        Ok(total_migrated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::memory::InMemoryCoordinator;
    use crate::coordinator::traits::ReplicaInfo;

    #[tokio::test]
    async fn test_migration_flow() {
        let from = Arc::new(InMemoryCoordinator::new());
        let to = Arc::new(InMemoryCoordinator::new());
        let ns = "migrate-test";

        // Register replica on from
        from.register(
            ns,
            ReplicaInfo {
                replica_id: "replica-1".to_string(),
                namespace: ns.to_string(),
                public_key: vec![1, 2, 3],
                schema_version: 3,
            },
        )
        .await
        .unwrap();

        // Push some mutations to from
        let mutations = vec![
            EncryptedMutation {
                id: "m-1".to_string(),
                namespace: ns.to_string(),
                replica_id: "replica-1".to_string(),
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                encrypted_blob: vec![10, 20],
                timestamp: 1000,
                schema_version: 3,
                key_version: 1,
            },
            EncryptedMutation {
                id: "m-2".to_string(),
                namespace: ns.to_string(),
                replica_id: "replica-1".to_string(),
                doc_id: "doc-1".to_string(),
                record_id: "rec-2".to_string(),
                encrypted_blob: vec![30, 40],
                timestamp: 1001,
                schema_version: 3,
                key_version: 1,
            },
        ];
        from.push(ns, mutations).await.unwrap();

        // Migrate from -> to
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let migrated = CoordinatorMigrationManager::migrate(
            from.clone(),
            to.clone(),
            ns,
            1, // chunk size of 1 to test pagination
            move |count| {
                counter_clone.store(count, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

        assert_eq!(migrated, 2);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Verify mutations on to
        let pulled = to.pull(ns, 0, 10).await.unwrap();
        assert_eq!(pulled.len(), 2);
        assert_eq!(pulled[0].id, "m-1");
        assert_eq!(pulled[1].id, "m-2");

        // Verify schema version on to
        let ver = to.schema_version(ns).await.unwrap();
        assert_eq!(ver, 3);

        // Verify replica registered on to
        let replicas = to.list_replicas(ns).await.unwrap();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].replica_id, "replica-1");
    }
}
