use crate::oplog::entry::{MutationType, OplogEntry, SyncStatus};
use crate::storage::traits::{KeyRecord, MigrationRecord, SchemaMeta, Storage};
use crate::sync::state::SyncState;
use std::sync::Arc;

pub async fn run_storage_conformance_suite(storage: Arc<dyn Storage>) {
    // 1. Document CRUD Roundtrips
    {
        let doc_id = "doc-crud";
        let rec_1 = "rec-1";
        let rec_2 = "rec-2";
        let bytes_1 = vec![1, 2, 3];
        let bytes_2 = vec![4, 5, 6];

        // Ensure initially empty
        assert!(storage.get_document(doc_id, rec_1).await.unwrap().is_none());

        // Insert
        storage
            .insert_document(doc_id, rec_1, &bytes_1)
            .await
            .unwrap();
        storage
            .insert_document(doc_id, rec_2, &bytes_2)
            .await
            .unwrap();

        // Get
        assert_eq!(
            storage.get_document(doc_id, rec_1).await.unwrap().unwrap(),
            bytes_1
        );
        assert_eq!(
            storage.get_document(doc_id, rec_2).await.unwrap().unwrap(),
            bytes_2
        );

        // List
        let docs = storage.list_documents(doc_id).await.unwrap();
        assert_eq!(docs.len(), 2);
        let mut map: std::collections::HashMap<String, Vec<u8>> = docs.into_iter().collect();
        assert_eq!(map.remove(rec_1).unwrap(), bytes_1);
        assert_eq!(map.remove(rec_2).unwrap(), bytes_2);

        // Delete
        storage.delete_document(doc_id, rec_1).await.unwrap();
        assert!(storage.get_document(doc_id, rec_1).await.unwrap().is_none());
        assert_eq!(
            storage.get_document(doc_id, rec_2).await.unwrap().unwrap(),
            bytes_2
        );

        // Clean up rec_2
        storage.delete_document(doc_id, rec_2).await.unwrap();
    }

    // 2. Document & Oplog atomic-like writes/deletes
    {
        let ns_atomic = "conformance-ns-atomic";
        let doc_id = "doc-atomic";
        let rec_id = "rec-atomic";
        let bytes = vec![7, 8, 9];
        let entry = OplogEntry {
            id: "atomic-entry".to_string(),
            replica_id: "replica-atomic".to_string(),
            namespace: ns_atomic.to_string(),
            mutation_type: MutationType::CrdtInsert,
            doc_id: doc_id.to_string(),
            record_id: rec_id.to_string(),
            yrs_update: bytes.clone(),
            encrypted_blob: None,
            timestamp: 1000,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: 1000,
        };

        storage
            .write_document_and_oplog(doc_id, rec_id, &bytes, &entry)
            .await
            .unwrap();

        // Verify doc and oplog inserted
        assert_eq!(
            storage.get_document(doc_id, rec_id).await.unwrap().unwrap(),
            bytes
        );
        let pending = storage.read_pending_oplog(ns_atomic, 10).await.unwrap();
        assert!(pending.iter().any(|e| e.id == "atomic-entry"));

        // Delete and oplog
        let del_entry = OplogEntry {
            id: "atomic-del-entry".to_string(),
            replica_id: "replica-atomic".to_string(),
            namespace: ns_atomic.to_string(),
            mutation_type: MutationType::CrdtDelete,
            doc_id: doc_id.to_string(),
            record_id: rec_id.to_string(),
            yrs_update: vec![],
            encrypted_blob: None,
            timestamp: 1001,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: 1001,
        };
        storage
            .delete_document_and_oplog(doc_id, rec_id, &del_entry)
            .await
            .unwrap();

        // Verify doc is deleted
        assert!(storage
            .get_document(doc_id, rec_id)
            .await
            .unwrap()
            .is_none());
        // Verify delete oplog entry is added
        let pending_after = storage.read_pending_oplog(ns_atomic, 10).await.unwrap();
        assert!(pending_after.iter().any(|e| e.id == "atomic-del-entry"));
    }

    // 3. OpLog Queries, Marking, and Ordering
    {
        let ns_oplog = "conformance-ns-oplog";
        let entry1 = OplogEntry {
            id: "e-1".to_string(),
            replica_id: "rep-1".to_string(),
            namespace: ns_oplog.to_string(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: "d-1".to_string(),
            record_id: "r-1".to_string(),
            yrs_update: vec![1],
            encrypted_blob: None,
            timestamp: 2000,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: 2000,
        };
        let entry2 = OplogEntry {
            id: "e-2".to_string(),
            replica_id: "rep-1".to_string(),
            namespace: ns_oplog.to_string(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: "d-1".to_string(),
            record_id: "r-1".to_string(),
            yrs_update: vec![2],
            encrypted_blob: None,
            timestamp: 3000,
            sequence: None,
            sync_status: SyncStatus::Pending,
            synced_at: None,
            created_at: 3000,
        };

        storage.append_oplog(&entry1).await.unwrap();
        storage.append_oplog(&entry2).await.unwrap();

        // Read pending
        let pending = storage.read_pending_oplog(ns_oplog, 10).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, "e-1");
        assert_eq!(pending[1].id, "e-2");

        // Mark synced
        storage.mark_synced("e-1", 10).await.unwrap();
        let pending = storage.read_pending_oplog(ns_oplog, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "e-2");

        // Mark failed
        storage.mark_failed("e-2", "mock error").await.unwrap();
        let pending = storage.read_pending_oplog(ns_oplog, 10).await.unwrap();
        assert_eq!(pending.len(), 0);

        // Read oplog after sequence
        let after = storage
            .read_oplog_after_sequence(ns_oplog, 5)
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].id, "e-1");
        assert_eq!(after[0].sequence, Some(10));
    }

    // 4. Sync State
    {
        let ns_sync = "conformance-ns-sync";
        assert!(storage.read_sync_state(ns_sync).await.unwrap().is_none());

        let state = SyncState {
            namespace: ns_sync.to_string(),
            replica_id: "rep-sync".to_string(),
            last_synced_sequence: 45,
            connection_status: crate::sync::state::ConnectionStatus::Connected,
            leader_status: Some(true),
            last_connected_at: Some(10000),
            last_sync_at: Some(20000),
            schema_version: 2,
        };

        storage.write_sync_state(&state).await.unwrap();
        let read = storage.read_sync_state(ns_sync).await.unwrap().unwrap();
        assert_eq!(read.replica_id, "rep-sync");
        assert_eq!(read.last_synced_sequence, 45);
        assert_eq!(read.leader_status, Some(true));
        assert_eq!(read.schema_version, 2);
    }

    // 5. Schema Meta
    {
        assert!(storage.read_schema("d-schema").await.unwrap().is_none());

        let schema = SchemaMeta {
            doc_id: "d-schema".to_string(),
            version: 3,
            schema_bytes: vec![100, 200],
        };
        storage.write_schema(&schema).await.unwrap();

        let read = storage.read_schema("d-schema").await.unwrap().unwrap();
        assert_eq!(read.version, 3);
        assert_eq!(read.schema_bytes, vec![100, 200]);
    }

    // 6. Migrations Record
    {
        let initial_mig = storage.read_migrations().await.unwrap();
        let initial_len = initial_mig.len();

        let record = MigrationRecord {
            version: "v1.2.3".to_string(),
            applied_at: 123456,
            checksum: "sha256-abc".to_string(),
        };
        storage.write_migration(&record).await.unwrap();

        let read = storage.read_migrations().await.unwrap();
        assert_eq!(read.len(), initial_len + 1);
        let found = read.iter().find(|m| m.version == "v1.2.3").unwrap();
        assert_eq!(found.checksum, "sha256-abc");
    }

    // 7. Key Record
    {
        let ns_keys = "keys-ns";
        assert_eq!(storage.read_keys(ns_keys).await.unwrap().len(), 0);

        let key = KeyRecord {
            namespace: ns_keys.to_string(),
            key_bytes: vec![9, 9, 9],
            version: 1,
        };
        storage.write_key(&key).await.unwrap();

        let keys = storage.read_keys(ns_keys).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_bytes, vec![9, 9, 9]);
    }

    // 8. reset_stale_pending, delete_synced_oplog_older_than, list_tombstoned_documents, update_oplog_encrypted_blob
    {
        let ns_advanced = "ns-advanced";

        // Setup stale failed entry
        let stale_failed = OplogEntry {
            id: "stale-failed-1".to_string(),
            replica_id: "rep-1".to_string(),
            namespace: ns_advanced.to_string(),
            mutation_type: MutationType::CrdtUpdate,
            doc_id: "d-adv".to_string(),
            record_id: "r-adv".to_string(),
            yrs_update: vec![1],
            encrypted_blob: Some(vec![10, 20]),
            timestamp: 100,
            sequence: None,
            sync_status: SyncStatus::Failed,
            synced_at: None,
            created_at: 100, // 100ms epoch - very old
        };
        storage.append_oplog(&stale_failed).await.unwrap();

        // 8a. update_oplog_encrypted_blob
        storage
            .update_oplog_encrypted_blob("stale-failed-1", &[99, 88])
            .await
            .unwrap();

        // 8b. reset_stale_pending
        // should reset "stale-failed-1" back to Pending
        let reset_count = storage
            .reset_stale_pending(ns_advanced, 1000)
            .await
            .unwrap();
        assert_eq!(reset_count, 1);

        let pending = storage.read_pending_oplog(ns_advanced, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "stale-failed-1");
        assert_eq!(pending[0].sync_status, SyncStatus::Pending);
        assert_eq!(pending[0].encrypted_blob, Some(vec![99, 88]));

        // 8c. delete_synced_oplog_older_than
        // Mark as synced first
        // Mark as synced first
        storage.mark_synced("stale-failed-1", 500).await.unwrap();

        // Wait 1.05 seconds so the system clock second advances, guaranteeing synced_at is strictly older than threshold
        tokio::time::sleep(std::time::Duration::from_millis(1050)).await;

        // If we delete synced oplog older than 0 seconds, it will target it.
        let removed = storage
            .delete_synced_oplog_older_than(ns_advanced, 0)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        // 8d. list_tombstoned_documents
        // Insert a CrdtDelete entry in synced status, created at 100ms
        let tombstone_entry = OplogEntry {
            id: "tomb-1".to_string(),
            replica_id: "rep-1".to_string(),
            namespace: ns_advanced.to_string(),
            mutation_type: MutationType::CrdtDelete,
            doc_id: "d-tomb".to_string(),
            record_id: "r-tomb".to_string(),
            yrs_update: vec![1],
            encrypted_blob: None,
            timestamp: 100,
            sequence: Some(600),
            sync_status: SyncStatus::Synced,
            synced_at: Some(10), // 10 seconds epoch
            created_at: 100,     // 100ms epoch
        };
        storage.append_oplog(&tombstone_entry).await.unwrap();

        let tombstones = storage
            .list_tombstoned_documents(ns_advanced, 3600)
            .await
            .unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0], ("d-tomb".to_string(), "r-tomb".to_string()));
    }
}
