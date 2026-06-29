use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use vaultsync_core::crdt::document::CRDTDocument;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor, KeyRing};
use vaultsync_core::oplog::entry::{MutationOrigin, MutationType, OplogEntry, SyncStatus};
use vaultsync_core::storage::sqlite::SQLiteStorage;
use vaultsync_core::storage::traits::Storage;

fn bench_crdt_insert(c: &mut Criterion) {
    c.bench_function("crdt_insert_1000", |b| {
        b.iter(|| {
            let mut doc = CRDTDocument::new("doc-1", "rec-1", 0);
            for i in 0..1000 {
                doc.set_field(&format!("key-{}", i), CrdtValue::Number(i as f64));
            }
            black_box(doc)
        })
    });
}

fn bench_crdt_merge(c: &mut Criterion) {
    c.bench_function("crdt_merge_500_concurrent", |b| {
        b.iter_with_setup(
            || {
                let mut base = CRDTDocument::new("doc-1", "rec-1", 0);
                base.set_field("init", CrdtValue::Boolean(true));
                let base_snapshot = base.to_snapshot();

                let mut replica_a = CRDTDocument::from_snapshot(&base_snapshot).unwrap();
                let mut replica_b = CRDTDocument::from_snapshot(&base_snapshot).unwrap();

                let mut updates_a = Vec::new();
                let mut updates_b = Vec::new();

                for i in 0..500 {
                    updates_a.push(
                        replica_a
                            .set_field(&format!("alice-key-{}", i), CrdtValue::Number(i as f64)),
                    );
                    updates_b.push(
                        replica_b.set_field(&format!("bob-key-{}", i), CrdtValue::Number(i as f64)),
                    );
                }
                (replica_a, replica_b, updates_a, updates_b)
            },
            |(mut replica_a, mut replica_b, updates_a, updates_b)| {
                for u in updates_b {
                    replica_a.apply_update(&u).unwrap();
                }
                for u in updates_a {
                    replica_b.apply_update(&u).unwrap();
                }
                black_box((replica_a, replica_b))
            },
        )
    });
}

fn bench_encrypt_decrypt(c: &mut Criterion) {
    let keyring = Arc::new(KeyRing::generate());
    let encryptor = E2eeEncryptor::new(keyring.clone());
    let decryptor = E2eeDecryptor::new(keyring.clone());
    let ns = "bench-ns";

    let mut doc = CRDTDocument::new("doc-1", "rec-1", 0);
    doc.set_field(
        "title",
        CrdtValue::String("benchmarking encrypt decrypt".to_string()),
    );
    let snapshot = doc.to_snapshot();

    c.bench_function("encrypt_decrypt_roundtrip", |b| {
        b.iter(|| {
            let encrypted = encryptor.encrypt_symmetric(&snapshot, ns).unwrap();
            let decrypted = decryptor.decrypt_symmetric(&encrypted, ns).unwrap();
            black_box(decrypted)
        })
    });
}

fn bench_oplog_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("oplog_append_100", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = tempfile::tempdir().unwrap();
                let db_path = temp_dir
                    .path()
                    .join("bench_append.db")
                    .to_string_lossy()
                    .to_string();
                let storage = SQLiteStorage::new(&db_path).unwrap();
                (temp_dir, storage)
            },
            |(_temp_dir, storage)| {
                rt.block_on(async {
                    for i in 0..100 {
                        let entry = OplogEntry {
                            id: format!("e-{}", i),
                            replica_id: "rep-1".to_string(),
                            namespace: "ns".to_string(),
                            mutation_type: MutationType::CrdtUpdate,
                            doc_id: "d".to_string(),
                            record_id: "r".to_string(),
                            yrs_update: vec![1, 2, 3],
                            encrypted_blob: None,
                            timestamp: 1000,
                            sequence: None,
                            sync_status: SyncStatus::Pending,
                            synced_at: None,
                            created_at: 1000,
                            origin: MutationOrigin::Unknown,
                            origin_context: String::new(),
                        };
                        storage.append_oplog(&entry).await.unwrap();
                    }
                });
            },
        )
    });
}

fn bench_oplog_read_pending(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("oplog_read_pending_100", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = tempfile::tempdir().unwrap();
                let db_path = temp_dir
                    .path()
                    .join("bench_read.db")
                    .to_string_lossy()
                    .to_string();
                let storage = SQLiteStorage::new(&db_path).unwrap();
                rt.block_on(async {
                    for i in 0..100 {
                        let entry = OplogEntry {
                            id: format!("e-{}", i),
                            replica_id: "rep-1".to_string(),
                            namespace: "ns".to_string(),
                            mutation_type: MutationType::CrdtUpdate,
                            doc_id: "d".to_string(),
                            record_id: "r".to_string(),
                            yrs_update: vec![1, 2, 3],
                            encrypted_blob: None,
                            timestamp: 1000,
                            sequence: None,
                            sync_status: SyncStatus::Pending,
                            synced_at: None,
                            created_at: 1000,
                            origin: MutationOrigin::Unknown,
                            origin_context: String::new(),
                        };
                        storage.append_oplog(&entry).await.unwrap();
                    }
                });
                (temp_dir, storage)
            },
            |(_temp_dir, storage)| {
                rt.block_on(async {
                    let pending = storage.read_pending_oplog("ns", 100).await.unwrap();
                    black_box(pending)
                });
            },
        )
    });
}

criterion_group!(
    benches,
    bench_crdt_insert,
    bench_crdt_merge,
    bench_encrypt_decrypt,
    bench_oplog_append,
    bench_oplog_read_pending
);
criterion_main!(benches);
