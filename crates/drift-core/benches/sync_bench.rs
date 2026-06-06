use criterion::{criterion_group, criterion_main, Criterion, black_box};
use std::sync::Arc;
use drift_core::crdt::document::CRDTDocument;
use drift_core::crdt::types::CrdtValue;
use drift_core::oplog::entry::{OplogEntry, MutationType, SyncStatus};
use drift_core::sync::reconciler::Reconciler;
use drift_core::subscription::engine::SubscriptionEngine;
use drift_core::storage::memory::InMemoryStorage;

fn bench_reconciler_apply_update(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("reconciler_apply_update", |b| {
        b.iter_with_setup(|| {
            let storage = Arc::new(InMemoryStorage::new());
            let subs = Arc::new(std::sync::Mutex::new(SubscriptionEngine::new()));
            let reconciler = Reconciler::new(storage, subs);
            
            let mut doc = CRDTDocument::new("doc-1", "rec-1", 0);
            doc.set_field("v", CrdtValue::Number(0.0));
            let snapshot = doc.to_snapshot();
            
            let entry = OplogEntry {
                id: "e-init".to_string(),
                replica_id: "rep-1".to_string(),
                namespace: "ns".to_string(),
                mutation_type: MutationType::CrdtInsert,
                doc_id: "doc-1".to_string(),
                record_id: "rec-1".to_string(),
                yrs_update: snapshot,
                encrypted_blob: None,
                timestamp: 1000,
                sequence: Some(1),
                sync_status: SyncStatus::Synced,
                synced_at: None,
                created_at: 1000,
            };
            (reconciler, entry)
        }, |(reconciler, entry)| {
            rt.block_on(async {
                reconciler.apply_remote_update(&entry).await.unwrap();
                black_box(())
            });
        })
    });
}

criterion_group!(benches, bench_reconciler_apply_update);
criterion_main!(benches);
