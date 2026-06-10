use criterion::{criterion_group, criterion_main, Criterion, black_box};
use std::sync::Arc;
use vaultsync_core::coordinator::traits::{Coordinator, EncryptedMutation};
use vaultsync_core::coordinator::memory::InMemoryCoordinator;

fn bench_coordinator_push_pull(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let coord = Arc::new(InMemoryCoordinator::new());
    let ns = "bench-ns";

    c.bench_function("coordinator_push_pull", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mutations = (0..50).map(|i| EncryptedMutation {
                    id: format!("m-{}", i),
                    namespace: ns.to_string(),
                    replica_id: "rep-1".to_string(),
                    doc_id: "d".to_string(),
                    record_id: "r".to_string(),
                    encrypted_blob: vec![1, 2, 3],
                    timestamp: 1000 + i,
                    schema_version: 0,
                    key_version: 1,
                }).collect::<Vec<_>>();

                let seqs = coord.push(ns, mutations).await.unwrap();
                let _pulled = coord.pull(ns, 0, 50).await.unwrap();
                black_box(seqs);
            });
        })
    });
}

criterion_group!(benches, bench_coordinator_push_pull);
criterion_main!(benches);
