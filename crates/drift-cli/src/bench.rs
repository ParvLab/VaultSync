use clap::Args;
use std::time::{Duration, Instant};

#[derive(Args)]
pub struct BenchArgs {
    /// Scenario: crdt_merge | oplog_append | subscription_fire | e2e_sync
    #[arg(long, default_value = "crdt_merge")]
    pub scenario: String,

    /// Duration in seconds
    #[arg(long, default_value = "10")]
    pub duration: u64,

    /// Fixed iterations (overrides duration)
    #[arg(long)]
    pub iterations: Option<u64>,
}

struct BenchResult {
    iterations: u64,
    elapsed: Duration,
    ns_per_op: f64,
    ops_per_sec: f64,
}

impl BenchResult {
    fn print(&self, name: &str) {
        println!("=== {} ===", name);
        println!("  iterations : {}", self.iterations);
        println!("  total time : {:?}", self.elapsed);
        println!("  ops/sec    : {:.0}", self.ops_per_sec);
        println!("  ns/op      : {:.1}", self.ns_per_op);
    }
}

fn run_bench<F: FnMut()>(
    mut f: F,
    duration: Duration,
    iterations: Option<u64>,
) -> BenchResult {
    let deadline = Instant::now() + duration;
    let start = Instant::now();
    let mut count = 0u64;
    loop {
        if let Some(n) = iterations {
            if count >= n { break; }
        } else if Instant::now() >= deadline {
            break;
        }
        f();
        count += 1;
    }
    let elapsed = start.elapsed();
    BenchResult {
        iterations: count,
        elapsed,
        ns_per_op: elapsed.as_nanos() as f64 / count.max(1) as f64,
        ops_per_sec: count as f64 / elapsed.as_secs_f64(),
    }
}

pub fn run(args: BenchArgs) {
    let duration = Duration::from_secs(args.duration);
    let iters = args.iterations;

    match args.scenario.as_str() {
        "crdt_merge" => {
            use drift_core::crdt::{document::CRDTDocument, types::CrdtValue};
            let mut doc = CRDTDocument::new("bench", "record:1", 0);
            let mut i = 0u64;
            let r = run_bench(|| {
                doc.set_field(&format!("field_{i}"), CrdtValue::String(format!("v{i}")));
                i += 1;
            }, duration, iters);
            r.print("crdt_merge");
        }
        "oplog_append" => {
            // In-memory oplog append benchmark
            let rt = tokio::runtime::Runtime::new().unwrap();
            use drift_core::storage::memory::InMemoryStorage;
            use drift_core::storage::traits::Storage;
            use drift_core::oplog::entry::{OplogEntry, MutationType, SyncStatus};
            let storage = InMemoryStorage::new();
            let mut i = 0u64;
            let r = run_bench(|| {
                let entry = OplogEntry {
                    id: format!("id_{i}"),
                    replica_id: "bench-replica".to_string(),
                    namespace: "bench".to_string(),
                    mutation_type: MutationType::CrdtInsert,
                    doc_id: "bench".to_string(),
                    record_id: "record:1".to_string(),
                    yrs_update: vec![1, 2, 3],
                    encrypted_blob: None,
                    timestamp: 1000,
                    sequence: None,
                    sync_status: SyncStatus::Pending,
                    synced_at: None,
                    created_at: 1000,
                };
                rt.block_on(async {
                    let _ = storage.append_oplog(&entry).await;
                });
                i += 1;
            }, duration, iters);
            r.print("oplog_append");
        }
        "subscription_fire" => {
            use drift_core::subscription::engine::SubscriptionEngine;
            let engine = SubscriptionEngine::new();
            let r = run_bench(|| {
                engine.fire("todos", "record:1", &std::collections::HashMap::new());
            }, duration, iters);
            r.print("subscription_fire");
        }
        "e2e_sync" => {
            println!("e2e_sync requires a live coordinator. Use: cargo bench -p drift-core");
        }
        other => {
            eprintln!("Unknown scenario: {other}");
            eprintln!("Available: crdt_merge | oplog_append | subscription_fire | e2e_sync");
            std::process::exit(1);
        }
    }
}
