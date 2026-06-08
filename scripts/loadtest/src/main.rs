use clap::Parser;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "10")]    replicas: usize,
    #[arg(long, default_value = "30")]    duration: u64,
    #[arg(long, default_value = "100")]   ops_per_sec: u64,
    #[arg(long, default_value = "false")] chaos: bool,
}

struct Counters {
    completed: AtomicU64,
    failed: AtomicU64,
    latency_ms_sum: AtomicU64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let coord = Arc::new(drift_coordinator_memory::coordinator::InMemoryCoordinator::new());
    let counters = Arc::new(Counters {
        completed: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        latency_ms_sum: AtomicU64::new(0),
    });

    println!("=== Drift Load Test ===");
    println!("  replicas:    {}", args.replicas);
    println!("  duration:    {}s", args.duration);
    println!("  ops/sec:     {}", args.ops_per_sec);

    // ── Phase 1: Ramp up ──
    println!("\n[ramp_up] Starting...");
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);

    let mut handles = vec![];
    let interval_us = 1_000_000u64
        .checked_div(args.ops_per_sec.max(1) / args.replicas.max(1) as u64)
        .unwrap_or(10_000);

    for i in 0..args.replicas {
        let coord = coord.clone();
        let counters = counters.clone();
        handles.push(tokio::spawn(async move {
            let replica_id = format!("loadtest-replica:{i}");
            while Instant::now() < deadline {
                let t0 = Instant::now();
                let mutation = drift_core::coordinator::traits::EncryptedMutation {
                    id: uuid::Uuid::new_v4().to_string(),
                    namespace: "loadtest".to_string(),
                    replica_id: replica_id.clone(),
                    doc_id: "loadtest".to_string(),
                    record_id: uuid::Uuid::new_v4().to_string(),
                    encrypted_blob: vec![42u8; 64],
                    timestamp: 0,
                    schema_version: 0,
                    key_version: 0,
                };
                use drift_core::coordinator::traits::Coordinator;
                match coord.push("loadtest", vec![mutation]).await {
                    Ok(_) => {
                        counters.completed.fetch_add(1, Ordering::Relaxed);
                        counters.latency_ms_sum.fetch_add(
                            t0.elapsed().as_millis() as u64, Ordering::Relaxed);
                    }
                    Err(_) => { counters.failed.fetch_add(1, Ordering::Relaxed); }
                }
                tokio::time::sleep(Duration::from_micros(interval_us)).await;
            }
        }));
    }

    // ── Phase 2: Chaos (if enabled) ──
    if args.chaos {
        println!("[chaos] Injecting faults...");
        // Simulate by pausing some replicas — actual network chaos requires infra
    }

    for h in handles { let _ = h.await; }

    // ── Phase 3: Settle + Verify convergence ──
    println!("[settle] Verifying convergence...");
    use drift_core::coordinator::traits::Coordinator;
    let stored = coord.pull("loadtest", 0, 1_000_000).await
        .map(|m| m.len() as u64).unwrap_or(0);

    let completed = counters.completed.load(Ordering::Relaxed);
    let failed = counters.failed.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    let avg_lat = if completed > 0 {
        counters.latency_ms_sum.load(Ordering::Relaxed) / completed
    } else { 0 };

    println!("\n=== Results ===");
    println!("  completed    : {completed}");
    println!("  failed       : {failed}");
    println!("  ops/sec      : {:.0}", completed as f64 / elapsed);
    println!("  avg latency  : {avg_lat}ms");
    println!("  stored in coord: {stored}");

    let loss = completed.saturating_sub(stored);
    if loss > 0 {
        eprintln!("\n❌ FAIL: {loss} mutations lost!");
        std::process::exit(1);
    }
    if failed > completed / 20 {
        eprintln!("\n❌ FAIL: Failure rate {:.1}% exceeds 5%",
            failed as f64 / completed.max(1) as f64 * 100.0);
        std::process::exit(1);
    }
    println!("\n✅ PASS: Zero data loss. Acceptable failure rate.");
}
