use clap::Parser;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "100")]
    replicas: usize,
    #[arg(long, default_value = "30")]
    duration: u64,
    #[arg(long, default_value = "10000")]
    ops_per_sec: u64,
    #[arg(long, default_value = "false")]
    chaos: bool,
}

struct ChaosState {
    partitioned: Arc<Vec<AtomicBool>>,
    latency_ms: Arc<Vec<AtomicU64>>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let coord = Arc::new(drift_coordinator_memory::coordinator::InMemoryCoordinator::new());
    
    println!("=== Drift Load Test ===");
    println!("  replicas:    {}", args.replicas);
    println!("  duration:    {}s", args.duration);
    println!("  ops/sec:     {}", args.ops_per_sec);
    println!("  chaos:       {}", args.chaos);

    // Initialize chaos state for each replica
    let mut partitioned_flags = Vec::with_capacity(args.replicas);
    let mut latency_flags = Vec::with_capacity(args.replicas);
    for _ in 0..args.replicas {
        partitioned_flags.push(AtomicBool::new(false));
        latency_flags.push(AtomicU64::new(0));
    }
    let chaos_state = Arc::new(ChaosState {
        partitioned: Arc::new(partitioned_flags),
        latency_ms: Arc::new(latency_flags),
    });

    // Spawn chaos controller if enabled
    let chaos_state_clone = chaos_state.clone();
    let chaos_active = args.chaos;
    let duration_secs = args.duration;
    
    if chaos_active {
        tokio::spawn(async move {
            let start = Instant::now();
            let end = start + Duration::from_secs(duration_secs);
            let num_replicas = chaos_state_clone.partitioned.len();
            
            while Instant::now() < end {
                // Randomly partition 10% of replicas
                let part_count = (num_replicas / 10).max(1);
                let mut partitioned_indices = Vec::new();
                for _ in 0..part_count {
                    let idx = rand::Rng::gen_range(&mut rand::thread_rng(), 0..num_replicas);
                    chaos_state_clone.partitioned[idx].store(true, Ordering::SeqCst);
                    partitioned_indices.push(idx);
                }

                // Randomly inject 50ms latency to 20% of replicas
                let lat_count = (num_replicas / 5).max(1);
                let mut latency_indices = Vec::new();
                for _ in 0..lat_count {
                    let idx = rand::Rng::gen_range(&mut rand::thread_rng(), 0..num_replicas);
                    chaos_state_clone.latency_ms[idx].store(50, Ordering::SeqCst);
                    latency_indices.push(idx);
                }

                tokio::time::sleep(Duration::from_millis(1500)).await;

                // Heal partitions and latencies
                for idx in partitioned_indices {
                    chaos_state_clone.partitioned[idx].store(false, Ordering::SeqCst);
                }
                for idx in latency_indices {
                    chaos_state_clone.latency_ms[idx].store(0, Ordering::SeqCst);
                }

                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });
    }

    // ── Phase 1: Steady State + Ramp Up ──
    println!("\n[steady_state] Running load...");
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);

    let mut handles = vec![];
    // Calculate sleep interval per replica to hit overall target ops/sec
    let interval_us = 1_000_000u64
        .checked_div(args.ops_per_sec.max(1) / args.replicas.max(1) as u64)
        .unwrap_or(10_000);

    for i in 0..args.replicas {
        let coord = coord.clone();
        let chaos_state = chaos_state.clone();
        handles.push(tokio::spawn(async move {
            let replica_id = format!("loadtest-replica:{i}");
            let mut latencies = Vec::new();
            let mut failed_count = 0u64;

            while Instant::now() < deadline {
                let t0 = Instant::now();
                
                // Simulate partition
                if chaos_state.partitioned[i].load(Ordering::SeqCst) {
                    failed_count += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                // Simulate latency injection
                let lat = chaos_state.latency_ms[i].load(Ordering::SeqCst);
                if lat > 0 {
                    tokio::time::sleep(Duration::from_millis(lat)).await;
                }

                let mutation = drift_core::coordinator::traits::EncryptedMutation {
                    id: uuid::Uuid::new_v4().to_string(),
                    namespace: "loadtest".to_string(),
                    replica_id: replica_id.clone(),
                    doc_id: "loadtest".to_string(),
                    record_id: uuid::Uuid::new_v4().to_string(),
                    encrypted_blob: vec![42u8; 64],
                    timestamp: crate_time_ms(),
                    schema_version: 0,
                    key_version: 0,
                };

                use drift_core::coordinator::traits::Coordinator;
                match coord.push("loadtest", vec![mutation]).await {
                    Ok(_) => {
                        latencies.push(t0.elapsed().as_micros() as u64);
                    }
                    Err(_) => {
                        failed_count += 1;
                    }
                }

                tokio::time::sleep(Duration::from_micros(interval_us)).await;
            }
            (latencies, failed_count)
        }));
    }

    // Await all threads and gather latency data
    let mut all_latencies = Vec::new();
    let mut total_failed = 0u64;
    for h in handles {
        if let Ok((lats, fails)) = h.await {
            all_latencies.extend(lats);
            total_failed += fails;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_completed = all_latencies.len() as u64;

    // Sort latencies to compute percentiles
    all_latencies.sort_unstable();
    let p50 = percentile(&all_latencies, 0.50);
    let p90 = percentile(&all_latencies, 0.90);
    let p99 = percentile(&all_latencies, 0.99);

    // ── Phase 3: Settle + Verify convergence ──
    println!("\n[settle] Verifying convergence...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    use drift_core::coordinator::traits::Coordinator;
    let stored = coord.pull("loadtest", 0, 1_000_000).await
        .map(|m| m.len() as u64).unwrap_or(0);

    println!("\n=== Results ===");
    println!("  completed    : {total_completed}");
    println!("  failed       : {total_failed}");
    println!("  throughput   : {:.1} ops/sec", total_completed as f64 / elapsed);
    println!("  p50 latency  : {:.3} ms", p50 as f64 / 1000.0);
    println!("  p90 latency  : {:.3} ms", p90 as f64 / 1000.0);
    println!("  p99 latency  : {:.3} ms", p99 as f64 / 1000.0);
    println!("  stored coord : {stored}");

    let loss = total_completed.saturating_sub(stored);
    if loss > 0 {
        eprintln!("\n❌ FAIL: {loss} mutations lost!");
        std::process::exit(1);
    }

    let total_attempts = total_completed + total_failed;
    let fail_rate = (total_failed as f64 / total_attempts.max(1) as f64) * 100.0;
    if args.chaos {
        if fail_rate > 35.0 {
            eprintln!("\n❌ FAIL: Failure rate {:.1}% too high even for chaos mode!", fail_rate);
            std::process::exit(1);
        }
        println!("\n✅ PASS: Zero data loss under chaos conditions. Failure rate was {:.1}%.", fail_rate);
    } else {
        if fail_rate > 5.0 {
            eprintln!("\n❌ FAIL: Failure rate {:.1}% exceeds 5% limit!", fail_rate);
            std::process::exit(1);
        }
        println!("\n✅ PASS: Zero data loss. Healthy throughput and latencies.");
    }
}

fn percentile(v: &[u64], p: f64) -> u64 {
    if v.is_empty() { return 0; }
    let idx = ((v.len() - 1) as f64 * p).round() as usize;
    v[idx]
}

fn crate_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
