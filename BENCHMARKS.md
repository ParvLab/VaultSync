# VaultSync Performance Benchmarks

This document outlines the performance targets and benchmarks for the VaultSync synchronization engine.

## Performance Targets

| Operation | Target | Measurement |
|---|---|---|
| CRDT merge (1000 ops) | < 10ms | `crdt_bench` |
| Encrypt + decrypt blob | < 1ms per op | `crdt_bench` |
| SQLite coordinator push (batch 100) | < 50ms | `coordinator_bench` |
| WASM bundle size | < 5MB | `wasm-pack` build output |
| Memory coordinator push (batch 100) | < 1ms | `coordinator_bench` |

## Benchmark Suites

The workspace contains three Criterion benchmark suites under `crates/vaultsync-core/benches/`:

1. **`crdt_bench.rs`**
   - Measures raw Yrs document insert and merge performance.
   - Measures symmetric and asymmetric encryption/decryption round-trip latency.
   - Measures oplog storage insertion and retrieval throughput on SQLite.

2. **`sync_bench.rs`**
   - Measures reconciler throughput when merging remote updates into local state.

3. **`coordinator_bench.rs`**
   - Measures push/pull throughput of mutations on the coordinator.

## Running Benchmarks

To run the benchmarks locally, execute:

```bash
cargo bench --workspace
```
