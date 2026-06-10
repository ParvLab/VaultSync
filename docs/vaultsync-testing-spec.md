# VaultSync — Testing & Verification Specification
### Complete Test Strategy for CRDT-Native, E2EE, Multi-Tab Sync Engine

---

## Table of Contents

1. [Testing Philosophy](#1-testing-philosophy)
2. [Testing Pyramid](#2-testing-pyramid)
3. [Unit Testing Strategy](#3-unit-testing-strategy)
4. [Integration Testing Strategy](#4-integration-testing-strategy)
5. [End-to-End (E2E) Testing Strategy](#5-end-to-end-e2e-testing-strategy)
6. [CRDT Correctness Testing](#6-crdt-correctness-testing)
7. [E2EE Correctness Testing](#7-e2ee-correctness-testing)
8. [Multi-Tab & Leader Election Testing](#8-multi-tab--leader-election-testing)
9. [Performance & Benchmark Testing](#9-performance--benchmark-testing)
10. [Fuzz Testing & Property-Based Testing](#10-fuzz-testing--property-based-testing)
11. [Chaos Testing](#11-chaos-testing)
12. [Migration Testing](#12-migration-testing)
13. [Coordinator Conformance Testing](#13-coordinator-conformance-testing)
14. [CI/CD Pipeline Design](#14-cicd-pipeline-design)
15. [Test Fixtures & Factories](#15-test-fixtures--factories)
16. [Test Coverage Targets](#16-test-coverage-targets)
17. [Test Matrix](#17-test-matrix)
18. [Appendix: Failure Mode Catalog](#18-appendix-failure-mode-catalog)

---

## 1. Testing Philosophy

VaultSync's testing strategy is built on five non-negotiable principles:

### 1.1 Deterministic Testing for Correctness

CRDTs guarantee deterministic convergence: given the same set of concurrent mutations applied in any order, all replicas produce identical state. Every CRDT test must verify this property programmatically, not by example.

```
Property: "For all concurrent mutation sequences, all replicas converge"
```

This is tested with property-based testing (`proptest` / `quickcheck`), not hand-written examples.

### 1.2 Every Latency Claim Is a CI Benchmark

The performance numbers in the spec (local read: 1–5 µs, local write: 5–20 µs, etc.) are not targets or hopes. They are Criterion.rs benchmarks that run on every PR. A regression >10% fails the build.

### 1.3 Chaos Is Not Optional

Network failures, process crashes, disk corruption, and clock skew are not edge cases — they are normal operating conditions in local-first sync. VaultSync must survive all of them without data loss.

### 1.4 All Tests Are Runnable by Users

No test requires a Cloudflare account, a specific cloud provider, or internal infrastructure. Users can clone the repo and run the entire test suite locally with `cargo test && npm test`. Coordinator-dependent tests use the SQLite or In-Memory coordinator by default.

### 1.5 CRDT Correctness Is Foundational

CRDT merge is the most critical code path in the engine. If CRDT merge is wrong, every other test is meaningless. CRDT correctness tests run **before** any other test gate in CI.

---

## 2. Testing Pyramid

```
                    ╱╲
                   ╱  ╲
                  ╱ E2E ╲                    ← 5%
                 ╱────────╲
                ╱          ╲
               ╱Integration ╲               ← 25%
              ╱──────────────╲
             ╱                ╲
            ╱   Unit Tests     ╲            ← 55%
           ╱────────────────────╲
          ╱                      ╲
         ╱ CRDT Correctness +    ╲         ← 15%
        ╱ Property-Based Tests    ╲
       ╱────────────────────────────╲
```

| Layer | % of Suite | Purpose | Runtime (CI) |
|---|---|---|---|
| **CRDT Correctness + Property** | 15% | Verify CRDT merge determinism, convergence, safety | 5 min |
| **Unit Tests** | 55% | Individual functions: CRDT merge, encrypt/decrypt, IPC, serialization, subscriptions | 2 min |
| **Integration Tests** | 25% | Component interactions: storage × transport, offline→online, multi-tab sync | 10 min |
| **E2E Tests** | 5% | Browser automation, full-system scenarios, cross-device simulation | 10 min |
| **Chaos Tests** | Parallel | Fault injection: network, crash, disk, clock skew | 15 min |
| **Benchmarks** | Parallel | Criterion performance gates | 5 min |
| **Fuzz Tests** | Parallel | Long-running (24h CI weekly) | 24h |

---

## 3. Unit Testing Strategy

### 3.1 CRDT Merge Unit Tests

Every CRDT merge operation has tests covering:

| Function | Tests |
|---|---|
| `CRDTDocument::apply_update` | Applies single update; applies concurrent updates; applies same update twice (idempotent); applies update from stale state |
| `LWWRegister::merge` | Later timestamp wins; equal timestamps deterministic by replica ID; field-level merge preserves unrelated fields |
| `PNCounter::merge` | Increment from two replicas sums; increment and decrement nets; zero state after equal inc/dec; overflow behavior |
| `ORSet::merge` | Add after remove keeps element; remove after add removes element; concurrent add+add has element once; tombstone GC does not resurrect removed elements |
| `YrsText::merge` | Concurrent inserts at same position (deterministic ordering); concurrent insert + delete (delete wins); concurrent delete + insert (delete wins); insert at position larger than length |
| `YrsArray::merge` | Concurrent inserts at same index; concurrent move operations; insert and delete at same position |

All CRDT unit tests verify **deterministic output**: running the same inputs twice produces byte-identical output. Tests use `#[test]` with explicit CRDT state construction.

### 3.2 E2EE Unit Tests

| Function | Tests |
|---|---|
| `encrypt` | Produces non-empty ciphertext; different plaintexts produce different ciphertexts; same plaintext twice produces different ciphertexts (nonce-based) |
| `decrypt` | Recovers original plaintext; rejects tampered ciphertext (bit flip); rejects wrong key; rejects truncated ciphertext; rejects replayed ciphertext with wrong nonce |
| `key_generate` | Produces valid X25519 keypair; public key derives from private key; two generations produce different keys |
| `key_rotate` | Old key still decrypts old data; new key encrypts new data; both keys coexist |

E2EE tests use known-answer tests (KATs): given specific inputs, verify specific outputs match the libsodium specification.

### 3.3 Multi-Tab IPC Unit Tests

| Function | Tests |
|---|---|
| `LeaderElection::try_acquire` | Acquires lock when free; fails when held; releases on drop; re-acquires after release |
| `LeaderElection::heartbeat` | Writes timestamp; updates heartbeat; reader detects stale heartbeat > timeout |
| `SharedMemory::write` | Writes data to shared region; reader reads same data; concurrent readers do not block |
| `SharedMemory::ring_buffer_push` | Pushes to ring; overwrites oldest when full; reads in FIFO order |
| `LeaderElection::crash_detection` | Detects missing heartbeat (simulated); promotes reader to leader; replays ring buffer on promotion |

IPC tests use in-process shared memory (not OS-level) for determinism. Platform-specific IPC (file lock, BroadcastChannel) is tested in integration tests.

### 3.4 Serialization Unit Tests

| Function | Tests |
|---|---|
| `YrsUpdate::serialize` / `deserialize` | Roundtrip produces identical Yrs document; large updates (1MB) roundtrip; empty update roundtrips |
| `Mutation::to_json` / `from_json` | All fields roundtrip; `encrypted` field is base64; `yrs_update` is base64; `sequence` null when pending |
| `SyncState::serialize` | All fields roundtrip; backwards compatibility with old version (version field) |
| `Schema::serialize` | CRDT type roundtrip; field definitions roundtrip; index definitions roundtrip |

Serialization tests verify that all wire formats are forward- and backward-compatible. A buffer serialized by version X can be deserialized by version X+1.

### 3.5 Storage Abstraction Unit Tests

| Function | Tests |
|---|---|
| `Storage::insert_document` | Inserts CRDT snapshot; reads back same snapshot; idempotent re-insertion |
| `Storage::append_oplog` | Appends mutation; reads back in order; reads by status; reads by namespace |
| `Storage::read_sync_state` | Returns default when empty; returns written state; updates existing state |
| `Storage::transaction_rollback` | Failing transaction does not persist writes; partial write is not visible |
| `Storage::concurrent_readers` | Multiple readers can read simultaneously; readers see committed writes |

Storage abstraction tests are run against every storage backend: SQLite (native and WASM), OPFS (if browser), IndexedDB (if browser), RocksDB (if native), In-Memory. Each backend must pass the same test suite.

### 3.6 Subscription Engine Unit Tests

| Function | Tests |
|---|---|
| `Subscription::register` | Registers callback; returns unique handle; fires on matching CRDT merge |
| `Subscription::filter_match` | Exact match (field = value) fires; partial match fires; non-match does not fire; OR-set contains match fires |
| `Subscription::unregister` | Handle is invalid after unregister; callback does not fire after unregister; double unregister is no-op |
| `Subscription::multi_fire` | 1000 subscriptions on same collection fire within 100ms; each receives correct data |
| `Subscription::no_fire_on_unrelated` | Different collection mutation does not fire unrelated subscription |

### 3.7 OpLog Unit Tests

| Function | Tests |
|---|---|
| `OpLog::append` | Appends mutation; increments total count; preserves insertion order |
| `OpLog::read_pending` | Returns mutations with status=pending; ordered by timestamp; returns empty when none pending |
| `OpLog::mark_synced` | Updates status from pending to synced; sets sequence number; preserves all other fields |
| `OpLog::compaction` | Removes mutations older than retention; keeps mutations newer than retention; keeps pending mutations regardless of age |

---

## 4. Integration Testing Strategy

Integration tests verify interactions between two or more components. They use `TestFixture` (see Section 15) which provides in-memory storage, a mock coordinator, and a deterministic clock.

### 4.1 Storage × Transport Integration

| Test | Setup | Verification |
|---|---|---|
| Write→Encrypt→Upload→Coordinator | Fixture with SQLite storage + Postgres coordinator | Mutation persisted in coordinator after upload |
| Download→Decrypt→Merge→Storage | Coordinator has pending mutations for replica | Mutations applied to local CRDT documents |
| Full sync cycle | Fixture: 1 replica writes, 1 replica pulls | Both replicas have identical CRDT document state |
| Large batch sync | 10,000 mutations pending upload | All upload successfully, all apply to remote |

### 4.2 Offline→Online Transition

| Test | Setup | Verification |
|---|---|---|
| Write offline → reconnect → sync | Fixture starts disconnected, writes 100 mutations, reconnects | All 100 mutations uploaded, sequences assigned, status→synced |
| Download offline → missed mutations | Replica goes offline for 5 min, other replica writes 50 mutations, reconnects | All 50 mutations downloaded, merged, converged |
| Partial offline | Replica writes while disconnected AND other replica writes concurrently | CRDT merge: both sets of mutations preserved |
| Reconnect with schema change | Offline replica misses a schema migration | Migration applied before sync resumes |

### 4.3 Multi-Tab Integration

| Test | Setup | Verification |
|---|---|---|
| Two tabs, one coordinator | Tab A (leader) + Tab B (reader), shared memory | Tab A writes, Tab B sees via shared memory within 50ms |
| Leader crash recovery | Tab A crashes, Tab B detects heartbeat timeout | Tab B acquires lock, replays ring buffer, becomes leader |
| Three tabs join/leave | Tab A (leader) + Tab B + Tab C (readers), Tab B closes | Tab A and C continue normally, Tab C does not become leader (Tab A still alive) |
| Coordinator reconnect with multi-tab | All tabs lose connection, coordinator comes back | Only leader reconnects and re-syncs; readers update via shared memory |

### 4.4 E2EE Integration

| Test | Setup | Verification |
|---|---|---|
| Encrypt→Upload→Coordinator→Download→Decrypt | Two replicas with shared namespace key | Original Yrs update recovered after full cycle |
| Coordinator tampering | Coordinator modifies encrypted blob between upload and download | Decrypt fails, mutation rejected, error logged |
| Key rotation mid-sync | Replica A rotates key while Replica B is offline | B can decrypt old mutations (old key) and new mutations (updated via key registry) |
| Cross-device encryption | Replica A (device 1) encrypts, Replica B (device 2, different key) decrypts | Both devices have independent keypairs; mutations encrypted for each recipient |

### 4.5 Multiple Coordinator Implementations

Every integration test runs against every coordinator implementation:

| Coordinator | Connection | Notes |
|---|---|---|
| PostgresCoordinator | `postgres://localhost:5432/vaultsync_test` | Requires Docker |
| RedisCoordinator | `redis://localhost:6379` | Requires Docker |
| SQLiteCoordinator | `:memory:` | Embedded, no Docker |
| InMemoryCoordinator | In-process | Embedded, no Docker |
| CustomCoordinator | Test-only impl | In-process mock |

A test is considered "blocked" if it requires a coordinator that is not available (e.g., no Docker). CI runs with all coordinators; user `cargo test` runs with SQLite + InMemory by default.

---

## 5. End-to-End (E2E) Testing Strategy

E2E tests simulate real user interactions with the full VaultSync stack including browser or native runtime.

### 5.1 Browser Automation (Playwright)

| Test | Scenario | Verification |
|---|---|---|
| Single tab CRUD | Open app, create todo, edit text, mark complete, delete | All operations reflected in UI without page reload |
| Two tab sync | Tab A: create todo. Tab B: open same namespace | Tab B sees todo appear within 5 seconds |
| Two tab offline | Tab A goes offline, writes 5 todos, comes online. Tab B already open | Tab B sees 5 todos within 5 seconds of reconnect |
| Two tab conflict-free | Tab A: edit text field. Tab B: edit completed field simultaneously | Both fields preserved in both tabs (CRDT merge) |
| Tab crash recovery | Tab A (leader) is JavaScript-crashed, Tab B detects | Tab B becomes leader within 3 seconds, no data loss |
| E2EE verification | Inspector: coordinator network tab shows only ciphertext | No plaintext field names or values in coordinator HTTP traffic |

### 5.2 Playwright Test Implementation

```typescript
// examples/todo-multitab/e2e/multitab.spec.ts
import { test, expect, Page } from "@playwright/test"

test.describe("multi-tab sync", () => {
  let tabA: Page
  let tabB: Page

  test.beforeEach(async ({ browser }) => {
    tabA = await browser.newPage()
    tabB = await browser.newPage()
    await tabA.goto("/")
    await tabB.goto("/")
  })

  test("Tab A write appears in Tab B", async () => {
    // Tab A creates a todo
    await tabA.fill('[data-testid="todo-input"]', "test item")
    await tabA.click('[data-testid="todo-submit"]')

    // Wait for sync (max 5 seconds)
    await expect(tabB.locator('[data-testid="todo-item"]'))
      .toContainText("test item", { timeout: 5000 })
  })

  test("leader crash recovery", async () => {
    // Get leader status from both tabs
    const leaderA = await tabA.evaluate(() =>
      window.__VAULTSYNC__.leaderStatus()
    )
    expect(leaderA.amILeader).toBe(true)

    // Crash Tab A (simulate by navigating away)
    await tabA.goto("about:blank")

    // Tab B detects and becomes leader
    await expect(async () => {
      const leaderB = await tabB.evaluate(() =>
        window.__VAULTSYNC__.leaderStatus()
      )
      expect(leaderB.amILeader).toBe(true)
    }).toPass({ timeout: 5000 })
  })
})
```

### 5.3 Cross-Device Simulation

VaultSync provides a `test::SimulatedNetwork` harness for multi-device scenarios:

```rust
#[tokio::test]
async fn test_three_replica_convergence() {
    let mut sim = SimulatedNetwork::new(PostgresCoordinator::new_test());

    // Create 3 replicas
    let mut alice = sim.add_replica("replica:alice", SQLiteStorage::in_memory());
    let mut bob   = sim.add_replica("replica:bob",   SQLiteStorage::in_memory());
    let mut carol = sim.add_replica("replica:carol", SQLiteStorage::in_memory());

    // Alice writes while online
    alice.write(insert("todos", "todo:1", text("book flight"))).await;
    sim.sync_all().await;

    // Bob goes offline, writes
    bob.disconnect();
    bob.write(insert("todos", "todo:2", text("pack bags"))).await;

    // Carol goes offline, writes conflicting edit
    carol.disconnect();
    carol.write(update("todos", "todo:1", text("book hotel"))).await;

    // All reconnect
    bob.reconnect().await;
    carol.reconnect().await;
    sim.sync_all().await;

    // Verify convergence
    let alice_state = alice.get_document("todos").await;
    let bob_state   = bob.get_document("todos").await;
    let carol_state = carol.get_document("todos").await;

    assert_crdt_convergence(&alice_state, &bob_state);
    assert_crdt_convergence(&bob_state, &carol_state);

    // Verify both edits preserved (CRDT merge):
    // todo:1 text = "book hotel" (carol's edit — later timestamp)
    // todo:2 text = "pack bags" (bob's insert — independent record)
    assert_eq!(alice_state.get_text("todo:1"), "book hotel");
    assert!(alice_state.has_record("todo:2"));
}
```

### 5.4 E2E Test Environments

| Environment | Tool | Test Coverage |
|---|---|---|
| Browser (Chromium) | Playwright | Single-tab, two-tab, offline-reconnect, leader crash |
| Browser (Firefox) | Playwright | Same tests as Chromium (cross-browser compatibility) |
| Browser (WebKit/Safari) | Playwright | Same tests (IndexedDB fallback path) |
| Node.js | Jest + Supertest | Server VaultSync instance, sync between two server processes |
| React Native | Detox | Mobile-specific: background/foreground, push notification sync |
| Electron | Playwright (Electron) | Window focus/blur, multiple windows, system tray |

---

## 6. CRDT Correctness Testing

This is the most important section. CRDT correctness failures would cause silent data corruption that no other test can catch.

### 6.1 Property-Based Tests (proptest)

Properties are tested with random operation sequences. Each test generates N random operations, assigns them to M replicas with random ordering, and verifies convergence.

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn crdt_convergence(
        ops in proptest::collection::vec(any_crdt_operation(), 1..100),
        replicas in 2..10usize,
    ) {
        let mut docs = vec![CRDTDocument::new(); replicas];

        // Shuffle operations and assign to random replicas
        let mut rng = StdRng::seed_from_u64(42);
        for op in ops {
            let replica_idx = rng.gen_range(0..replicas);
            docs[replica_idx].apply(&op);
        }

        // Verify all replicas have identical state
        let reference = docs[0].state();
        for doc in &docs[1..] {
            assert_eq!(doc.state(), reference, "CRDT documents diverged");
        }
    }
}
```

### 6.2 CRDT Property Tests

| Property | Description | Generator |
|---|---|---|
| **Commutativity** | A then B produces same state as B then A (for concurrent ops) | Two random CRDT operations |
| **Associativity** | (A then B) then C produces same state as A then (B then C) | Three random CRDT operations |
| **Idempotence** | Applying the same mutation twice produces same state as once | Single CRDT operation |
| **Convergence** | N replicas with random operation ordering all converge | `n_crdt_ops(1..200)` |
| **Absorption** | Concurrent insert+delete produces same state as delete+insert (delete wins) | One insert, one delete on same key |
| **Monotonicity** | Applying mutations only adds information (never removes without explicit delete) | Sequence of CRDT operations |
| **LWW Register** | Later timestamp always wins; equal timestamps tiebreak by replica ID | Pairs of LWW writes with controlled timestamps |
| **PN Counter** | Sum of all increments/decrements equals final counter value | Random increments/decrements spread across replicas |
| **OR-Set** | Element is present iff it was added and never subsequently removed | Add/remove sequences with interleaving |

### 6.3 Convergence Test Scenarios

Beyond random property tests, specific edge cases are hand-crafted:

| Scenario | Description | Expected |
|---|---|---|
| **Concurrent create + delete** | Replica A creates record, Replica B deletes same record simultaneously | Record does not exist (delete wins per CRDT semantics) |
| **Concurrent create + update** | Replica A creates record, Replica B updates same record simultaneously | Record exists with all fields set (merge of both) |
| **Three-way concurrent** | Replicas A, B, C all modify same record simultaneously | All three changes preserved (CRDT merge) |
| **Cascading offline** | A writes, goes offline. B writes seeing A's changes, goes offline. C writes seeing B's changes, A reconnects | All changes preserved in all replicas |
| **Tombstone GC then merge** | Delete record on all replicas, GC tombstones, then a stale replica reconnects with pre-delete state | Delete is maintained (tombstone GC + stale update = delete wins) |
| **Name-based vs content-based** | Two replicas insert different content at same position in ordered list | Deterministic ordering (replica ID tiebreak) |

### 6.4 Yrs Integration Test Reuse

VaultSync uses the Yrs CRDT library, which has its own extensive test suite. VaultSync's CRDT tests should:

1. **Reuse Yrs's known-answer tests**: Yrs publishes test vectors for merge scenarios
2. **Extend beyond Yrs's tests**: Add VaultSync-specific scenarios (E2EE + CRDT, schema migrations + CRDT)
3. **Test Yrs at VaultSync's scale**: 10,000 concurrent mutations across 100 replicas

---

## 7. E2EE Correctness Testing

### 7.1 Known-Answer Tests (KATs)

Each encryption operation has a KAT that verifies VaultSync's implementation matches the libsodium specification:

```rust
#[test]
fn encrypt_kat() {
    // Known test vector from libsodium crypto_box specification
    let alice_sk = SecretKey::from_hex("...");
    let bob_pk   = PublicKey::from_hex("...");
    let plaintext = b"Hello, VaultSync!";
    let expected_ciphertext = hex::decode("...").unwrap();

    let ciphertext = encrypt(&plaintext, &alice_sk, &bob_pk);
    assert_eq!(ciphertext, expected_ciphertext);
}
```

### 7.2 E2EE Property Tests

| Property | Test |
|---|---|
| **Correctness** | `decrypt(encrypt(pt, sk_a, pk_b), sk_b, pk_a) == pt` for all pt |
| **Confidentiality** | `decrypt(encrypt(pt, sk_a, pk_b), sk_b, pk_c)` fails (wrong key) |
| **Integrity** | Flipping any bit in ciphertext causes decrypt to fail |
| **Nonce uniqueness** | Two encryptions of same plaintext produce different ciphertexts |
| **Length hiding** | Ciphertext length leaks only approximate plaintext length (within block size) |
| **Key rotation** | Old ciphertext decryptable with old key; new ciphertext with new key |

### 7.3 E2EE Integration with CRDT

| Test | Description |
|---|---|
| **Encrypt→CRDT apply** | Encrypt a Yrs update, then decrypt and apply to document: document state matches original |
| **Encrypt→Upload→Download→Decrypt→CRDT apply** | Full roundtrip through coordinator (mock): document converges |
| **Tampered ciphertext after CRDT apply** | Coordinator returns modified ciphertext: decrypt fails, document unchanged |
| **Key rotation mid-operation** | Client encrypts with key v1, rotates to key v2, encrypts with key v2: both decryptable |

---

## 8. Multi-Tab & Leader Election Testing

### 8.1 Unit Tests (Controlled Environment)

| Test | Description |
|---|---|
| **Acquire lock** | Process A acquires file lock; Process B attempts to acquire and fails |
| **Heartbeat write** | Leader writes heartbeat timestamp; reader reads and validates |
| **Heartbeat timeout** | Leader stops writing heartbeat; reader detects timeout after configured interval |
| **Leader promotion** | Reader detects leader dead, acquires lock, writes heartbeat as new leader |
| **Ring buffer replay** | Leader crashes with 5 uncommitted ops in ring buffer; new leader replays them |

### 8.2 Integration Tests (Multi-Process)

These tests spawn multiple OS processes or browser tabs and verify cross-process behavior.

```rust
#[tokio::test]
async fn test_leader_crash_recovery() {
    let dir = tempdir();

    // Spawn leader process
    let leader = spawn_vaultsync_process(&dir, "leader");
    leader.wait_for_leader().await;

    // Spawn reader process
    let reader = spawn_vaultsync_process(&dir, "reader");
    reader.wait_for_reader_of("leader").await;

    // Leader writes a mutation
    leader.write("todos", insert("todo:1", text("test"))).await;

    // Kill leader (simulate crash)
    leader.kill();

    // Reader should detect, promote, and replay
    reader.wait_for_leader().await;

    // Verify the mutation was preserved
    let state = reader.get_document("todos").await;
    assert!(state.has_record("todo:1"));

    // Spawn new process that was not in the original set
    let latecomer = spawn_vaultsync_process(&dir, "latecomer");
    latecomer.wait_for_reader_of("reader").await;

    // Latecomer should see the mutation via shared memory
    let late_state = latecomer.get_document("todos").await;
    assert!(late_state.has_record("todo:1"));
}
```

### 8.3 Browser Multi-Tab Tests (Playwright)

Described in Section 5.1. Key scenarios:

- Two tabs, leader writes, reader sees
- Leader crash → reader promotion
- Three tabs, leader leaves → remaining tabs re-elect
- Coordinator disconnect → leader detects → reconnects → all tabs update
- Tab opens in new window (not just new tab)

### 8.4 Edge Cases

| Scenario | Expected |
|---|---|
| Two readers start simultaneously, no leader | First to acquire lock becomes leader; second becomes reader |
| Leader's heartbeat file deleted | Leader detects, re-creates heartbeat, continues |
| Leader's process is suspended (SIGSTOP) | Readers detect timeout, elect new leader; original leader resumes, detects it is no longer leader, becomes reader |
| Network partition splits readers into two groups | No split-brain: only one group has the file lock (single writer to disk) |
| 50 tabs simultaneously | All readers register, leader elected within 5 seconds, no crashes |

---

## 9. Performance & Benchmark Testing

### 9.1 Benchmark Harness

All benchmarks use **Criterion.rs** with the following methodology:

- Warm-up: 10,000 iterations before measurement
- Measurement: 100,000 iterations minimum
- Regression detection: compare against `main` branch baseline
- Threshold: >10% regression fails CI (configurable per benchmark)
- Cross-platform: run on Linux (native), macOS (native), Windows (native), Browser (WASM)

### 9.2 Core Benchmarks

| Benchmark Name | What It Measures | Current Target | Regression Threshold |
|---|---|---|---|
| `local_read_1row` | Read single CRDT record from local storage | 1–5 µs (native), 0.5–2 ms (WASM) | 15% |
| `local_write_1row` | Write single CRDT mutation, WAL commit | 5–20 µs (native), 1–5 ms (WASM) | 15% |
| `crdt_merge_1op` | Merge single Yrs update into document | 1–5 µs | 10% |
| `crdt_merge_1000ops` | Merge 1000 Yrs updates sequentially | 1–5 ms | 15% |
| `e2ee_encrypt_1op` | Encrypt single Yrs update (X25519 + ChaCha20) | 5–15 µs (native), 50–200 µs (WASM) | 15% |
| `e2ee_decrypt_1op` | Decrypt single Yrs update | 5–15 µs (native), 50–200 µs (WASM) | 15% |
| `snapshot_load_1mb` | Load 1MB Yrs CRDT snapshot from storage | < 1 ms (native), 10–50 ms (WASM) | 20% |
| `snapshot_save_1mb` | Save 1MB Yrs CRDT snapshot to storage | < 1 ms (native), 10–50 ms (WASM) | 20% |
| `tab_to_tab_latency` | Tab A writes → Tab B sees via shared memory | 2–10 µs (native), 1–5 ms (WASM) | 25% |
| `full_operation_path` | Write → CRDT merge → encrypt → oplog → subscription fire | 10–40 µs (native), 2–10 ms (WASM) | 15% |
| `subscription_fire_1000` | 1000 subscriptions fire on single mutation | < 10 ms | 20% |
| `oplog_append_10000` | Append 10,000 mutations to oplog | < 100 ms | 20% |
| `coordinator_push_100` | Push 100 mutations to Postgres coordinator | < 200 ms (network-bound) | 30% |
| `coordinator_pull_1000` | Pull 1000 mutations from Postgres coordinator | < 500 ms (network-bound) | 30% |

### 9.3 Performance Regression Workflow

```
PR submitted
    │
    ▼
CI runs benchmarks on dedicated hardware (same CPU each run)
    │
    ▼
Compare with `main` branch baseline
    │
    ▼
All benchmarks within threshold? → ✅ Pass
Any benchmark exceeds threshold? → ❌ Fail with diff output
    │
    ▼
Developer investigates:
  - Was this an environmental variance? → Rerun
  - Was this a genuine regression? → Fix before merge
```

### 9.4 Browser Performance Testing

WASM benchmarks run in a headless Chromium via `wasm-bindgen-test`:

- Same benchmarks as native, but compiled to WASM
- Measure `performance.now()` with 1000+ sample minimum
- Report as "WASM overhead ratio" relative to native baseline
- Tracked over time: "WASM overhead has increased from 3x to 5x — investigate"

---

## 10. Fuzz Testing & Property-Based Testing

### 10.1 Fuzz Targets (cargo fuzz)

| Target | Input | Duration (CI) |
|---|---|---|
| `fuzz_crdt_merge` | Random byte sequences as Yrs updates | 5 min (quick), 24h (weekly) |
| `fuzz_e2ee_ciphertext` | Random byte sequences as ciphertexts | 5 min (quick), 24h (weekly) |
| `fuzz_ipc_message` | Random byte sequences as IPC messages | 5 min (quick), 24h (weekly) |
| `fuzz_coordinator_protocol` | Random byte sequences as WebSocket frames | 5 min (quick), 24h (weekly) |
| `fuzz_snapshot` | Random byte sequences as Yrs snapshot blobs | 5 min (quick), 24h (weekly) |
| `fuzz_oplog_entry` | Random byte sequences as oplog entries | 5 min (quick), 24h (weekly) |

### 10.2 Fuzz Harness Example

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz CRDT merge: try to apply random bytes as Yrs updates
    if let Ok(update) = YrsUpdate::from_bytes(data) {
        let mut doc = CRDTDocument::new();
        // Must never panic, crash, or produce corrupted state
        let result = doc.apply_safe(&update);
        assert!(result.is_ok() || result.is_err());
        // Even on error, document state must be internally consistent
        assert!(doc.is_consistent());
    }
});
```

### 10.3 Property-Based Test Generators

```rust
fn any_crdt_operation() -> impl Strategy<Value = CRDTOperation> {
    prop_oneof![
        any_lww_write(),
        any_counter_update(),
        any_orset_add(),
        any_orset_remove(),
        any_text_insert(),
        any_text_delete(),
    ]
}

fn any_lww_write() -> impl Strategy<Value = CRDTOperation> {
    (any_string(), any_string(), any_timestamp())
        .prop_map(|(key, value, ts)| CRDTOperation::LWWWrite {
            key, value, timestamp: ts
        })
}

fn any_timestamp() -> impl Strategy<Value = u64> {
    prop::num::u64::ANY
        .prop_filter("timestamp must not overflow", |ts| *ts < u64::MAX / 2)
}
```

---

## 11. Chaos Testing

### 11.1 Chaos Testing Principles

1. **Inject real faults**: VaultSync's chaos tests use `toxiproxy` (network fault injection) and OS-level process control (SIGKILL, SIGSTOP), not simulated timeouts
2. **Verify data integrity**: After every chaos scenario, run a full CRDT convergence check across all replicas
3. **Reproducible**: Each chaos test has a fixed random seed for the fault sequence
4. **CI-compatible**: Short chaos scenarios (5 min) in CI; long scenarios (24h) in weekly runs

### 11.2 Chaos Scenarios

| Scenario | Fault Injected | Duration | Verification |
|---|---|---|---|
| **Network partition** | Coordinator blocked for 30s via toxiproxy | 60s | All ops queued locally, no data loss, all converge after restore |
| **Intermittent network** | 50% packet loss for 60s | 120s | Ops retry with backoff, no duplicates, all converge |
| **High latency** | 500ms added latency to coordinator for 60s | 120s | Ops upload despite high latency, no timeouts cause data loss |
| **Leader crash** | SIGKILL leader process | 30s | Reader promotes, replays ring buffer, no data loss |
| **Coordinator crash** | SIGKILL coordinator database | 60s | Coordinator restarts, replicas reconnect, ops queued during downtime |
| **Disk full** | Storage backend returns "no space" (simulated) | 30s | App receives error, ops queued in memory, storage available again → ops written |
| **Clock skew** | System clock jumps 5 seconds forward then back | 30s | CRDT LWW timestamps handle skew; deterministic tiebreak by replica ID |
| **Simultaneous crash** | All replicas crash at the same time | 60s | All ops recovered from persistent storage on restart |
| **Corrupt oplog entry** | Random byte modified in oplog file | 30s | VaultSync detects corruption, logs error, replays from last good entry |
| **Mixed fault** | Network partition + leader crash simultaneously | 60s | Remaining replicas elect leader, queue ops, all converge after partition heals |

### 11.3 Chaos Harness

```rust
#[tokio::test]
async fn chaos_network_partition() {
    let mut sim = SimulatedNetwork::new(PostgresCoordinator::new_test());

    let mut replica_a = sim.add_replica("replica:a", SQLiteStorage::in_memory());
    let mut replica_b = sim.add_replica("replica:b", SQLiteStorage::in_memory());

    // Inject fault: block coordinator for replica_a
    let fault = sim.inject_fault(Fault::NetworkPartition {
        target: "replica:a",
        duration: Duration::from_secs(30),
    });

    // While partitioned, both replicas write
    let a_ops = replica_a.write_many(50, insert_generator("todos")).await;
    let b_ops = replica_b.write_many(50, insert_generator("todos")).await;

    // Wait for partition to heal
    fault.heal().await;

    // Let sync complete
    sim.sync_all_with_timeout(Duration::from_secs(30)).await;

    // Verify convergence
    let a_state = replica_a.get_document("todos").await;
    let b_state = replica_b.get_document("todos").await;
    assert_crdt_convergence(&a_state, &b_state);

    // Verify total operation count matches (no duplicates, no losses)
    assert_eq!(a_state.operation_count(), 100);  // 50 + 50
    assert_eq!(b_state.operation_count(), 100);
}
```

### 11.4 Chaos Test Schedule

| Frequency | Scenarios | Duration |
|---|---|---|
| Every PR (CI) | Network partition, leader crash, coordinator crash | 15 min |
| Daily | Intermittent network, disk full, clock skew | 30 min |
| Weekly | Simultaneous crash, corrupt oplog, mixed fault, all scenarios | 4 hours |
| Monthly | Long-duration: 24h continuous fault injection | 24 hours |

---

## 12. Migration Testing

### 12.1 Schema Migration Tests

| Test | Description |
|---|---|
| **Add field (v1→v2)** | Existing data preserved after adding a new CRDT field to a document |
| **Add document type (v1→v2)** | New document collection created; existing collections unchanged |
| **Migration rollback** | Migration fails; rollback restores previous state; data intact |
| **Migration checksum validation** | Tampered migration code detected by checksum mismatch; migration refused |
| **Concurrent migration** | 3 replicas all apply same migration simultaneously; all converge on new schema |
| **Offline replica migration** | Replica goes offline before migration, reconnects after; migration applied before sync resumes |
| **Cross-version compatibility** | Replica on v1 receives ops from replica on v2 (with new field); v1 ignores new field; no crash |

### 12.2 Migration Test Harness

```rust
#[tokio::test]
async fn test_migration_add_field() {
    let mut fixture = TestFixture::new();

    // Start with v1 schema
    fixture.define_schema("todos", Schema::v1());
    fixture.apply_migration("v1", migration_v1()).await;

    // Insert data under v1
    fixture.write(insert("todos", "todo:1", text("original"))).await;

    // Migrate to v2 (adds "priority" field)
    fixture.apply_migration("v2", migration_v2()).await;

    // Verify v1 data is intact
    let state = fixture.get_document("todos", "todo:1").await;
    assert_eq!(state.get_text("text"), "original");

    // Verify new field works
    fixture.write(update("todos", "todo:1", priority(5))).await;
    let state = fixture.get_document("todos", "todo:1").await;
    assert_eq!(state.get_counter("priority"), 5);
}
```

---

## 13. Coordinator Conformance Testing

### 13.1 Conformance Test Suite

Every coordinator implementation must pass the exact same test suite. This ensures that users can switch coordinators with zero behavioral changes.

```rust
/// Conformance test macro — runs the same tests against any coordinator
macro_rules! coordinator_conformance_tests {
    ($coordinator_constructor:expr) => {
        mod conformance {
            use super::*;

            #[tokio::test]
            async fn push_and_pull() { /* ... */ }

            #[tokio::test]
            async fn push_assigns_sequences() { /* ... */ }

            #[tokio::test]
            async fn pull_after_sequence() { /* ... */ }

            #[tokio::test]
            async fn subscribe_receives_new_mutations() { /* ... */ }

            #[tokio::test]
            async fn register_replica() { /* ... */ }

            #[tokio::test]
            async fn heartbeat_updates_last_seen() { /* ... */ }

            #[tokio::test]
            async fn deduplicates_mutation_id() { /* ... */ }

            #[tokio::test]
            async fn namespace_isolation() { /* ... */ }

            #[tokio::test]
            async fn large_batch_10000_ops() { /* ... */ }

            #[tokio::test]
            async fn snapshot_registration() { /* ... */ }

            #[tokio::test]
            async fn concurrent_push_from_multiple_replicas() { /* ... */ }
        }
    };
}

// Run the same tests against every coordinator
#[cfg(test)]
mod tests {
    coordinator_conformance_tests!(PostgresCoordinator::new_test());
    coordinator_conformance_tests!(RedisCoordinator::new_test());
    coordinator_conformance_tests!(SQLiteCoordinator::new_test());
    coordinator_conformance_tests!(InMemoryCoordinator::new_test());
    coordinator_conformance_tests!(CustomTestCoordinator::new());
}
```

### 13.2 Conformance Test Requirements

| Requirement | All Coordinators | Cloudflare DO | Postgres | Redis | SQLite | Custom |
|---|---|---|---|---|---|---|
| push + pull | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Sequence assignment | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Subscribe (real-time) | ✅ | ✅ | ✅ (LISTEN/NOTIFY) | ✅ (Pub/Sub) | ✅ (polling) | ✅ |
| Replica registration | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Heartbeat | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Namespace isolation | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Mutation dedup | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 10K batch push | ✅ | ✅ | ✅ | ✅ | ✅ | TBD |
| Snapshot storage | Optional | ✅ | ✅ | ❌ | ❌ | Optional |

---

## 14. CI/CD Pipeline Design

### 14.1 Pipeline Stages

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CI Pipeline (VaultSync)                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │   Lint   │  │   Build  │  │  CRDT    │  │   Unit   │  │   Fuzz   │  │
│  │  clippy  │──►│  cargo   │──►│ Correct- │──►│  Tests   │──►│ (5 min)  │  │
│  │  + fmt   │  │  + wasm  │  │  ness    │  │          │  │          │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│                        │                             │                   │
│                        ▼                             ▼                   │
│                  ┌──────────┐                  ┌──────────┐            │
│                  │Integra-  │                  │Benchmarks│            │
│                  │tion Tests│                  │Criterion │            │
│                  │ + Chaos  │                  │          │            │
│                  └──────────┘                  └──────────┘            │
│                        │                             │                   │
│                        ▼                             ▼                   │
│                  ┌──────────┐                  ┌──────────┐            │
│                  │   E2E    │                  │   Gate   │            │
│                  │Playwright│                  │ Pass/Fail│            │
│                  └──────────┘                  └──────────┘            │
│                        │                             │                   │
│                        └──────────────┬──────────────┘                   │
│                                       ▼                                  │
│                                 ┌──────────┐                            │
│                                 │   Merge  │                            │
│                                 │  or Fail │                            │
│                                 └──────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

### 14.2 Pipeline Configuration

```yaml
# .github/workflows/ci.yml
name: VaultSync CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo fmt --check

  build:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    steps:
      - uses: actions/checkout@v4
      - run: cargo build --all-targets
      - run: cargo build --target wasm32-unknown-unknown
      - run: wasm-pack build --target web

  crdt-correctness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --test crdt_correctness -- --include-pending
      - run: cargo test --test crdt_property --release

  unit:
    runs-on: ubuntu-latest
    needs: [lint, build]
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --lib
      - run: cargo test --test e2ee_tests
      - run: cargo test --test ipc_tests

  fuzz:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo fuzz run fuzz_crdt_merge --timeout=300
      - run: cargo fuzz run fuzz_e2ee_ciphertext --timeout=300

  integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: vaultsync_test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
      redis:
        image: redis:7
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --test integration_tests -- --test-threads=4
      - run: cargo test --test coordinator_conformance

  chaos:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: vaultsync_test
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --test chaos_tests -- --test-threads=1

  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo bench -- --baseline main

  e2e:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: vaultsync_test
    steps:
      - uses: actions/checkout@v4
      - run: wasm-pack build --target web
      - run: npm ci
      - run: npx playwright install
      - run: npm run e2e
```

### 14.3 Pipeline Timing Targets

| Stage | Parallelism | Timeout | Gate |
|---|---|---|---|
| Lint | — | 2 min | Must pass |
| Build | 3 OS | 10 min | Must pass |
| CRDT Correctness | 1 | 5 min | Must pass |
| Unit Tests | 8 workers | 5 min | Must pass |
| Integration Tests | 4 workers | 15 min | Must pass |
| Fuzz (quick) | 2 workers | 5 min | Must pass |
| Chaos | 1 | 15 min | Must pass |
| Benchmarks | 1 | 10 min | No >10% regression |
| E2E | 3 workers | 10 min | Must pass |
| **Total** | **Pipeline** | **~45 min** | **All gates** |

---

## 15. Test Fixtures & Factories

### 15.1 `VaultSyncFixture`

The primary test fixture for integration tests. Provides pre-configured VaultSync instances with isolated storage, mock coordinator, and deterministic clock.

```rust
pub struct VaultSyncFixture {
    pub storage: Box<dyn Storage>,
    pub coordinator: Box<dyn Coordinator>,
    pub clock: DeterministicClock,
    pub encryption_keys: NamespaceKeypair,
    pub schema: SchemaRegistry,
    pub subscriptions: SubscriptionEngine,
}

impl VaultSyncFixture {
    pub fn new() -> Self {
        Self {
            storage: Box::new(InMemoryStorage::new()),
            coordinator: Box::new(InMemoryCoordinator::new()),
            clock: DeterministicClock::new(),
            encryption_keys: NamespaceKeypair::generate_for_test(),
            schema: SchemaRegistry::new(),
            subscriptions: SubscriptionEngine::new(),
        }
    }

    pub fn with_storage(mut self, storage: impl Storage + 'static) -> Self { ... }
    pub fn with_coordinator(mut self, coord: impl Coordinator + 'static) -> Self { ... }
    pub fn with_clock(mut self, clock: DeterministicClock) -> Self { ... }

    /// Run a full write cycle: CRDT merge → encrypt → oplog → upload
    pub async fn write(&mut self, op: Operation) -> Result<SequenceId> { ... }

    /// Run a full read cycle: download → decrypt → CRDT merge → subscription
    pub async fn sync(&mut self) -> Result<SyncResult> { ... }

    /// Get current CRDT document state for verification
    pub fn get_document_state(&self, doc_id: &str) -> CRDTState { ... }

    /// Verify CRDT convergence with another fixture
    pub fn assert_converges_with(&self, other: &VaultSyncFixture) { ... }
}
```

### 15.2 `SimulatedNetwork`

Multi-replica simulation harness for convergence and chaos testing.

```rust
pub struct SimulatedNetwork {
    coordinator: Box<dyn Coordinator>,
    replicas: Vec<SimulatedReplica>,
    faults: Vec<Box<dyn Fault>>,
}

pub struct SimulatedReplica {
    id: String,
    storage: Box<dyn Storage>,
    client: VaultSyncClient,
    connected: bool,
}

impl SimulatedNetwork {
    pub fn new(coordinator: impl Coordinator + 'static) -> Self { ... }
    pub fn add_replica(&mut self, id: &str, storage: impl Storage + 'static) -> &mut SimulatedReplica { ... }
    pub fn inject_fault(&mut self, fault: impl Fault + 'static) -> FaultHandle { ... }
    pub async fn sync_all(&mut self) { ... }
    pub async fn sync_all_with_timeout(&mut self, timeout: Duration) -> Result<(), TimeoutError> { ... }
}
```

### 15.3 `DeterministicClock`

Prevents timing-dependent test flakiness. All components read time from this clock, not `SystemTime::now()`.

```rust
pub struct DeterministicClock {
    time: Arc<AtomicU64>,
}

impl DeterministicClock {
    pub fn new() -> Self { Self { time: Arc::new(AtomicU64::new(0)) } }
    pub fn advance(&self, ms: u64) { self.time.fetch_add(ms, Ordering::SeqCst); }
    pub fn now_ms(&self) -> u64 { self.time.load(Ordering::SeqCst) }
}
```

### 15.4 `TestScenarioBuilder`

Fluent builder for complex test scenarios:

```rust
TestScenario::new("three-way convergence")
    .with_coordinator(InMemoryCoordinator::new())
    .add_replica("alice", SQLiteStorage::in_memory())
    .add_replica("bob",   SQLiteStorage::in_memory())
    .add_replica("carol", SQLiteStorage::in_memory())
    .with_generator(insert_generator("todos", 50))
    .with_fault(Fault::NetworkPartition {
        target: "bob",
        duration: Duration::from_secs(10),
    })
    .with_fault(Fault::RandomLatency {
        target: "coordinator",
        min_ms: 100,
        max_ms: 500,
    })
    .with_verification(|state| {
        assert_crdt_convergence(&state["alice"], &state["bob"]);
        assert_crdt_convergence(&state["bob"], &state["carol"]);
        assert!(state["alice"].operation_count() >= 100);
    })
    .run()
    .await
```

---

## 16. Test Coverage Targets

### 16.1 Coverage by Component

| Component | Line Coverage | Branch Coverage | Notes |
|---|---|---|---|
| **CRDT Merge Engine** | 100% | 100% | Any missed branch can cause silent data corruption |
| **E2EE Layer** | 100% | 100% | Any missed branch can cause data leakage or corruption |
| **Multi-Tab IPC** | 100% | 100% | Any missed branch can cause split-brain or data loss |
| **Storage Abstraction** | 95% | 90% | Backend-specific code (SQLite, OPFS, etc.) tested per-backend |
| **Oplog Engine** | 95% | 90% | Core paths (append, read, mark) are 100% |
| **Subscription Engine** | 95% | 90% | Filter matching is 100% branch-covered |
| **Schema + Migration** | 90% | 85% | Additive changes only; destructive paths are error-handled |
| **Coordinator Trait** | 90% | 85% | Core trait methods tested; per-backend impls in integration |
| **Transport Layer** | 90% | 85% | WebSocket, HTTP, WebRTC paths |
| **Observability** | 80% | 75% | Tracing and metrics are secondary; errors still logged |
| **SDK (TypeScript)** | 90% | 85% | Integration-tested via E2E; unit-tested with Jest |
| **Total (Rust core)** | **94%** | **91%** | |
| **Total (TypeScript SDK)** | **85%** | **80%** | |

### 16.2 Coverage Enforcement

```yaml
# .github/workflows/coverage.yml
- name: Coverage report
  run: cargo tarpaulin --out Xml --skip-clean

- name: Enforce thresholds
  run: |
    # CRDT + E2EE + IPC must be 100% line and branch
    assert_coverage "vaultsync-core/src/crdt" 100 100
    assert_coverage "vaultsync-core/src/e2ee" 100 100
    assert_coverage "vaultsync-core/src/ipc"  100 100
    # Overall must meet targets
    assert_coverage "vaultsync-core" 94 91
```

### 16.3 What Is Excluded from Coverage

- Integration test helpers and harness code
- CLI tools (`vaultsync inspect`, `vaultsync replay`)
- Debug HTTP API handlers
- Build/configuration code (`build.rs`, cargo manifests)
- Generated code (protobuf, WASM bindings)

---

## 17. Test Matrix

### 17.1 Compatibility Matrix

| Storage \ Coordinator | Postgres | Redis | SQLite | Cloudflare DO | Supabase | Custom | In-Memory |
|---|---|---|---|---|---|---|---|
| **SQLite (native)** | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI |
| **OPFS (WASM)** | ✅ CI | ⏳ | ✅ CI | ⏳ | ⏳ | ⏳ | ✅ CI |
| **IndexedDB** | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ | ✅ CI |
| **RocksDB** | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ |
| **In-Memory** | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI | ✅ CI |

### 17.2 OS × Browser Matrix

| Platform | Native | WASM | E2E |
|---|---|---|---|
| **Linux** | ✅ CI | ✅ CI | ✅ CI (Chromium + Firefox) |
| **macOS** | ✅ CI | ✅ CI | ⏳ |
| **Windows** | ✅ CI | ✅ CI | ⏳ |
| **iOS** | ⏳ (Swift SDK) | ✅ (Safari) | ⏳ |
| **Android** | ⏳ (Kotlin SDK) | ✅ (Chrome) | ⏳ |

### 17.3 Legend

| Symbol | Meaning |
|---|---|
| ✅ CI | Tested in CI on every PR |
| ✅ Daily | Tested in daily CI run |
| ⏳ | Planned — not yet implemented |
| ❌ | Not supported |

---

## 18. Appendix: Failure Mode Catalog

### 18.1 Failure Classifications

| Class | Description | Example |
|---|---|---|
| **Silent Data Corruption** | Data is modified without error | CRDT merge produces wrong state |
| **Silent Data Loss** | Data disappears without error | OpLog compaction deletes unsynced ops |
| **Crash** | Process terminates unexpectedly | Crash on corrupted Yrs update |
| **Hang** | Process stops responding | Deadlock in IPC lock acquisition |
| **Leak** | Resource not released | Shared memory not freed on process exit |
| **Split-Brain** | Two leaders elected simultaneously | Two tabs both think they are writer |
| **Replay** | Duplicate operation applied | Coordinator does not deduplicate mutation ID |
| **Eavesdrop** | Coordinator reads plaintext data | E2EE not applied before upload |

### 18.2 Failure Catalog

| ID | Failure | Class | Component | Detection | Test |
|---|---|---|---|---|---|
| F001 | LWW-Register: wrong timestamp wins (clock skew) | Silent Corruption | CRDT | CRDT property test with clock skew | `crdt_lww_clock_skew` |
| F002 | PN-Counter: overflow without wrap handling | Silent Corruption | CRDT | CRDT property test with large values | `crdt_counter_overflow` |
| F003 | OR-Set: tombstone GC removes before merge | Silent Data Loss | CRDT | CRDT convergence test with offline replica | `crdt_orset_gc_offline` |
| F004 | Yrs update: malformed binary causes panic | Crash | CRDT | Fuzz test | `fuzz_crdt_merge` |
| F005 | E2EE: wrong nonce reuse | Eavesdrop | E2EE | KAT test with repeated nonce | `e2ee_nonce_reuse` |
| F006 | E2EE: decrypt succeeds with wrong key | Eavesdrop | E2EE | Wrong-key decryption test | `e2ee_wrong_key` |
| F007 | IPC: reader acquires lock while leader alive | Split-Brain | IPC | Multi-tab integration test | `multitab_split_brain` |
| F008 | IPC: ring buffer overwrites uncommitted ops | Silent Data Loss | IPC | Leader crash recovery test | `multitab_ring_buffer_overwrite` |
| F009 | Coordinator: mutation ID collision accepted | Replay | Coordinator | Conformance test with duplicate ID | `coord_dedup_mutation_id` |
| F010 | Coordinator: sequence gap after coordinator restart | Silent Data Loss | Coordinator | Chaos: coordinator crash test | `chaos_coordinator_crash` |
| F011 | Storage: transaction partial commit | Silent Corruption | Storage | Transaction rollback unit test | `storage_rollback` |
| F012 | Subscription: callback fires after unregister | Misbehavior | Subscription | Unregister unit test | `subscription_unregister` |
| F013 | Migration: checksum mismatch not detected | Misbehavior | Schema | Migration checksum test | `migration_checksum` |
| F014 | Offline: pending ops lost on leader crash | Silent Data Loss | Multi-Tab | Leader crash recovery integration test | `multitab_leader_crash_pending_ops` |
| F015 | Reconnect: ops re-uploaded after already synced | Duplicate | Sync | Offline→online integration test | `offline_reconnect_no_duplicates` |

Each failure in the catalog has a corresponding test that verifies the failure cannot occur.

---

*VaultSync — Testing & Verification Specification*
*Every failure mode is cataloged. Every component is covered. Every latency number is measured.*
*MIT License — Open source. Auditable. No black boxes.*
