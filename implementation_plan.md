# Drift — Complete Remaining Phases Implementation Plan

> This plan covers every phase not yet done, from Alpha blockers through Production Hardening (Stable).
> Phases are ordered by dependency. Each phase is self-contained and can be executed sequentially.

---

## Milestone Map

```
ALPHA (current ~80%)
  ├── Phase 14a: @drift/node SDK              ← Alpha blocker
  ├── Phase 14b: Migration Auto-Execution     ← Alpha blocker
  └── Phase 14c: SDK TypeScript Build CI      ← Alpha blocker

BETA
  ├── Phase 15: P2P Transport (WebRTC)
  ├── Phase 16: Cloudflare Durable Objects Coordinator
  ├── Phase 17: Supabase Coordinator
  ├── Phase 18: Full CRDT Snapshot + Compaction
  ├── Phase 19: Multi-Namespace Connection Multiplexing
  └── Phase 20: E2EE Key Rotation

STABLE
  ├── Phase 21: Coordinator Conformance Test Suite
  ├── Phase 22: Coordinator High-Availability Failover
  ├── Phase 23: Swift SDK (iOS/macOS)
  ├── Phase 24: Kotlin SDK (Android)
  ├── Phase 25: Advanced Observability Dashboard
  ├── Phase 26: Mesh Transport (libp2p)
  └── Phase 27: Production Hardening
```

---

## Phase 14a — `@drift/node` SDK (Alpha Blocker)

### Goal
Build `packages/node` — the Node.js coordinator SDK that wraps the Rust `drift-core` engine via a native add-on (NAPI-RS), enabling server-side replicas.

### Why It's Blocking Alpha
The spec §21 (SDK Reference) lists `@drift/node` as a core SDK. Alpha cannot ship without a Node.js path.

### New Files

| Location | File | Purpose |
|---|---|---|
| `crates/drift-napi/` | `Cargo.toml` | New crate: NAPI-RS bindings |
| `crates/drift-napi/src/` | `lib.rs` | `#[napi]` exports wrapping `DriftClient` |
| `crates/drift-napi/src/` | `client.rs` | Node-friendly async `DriftNodeClient` |
| `sdk/packages/node/` | `package.json` | `@drift/node` NPM package |
| `sdk/packages/node/src/` | `index.ts` | TypeScript wrapper + types |
| `sdk/packages/node/src/` | `coordinator.ts` | `NodeCoordinator` factory (HTTP/WS) |

### Steps

1. **Add `drift-napi` crate** — `Cargo.toml` with `napi`, `napi-derive`, `napi-build` dependencies. Feature `napi` gated on `not(target_arch = "wasm32")`.
2. **Wrap `DriftClient`** — export `create()`, `insert()`, `update()`, `delete()`, `get()`, `subscribe()`, `shutdown()` via `#[napi]` async functions.
3. **Scaffold `packages/node`** — `package.json` (`@drift/node`), `index.ts` importing the native add-on, full TypeScript types matching `@drift/web`.
4. **Add build script** — `sdk/package.json` gets `"build:node": "cargo build -p drift-napi --release"` script.
5. **Tests** — `packages/node/__tests__/client.test.ts` using Jest + `@drift/node` against `InMemoryCoordinator`.

### Verification
```powershell
cargo build -p drift-napi --release
cd sdk && npm -w packages/node test
```

---

## Phase 14b — Migration Auto-Execution (Alpha Blocker)

### Goal
When `DriftClient` starts, automatically detect and execute pending schema migrations from `drift_migrations` table.

### What Exists
- `src/schema/migration.rs` — `MigrationDef` struct and `MigrationRegistry`
- `src/schema/registry.rs` — `SchemaRegistry` stores field definitions
- `drift_migrations` table in SQLite schema

### What's Missing
- Auto-run migrations on client startup
- Migration checksum validation
- Schema version negotiation with coordinator

### Files to Modify

#### [MODIFY] `crates/drift-core/src/schema/migration.rs`
Add:
```rust
pub struct MigrationRunner {
    storage: Arc<dyn Storage>,
    registry: MigrationRegistry,
}

impl MigrationRunner {
    /// Run all pending migrations in version order.
    pub async fn run_pending(&self) -> Result<(), DriftError>;

    /// Validate checksums of already-applied migrations.
    pub async fn validate_applied(&self) -> Result<(), DriftError>;
}
```

#### [MODIFY] `crates/drift-core/src/client.rs`
In `DriftClient::new()`, after storage init:
```rust
let runner = MigrationRunner::new(storage.clone(), migration_registry);
runner.validate_applied().await?;
runner.run_pending().await?;
```

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
In `subscribe()` handshake, send `SCHEMA_SYNC (0x10)` frame with current version. Handle `SCHEMA_MIGRATION (0x11)` response.

### Steps
1. Implement `MigrationRunner::run_pending()` — query `drift_migrations`, compare with registered migrations, run in order.
2. Implement checksum validation — SHA256 of migration code, compare with stored `checksum` column.
3. Hook `run_pending()` into `DriftClient::new()`.
4. Add `SCHEMA_SYNC`/`SCHEMA_MIGRATION` WS frame handling in `ws.rs` and `http.rs`.
5. Tests: `cargo test -p drift-core -- migration`.

### Verification
```powershell
cargo test -p drift-core -- migration --nocapture
```

---

## Phase 14c — SDK TypeScript Build CI (Alpha Blocker)

### Goal
Ensure `@drift/web` and `@drift/react` TypeScript packages compile cleanly and pass type checks.

### Steps
1. **Fix import paths** — verify `sdk/packages/web/src/index.ts` imports from correct WASM paths.
2. **Add `npm run build` script** — runs `tsc` in each package.
3. **Fix any TS errors** — type-check all files.
4. **Add to CI** — `cargo.yml` GitHub Actions: `npm ci && npm run build`.

### Verification
```powershell
cd sdk
npm ci
npm run build
```

---

## Phase 15 — P2P Transport (WebRTC)

### Goal
Enable direct browser-to-browser or peer-to-peer CRDT sync over WebRTC data channels, using the coordinator as a signaling server. Falls back to coordinator transport when P2P is unavailable.

### New Crate: `drift-transport-webrtc`

```
crates/drift-transport-webrtc/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── signaling.rs     # Exchange SDP offer/answer via coordinator WS
    ├── channel.rs       # WebRTC DataChannel as binary frame transport
    ├── coordinator.rs   # PeerCoordinator — implements Coordinator trait over P2P
    └── discovery.rs     # Peer discovery: which replicas are online in namespace
```

### Wire Protocol Changes

Add 2 new WS message types to the coordinator server:

| Type | Value | Purpose |
|---|---|---|
| `P2P_SIGNAL` | `0x13` | Client → Server: SDP offer/answer or ICE candidate relay |
| `P2P_SIGNAL_ACK` | `0x14` | Server → Client: relay P2P signal to target replica |

#### [MODIFY] `crates/drift-core/src/coordinator/ws_proto.rs`
Add: `pub const MSG_P2P_SIGNAL: u8 = 0x13;`  
Add: `pub const MSG_P2P_SIGNAL_ACK: u8 = 0x14;`

#### [MODIFY] `crates/drift-coordinator-server/src/ws.rs`
Handle `P2P_SIGNAL` frames: relay to target replica's WebSocket session in the same namespace.

### Client Integration

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
After WebSocket connection established, attempt P2P:
```rust
if config.enable_p2p {
    self.discover_peers_and_connect().await?;
}
```

### Transport Priority
```
P2P (WebRTC, <5ms)  →  WebSocket to coordinator (30–200ms)  →  HTTP fallback
```

### Steps
1. Create `drift-transport-webrtc` crate with `webrtc-rs` or `web-sys` WebRTC bindings.
2. Implement `signaling.rs` — SDP exchange via coordinator relay.
3. Implement `channel.rs` — `DataChannel` wrapped as `Coordinator` trait impl.
4. Implement `discovery.rs` — fetch active peers for namespace from coordinator.
5. Add P2P_SIGNAL frame handlers in coordinator server.
6. Wire P2P transport into `DriftClient` behind `enable_p2p` config flag.
7. Tests: two-replica P2P sync test in `drift-transport-webrtc/tests/`.

### Verification
```powershell
cargo test -p drift-transport-webrtc
cargo check --workspace
```

---

## Phase 16 — Cloudflare Durable Objects Coordinator

### Goal
Implement `drift-coordinator-cf` — a Cloudflare Durable Objects coordinator using D1 (SQL) for persistence and the DO WebSocket hibernation API for real-time push.

### New Crate: `drift-coordinator-cf`

```
crates/drift-coordinator-cf/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── coordinator.rs   # Implements Coordinator trait
    ├── d1.rs            # D1 SQL queries (create, push, pull, replicas)
    ├── websocket.rs     # CF WebSocket hibernation API bindings
    └── worker.rs        # CF Worker entrypoint (wasm-bindgen exports)
```

### Deploy Config

```
wrangler.toml                  # CF Worker config
migrations/                    # D1 SQL migrations
  0001_init.sql
```

### Coordinator Trait Implementation

`CloudflareCoordinator` implements:
- `push()` → D1 INSERT + DO WebSocket broadcast
- `pull()` → D1 SELECT with sequence filter
- `subscribe()` → CF WebSocket hibernation (DO broadcasts to all connected WebSockets)
- `register()` → D1 INSERT into replicas table
- `heartbeat()` → D1 UPDATE last_heartbeat

### D1 Schema (same as coordinator schema, D1-compatible SQL)
```sql
-- Same tables as Postgres coordinator, adapted for D1 syntax
CREATE TABLE drift_coordinator_mutations (
  sequence      INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace     TEXT    NOT NULL,
  replica_id    TEXT    NOT NULL,
  mutation_id   TEXT    NOT NULL UNIQUE,
  encrypted     BLOB    NOT NULL,
  timestamp     INTEGER NOT NULL,
  created_at    INTEGER NOT NULL DEFAULT (unixepoch())
);
```

### Steps
1. Create `drift-coordinator-cf` crate targeting `wasm32-unknown-unknown`.
2. Implement `d1.rs` — D1 SQL bindings via `worker` crate.
3. Implement `coordinator.rs` with full Coordinator trait.
4. Implement `websocket.rs` — CF WebSocket hibernation for real-time push.
5. Add `wrangler.toml` and D1 migration SQL files.
6. Tests: integration tests against Wrangler local dev (`wrangler dev`).

### Verification
```powershell
wasm-pack build crates/drift-coordinator-cf --target bundler
wrangler dev --local
cargo test -p drift-coordinator-cf
```

---

## Phase 17 — Supabase Coordinator

### Goal
Implement `drift-coordinator-supabase` — a coordinator using Supabase (hosted Postgres + Realtime channels) for push/pull and real-time fan-out.

### New Crate: `drift-coordinator-supabase`

```
crates/drift-coordinator-supabase/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── coordinator.rs   # Implements Coordinator trait
    ├── realtime.rs      # Supabase Realtime WebSocket channel subscription
    └── migrations.rs    # SQL migration for Supabase schema
```

### Implementation Notes
- `push()` / `pull()` → Supabase REST API (`postgrest-rs` client) or raw `sqlx` Postgres connection
- `subscribe()` → Supabase Realtime channel: subscribe to `drift_coordinator_mutations` INSERT events
- Fan-out: Realtime delivers `MUTATION_PUSH` to all connected replicas in the namespace

### Steps
1. Create crate with `postgrest-rs` or `sqlx` + `supabase-realtime` dependencies.
2. Implement all Coordinator trait methods.
3. Add Supabase SQL migration (run via Supabase dashboard or CLI).
4. Tests: integration tests against `supabase start` local dev environment.

### Verification
```powershell
supabase start
cargo test -p drift-coordinator-supabase
```

---

## Phase 18 — Full CRDT Snapshot + Compaction

### Goal
Complete the snapshot and compaction system so the coordinator can: (a) create compressed snapshots of mutation history, (b) serve snapshots to new/stale replicas for fast bootstrap, and (c) prune old mutations safely.

### Files to Complete

#### [MODIFY] `crates/drift-core/src/crdt/snapshot.rs`
Complete from stub to full implementation:
```rust
pub struct CrdtSnapshot {
    pub namespace: String,
    pub sequence: SequenceId,
    pub mutation_count: u32,
    pub created_at: u64,
    pub data: Vec<u8>,  // zstd-compressed batch of encrypted mutations
}

impl CrdtSnapshot {
    pub fn create(mutations: &[PendingMutation]) -> Result<Self, DriftError>;
    pub fn decompress(&self) -> Result<Vec<PendingMutation>, DriftError>;
    pub fn verify_integrity(&self) -> Result<(), DriftError>;
}
```

#### [MODIFY] `crates/drift-core/src/sync/compaction.rs`
Complete compaction logic:
```rust
pub struct CompactionEngine {
    coordinator: Arc<dyn Coordinator>,
    storage: Arc<dyn Storage>,
    config: CompactionConfig,
}

impl CompactionEngine {
    /// Run if mutation count since last snapshot exceeds threshold.
    pub async fn maybe_compact(&self, namespace: &str) -> Result<(), DriftError>;
    /// Force create + store a new snapshot immediately.
    pub async fn force_snapshot(&self, namespace: &str) -> Result<CrdtSnapshot, DriftError>;
    /// Prune mutations older than min_replica_sequence across active replicas.
    pub async fn prune_old_mutations(&self, namespace: &str) -> Result<u64, DriftError>;
}
```

#### [MODIFY] `crates/drift-coordinator-server/src/ws.rs`
Handle `REGISTER_ACK` with `snapshot_available: true` — serve snapshot URL or inline snapshot bytes.

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
In reconnect flow, check `REGISTER_ACK.snapshot_available`:
```rust
if register_ack.snapshot_available {
    self.bootstrap_from_snapshot(register_ack.snapshot_url).await?;
}
```

#### [NEW] `crates/drift-coordinator-server/src/snapshot.rs`
Server-side snapshot store: `GET /namespace/:ns/snapshot/latest`, `POST /namespace/:ns/snapshot`.

### Coordinator Schema Additions
```sql
CREATE TABLE drift_coordinator_snapshots (
  id              TEXT    PRIMARY KEY,
  namespace       TEXT    NOT NULL,
  sequence        BIGINT  NOT NULL,
  snapshot        BYTEA   NOT NULL,  -- zstd-compressed
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Steps
1. Complete `CrdtSnapshot::create()` — collect mutations, zstd-compress.
2. Implement `CompactionEngine::maybe_compact()` — runs after every `snapshot_interval_mutations` pushes.
3. Add `drift_coordinator_snapshots` table migration to all coordinator backends.
4. Add snapshot HTTP endpoint to coordinator server.
5. Add bootstrap-from-snapshot to `HttpCoordinator` reconnect flow.
6. Add Yrs tombstone GC: call `yrs::GarbageCollector` after snapshot create.
7. Tests: `ws_test.rs` — new replica bootstrap from snapshot, stale replica rejection.

### Verification
```powershell
cargo test -p drift-coordinator-server -- snapshot
cargo test -p drift-core -- compaction
```

---

## Phase 19 — Multi-Namespace Connection Multiplexing

### Goal
Allow a single WebSocket connection to carry traffic for multiple namespaces, matching spec §27.

### Wire Protocol Change
All existing frame payloads already include `namespace` field. The change is:
- One physical WS connection per coordinator host (not one per namespace)
- Multiple `REGISTER` frames on one connection
- All `PUSH`/`PULL`/`MUTATION_PUSH` frames already carry `namespace` — no format change needed

### Files to Modify

#### [MODIFY] `crates/drift-coordinator-server/src/ws.rs`
- Session state changes from single-namespace to `HashSet<namespace>`
- `handle_ws_session()` becomes namespace-multiplexed
- Fan-out is keyed by `(namespace, session_id)` not just `namespace`

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
- `HttpCoordinator` stores one shared `Arc<WsSink>` per coordinator host
- `subscribe()` for a second namespace sends a second `REGISTER` on the existing connection
- Route incoming `MUTATION_PUSH` frames to the correct namespace handler by `frame.namespace`

#### [MODIFY] `crates/drift-core/src/client.rs`
- `DriftClient::with_additional_namespace()` — attach a new namespace to the existing coordinator connection

#### [NEW] `crates/drift-core/src/coordinator/connection_pool.rs`
```rust
/// Maintains one WS connection per (host, auth_token) pair.
/// Multiple DriftClient instances sharing same host reuse the connection.
pub struct ConnectionPool {
    connections: HashMap<String, Arc<SharedWsConnection>>,
}
```

### Steps
1. Implement `ConnectionPool` with host-keyed connection sharing.
2. Refactor server session to hold `Vec<String>` namespaces, not one.
3. Update server fan-out to route by `(namespace, session)` pair.
4. Update client `subscribe()` to reuse existing connection for same host.
5. Tests: two-namespace test — one WS connection, two namespace streams.

### Verification
```powershell
cargo test -p drift-coordinator-server -- multiplexing
cargo test -p drift-core -- connection_pool
```

---

## Phase 20 — E2EE Key Rotation

### Goal
Allow a replica to rotate its device keypair without data loss, and distribute the rotation to all peers via the coordinator.

### Wire Protocol
Already defined in spec (MSG `0x12` `KEY_CHANGED` exists in `ws_proto.rs`). Need to implement the full flow.

### Files to Modify/Create

#### [MODIFY] `crates/drift-core/src/e2ee/keyring.rs`
Add:
```rust
impl KeyRing {
    /// Generate new keypair and sign the rotation with old key.
    pub async fn rotate_keypair(&mut self, namespace: &str) -> Result<KeyRotationProof, DriftError>;

    /// Apply incoming key rotation from another replica.
    pub async fn apply_peer_key_rotation(&mut self, proof: KeyRotationProof) -> Result<(), DriftError>;
}

pub struct KeyRotationProof {
    pub replica_id: String,
    pub old_key_version: u32,
    pub new_key_version: u32,
    pub new_public_key: Vec<u8>,
    pub signature: Vec<u8>,  // sign(old_sk, concat(replica_id, new_pk, new_version))
}
```

#### [MODIFY] `crates/drift-coordinator-server/src/ws.rs`
Handle `KEY_CHANGED (0x12)` broadcast:
- Verify signature with stored old public key
- Update `drift_coordinator_replicas.public_key` and `key_version`
- Broadcast `KEY_CHANGED` to all connected replicas in the namespace

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
Handle incoming `KEY_CHANGED` frame — call `keyring.apply_peer_key_rotation()`.

#### [NEW] `crates/drift-core/src/e2ee/key_fetch.rs`
Implement `KEY_FETCH (0x0D)` / `KEY_RESPONSE (0x0E)` WS frames:
```rust
pub async fn fetch_peer_keys(namespace: &str) -> Result<Vec<ReplicaPublicKey>, DriftError>;
```

### Steps
1. Complete `KeyRing::rotate_keypair()` — keygen, sign, upload rotation proof.
2. Implement coordinator-side key rotation handler in `ws.rs`.
3. Implement `KEY_FETCH`/`KEY_RESPONSE` wire frames in client and server.
4. Add TOFU key pinning: warn on unexpected key change.
5. Add `strict` key validation mode (operator approval required).
6. Tests: rotation test — rotate key, verify old mutations still decryptable, new mutations use new key.

### Verification
```powershell
cargo test -p drift-core -- key_rotation
cargo test -p drift-coordinator-server -- key_rotation
```

---

## Phase 21 — Coordinator Conformance Test Suite

### Goal
A single `drift-conformance` test crate that all coordinator implementations must pass identically.

### New Crate: `drift-conformance`

```
crates/drift-conformance/
├── Cargo.toml
└── tests/
    ├── push_pull.rs        # push + pull roundtrip
    ├── subscribe.rs        # real-time subscription delivery
    ├── register.rs         # replica registration + heartbeat
    ├── dedup.rs            # mutation dedup by mutation_id
    ├── schema_version.rs   # schema version enforcement
    ├── snapshot.rs         # snapshot create + bootstrap
    ├── compaction.rs       # mutation pruning after compaction
    ├── fanout.rs           # multi-replica fan-out ordering
    ├── partial_sync.rs     # partial sync filter enforcement
    └── stale_replica.rs    # REPLICA_TOO_STALE detection
```

### Test Matrix
Each test file runs against all coordinator backends via a `CoordinatorFactory` trait:

```rust
#[async_trait]
pub trait CoordinatorFactory {
    async fn create(&self) -> Arc<dyn Coordinator>;
    async fn teardown(&self, c: Arc<dyn Coordinator>);
}

// Test invocation:
run_conformance_tests(MemoryCoordinatorFactory).await;
run_conformance_tests(SqliteCoordinatorFactory).await;
run_conformance_tests(PostgresCoordinatorFactory::from_env()).await;
run_conformance_tests(RedisCoordinatorFactory::from_env()).await;
```

### Steps
1. Create `drift-conformance` crate with `CoordinatorFactory` trait.
2. Write all 10 test modules (50+ individual test cases).
3. Add factory impls for all 4 existing backends.
4. Add `drift-coordinator-cf` and `drift-coordinator-supabase` factories (after Phase 16/17).
5. CI: run conformance suite for memory + sqlite always; postgres + redis in integration CI.

### Verification
```powershell
cargo test -p drift-conformance --features memory,sqlite
cargo test -p drift-conformance --features postgres,redis  # requires running DB
```

---

## Phase 22 — Coordinator High-Availability Failover

### Goal
Support multi-instance coordinator deployments with automatic client failover.

### Files to Modify

#### [MODIFY] `crates/drift-core/src/coordinator/http.rs`
Add multi-endpoint support:
```rust
pub struct HttpCoordinatorConfig {
    pub endpoints: Vec<String>,           // multiple coordinator URLs
    pub strategy: FailoverStrategy,       // RoundRobin | Failover
    // ...existing fields
}

enum FailoverStrategy { RoundRobin, Failover }
```

In reconnect loop:
```rust
let endpoint = self.config.next_endpoint();  // rotate on failure
self.connect_to(endpoint).await
```

#### [MODIFY] `crates/drift-coordinator-server/src/main.rs`
Add `--ha-peer-url` flag: coordinator instances register with each other for cross-instance fan-out via Redis pub/sub or Postgres LISTEN/NOTIFY.

### Steps
1. Add `endpoints: Vec<String>` to `HttpCoordinatorConfig`.
2. Implement `next_endpoint()` with round-robin and failover strategies.
3. Update reconnect loop to rotate endpoints on failure.
4. Document HA deployment pattern (HAProxy + Postgres primary/replica).
5. Tests: failover test — kill coordinator A, verify client reconnects to B with no data loss.

### Verification
```powershell
cargo test -p drift-core -- failover
```

---

## Phase 23 — Swift SDK (iOS/macOS)

### Goal
Ship `drift-swift` — a Swift Package Manager library wrapping `drift-core` via a static Rust library.

### Architecture
```
drift-swift/
├── Package.swift
├── Sources/
│   └── Drift/
│       ├── DriftClient.swift         # Swift-native async/await API
│       ├── DriftCoordinator.swift    # WebSocket coordinator
│       ├── DriftStorage.swift        # SQLite (via GRDB or SQLite.swift)
│       └── DriftBridge.swift         # FFI bridge to libdrift.a
└── Tests/
    └── DriftTests/
        └── SyncTests.swift
```

### Rust Side
#### [NEW] `crates/drift-ffi/`
```
crates/drift-ffi/
├── Cargo.toml            # cdylib + staticlib
└── src/
    ├── lib.rs            # C-compatible FFI exports via cbindgen
    ├── client.rs         # drift_client_new(), drift_client_insert(), etc.
    └── types.rs          # FFI-safe types (raw pointers, C strings)
```

```rust
// C-compatible FFI
#[no_mangle]
pub extern "C" fn drift_client_new(config: *const DriftConfigFFI) -> *mut DriftClientHandle;

#[no_mangle]
pub extern "C" fn drift_client_insert(
    handle: *mut DriftClientHandle,
    doc_id: *const c_char,
    record_id: *const c_char,
    fields_json: *const c_char,
    callback: extern "C" fn(error: *const c_char),
);
```

### Steps
1. Create `drift-ffi` crate generating `libdrift.a` + C header via `cbindgen`.
2. Build fat static lib for `aarch64-apple-ios` + `aarch64-apple-darwin` (XCFramework).
3. Scaffold `drift-swift` Swift Package with FFI bridge.
4. Implement Swift async/await wrappers (`async throws` for all operations).
5. Implement `DriftStorage` using GRDB (Sqlite) for iOS.
6. Tests: Swift unit tests using `DriftClient` with `InMemoryCoordinator`.

### Verification
```bash
cargo build -p drift-ffi --target aarch64-apple-ios --release
xcodebuild test -scheme Drift -destination 'platform=iOS Simulator,name=iPhone 15'
```

---

## Phase 24 — Kotlin SDK (Android)

### Goal
Ship `drift-android` — a Kotlin Android library using JNI bindings to `drift-core`.

### Architecture
```
drift-android/
├── build.gradle.kts
├── src/
│   └── main/
│       ├── kotlin/io/drift/
│       │   ├── DriftClient.kt        # Kotlin coroutine-based API
│       │   ├── DriftCoordinator.kt   # WebSocket coordinator
│       │   └── DriftStorage.kt       # SQLite (Room-compatible)
│       └── jni/
│           └── libdrift.so           # built from drift-ffi
└── src/test/
    └── kotlin/io/drift/
        └── SyncTest.kt
```

### Rust Side
Reuse `drift-ffi` crate from Phase 23, targeting `aarch64-linux-android`.

### Steps
1. Add Android target to `drift-ffi`: `cargo build --target aarch64-linux-android`.
2. Scaffold Android library project with Gradle.
3. Write JNI wrapper Kotlin file loading `libdrift.so`.
4. Implement `DriftClient.kt` with Kotlin `suspend fun` wrappers.
5. Implement Room-compatible `DriftStorage.kt`.
6. Tests: Android JUnit instrumented tests.

### Verification
```bash
cargo build -p drift-ffi --target aarch64-linux-android --release
./gradlew test
./gradlew connectedAndroidTest  # runs on emulator
```

---

## Phase 25 — Advanced Observability Dashboard

### Goal
Build a web-based observability dashboard for live Drift debugging: conflict tracking, sync analytics, replica status, oplog viewer.

### New Package: `sdk/packages/dashboard`

```
sdk/packages/dashboard/
├── package.json           # @drift/dashboard
├── index.html
└── src/
    ├── main.ts            # Vite + vanilla TS
    ├── api.ts             # Polls /debug/drift/* endpoints
    ├── components/
    │   ├── ReplicaMap.ts  # Live replica connection graph
    │   ├── OplogViewer.ts # Real-time oplog tail
    │   ├── SyncLag.ts     # Sync lag histogram chart
    │   ├── MetricsPanel.ts # Prometheus metrics display
    │   └── MutationInspector.ts  # Decode + display mutations
    └── charts/
        └── timeseries.ts  # Lightweight chart library
```

### Backend Additions

#### [MODIFY] `crates/drift-coordinator-server/src/routes.rs`
Extend debug API:
```
GET  /debug/drift/sync-analytics      → per-namespace sync lag, throughput
GET  /debug/drift/replica-map         → replica topology as graph JSON
GET  /debug/drift/mutations/stream    → SSE stream of live mutations (debug only)
GET  /debug/drift/metrics/prometheus  → Prometheus text format
```

#### [MODIFY] `crates/drift-core/src/telemetry/debug.rs`
Add sync analytics endpoint: lag percentiles, mutations/sec, error rates.

### CLI Extension

#### [MODIFY] `crates/drift-cli/src/inspect.rs`
Add `drift inspect --server wss://host/drift` — connect to live coordinator and stream mutations.

### Steps
1. Extend debug API routes on coordinator server.
2. Build `@drift/dashboard` Vite app with real-time charts.
3. Extend `drift inspect` CLI to connect to live server.
4. Add Prometheus text-format metrics endpoint.
5. Add SSE stream for live mutation debugging.

### Verification
```powershell
cargo run -p drift-coordinator-server
cd sdk/packages/dashboard && npm run dev
# Open http://localhost:3000 — verify live replica map, oplog tail, sync lag
```

---

## Phase 26 — Mesh Transport (libp2p)

### Goal
Enable server-to-server CRDT sync without a central coordinator via libp2p gossip, enabling fully decentralized mesh topologies.

### New Crate: `drift-transport-libp2p`

```
crates/drift-transport-libp2p/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── mesh.rs          # libp2p swarm + Kademlia DHT for peer discovery
    ├── gossip.rs        # gossipsub protocol for mutation broadcast
    ├── coordinator.rs   # MeshCoordinator implementing Coordinator trait
    └── bootstrap.rs     # Bootstrap nodes for DHT
```

### Architecture
```
Server A (libp2p)  ←→  Server B (libp2p)
       ↕                      ↕
Server C (libp2p)  ←→  Server D (libp2p)
    (Kademlia DHT — peer discovery)
    (GossipSub — mutation broadcast)
```

### Coordinator Trait Implementation
`MeshCoordinator`:
- `push()` → GossipSub publish on `drift/{namespace}` topic
- `pull()` → Kademlia query for mutations since sequence
- `subscribe()` → GossipSub subscription → stream mutations as they arrive
- `register()` → DHT put: `drift/replica/{replica_id}` → replica metadata

### Steps
1. Create crate with `libp2p` dependency (gossipsub + kademlia).
2. Implement `MeshCoordinator` with all Coordinator trait methods.
3. Implement peer discovery via Kademlia.
4. Implement mutation broadcast via GossipSub.
5. Add bootstrap node configuration.
6. Tests: 4-node mesh test — A pushes mutation, verify B/C/D receive.

### Verification
```powershell
cargo test -p drift-transport-libp2p
```

---

## Phase 27 — Production Hardening (Stable Milestone)

### Goal
Complete all items required for the Stable release: full test coverage, fuzz CI, chaos tests, security audit prep, performance optimization.

### Sub-tasks

#### 27a — Performance Benchmark CI Gates
#### [NEW] `benches/drift_benchmarks.rs` (in `drift-core`)
```rust
// Criterion benchmarks — run in CI, fail if >10% regression vs main
bench_local_read_1row
bench_local_write_1row
bench_crdt_merge_1op
bench_e2ee_encrypt_1op
bench_e2ee_decrypt_1op
bench_snapshot_load_1mb
bench_tab_to_tab_latency
bench_full_operation_path
```

Add to CI: `cargo bench --workspace -- --output-format=bencher | tee bench_results.txt`

#### 27b — Fuzz Testing CI (24h)
#### [MODIFY] `crates/drift-fuzz/fuzz_targets/`
Add new targets:
- `fuzz_ws_frame.rs` — random binary input into WS frame decoder
- `fuzz_snapshot_load.rs` — random bytes into snapshot decompressor
- `fuzz_migration_runner.rs` — random migration sequences

Add to CI: scheduled 24h fuzz run via GitHub Actions cron.

#### 27c — Chaos Test Suite
#### [NEW] `tests/chaos/`
```
tests/chaos/
├── network_partition.rs     # Drop WS, verify reconnect + no data loss
├── clock_skew.rs            # Simulate ±500ms clock skew across replicas
├── coordinator_crash.rs     # Kill coordinator mid-push, verify retry
├── leader_crash.rs          # Kill leader tab, verify re-election
└── replica_stale.rs         # Offline 91 days, verify bootstrap from snapshot
```

#### 27d — Security Hardening
- Audit all `unsafe` blocks (currently 0 in drift-core — verify)
- Add `cargo audit` to CI (checks for known CVEs in dependencies)
- Add `cargo deny` to CI (license + dependency policy)
- Validate E2EE key material never appears in logs

#### 27e — Test Coverage Gate
- Add `cargo tarpaulin` to CI
- Gate: critical paths (crdt/, e2ee/, sync/, coordinator/) must have ≥ 90% line coverage

#### 27f — Docker Image
#### [NEW] `Dockerfile` (coordinator server)
```dockerfile
FROM rust:1.78 AS builder
WORKDIR /app
COPY . .
RUN cargo build -p drift-coordinator-server --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/drift-coordinator-server /usr/local/bin/
ENTRYPOINT ["drift-coordinator-server"]
```

### Verification
```powershell
# Benchmarks
cargo bench --workspace

# Fuzz (short run)
cargo fuzz run fuzz_ws_frame -- -max_total_time=60

# Chaos
cargo test -p drift-chaos

# Coverage
cargo tarpaulin --workspace --exclude drift-fuzz --out Html

# Audit
cargo audit
cargo deny check

# Docker
docker build -t drift-coordinator:latest .
docker run -p 8080:8080 -e DRIFT_DB_URL=sqlite:///data/drift.db drift-coordinator:latest
```

---

## Execution Order (Recommended)

```
NOW (Alpha blockers — do these first):
  Phase 14a: @drift/node SDK          (~3 days)
  Phase 14b: Migration auto-execution (~1 day)
  Phase 14c: SDK TS build CI          (~0.5 day)

BETA (in parallel where possible):
  Phase 18: CRDT Snapshot + Compaction     (~3 days)  ← needed by 16, 21, 22, 23
  Phase 19: Multi-Namespace Multiplexing   (~2 days)
  Phase 20: E2EE Key Rotation              (~2 days)
  Phase 15: P2P WebRTC                     (~5 days)
  Phase 16: Cloudflare DO Coordinator      (~3 days)
  Phase 17: Supabase Coordinator           (~2 days)

STABLE:
  Phase 21: Conformance Suite              (~3 days)  ← run after all coordinators done
  Phase 22: HA Failover                    (~2 days)
  Phase 23: Swift SDK                      (~1 week)
  Phase 24: Kotlin SDK                     (~1 week)
  Phase 25: Observability Dashboard        (~3 days)
  Phase 26: Mesh Transport (libp2p)        (~1 week)
  Phase 27: Production Hardening           (~1 week)
```

## Total Estimate: ~9–11 weeks to Stable
