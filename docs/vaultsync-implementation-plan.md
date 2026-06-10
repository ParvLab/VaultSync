# VaultSync — End-to-End Implementation Plan
### Monorepo Structure, Phase Breakdown, Dependency Graph & Milestones

---

## Table of Contents

1. [Monorepo Structure](#1-monorepo-structure)
2. [Crate & Package Dependency Graph](#2-crate--package-dependency-graph)
3. [Phase Breakdown](#3-phase-breakdown)
4. [Phase 0 — Scaffolding & Build System](#4-phase-0--scaffolding--build-system)
5. [Phase 1 — Core Engine (Rust)](#5-phase-1--core-engine-rust)
6. [Phase 2 — WASM & Browser Support](#6-phase-2--wasm--browser-support)
7. [Phase 3 — Multi-Tab & IPC](#7-phase-3--multi-tab--ipc)
8. [Phase 4 — Coordinator Implementations](#8-phase-4--coordinator-implementations)
9. [Phase 5 — TypeScript SDK & React](#9-phase-5--typescript-sdk--react)
10. [Phase 6 — Testing Infrastructure](#10-phase-6--testing-infrastructure)
11. [Phase 7 — CLI & Observability](#11-phase-7--cli--observability)
12. [Phase 8 — Production Hardening](#12-phase-8--production-hardening)
13. [Phase 9 — Additional SDKs & Transports](#13-phase-9--additional-sdks--transports)
14. [Build & Release Strategy](#14-build--release-strategy)
15. [Milestone Timeline](#15-milestone-timeline)
16. [Risk Register](#16-risk-register)

---

## 1. Monorepo Structure

```
E:\vaultsync\
│
├── Cargo.toml                          # Workspace root
├── package.json                        # NPM workspace root
├── tsconfig.json                       # TypeScript base config
├── .github/
│   ├── workflows/
│   │   ├── ci.yml                      # Full CI pipeline
│   │   ├── coverage.yml                # Coverage enforcement
│   │   ├── nightly.yml                 # Nightly: fuzz, chaos, long benchmarks
│   │   └── release.yml                 # Package publish
│   └── CODEOWNERS
│
├── docs/
│   ├── vaultsync-spec.md                   # System specification (complete)
│   ├── vaultsync-testing-spec.md           # Testing specification (complete)
│   └── vaultsync-implementation-plan.md    # This file
│
├── crates/
│   ├── vaultsync-core/                     # Core Rust library
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── crdt/
│   │       │   ├── mod.rs
│   │       │   ├── document.rs         # CRDTDocument: wraps Yrs doc
│   │       │   ├── merge.rs            # Merge strategies (LWW, Counter, OR-Set, Text)
│   │       │   ├── types.rs            # CRDT type definitions
│   │       │   ├── snapshot.rs         # Yrs snapshot serialization
│   │       │   └── compaction.rs       # Tombstone GC + snapshot compaction
│   │       ├── e2ee/
│   │       │   ├── mod.rs
│   │       │   ├── keyring.rs          # Key generation, storage, rotation
│   │       │   ├── encrypt.rs          # X25519 + ChaCha20-Poly1305
│   │       │   ├── decrypt.rs
│   │       │   └── kat.rs              # Known-answer test vectors
│   │       ├── storage/
│   │       │   ├── mod.rs
│   │       │   ├── traits.rs           # Storage trait
│   │       │   ├── sqlite.rs           # SQLite (rusqlite) implementation
│   │       │   ├── memory.rs           # In-memory implementation
│   │       │   └── encryption_shim.rs  # Encrypt-at-rest wrapper
│   │       ├── oplog/
│   │       │   ├── mod.rs
│   │       │   ├── entry.rs            # OplogEntry struct
│   │       │   ├── log.rs              # OpLog: append, read, mark, compact
│   │       │   └── cleanup.rs          # Compaction and archival
│   │       ├── schema/
│   │       │   ├── mod.rs
│   │       │   ├── registry.rs         # SchemaRegistry: define, validate
│   │       │   ├── migration.rs        # Migration: apply, rollback, checksum
│   │       │   └── field.rs           # FieldDefinition with CRDT type
│   │       ├── subscription/
│   │       │   ├── mod.rs
│   │       │   ├── engine.rs           # SubscriptionEngine: register, unregister, fire
│   │       │   ├── filter.rs           # Query filter matching (CRDT-aware)
│   │       │   └── observer.rs         # CRDT document change watcher
│   │       ├── sync/
│   │       │   ├── mod.rs
│   │       │   ├── upload.rs           # UploadQueue: poll, batch, send, retry
│   │       │   ├── download.rs         # DownloadQueue: poll, receive, decrypt, merge
│   │       │   ├── retry.rs            # RetryEngine: backoff, jitter, max attempts
│   │       │   ├── reconciler.rs       # Reconciliation: apply remote mutations
│   │       │   └── state.rs            # SyncState: per-namespace tracking
│   │       ├── coordinator/
│   │       │   ├── mod.rs
│   │       │   ├── traits.rs           # Coordinator trait definition
│   │       │   ├── postgres.rs         # PostgresCoordinator
│   │       │   ├── memory.rs           # InMemoryCoordinator (testing)
│   │       │   └── mock.rs             # MockCoordinator (deterministic testing)
│   │       ├── ipc/
│   │       │   ├── mod.rs
│   │       │   ├── leader_election.rs  # File lock / named pipe / BroadcastChannel
│   │       │   ├── shared_memory.rs    # Mmap / SharedArrayBuffer ring buffer
│   │       │   ├── heartbeat.rs        # Heartbeat writer + detector
│   │       │   └── crash_recovery.rs   # Ring buffer replay on leader promotion
│   │       ├── telemetry/
│   │       │   ├── mod.rs
│   │       │   ├── tracing.rs          # OpenTelemetry span initialization
│   │       │   ├── metrics.rs          # Prometheus metric registration
│   │       │   └── debug.rs            # Debug HTTP API handlers
│   │       ├── client.rs               # VaultSyncClient: public API
│   │       ├── config.rs               # Configuration structs
│   │       ├── error.rs                # Error types
│   │       └── test_utils.rs           # TestFixture, SimulatedNetwork
│   │
│   ├── vaultsync-wasm/                     # WASM bindings (wasm-pack)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs               # WASM-exposed VaultSyncClient
│   │       ├── storage.rs              # OPFS + IndexedDB storage via wasm
│   │       ├── ipc.rs                  # BroadcastChannel + SharedArrayBuffer
│   │       ├── e2ee.rs                 # WebCrypto E2EE bindings
│   │       └── transport.rs            # WebSocket via web-sys
│   │
│   ├── vaultsync-coordinator-postgres/     # Postgres coordinator package
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   │
│   ├── vaultsync-coordinator-redis/        # Redis coordinator package
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   │
│   ├── vaultsync-coordinator-sqlite/       # Embedded SQLite coordinator
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   │
│   ├── vaultsync-coordinator-memory/       # In-memory coordinator (testing)
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   │
│   ├── vaultsync-cli/                      # CLI tools
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       ├── inspect.rs              # `vaultsync inspect` — attach to running process
│   │       ├── replay.rs               # `vaultsync replay` — replay trace file
│   │       └── bench.rs                # `vaultsync bench` — run benchmarks manually
│   │
│   └── vaultsync-fuzz/                     # Fuzz targets (cargo fuzz)
│       ├── Cargo.toml
│       └── fuzz_targets/
│           ├── crdt_merge.rs
│           ├── e2ee_ciphertext.rs
│           ├── ipc_message.rs
│           ├── coordinator_protocol.rs
│           ├── snapshot.rs
│           └── oplog_entry.rs
│
├── packages/
│   ├── web/                            # @vaultsync/web — Browser SDK
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── src/
│   │   │   ├── index.ts
│   │   │   ├── vaultsync.ts               # VaultSync class (initialization)
│   │   │   ├── db.ts                  # Database proxy
│   │   │   ├── schema.ts              # Schema definition
│   │   │   ├── subscription.ts        # Subscription engine wrapper
│   │   │   ├── sync.ts                # Sync status
│   │   │   ├── keys.ts                # E2EE key management
│   │   │   └── coordinator/
│   │   │       ├── postgres.ts         # Postgres coordinator client
│   │   │       ├── redis.ts            # Redis coordinator client
│   │   │       └── custom.ts           # Custom coordinator client (WebSocket)
│   │   ├── __tests__/
│   │   └── wasm/                      # Compiled vaultsync-wasm output
│   │
│   ├── node/                           # @vaultsync/node — Node.js SDK
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── src/
│   │   │   ├── index.ts
│   │   │   ├── vaultsync.ts
│   │   │   ├── db.ts
│   │   │   ├── storage.ts             # Node SQLite bindings (better-sqlite3)
│   │   │   └── coordinator/
│   │   └── __tests__/
│   │
│   ├── react/                          # @vaultsync/react — React hooks
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── src/
│   │   │   ├── index.ts
│   │   │   ├── provider.tsx            # VaultSyncProvider context
│   │   │   ├── hooks.ts               # useVaultSyncQuery, useVaultSyncOne, useVaultSyncSync
│   │   │   ├── mutations.ts           # useVaultSyncMutations
│   │   │   └── sync-indicator.tsx      # SyncIndicator component
│   │   └── __tests__/
│   │
│   ├── coordinator-postgres/           # @vaultsync/coordinator-postgres
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── index.ts
│   │       └── postgres-coordinator.ts # TypeScript Postgres coordinator
│   │
│   ├── coordinator-redis/              # @vaultsync/coordinator-redis
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.ts
│   │       └── redis-coordinator.ts
│   │
│   ├── coordinator-supabase/           # @vaultsync/coordinator-supabase
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.ts
│   │       └── supabase-coordinator.ts
│   │
│   └── coordinator-cloudflare/         # @vaultsync/coordinator-cloudflare
│       ├── package.json
│       ├── tsconfig.json
│       ├── src/
│       │   ├── index.ts
│       │   ├── durable-object.ts       # Durable Object implementation
│       │   └── workers.ts             # Cloudflare Workers API
│       └── wrangler.toml
│
├── examples/                           # Example projects (scaffolded, implemented later)
│   ├── todo-basic/
│   ├── todo-multitab/
│   ├── collaborative-editor/
│   ├── offline-mobile/
│   ├── custom-coordinator/
│   └── chaos-demo/
│
└── scripts/
    ├── setup-dev.sh                    # Install dependencies, setup DBs
    ├── run-all-tests.sh               # Run full test suite locally
    ├── benchmark-compare.sh            # Compare benchmarks against main
    └── publish.sh                      # Build and publish packages
```

---

## 2. Crate & Package Dependency Graph

### Rust Crate Dependencies

```
                    ┌─────────────┐
                    │  vaultsync-fuzz │
                    └──────┬──────┘
                           │ (dev-dependency)
                    ┌──────▼──────┐
                    │  vaultsync-core  │◄──────────────────────┐
                    │              │                        │
                    │ deps:        │                        │
                    │  yrs         │                        │
                    │  libsodium   │                        │
                    │  tokio       │                        │
                    │  serde       │                        │
                    │  tracing     │                        │
                    │  opentelemetry│                       │
                    │  rusqlite    │                        │
                    └──────┬──────┘                        │
                           │                               │
          ┌────────────────┼────────────────┬──────────────┘
          │                │                │
   ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
   │vaultsync-wasm   │  │vaultsync-coord- │  │vaultsync-coord- │
   │             │  │postgres     │  │redis        │
   │ deps:       │  │             │  │             │
   │  wasm-bindgen│  │ deps:       │  │ deps:       │
   │  web-sys    │  │  sqlx       │  │  redis-rs   │
   │  js-sys     │  │  tokio-postgres│ │  tokio      │
   └─────────────┘  └─────────────┘  └─────────────┘
                           │
                    ┌──────▼──────┐
                    │vaultsync-coord- │
                    │sqlite       │
                    │             │
                    │ deps:       │
                    │  rusqlite   │
                    └─────────────┘

                    ┌─────────────┐
                    │  vaultsync-cli  │
                    │             │
                    │ deps:       │
                    │  vaultsync-core │
                    │  clap       │
                    │  tokio      │
                    └─────────────┘
```

### NPM Package Dependencies

```
                    ┌─────────────────────────┐
                    │  @vaultsync/web             │
                    │                         │
                    │ depends on:             │
                    │  vaultsync-wasm (wasm-pack) │
                    │  EventSource            │
                    │  broadcast-channel      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  @vaultsync/node             │
                    │                         │
                    │ depends on:             │
                    │  vaultsync-core (native)    │
                    │  better-sqlite3         │
                    └────────────┬────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
   ┌──────▼──────┐       ┌──────▼──────┐       ┌──────▼──────┐
   │@vaultsync/react │       │@vaultsync/      │       │@vaultsync/      │
   │             │       │coordinator  │       │coordinator  │
   │ depends on: │       │postgres    │       │redis        │
   │  @vaultsync/web │       │             │       │             │
   │  React      │       │ deps:       │       │ deps:       │
   │             │       │  pg         │       │  ioredis    │
   └─────────────┘       └─────────────┘       └─────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  @vaultsync/coordinator-cloudflare                           │
   │                                                          │
   │  depends on:                                             │
   │    @cloudflare/workers-types                             │
   │    wrangler (dev)                                        │
   └──────────────────────────────────────────────────────────┘
```

---

## 3. Phase Breakdown

| Phase | Name | Duration | Dependencies |
|---|---|---|---|
| **P0** | Scaffolding & Build System | Week 1 | None |
| **P1** | Core Engine (Rust) | Weeks 2–6 | P0 |
| **P2** | WASM & Browser Support | Weeks 7–8 | P1 |
| **P3** | Multi-Tab & IPC | Weeks 9–10 | P1 |
| **P4** | Coordinator Implementations | Weeks 11–13 | P1 |
| **P5** | TypeScript SDK & React | Weeks 14–16 | P1 + P2 + P4 |
| **P6** | Testing Infrastructure | Weeks 17–19 | P1 + P2 + P3 + P4 + P5 |
| **P7** | CLI & Observability | Weeks 20–21 | P1 + P3 |
| **P8** | Production Hardening | Weeks 22–26 | All prior phases |
| **P9** | Additional SDKs & Transports | Weeks 27–32 | P1 + P5 |

**Total estimated time: 32 weeks (~8 months) for beta-quality release.**

---

## 4. Phase 0 — Scaffolding & Build System

### Goal
Set up the monorepo structure, build toolchain, CI pipeline stub, and dependency configuration. No application code — just infrastructure.

### Tasks

| Task | File(s) | Details |
|---|---|---|
| **P0.1** Create workspace root | `Cargo.toml` | `[workspace]` with members for all crates. Pin Rust edition 2024 |
| **P0.2** Configure NPM workspace | `package.json` | `"workspaces": ["packages/*"]`, `"private": true` |
| **P0.3** Create vaultsync-core crate | `crates/vaultsync-core/Cargo.toml` | Core dependencies: `yrs = "0.19"`, `libsodium-sys`, `tokio`, `serde`, `tracing`, `opentelemetry`, `rusqlite` |
| **P0.4** Create vaultsync-wasm crate | `crates/vaultsync-wasm/Cargo.toml` | Add `wasm-bindgen`, `wasm-pack` build config |
| **P0.5** Create coordinator crates | `crates/vaultsync-coordinator-*/Cargo.toml` | Postgres (`sqlx`), Redis (`redis-rs`), SQLite (`rusqlite`), Memory (none) |
| **P0.6** Create vaultsync-cli crate | `crates/vaultsync-cli/Cargo.toml` | `clap` for CLI args, `tokio` for async |
| **P0.7** Create vaultsync-fuzz crate | `crates/vaultsync-fuzz/Cargo.toml` | `cargo-fuzz` config, libfuzzer-sys dependency |
| **P0.8** Create NPM package stubs | `packages/*/package.json` | Each with name, version `0.0.0`, deps, tsconfig |
| **P0.9** Create TS config base | `tsconfig.json` | Strict mode, ES2022 target, module resolution |
| **P0.10** Setup CI stub | `.github/workflows/ci.yml` | Minimal: lint + build only. Expand per phase |
| **P0.11** Setup development environment | `scripts/setup-dev.sh` | Install Rust, wasm-pack, Node, Docker for coordinators |
| **P0.12** Configure Rust toolchain | `rust-toolchain.toml` | Pin stable, add wasm32 target |

### Deliverables
- Working `cargo build` across all crates
- Working `npm install` across all packages
- CI pipeline passes lint + build
- Developer environment script documented

---

## 5. Phase 1 — Core Engine (Rust)

### Goal
Build the complete Rust core library: CRDT document store, E2EE, storage abstraction, oplog, schema, subscriptions, sync engine, coordinator trait.

### Task Dependency Order (Must Build in This Sequence)

```
Step 1: CRDT Document Layer
  ├── document.rs → merge.rs → types.rs
  └── snapshot.rs → compaction.rs

Step 2: E2EE Layer
  ├── keyring.rs → encrypt.rs → decrypt.rs
  └── kat.rs (tests)

Step 3: Storage Trait + SQLite
  ├── traits.rs
  ├── sqlite.rs
  └── memory.rs

Step 4: OpLog
  ├── entry.rs → log.rs
  └── cleanup.rs

Step 5: Schema + Migrations
  ├── field.rs → registry.rs
  └── migration.rs

Step 6: Subscriptions
  ├── observer.rs → engine.rs
  └── filter.rs

Step 7: Sync Engine
  ├── state.rs → upload.rs → download.rs
  ├── retry.rs
  └── reconciler.rs

Step 8: Coordinator Trait
  ├── traits.rs
  ├── memory.rs
  └── mock.rs

Step 9: Client API
  ├── config.rs → client.rs
  └── error.rs

Step 10: Telemetry Stub
  ├── tracing.rs
  ├── metrics.rs
  └── debug.rs
```

### Detailed File Implementation Plan

#### Step 1.1: CRDT Document Layer (`crates/vaultsync-core/src/crdt/`)

**`types.rs`** — CRDT type enum and field mapping:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtType {
    LwwRegister,   // Last-write-wins via Yrs Map
    PnCounter,     // Yrs Counter
    OrSet,         // Yrs Array with tombstone semantics
    Text,          // Yrs Text
    Array,         // Yrs Array
    Custom(String), // Plugin-defined
}

impl CrdtType {
    pub fn to_yrs_type(&self) -> yrs::types::Type;
}
```

**`document.rs`** — CRDTDocument wraps Yrs Doc:
```rust
pub struct CRDTDocument {
    pub doc_id: String,
    pub record_id: String,
    inner: yrs::Doc,          // Yrs document
    root: yrs::types::MapRef, // Root map
    schema_version: u64,
}

impl CRDTDocument {
    pub fn new(doc_id: &str, record_id: &str, schema: &DocumentSchema) -> Self;
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self>;
    pub fn to_snapshot(&self) -> Vec<u8>;
    pub fn apply_update(&mut self, update: &[u8]) -> Result<()>;
    pub fn get_field(&self, field: &str) -> Option<CrdtValue>;
    pub fn set_field(&mut self, field: &str, value: CrdtValue) -> YrsUpdate;
    pub fn delete_field(&mut self, field: &str) -> YrsUpdate;
    pub fn encode_update(&self) -> Vec<u8>;  // Yrs binary diff
    pub fn is_consistent(&self) -> bool;       // Invariant check
}
```

**`merge.rs`** — Yrs-level merge is handled by Yrs itself. But we need wrapper functions for application-level merge logic:
```rust
pub enum CrdtValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<CrdtValue>),
    Map(HashMap<String, CrdtValue>),
    Null,
}

pub fn merge_update_into_document(doc: &mut CRDTDocument, update: &[u8]) -> Result<()>;
pub fn merge_batch(doc: &mut CRDTDocument, updates: &[Vec<u8>]) -> Result<()>;
pub fn assert_deterministic(a: &CRDTDocument, b: &CRDTDocument);
```

**`snapshot.rs`** — Snapshot serialization/deserialization:
```rust
pub fn create_snapshot(doc: &CRDTDocument) -> Vec<u8>;
pub fn load_snapshot(bytes: &[u8]) -> Result<CRDTDocument>;
pub fn snapshot_size(bytes: &[u8]) -> u64;
```

**`compaction.rs`** — Tombstone GC and compaction:
```rust
pub struct CompactionConfig {
    pub tombstone_ttl: Duration,   // How long tombstones survive
    pub snapshot_interval: usize,  // Ops between snapshots
    pub max_oplog_age: Duration,   // Max age before compaction
}

pub fn gc_tombstones(doc: &mut CRDTDocument, config: &CompactionConfig);
pub fn should_compact(oplog_len: usize, config: &CompactionConfig) -> bool;
pub fn compact(doc: &mut CRDTDocument, oplog: &mut OpLog, config: &CompactionConfig);
```

#### Step 1.2: E2EE Layer (`crates/vaultsync-core/src/e2ee/`)

**`keyring.rs`**:
```rust
pub struct NamespaceKeypair {
    pub public_key: [u8; 32],   // X25519
    pub private_key: [u8; 32],
    pub version: u64,
    pub created_at: u64,
}

pub struct KeyRing {
    keys: Vec<NamespaceKeypair>,
    active_version: u64,
}

impl KeyRing {
    pub fn generate() -> Self;
    pub fn add_key(key: NamespaceKeypair);
    pub fn active_key(&self) -> &NamespaceKeypair;
    pub fn key_by_version(&self, version: u64) -> Option<&NamespaceKeypair>;
    pub fn rotate(&mut self) -> NamespaceKeypair;
    pub fn encrypt_private_key(&self, device_key: &[u8]) -> Vec<u8>;
    pub fn decrypt_private_key(encrypted: &[u8], device_key: &[u8]) -> Result<NamespaceKeypair>;
}
```

**`encrypt.rs`**:
```rust
pub fn encrypt(
    plaintext: &[u8],        // Yrs update
    sender_sk: &[u8; 32],    // Sender's private key
    recipient_pk: &[u8; 32], // Recipient's public key
) -> Result<Vec<u8>>;        // AEAD ciphertext
```

**`decrypt.rs`**:
```rust
pub fn decrypt(
    ciphertext: &[u8],
    recipient_sk: &[u8; 32],
    sender_pk: &[u8; 32],
) -> Result<Vec<u8>>;
```

**`kat.rs`** — Known-answer test vectors:
```rust
#[cfg(test)]
mod tests {
    #[test]
    fn encrypt_kat() {
        // Test vectors from libsodium crypto_box specification
    }
    #[test]
    fn decrypt_kat() { ... }
}
```

#### Step 1.3: Storage Trait + SQLite (`crates/vaultsync-core/src/storage/`)

**`traits.rs`**:
```rust
#[async_trait]
pub trait Storage: Send + Sync + Debug {
    // CRDT Documents
    async fn insert_document(&self, doc: &CRDTDocument) -> Result<()>;
    async fn get_document(&self, doc_id: &str, record_id: &str) -> Result<Option<CRDTDocument>>;
    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<()>;
    async fn list_documents(&self, doc_id: &str) -> Result<Vec<CRDTDocument>>;

    // OpLog
    async fn append_oplog(&self, entry: &OplogEntry) -> Result<()>;
    async fn read_pending(&self, namespace: &str, limit: usize) -> Result<Vec<OplogEntry>>;
    async fn mark_synced(&self, id: &str, sequence: u64) -> Result<()>;
    async fn mark_failed(&self, id: &str, error: &str) -> Result<()>;
    async fn read_after_sequence(&self, namespace: &str, seq: u64) -> Result<Vec<OplogEntry>>;

    // Sync State
    async fn read_sync_state(&self, namespace: &str) -> Result<SyncState>;
    async fn write_sync_state(&self, state: &SyncState) -> Result<()>;

    // Schema
    async fn read_schema(&self, doc_id: &str) -> Result<Option<SchemaMeta>>;
    async fn write_schema(&self, meta: &SchemaMeta) -> Result<()>;
    async fn read_migrations(&self) -> Result<Vec<MigrationRecord>>;
    async fn write_migration(&self, record: &MigrationRecord) -> Result<()>;

    // Keys
    async fn read_keys(&self, namespace: &str) -> Result<Vec<KeyRecord>>;
    async fn write_key(&self, key: &KeyRecord) -> Result<()>;

    // Transactions
    async fn transaction<F, T>(&self, f: F) -> Result<T>
    where F: FnOnce(&dyn Storage) -> Result<T> + Send;
}
```

**`sqlite.rs`** — SQLite implementation using `rusqlite`:
```rust
pub struct SQLiteStorage {
    conn: rusqlite::Connection,
    path: Option<PathBuf>,
}

impl SQLiteStorage {
    pub fn new(path: &Path) -> Result<Self>;
    pub fn in_memory() -> Result<Self>;
    pub fn enable_wal_mode(&self) -> Result<()>;
}

#[async_trait]
impl Storage for SQLiteStorage { ... }
```

**`memory.rs`** — In-memory implementation using `HashMap`:
```rust
pub struct InMemoryStorage {
    documents: RwLock<HashMap<(String, String), Vec<u8>>>,
    oplog: RwLock<Vec<OplogEntry>>,
    sync_state: RwLock<HashMap<String, SyncState>>,
    schema: RwLock<HashMap<String, SchemaMeta>>,
    migrations: RwLock<Vec<MigrationRecord>>,
    keys: RwLock<Vec<KeyRecord>>,
}
```

**`encryption_shim.rs`**:
```rust
pub struct EncryptedStorage {
    inner: Box<dyn Storage>,
    device_key: [u8; 32],
}

impl Storage for EncryptedStorage {
    // Encrypts document bytes and oplog entries before delegating to inner
    // Decrypts on read
}
```

#### Step 1.4: OpLog (`crates/vaultsync-core/src/oplog/`)

**`entry.rs`**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OplogEntry {
    pub id: String,
    pub replica_id: String,
    pub namespace: String,
    pub mutation_type: MutationType,  // CRDT_UPDATE | CRDT_INSERT | CRDT_DELETE | CRDT_BATCH
    pub doc_id: String,
    pub record_id: String,
    pub yrs_update: Vec<u8>,         // Yrs binary diff (plaintext locally, encrypted in transit)
    pub encrypted_blob: Option<Vec<u8>>, // AEAD ciphertext (for upload verification)
    pub timestamp: u64,
    pub sequence: Option<u64>,       // None until coordinator assigns
    pub sync_status: SyncStatus,
    pub synced_at: Option<u64>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncStatus {
    Pending,
    Synced,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MutationType {
    CrdtUpdate,
    CrdtInsert,
    CrdtDelete,
    CrdtBatch,
}
```

**`log.rs`**:
```rust
pub struct OpLog {
    storage: Box<dyn Storage>,
    namespace: String,
}

impl OpLog {
    pub fn new(storage: Box<dyn Storage>, namespace: &str) -> Self;
    pub async fn append(&self, entry: OplogEntry) -> Result<()>;
    pub async fn read_pending(&self, limit: usize) -> Result<Vec<OplogEntry>>;
    pub async fn read_after_sequence(&self, seq: u64, limit: usize) -> Result<Vec<OplogEntry>>;
    pub async fn mark_synced(&self, id: &str, sequence: u64) -> Result<()>;
    pub async fn mark_failed(&self, id: &str, error: &str) -> Result<()>;
    pub async fn count_pending(&self) -> Result<usize>;
    pub async fn delete_compacted(&self, before_sequence: u64) -> Result<usize>;
}
```

#### Step 1.5: Schema + Migrations (`crates/vaultsync-core/src/schema/`)

**`field.rs`**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub value_type: ValueType,  // string, number, boolean, array, object
    pub crdt_type: CrdtType,    // lww, counter, orset, text, array, custom
    pub primary_key: bool,
    pub indexed: bool,
    pub sync: bool,              // false = local-only
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType { String, Number, Boolean, Array, Object }
```

**`registry.rs`**:
```rust
pub struct SchemaRegistry {
    schemas: HashMap<String, DocumentSchema>,
    migrations: Vec<MigrationDefinition>,
}

impl SchemaRegistry {
    pub fn new() -> Self;
    pub fn define(&mut self, doc_id: &str, schema: DocumentSchema) -> Result<()>;
    pub fn get(&self, doc_id: &str) -> Option<&DocumentSchema>;
    pub fn validate_update(&self, doc_id: &str, field: &str, value: &CrdtValue) -> Result<()>;
    pub fn register_migration(&mut self, version: &str, migration: MigrationDefinition);
}
```

**`migration.rs`**:
```rust
pub struct MigrationDefinition {
    pub version: String,
    pub checksum: String,     // SHA256
    pub apply: Box<dyn Fn(&mut SchemaRegistry) -> Result<()>>,
    pub rollback: Option<Box<dyn Fn(&mut SchemaRegistry) -> Result<()>>>,
}

impl MigrationDefinition {
    pub fn new(version: &str, apply_fn: impl Fn(&mut SchemaRegistry) -> Result<()> + 'static) -> Self;
    pub fn with_rollback(mut self, rollback_fn: impl Fn(&mut SchemaRegistry) -> Result<()> + 'static) -> Self;
    pub fn verify_checksum(&self) -> bool;
}
```

#### Step 1.6: Subscriptions (`crates/vaultsync-core/src/subscription/`)

**`engine.rs`**:
```rust
pub struct SubscriptionEngine {
    watchers: HashMap<String, Vec<Subscription>>,  // doc_id → subscriptions
}

impl SubscriptionEngine {
    pub fn new() -> Self;
    pub fn register(&mut self, doc_id: &str, filter: Option<Filter>, callback: Box<dyn Fn(QueryResult) + Send>) -> SubscriptionHandle;
    pub fn unregister(&mut self, handle: SubscriptionHandle) -> Result<()>;
    pub fn fire(&self, doc_id: &str, changed_records: &[String], state: &CRDTState);
}
```

**`filter.rs`**:
```rust
#[derive(Debug, Clone)]
pub enum Filter {
    FieldEquals { field: String, value: CrdtValue },
    FieldContains { field: String, value: CrdtValue },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    pub fn matches(&self, doc: &CRDTDocument) -> bool;
    pub fn is_empty(&self) -> bool;
}
```

**`observer.rs`**:
```rust
pub struct CrdtObserver {
    watched_docs: HashSet<(String, String)>,
    last_states: HashMap<(String, String), Vec<u8>>,
}

impl CrdtObserver {
    pub fn new() -> Self;
    pub fn watch(&mut self, doc_id: &str, record_id: &str);
    pub fn unwatch(&mut self, doc_id: &str, record_id: &str);
    pub fn detect_changes(&self, doc_id: &str, current: &CRDTDocument) -> Vec<String>;
}
```

#### Step 1.7: Sync Engine (`crates/vaultsync-core/src/sync/`)

**`state.rs`**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub namespace: String,
    pub replica_id: String,
    pub last_synced_sequence: u64,
    pub pending_upload_count: usize,
    pub connection_status: ConnectionStatus,
    pub leader_status: LeaderStatus,
    pub last_connected_at: Option<u64>,
    pub last_sync_at: Option<u64>,
    pub schema_version: u64,
}
```

**`upload.rs`**:
```rust
pub struct UploadQueue {
    oplog: Arc<OpLog>,
    coordinator: Arc<dyn Coordinator>,
    encryptor: Arc<E2eeEncryptor>,
    config: UploadConfig,
}

impl UploadQueue {
    pub fn new(...) -> Self;
    pub async fn process_batch(&self) -> Result<UploadResult>;
    pub async fn process_all(&self) -> Result<UploadResult>;
    pub async fn pending_count(&self) -> Result<usize>;

    // Internal: called by process_batch
    async fn read_batch(&self) -> Result<Vec<OplogEntry>>;
    async fn encrypt_batch(&self, entries: &[OplogEntry]) -> Result<Vec<EncryptedMutation>>;
    async fn send_batch(&self, mutations: &[EncryptedMutation]) -> Result<Vec<SequenceId>>;
    async fn acknowledge(&self, ids: &[String], sequences: &[SequenceId]) -> Result<()>;
}
```

**`download.rs`**:
```rust
pub struct DownloadQueue {
    coordinator: Arc<dyn Coordinator>,
    decryptor: Arc<E2eeDecryptor>,
    reconciler: Arc<Reconciler>,
    state: Arc<SyncState>,
}

impl DownloadQueue {
    pub fn new(...) -> Self;
    pub async fn process_batch(&self) -> Result<DownloadResult>;
    pub async fn process_all(&self) -> Result<DownloadResult>;

    async fn fetch_batch(&self, after: u64) -> Result<Vec<PendingMutation>>;
    async fn decrypt_batch(&self, mutations: &[PendingMutation]) -> Result<Vec<Vec<u8>>>;
    async fn merge_batch(&self, updates: &[Vec<u8>]) -> Result<()>;
    async fn update_last_sequence(&self, seq: u64) -> Result<()>;
}
```

**`retry.rs`**:
```rust
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_factor: f64,   // 2.0 = exponential
    pub jitter: bool,
}

pub struct RetryEngine {
    config: RetryConfig,
    attempts: HashMap<String, u32>,
    next_retry: HashMap<String, Instant>,
}

impl RetryEngine {
    pub fn new(config: RetryConfig) -> Self;
    pub fn record_failure(&mut self, id: &str) -> Result<Option<Duration>>; // Some(delay) = retry after, None = max exceeded
    pub fn record_success(&mut self, id: &str);
    pub fn pending_retries(&self) -> Vec<(String, Instant)>;
    pub fn is_exhausted(&self, id: &str) -> bool;
}
```

**`reconciler.rs`**:
```rust
pub struct Reconciler {
    storage: Arc<dyn Storage>,
    schema: Arc<SchemaRegistry>,
    subscriptions: Arc<SubscriptionEngine>,
}

impl Reconciler {
    pub fn new(...) -> Self;
    pub async fn apply_remote_update(&self, mutation: &OplogEntry) -> Result<()>;
    pub async fn apply_batch(&self, mutations: &[OplogEntry]) -> Result<()>;

    // Internal
    async fn decrypt_and_merge(&self, entry: &OplogEntry) -> Result<()>;
    async fn store_and_notify(&self, doc: &CRDTDocument);
}
```

#### Step 1.8: Coordinator Trait (`crates/vaultsync-core/src/coordinator/`)

**`traits.rs`**:
```rust
#[async_trait]
pub trait Coordinator: Send + Sync + Debug {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError>;
    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError>;
    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation>>, CoordinatorError>;
    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError>;
    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError>;
    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError>;
    async fn report_metrics(&self, metrics: CoordinatorMetrics) -> Result<(), CoordinatorError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedMutation {
    pub id: String,
    pub namespace: String,
    pub replica_id: String,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
    pub schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMutation {
    pub id: String,
    pub namespace: String,
    pub replica_id: String,
    pub sequence: u64,
    pub encrypted_blob: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub namespace: String,
    pub public_key: Vec<u8>,
    pub device_info: Option<Value>,
    pub schema_version: u64,
}
```

**`memory.rs`** — In-memory coordinator for testing:
```rust
pub struct InMemoryCoordinator {
    ops: Arc<RwLock<BTreeMap<(String, u64), PendingMutation>>>,  // (namespace, sequence)
    replicas: Arc<RwLock<HashMap<String, ReplicaInfo>>>,
    subscribers: Arc<RwLock<HashMap<String, Vec<mpsc::Sender<PendingMutation>>>>>,
    next_sequence: Arc<AtomicU64>,
}

impl InMemoryCoordinator {
    pub fn new() -> Self;
}
```

**`mock.rs`** — Deterministic mock for unit tests:
```rust
pub struct MockCoordinator {
    expected_pushes: Vec<Vec<EncryptedMutation>>,
    push_results: Vec<Result<Vec<SequenceId>>>,
    pull_results: Vec<Result<Vec<PendingMutation>>>,
    push_index: AtomicUsize,
    pull_index: AtomicUsize,
}

impl MockCoordinator {
    pub fn new() -> Self;
    pub fn expect_push(&mut self, result: Result<Vec<SequenceId>>);
    pub fn expect_pull(&mut self, result: Result<Vec<PendingMutation>>);
    pub fn verify_all_expected(&self);
}
```

#### Step 1.9: Client API (`crates/vaultsync-core/src/`)

**`config.rs`**:
```rust
#[derive(Debug, Clone)]
pub struct VaultSyncConfig {
    pub namespace: String,
    pub replica_id: String,
    pub storage: StorageConfig,
    pub coordinator: CoordinatorConfig,
    pub sync: SyncConfig,
    pub multi_tab: MultiTabConfig,
    pub encryption: EncryptionConfig,
    pub telemetry: TelemetryConfig,
}
```

**`client.rs`** — Public API:
```rust
pub struct VaultSyncClient {
    config: VaultSyncConfig,
    storage: Arc<dyn Storage>,
    coordinator: Arc<dyn Coordinator>,
    crdt_engine: Arc<CrdtEngine>,
    keyring: Arc<KeyRing>,
    oplog: Arc<OpLog>,
    schema: Arc<SchemaRegistry>,
    subscriptions: Arc<SubscriptionEngine>,
    upload_queue: Arc<UploadQueue>,
    download_queue: Arc<DownloadQueue>,
    reconciller: Arc<Reconciler>,
    leader_election: Option<Arc<LeaderElection>>,
    telemetry: Arc<Telemetry>,
}

impl VaultSyncClient {
    pub async fn new(config: VaultSyncConfig) -> Result<Self>;
    pub async fn initialize(&self) -> Result<()>;
    pub async fn shutdown(&self) -> Result<()>;

    // CRUD
    pub async fn insert(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<()>;
    pub async fn update(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<()>;
    pub async fn delete(&self, doc_id: &str, record_id: &str) -> Result<()>;
    pub async fn get(&self, doc_id: &str, record_id: &str) -> Result<Option<CrdtValueMap>>;
    pub async fn find(&self, doc_id: &str, filter: Option<Filter>) -> Result<Vec<CrdtValueMap>>;

    // Subscriptions
    pub fn subscribe(&self, doc_id: &str, filter: Option<Filter>, callback: Box<dyn Fn(QueryResult) + Send>) -> SubscriptionHandle;
    pub fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()>;

    // Sync
    pub async fn force_sync(&self) -> Result<SyncResult>;
    pub async fn sync_status(&self) -> Result<SyncStatusInfo>;
    pub async fn pending_uploads(&self) -> Result<usize>;

    // E2EE
    pub async fn rotate_keys(&self) -> Result<()>;
    pub async fn export_public_key(&self) -> Result<Vec<u8>>;

    // Multi-Tab
    pub async fn leader_status(&self) -> Result<LeaderStatus>;
    pub async fn force_election(&self) -> Result<()>;

    // Schema
    pub async fn define_schema(&self, doc_id: &str, schema: DocumentSchema) -> Result<()>;
    pub async fn apply_migration(&self, migration: MigrationDefinition) -> Result<()>;
}
```

### Phase 1 Deliverables
- Complete `vaultsync-core` crate with all modules
- All unit tests passing for CRDT, E2EE, storage, oplog, schema, subscriptions, sync, coordinator
- In-memory coordinator and storage for testing
- `VaultSyncClient` API ready (Rust)
- Benchmark stubs ready

---

## 6. Phase 2 — WASM & Browser Support

### Goal
Compile `vaultsync-core` to WASM for browser use. Implement OPFS and IndexedDB storage backends. Expose VaultSyncClient via TypeScript bindings.

### Tasks

| Task | File(s) | Details |
|---|---|---|
| **P2.1** WASM storage trait | `crates/vaultsync-wasm/src/storage.rs` | Implement Storage trait for OPFS (via `send_sync`) and IndexedDB (via web-sys). OPFS: open SQLite via WASM in Web Worker, use synchronous access. IndexedDB: store Yrs snapshots as IDB records |
| **P2.2** WASM IPC | `crates/vaultsync-wasm/src/ipc.rs` | Wraps `BroadcastChannel` for cross-tab messaging, `navigator.locks` for leader election fallback |
| **P2.3** WASM E2EE | `crates/vaultsync-wasm/src/e2ee.rs` | Maps `Encrypt`/`Decrypt` to WebCrypto `subtle.encrypt`/`subtle.decrypt` (X25519 not natively available in all browsers — fallback to WASM-compiled libsodium) |
| **P2.4** WASM transport | `crates/vaultsync-wasm/src/transport.rs` | WebSocket via `web_sys::WebSocket`. HTTP/SSE fallback for coordinators that don't support WS |
| **P2.5** WASM client bindings | `crates/vaultsync-wasm/src/client.rs` | #[wasm_bindgen] annotated wrapper: exposes `VaultSyncClient` methods to JS |
| **P2.6** TypeScript types | `packages/web/src/` | Manual TypeScript type definitions that match the WASM exported API |
| **P2.7** Storage detection | `packages/web/src/vaultsync.ts` | `VaultSync.detectStorage()`: try OPFS first, fallback to IndexedDB |

### WASM Build Configuration

```toml
# crates/vaultsync-wasm/Cargo.toml
[package]
name = "vaultsync-wasm"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
vaultsync-core = { path = "../vaultsync-core" }
wasm-bindgen = "0.2"
web-sys = { version = "0.3", features = [
    "WebSocket", "BroadcastChannel", "Storage", "Window",
    "SharedArrayBuffer", "Atomics", "Crypto", "SubtleCrypto"
] }
js-sys = "0.3"
console_error_panic_hook = "0.1"
getrandom = { version = "0.2", features = ["js"] }
```

### Phase 2 Deliverables
- WASM-compiled `vaultsync-wasm` (`.wasm` + JS glue)
- OPFS storage: open SQLite database in OPFS, read/write CRDT snapshots
- IndexedDB fallback storage
- WebSocket transport in browser
- CustomEvent/BroadcastChannel IPC
- `npm run build:wasm` produces `packages/web/wasm/`

---

## 7. Phase 3 — Multi-Tab & IPC

### Goal
Implement cross-tab and cross-process leader election, shared memory state distribution, heartbeat-based crash detection, and recovery.

### Tasks

| Task | File(s) | Details |
|---|---|---|
| **P3.1** File lock (native) | `crates/vaultsync-core/src/ipc/leader_election.rs` | On Linux/macOS: `flock(fd, LOCK_EX | LOCK_NB)`. On Windows: `CreateMutex` via `winapi`. Backed by SQLite WAL lock file |
| **P3.2** BroadcastChannel (browser) | `crates/vaultsync-wasm/src/ipc.rs` | `web_sys::BroadcastChannel` with channel name = namespace. Messages: heartbeats, leader announcements, state change notifications |
| **P3.3** Shared memory (native) | `crates/vaultsync-core/src/ipc/shared_memory.rs` | Mmap shared file. Layout: header (leader_id, heartbeat, generation) + Yrs doc snapshot + ring buffer |
| **P3.4** SharedArrayBuffer (browser) | `crates/vaultsync-wasm/src/ipc.rs` | `SharedArrayBuffer` (requires COOP/COEP headers). Atomics-based synchronization |
| **P3.5** Heartbeat | `crates/vaultsync-core/src/ipc/heartbeat.rs` | Leader writes timestamp + sequence every 1s. Readers check every 500ms |
| **P3.6** Crash recovery | `crates/vaultsync-core/src/ipc/crash_recovery.rs` | On leader promotion: read ring buffer, replay uncommitted Yrs updates, update sync_state |
| **P3.7** Leader election integration | `crates/vaultsync-core/src/client.rs` | On `VaultSyncClient::initialize()`: attempt leader election. If elected, bind write lock. If reader, route writes to leader via IPC |

### Leader Election Protocol Detail

```
State Machine:
                    ┌──────────┐
                    │  Init    │
                    └────┬─────┘
                         │
                    ┌────▼─────┐
              ┌─────│  Electing │─────┐
              │     └────┬─────┘     │
         acquire lock    │      no lock
              │     lock acquired    │
         ┌────▼────┐          ┌─────▼──────┐
         │  Leader │          │   Reader    │
         │ (writer)│          │  (read-only)│
         └────┬────┘          └─────┬──────┘
              │                     │
       heartbeat timer         heartbeat check
              │                     │
         write heartbeat       detect stale?
              │               ┌────┴────┐
         on crash ───────────►│  yes    │  no
                              │         │
                         try acquire  stay reader
                              │
                         become leader
                              │
                         replay ring buffer
```

### Shared Memory Layout

```rust
#[repr(C)]
struct SharedMemoryHeader {
    leader_id: [u8; 64],       // Fixed-size string
    leader_timestamp: u64,      // Monotonic clock
    generation: u64,            // Incremented on each leader promotion
    ring_buffer_head: u32,
    ring_buffer_tail: u32,
    ring_buffer_capacity: u32,
    snapshot_offset: u64,       // File offset of latest Yrs doc snapshot
    snapshot_size: u64,
}

#[repr(C)]
struct RingBufferEntry {
    id: [u8; 64],               // Mutation ID
    yrs_update_size: u32,
    timestamp: u64,
    // followed by yrs_update bytes
}
```

### Phase 3 Deliverables
- Native multi-process leader election (file lock)
- Browser multi-tab leader election (BroadcastChannel + SharedArrayBuffer)
- Heartbeat + crash detection + automatic promotion
- Shared memory ring buffer for uncommitted ops
- Cross-tab subscription forwarding
- All multi-tab unit + integration tests passing

---

## 8. Phase 4 — Coordinator Implementations

### Goal
Implement full coordinator backends: Postgres, Redis, SQLite, In-Memory. Each must pass the conformance test suite.

### Implementation Order (by complexity)

1. **InMemoryCoordinator** — Already built in Phase 1 for testing
2. **SQLiteCoordinator** — Built in Phase 1 (embedding). Simplest real coordinator
3. **PostgresCoordinator** — Production workhorse
4. **RedisCoordinator** — High-throughput option
5. **CloudflareCoordinator** — Edge deployment option
6. **SupabaseCoordinator** — Hosted option

### PostgresCoordinator (`crates/vaultsync-coordinator-postgres/`)

```rust
pub struct PostgresCoordinator {
    pool: sqlx::PgPool,
}

impl PostgresCoordinator {
    pub async fn new(connection_string: &str) -> Result<Self>;
    pub async fn run_migrations(&self) -> Result<()>;  // Create tables
}

#[async_trait]
impl Coordinator for PostgresCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>> {
        // INSERT INTO vaultsync_coordinator_mutations ... RETURNING sequence
        // Use LISTEN/NOTIFY for real-time subscribers
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>> {
        // SELECT * FROM vaultsync_coordinator_mutations WHERE namespace=$1 AND sequence > $2 ORDER BY sequence LIMIT $3
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation>>> {
        // LISTEN vaultsync_channel_{namespace}
        // On NOTIFY: pull from from_sequence
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<()> {
        // UPSERT INTO vaultsync_coordinator_replicas
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<()> {
        // UPDATE vaultsync_coordinator_replicas SET last_heartbeat = NOW() WHERE id = $1
    }
}
```

### RedisCoordinator (`crates/vaultsync-coordinator-redis/`)

```rust
pub struct RedisCoordinator {
    conn: redis::aio::ConnectionManager,
}

impl RedisCoordinator {
    pub async fn new(url: &str) -> Result<Self>;
}

// Implementation approach:
// - push: XADD vaultsync:mutes:{namespace} * sequence <auto> id ... encrypted ...
// - pull: XRANGE vaultsync:mutes:{namespace} {after_sequence} +
// - subscribe: XREAD BLOCK 0 STREAMS vaultsync:mutes:{namespace} {after_sequence}
// - register: HSET vaultsync:replicas:{namespace} {replica_id} {json}
// - heartbeat: EXPIRE vaultsync:replicas:{namespace}:{replica_id} 30
```

### Coordinator Database Schema (All Backends)

```sql
-- Every coordinator backend must store equivalent data:

CREATE TABLE vaultsync_coordinator_mutations (
    sequence      BIGINT  PRIMARY KEY,
    namespace     TEXT    NOT NULL,
    replica_id    TEXT    NOT NULL,
    mutation_id   TEXT    NOT NULL UNIQUE,
    encrypted     BLOB    NOT NULL,
    timestamp     BIGINT  NOT NULL,
    created_at    INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE vaultsync_coordinator_replicas (
    id              TEXT    PRIMARY KEY,
    namespace       TEXT    NOT NULL,
    public_key      BLOB    NOT NULL,
    device_info     TEXT,
    last_heartbeat  INTEGER,
    last_sequence   BIGINT  NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE vaultsync_coordinator_snapshots (
    id              TEXT    PRIMARY KEY,
    namespace       TEXT    NOT NULL,
    sequence        BIGINT  NOT NULL,
    snapshot        BLOB    NOT NULL,
    created_at      INTEGER NOT NULL DEFAULT (unixepoch())
);
```

### Coordinator Migration Manager

```rust
pub struct CoordinatorMigrationManager {
    coordinators: Vec<Box<dyn Coordinator>>,
}

impl CoordinatorMigrationManager {
    pub fn new() -> Self;
    pub fn add_coordinator(&mut self, coord: Box<dyn Coordinator>);
    pub async fn migrate_namespace(&self, namespace: &str, from: Box<dyn Coordinator>, to: Box<dyn Coordinator>) -> Result<()> {
        // 1. Drain pending writes from from
        // 2. Pull all mutations from from
        // 3. Push all mutations to to
        // 4. Update replica registrations
        // 5. Verify consistency
    }
    pub async fn replicate_namespace(&self, namespace: &str, primary: &dyn Coordinator, replica: &dyn Coordinator) -> Result<()> {
        // Real-time replication from primary to replica coordinator
    }
}
```

### Docker Compose for Development

```yaml
# docker-compose.yml
version: '3.8'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: vaultsync_dev
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s

  redis:
    image: redis:7
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
```

### Phase 4 Deliverables
- All 6 coordinator implementations passing conformance tests
- Docker Compose for local development
- Coordinator migration tool (switch coordinators at runtime)
- Coordinator metrics reporting (operation count, latency, error rate)

---

## 9. Phase 5 — TypeScript SDK & React

### Goal
Build the TypeScript SDKs that wrap the WASM/Native Rust core and provide idiomatic JS/React APIs.

### Package Architecture

```
@vaultsync/web (browser)
  ├── VaultSync class (facade over WASM exported functions)
  ├── DbProxy (CRUD with auto-generated TypeScript types)
  ├── SchemaDefinition (fluent builder)
  ├── Subscription (EventEmitter-based)
  ├── SyncStatus (observable)
  ├── KeyManager (E2EE key operations)
  ├── Coordinator client classes
  └── Storage detection + fallback

@vaultsync/node (server)
  ├── VaultSync class (facade over native Rust binary via FFI)
  ├── SQLiteStorage (better-sqlite3)
  └── Same CRUD/subscription API as @vaultsync/web

@vaultsync/react
  ├── VaultSyncProvider (React context)
  ├── useVaultSyncQuery (auto-subscribe, re-render on change)
  ├── useVaultSyncOne (single record subscription)
  ├── useVaultSyncMutations (insert, update, delete)
  ├── useVaultSyncSync (sync status hook)
  └── SyncIndicator (pre-built component)
```

### Web SDK Core (`packages/web/src/vaultsync.ts`)

```typescript
import init, { VaultSyncClient } from "../wasm/vaultsync_wasm.js"

export interface VaultSyncWebConfig {
  storage: "opfs" | "indexeddb"
  namespace: string
  replicaId: string
  coordinator: ICoordinator
  sync?: SyncConfig
  multiTab?: MultiTabConfig
  encryption?: EncryptionConfig
}

export class VaultSync {
  private client: VaultSyncClient
  private coordinator: ICoordinator

  static async create(config: VaultSyncWebConfig): Promise<VaultSync> {
    // 1. Load WASM module
    await init()

    // 2. Detect storage
    const storage = await this.detectStorage(config.storage)

    // 3. Initialize Rust client
    const client = new VaultSyncClient()
    await client.initialize({
      storage,
      namespace: config.namespace,
      replicaId: config.replicaId,
      coordinatorUrl: config.coordinator.url,
      // ...
    })

    return new VaultSync(client, config)
  }

  static async detectStorage(preferred?: "opfs" | "indexeddb"): Promise<string> {
    if (preferred === "opfs" && await supportsOPFS()) return "opfs"
    if (preferred === "indexeddb") return "indexeddb"
    // Auto-detect
    if (await supportsOPFS()) return "opfs"
    return "indexeddb"
  }

  get db(): DbProxy { /* returns CRUD proxy */ }
  get sync(): SyncStatus { /* returns sync status observable */ }
  get keys(): KeyManager { /* returns E2EE key manager */ }
  get leader(): LeaderStatus { /* returns leader status */ }

  async shutdown(): Promise<void> { await this.client.shutdown() }
}
```

### DbProxy (`packages/web/src/db.ts`)

```typescript
export class DbProxy {
  constructor(private client: VaultSyncClient) {}

  // Auto-generate typed collections from schema
  todos = new Collection<Todo>("todos", this.client)
}

export class Collection<T> {
  constructor(private name: string, private client: VaultSyncClient) {}

  async insert(record: T & { id: string }): Promise<void> {
    await this.client.insert(this.name, record.id, record as any)
  }

  async update(id: string, fields: Partial<T>): Promise<void> {
    await this.client.update(this.name, id, fields as any)
  }

  async delete(id: string): Promise<void> {
    await this.client.delete(this.name, id)
  }

  async findById(id: string): Promise<T | null> {
    const result = await this.client.get(this.name, id)
    return result as T | null
  }

  async findAll(filter?: QueryFilter): Promise<T[]> {
    return await this.client.find(this.name, filter) as T[]
  }

  subscribe(callback: (results: T[]) => void, filter?: QueryFilter): () => void {
    const handle = this.client.subscribe(this.name, filter, (results) => {
      callback(results as T[])
    })
    return () => this.client.unsubscribe(handle)
  }

  subscribeOne(id: string, callback: (record: T | null) => void): () => void {
    const handle = this.client.subscribe(this.name, { recordId: id }, (results) => {
      callback((results[0] ?? null) as T | null)
    })
    return () => this.client.unsubscribe(handle)
  }
}
```

### React Hooks (`packages/react/src/hooks.ts`)

```typescript
import { useState, useEffect, useContext } from "react"
import { VaultSyncContext } from "./provider"

export function useVaultSyncQuery<T>(
  queryFn: (db: DbProxy) => Promise<T[]>,
  deps?: any[]
): T[] {
  const vaultsync = useContext(VaultSyncContext)
  const [results, setResults] = useState<T[]>([])

  useEffect(() => {
    let unsub: () => void

    async function setup() {
      const db = vaultsync.db
      const initial = await queryFn(db)
      setResults(initial)

      // Subscribe to changes using the collection from the query
      // This requires parsing the queryFn to extract collection name
      // Simplified: subscribe to all collections involved
      unsub = db.todos.subscribe(async () => {
        const updated = await queryFn(db)
        setResults(updated)
      })
    }

    setup()
    return () => unsub?.()
  }, [vaultsync, ...(deps || [])])

  return results
}

export function useVaultSyncOne<T>(
  queryFn: (db: DbProxy) => Promise<T | null>
): T | null {
  const vaultsync = useContext(VaultSyncContext)
  const [result, setResult] = useState<T | null>(null)

  useEffect(() => {
    let unsub: () => void

    async function setup() {
      const db = vaultsync.db
      const initial = await queryFn(db)
      setResult(initial)

      // Parse record ID from queryFn and subscribe to that single record
      // ...
    }

    setup()
    return () => unsub?.()
  }, [vaultsync])

  return result
}

export function useVaultSyncSync() {
  const vaultsync = useContext(VaultSyncContext)
  const [status, setStatus] = useState({
    connected: false,
    pendingUploads: 0,
    lastSyncAt: null,
  })

  useEffect(() => {
    const unsub = vaultsync.subscribe("sync:status", setStatus)
    return unsub
  }, [vaultsync])

  return status
}
```

### Schema Definition (`packages/web/src/schema.ts`)

```typescript
export function defineSchema(vaultsync: VaultSync, schema: SchemaDefinition) {
  // Convert TS schema to Rust-compatible JSON
  // Call vaultsync.client.defineSchema(docId, schemaJson)
}

interface SchemaDefinition {
  fields: Record<string, FieldDef>
  indexes?: IndexDef[]
  softDelete?: boolean
}

interface FieldDef {
  type: "string" | "number" | "boolean" | "array"
  crdtType: "lww" | "counter" | "orset" | "text" | "array"
  primaryKey?: boolean
  indexed?: boolean
  sync?: boolean
}
```

### Phase 5 Deliverables
- `@vaultsync/web` package loadable in browser
- `@vaultsync/node` package loadable in Node.js
- `@vaultsync/react` with `VaultSyncProvider`, `useVaultSyncQuery`, `useVaultSyncOne`, `useVaultSyncSync`
- Schema definition API in TypeScript
- E2E tests passing (Playwright: 2-tab sync, leader crash, offline→online)
- Example `todo-basic` app runnable

---

## 10. Phase 6 — Testing Infrastructure

### Goal
Implement the full test suite defined in `vaultsync-testing-spec.md`. This phase runs parallel to P1–P5 but culminates here.

### Tasks

| Task | Test Type | Files | Details |
|---|---|---|---|
| **P6.1** CRDT property tests | Unit | `crates/vaultsync-core/src/crdt/*.rs` (test modules) | proptest convergence, commutativity, associativity, idempotence |
| **P6.2** E2EE KAT tests | Unit | `crates/vaultsync-core/src/e2ee/kat.rs` | Known-answer vectors from libsodium spec |
| **P6.3** Storage conformance | Integration | `crates/vaultsync-core/src/storage/tests/` | Same test suite runs against SQLite, InMemory, RocksDB |
| **P6.4** Sync cycle integration | Integration | `crates/vaultsync-core/tests/sync_cycle.rs` | Write→encrypt→upload→coord→download→decrypt→merge→subscribe |
| **P6.5** Offline→online | Integration | `crates/vaultsync-core/tests/offline_reconnect.rs` | Write offline, reconnect, sync, verify convergence |
| **P6.6** Multi-tab | Integration | `crates/vaultsync-core/tests/multitab.rs` | Multi-process leader election, crash recovery |
| **P6.7** Coordinator conformance | Integration | `crates/vaultsync-core/tests/coordinator_conformance.rs` | Macro-generated tests per coordinator |
| **P6.8** Chaos tests | Chaos | `crates/vaultsync-core/tests/chaos/` | toxiproxy network faults, SIGKILL crash, clock skew |
| **P6.9** Migration tests | Integration | `crates/vaultsync-core/tests/migrations.rs` | Schema migration forward/backward, concurrent migration |
| **P6.10** Benchmarks | Performance | `crates/vaultsync-core/benches/` | Criterion benchmarks for all perf targets |
| **P6.11** Fuzz targets | Fuzz | `crates/vaultsync-fuzz/fuzz_targets/` | 6 cargo-fuzz targets |
| **P6.12** E2E (Playwright) | E2E | `packages/web/__tests__/e2e/` | Cross-tab sync, leader crash, offline→online in browser |
| **P6.13** TS SDK unit tests | Unit | `packages/*/__tests__/` | Jest tests for TypeScript wrappers |
| **P6.14** Coverage enforcement | CI | `.github/workflows/coverage.yml` | tarpaulin + threshold checks |

### Test Run Commands

```bash
# Run everything (developer)
./scripts/run-all-tests.sh

# Rust tests (all layers except E2E)
cargo test --workspace --exclude vaultsync-fuzz

# Quick check (before commit)
cargo test --lib && cargo test --test crdt_correctness

# Benchmarks
cargo bench -- --baseline main

# Fuzz (5 min quick)
cargo fuzz run fuzz_crdt_merge --timeout=300

# TS tests
npm test --workspaces --if-present

# E2E (requires Docker for coordinators)
npm run e2e
```

### Phase 6 Deliverables
- All tests passing in CI
- All coverage targets met (CRDT/E2EE/IPC: 100% line+branch, total: 94%)
- Benchmark regression gates active
- Chaos tests green
- Fuzz tests with no crashes after 24h run
- E2E tests in Playwright

---

## 11. Phase 7 — CLI & Observability

### Goal
Build CLI tools (`vaultsync inspect`, `vaultsync replay`) and complete OpenTelemetry integration.

### CLI (`crates/vaultsync-cli/`)

```rust
// vaultsync-inspect: attach to running VaultSync process
// Usage: vaultsync inspect --pid <pid> [--stream] [--state]

#[derive(Parser)]
enum Cli {
    Inspect {
        pid: u32,
        #[arg(long)]
        stream: bool,
        #[arg(long)]
        state: bool,
        #[arg(long)]
        export_trace: Option<PathBuf>,
    },
    Replay {
        trace_file: PathBuf,
    },
    Bench {
        #[arg(default_value = "all")]
        benchmark: String,
    },
}
```

**`inspect.rs`** — Attaches to running VaultSync process via:
- Linux: signals + `/proc/<pid>/fd/` inspection
- macOS: `task_for_pid` or shared memory attachment
- Windows: `OpenProcess` + ReadProcessMemory
- Alternative: connect to debug HTTP API on localhost:9876

**`replay.rs`** — Reads an exported trace file and replays all operations:
```rust
pub async fn replay(trace_path: &Path) -> Result<()> {
    let trace = TraceFile::load(trace_path)?;
    let mut sim = SimulatedNetwork::new(InMemoryCoordinator::new());

    for event in trace.events {
        match event.event_type {
            EventType::Write => sim.replicas[event.replica_id].write(event.mutation),
            EventType::Disconnect => sim.replicas[event.replica_id].disconnect(),
            EventType::Reconnect => sim.replicas[event.replica_id].reconnect(),
            EventType::Crash => sim.replicas[event.replica_id].crash(),
        }
    }

    sim.sync_all().await;
    sim.verify_convergence();
    println!("✅ Trace replay successful — all replicas converged");
}
```

### OpenTelemetry Integration

```rust
// crates/vaultsync-core/src/telemetry/tracing.rs

pub fn init_tracing(service_name: &str, endpoint: Option<&str>) -> Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_resource(Resource::new(vec![
                    KeyValue::new("service.name", service_name),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ]))
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::Registry::default()
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer().json())
        .with(tracing_subscriber::EnvFilter::from_default_env());

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

// Usage in VaultSyncClient:
pub async fn insert(&self, doc_id: &str, record_id: &str, fields: HashMap<String, CrdtValue>) -> Result<()> {
    let span = tracing::info_span!(
        "vaultsync.write",
        doc_id = doc_id,
        record_id = record_id,
        namespace = %self.config.namespace,
    );
    async {
        let _enter = span.enter();

        // CRDT merge
        let merge_span = tracing::debug_span!("crdt.merge");
        let yrs_update = {
            let _ = merge_span.enter();
            self.crdt_engine.merge(doc_id, record_id, fields)?
        };
        drop(merge_span);

        // Encrypt
        let encrypt_span = tracing::debug_span!("e2ee.encrypt");
        let encrypted = {
            let _ = encrypt_span.enter();
            self.keyring.encrypt(&yrs_update)?
        };
        drop(encrypt_span);

        // Oplog append
        let oplog_span = tracing::debug_span!("oplog.append");
        self.oplog.append(OplogEntry {
            yrs_update,
            encrypted_blob: Some(encrypted),
            sync_status: SyncStatus::Pending,
            // ...
        }).await?;
        drop(oplog_span);

        Ok(())
    }.instrument(span).await
}
```

### Debug HTTP API

```rust
// crates/vaultsync-core/src/telemetry/debug.rs

pub struct DebugApi {
    client: Arc<VaultSyncClient>,
}

impl DebugApi {
    pub fn new(client: Arc<VaultSyncClient>) -> Self;

    pub async fn serve(&self, port: u16) -> Result<()> {
        let app = axum::Router::new()
            .route("/debug/vaultsync/state", axum::routing::get(self.get_state))
            .route("/debug/vaultsync/state/oplog", axum::routing::get(self.get_oplog))
            .route("/debug/vaultsync/state/documents", axum::routing::get(self.get_documents))
            .route("/debug/vaultsync/state/keys", axum::routing::get(self.get_keys))
            .route("/debug/vaultsync/state/replicas", axum::routing::get(self.get_replicas))
            .route("/debug/vaultsync/metrics", axum::routing::get(self.get_metrics))
            .route("/debug/vaultsync/leader", axum::routing::get(self.get_leader))
            .route("/debug/vaultsync/force-sync", axum::routing::post(self.force_sync))
            .route("/debug/vaultsync/force-election", axum::routing::post(self.force_election));

        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn get_state(&self) -> Json<serde_json::Value> {
        // Returns full internal state as JSON
    }
}
```

### Metrics Endpoint (Prometheus)

```rust
#[derive(Clone)]
pub struct VaultSyncMetrics {
    // Counters
    pub mutations_total: Counter,     // labels: status, namespace
    pub mutations_failed: Counter,    // labels: reason, namespace
    pub conflicts_total: Counter,

    // Gauges
    pub mutations_pending: Gauge,     // labels: namespace
    pub connection_status: Gauge,     // labels: namespace (1=connected, 0=disconnected)
    pub leader_status: Gauge,         // 1=leader, 0=reader
    pub oplog_size: Gauge,            // labels: namespace

    // Histograms
    pub sync_lag_ms: Histogram,       // labels: namespace
    pub download_lag_ms: Histogram,   // labels: namespace
    pub encryption_time_us: Histogram,
    pub crdt_merge_time_us: Histogram,
}

impl VaultSyncMetrics {
    pub fn register(registry: &prometheus::Registry) -> Self { ... }
    pub fn report(&self) -> Result<()> { /* push to coordinator or expose via /metrics */ }
}
```

### Phase 7 Deliverables
- `vaultsync inspect` CLI (attach to process, stream ops, dump state)
- `vaultsync replay` CLI (reproduce sync scenarios from traces)
- OpenTelemetry traces on every write/read/sync operation
- Prometheus metrics endpoint
- Debug HTTP API (localhost:9876)
- Structured JSON logging throughout

---

## 12. Phase 8 — Production Hardening

### Goal
Security audit, performance optimization, documentation, edge case handling, and release preparation.

### Tasks

| Task | Details |
|---|---|
| **P8.1** Security audit | Third-party audit of: E2EE implementation, key management, transport security, local storage encryption, auth integration |
| **P8.2** Performance optimization | Profile hotspot code paths. Target: reduce WASM overhead, optimize Yrs merge for large documents, reduce memory allocation on hot paths |
| **P8.3** Edge case handling | A: Replica with clock skew > 24h. B: Coordinator behind a load balancer that terminates WebSocket. C: Very large documents (>100MB). D: Millions of oplog entries. E: Unicode/collation in CRDT text |
| **P8.4** Documentation | API reference (TypeDoc + rustdoc). Migration guide from other sync engines. Coordinator deployment guide. Production checklist |
| **P8.5** Coordinator failover | Hot-standby coordinator: replicate mutations to a secondary coordinator, automatic failover on primary loss |
| **P8.6** Load testing | 100 concurrent replicas, 10K ops/sec, measure coordinator throughput and latency |
| **P8.7** License + Contribution docs | LICENSE (MIT), CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md |
| **P8.8** Release pipeline | `.github/workflows/release.yml`: build, sign, publish crates to crates.io and packages to npm |

### Performance Optimization Targets

| Area | Current Target | Optimization Target |
|---|---|---|
| WASM CRDT merge | 10–50 µs | < 20 µs (reduce JS↔WASM crossings) |
| Native E2EE encrypt | 5–15 µs | < 5 µs (batch encryption, pre-compute shared secrets) |
| Oplog append (SQLite) | 5–20 µs | < 10 µs (WAL mode, prepared statements cached) |
| Subscription fire (1000 subs) | < 10 ms | < 5 ms (indexed watcher lookup) |
| Snapshot save (1MB) | < 1 ms | < 0.5 ms (async write, non-blocking) |

### Load Testing Scenario

```yaml
# loadtest/scenario.yaml
replicas: 100
duration: 30m
operations_per_second: 10000
coordinator: postgres://localhost:5432/vaultsync_loadtest

stages:
  - name: ramp_up
    duration: 5m
    ops_per_second: 100 → 10000

  - name: steady_state
    duration: 20m
    ops_per_second: 10000

  - name: chaos
    duration: 5m
    faults:
      - type: network_partition
        replicas: 10%
        duration: 30s
      - type: high_latency
        replicas: 20%
        latency: 500ms
      - type: leader_crash
        replicas: 5%

  - name: settle
    duration: 5m
    ops_per_second: 0

verification:
  - all_replicas_converge: true
  - max_sync_lag_p99: 5000ms
  - data_loss: 0
  - coordinator_throughput_p99: 5000 ops/s
```

### Phase 8 Deliverables
- Security audit report with all findings resolved
- Load test results meeting targets
- Performance within optimization targets
- Full API documentation published
- Coordinator failover implementation
- Release pipeline: `cargo publish` + `npm publish`
- Production checklist verified

---

## 13. Phase 9 — Additional SDKs & Transports

### Goal
Build P2P (WebRTC) and Mesh (libp2p) transports. Implement Go, Swift, Kotlin SDKs.

### P2P Transport (WebRTC)

```rust
// crates/vaultsync-core/src/transport/webrtc.rs (feature-gated)

pub struct P2PTransport {
    peer_connection: RTCPeerConnection,
    data_channel: RTCDataChannel,
    discovery: PeerDiscovery,
}

impl P2PTransport {
    pub async fn connect(peer_id: &str, signaling: &dyn SignalingServer) -> Result<Self>;
    pub async fn send_mutation(&self, mutation: EncryptedMutation) -> Result<()>;
    pub async fn receive_stream(&self) -> Result<impl Stream<Item = EncryptedMutation>>;
}

impl Transport for P2PTransport {
    async fn send(&self, data: &[u8]) -> Result<()>;
    async fn receive(&self) -> Result<Vec<u8>>;
}
```

### Mesh Transport (libp2p)

```rust
// crates/vaultsync-core/src/transport/mesh.rs (feature-gated)

pub struct MeshTransport {
    swarm: libp2p::Swarm<VaultSyncBehaviour>,
    peers: HashSet<PeerId>,
}

impl MeshTransport {
    pub async fn new(config: MeshConfig) -> Result<Self>;
    pub async fn gossip_mutation(&self, mutation: EncryptedMutation) -> Result<()>;
    pub async fn anti_entropy_sync(&self) -> Result<()>;
}
```

### Additional SDKs (Scaffolding Only in This Phase)

| SDK | Location | Approach |
|---|---|---|
| **Go** | `sdks/go/vaultsync.go` | CGo bindings to `vaultsync-core` compiled as C library. Expose `VaultSyncClient`, `Schema`, `Subscription` types |
| **Swift** | `sdks/swift/VaultSync.swift` | Swift Package Manager. Wraps `vaultsync-core` via C interop. Native async/await API |
| **Kotlin** | `sdks/kotlin/VaultSync.kt` | JNI bindings to `vaultsync-core`. Expose as Kotlin coroutines |

Each SDK repeats the same pattern:
1. Compile `vaultsync-core` as a C dynamic library (`cdylib`)
2. Generate C headers (cbindgen)
3. Wrap C API in idiomatic language bindings
4. Publish to language-specific package manager

### Phase 9 Deliverables
- P2P transport (WebRTC) passing integration tests
- Mesh transport (libp2p) passing integration tests
- Go SDK (alpha quality, basic CRUD + sync)
- Swift SDK (alpha quality, iOS/macOS)
- Kotlin SDK (alpha quality, Android)
- P2P + Coordinator hybrid mode tested (LAN P2P + cloud coordinator fallback)

---

## 14. Build & Release Strategy

### Versioning

```
Semantic Versioning: MAJOR.MINOR.PATCH

Alpha: 0.1.0 to 0.9.0
Beta:  0.10.0 to 0.99.0
Stable: 1.0.0+

Pre-release tags: -alpha.1, -beta.2, -rc.1
```

### Package Distribution

| Artifact | Registry | Trigger |
|---|---|---|
| `vaultsync-core` (Rust crate) | crates.io | `cargo publish` in release pipeline |
| `vaultsync-wasm` (WASM) | npm (`@vaultsync/web`) | `wasm-pack publish` |
| `vaultsync-coordinator-*` | crates.io | `cargo publish` per crate |
| `@vaultsync/web` | npm | `npm publish` |
| `@vaultsync/node` | npm | `npm publish` |
| `@vaultsync/react` | npm | `npm publish` |
| `@vaultsync/coordinator-*` | npm | `npm publish` per package |
| `vaultsync-swift` | SPM | Git tag |
| `vaultsync-android` | Maven Central | Gradle publish |

### Release Pipeline

```yaml
# .github/workflows/release.yml
name: Release VaultSync

on:
  push:
    tags: ["v*.*.*"]

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/run-all-tests.sh
      - run: cargo bench -- --baseline prev_release

  publish-rust:
    needs: [verify]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo publish -p vaultsync-core
      - run: cargo publish -p vaultsync-coordinator-postgres
      - run: cargo publish -p vaultsync-coordinator-redis
      - run: cargo publish -p vaultsync-coordinator-sqlite
      - run: cargo publish -p vaultsync-coordinator-memory
      - run: cargo publish -p vaultsync-cli

  publish-wasm:
    needs: [verify]
    runs-on: ubuntu-latest
    steps:
      - run: wasm-pack build crates/vaultsync-wasm --target web
      - run: cd packages/web && npm publish

  publish-npm:
    needs: [verify]
    runs-on: ubuntu-latest
    steps:
      - run: cd packages/node && npm publish
      - run: cd packages/react && npm publish
      - run: cd packages/coordinator-postgres && npm publish
      - run: cd packages/coordinator-redis && npm publish

  release-github:
    needs: [publish-rust, publish-wasm, publish-npm]
    runs-on: ubuntu-latest
    steps:
      - run: gh release create ${{ github.ref_name }}
        --title "VaultSync ${{ github.ref_name }}"
        --notes-file CHANGELOG.md
```

---

## 15. Milestone Timeline

```
Week  1: P0 — Scaffolding                    ████████░░░░░░░░░░░░░░░░░░░░░░░░░░
Week  2: P1 — Core Engine (CRDT)             ░███████████████████████░░░░░░░░░░
Week  3: P1 — Core Engine (E2EE)             ░░░░░░░░░░░██████████████████░░░░
Week  4: P1 — Core Engine (Storage)          ░░░░░░░░░░░░░░░░░░░░████████████░░
Week  5: P1 — Core Engine (OpLog + Schema)   ░░░░░░░░░░░░░░░░░░░░░░░░░░████████
Week  6: P1 — Core Engine (Sync + Coordinator)░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week  7: P2 — WASM & Browser                 ░██████████████████████████████░░░░
Week  8: P2 — WASM & Browser (cont)          ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week  9: P3 — Multi-Tab & IPC                ░████████████████████████░░░░░░░░░░
Week 10: P3 — Multi-Tab & IPC (cont)         ░░░░░░░░░░░░░░░░░░░░░░░░██████████
Week 11: P4 — PostgresCoordinator            ░████████████████████░░░░░░░░░░░░░░
Week 12: P4 — RedisCoordinator               ░░░░░░░░░░░░░░░░░░░░██████████░░░░
Week 13: P4 — Remaining Coordinators         ░░░░░░░░░░░░░░░░░░░░░░░░░░░░██████
Week 14: P5 — TypeScript SDK (web)           ░████████████████████████░░░░░░░░░░
Week 15: P5 — TypeScript SDK (node)          ░░░░░░░░░░░░░░░░░░░░░░░░████████░░
Week 16: P5 — React Hooks                    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week 17: P6 — Testing (CRDT + E2EE)          ░██████████████░░░░░░░░░░░░░░░░░░░░
Week 18: P6 — Testing (Integration + Chaos)  ░░░░░░░░░░░░░░░░████████████████░░
Week 19: P6 — Testing (E2E + Bench + Fuzz)   ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week 20: P7 — CLI & Observability            ░████████████████████████░░░░░░░░░░
Week 21: P7 — CLI & Observability (cont)     ░░░░░░░░░░░░░░░░░░░░░░░░██████████
Week 22: P8 — Security Audit                 ░████████░░░░░░░░░░░░░░░░░░░░░░░░░░
Week 23: P8 — Performance Optimization        ░░░░░░░░████████████░░░░░░░░░░░░░░
Week 24: P8 — Edge Cases + Documentation      ░░░░░░░░░░░░░░░░░░░░████████████░░
Week 25: P8 — Coordinator Failover + Load Test░░░░░░░░░░░░░░░░░░░░░░░░░░░░██████
Week 26: P8 — Release Prep                    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week 27: P9 — P2P Transport (WebRTC)         ░████████████████░░░░░░░░░░░░░░░░░░
Week 28: P9 — Mesh Transport (libp2p)        ░░░░░░░░░░░░░░░░████████████░░░░░░
Week 29: P9 — Go SDK                         ░░░░░░░░░░░░░░░░░░░░░░░░████████░░
Week 30: P9 — Swift SDK                      ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week 31: P9 — Kotlin SDK                     ░██████████████████████████████████
Week 32: P9 — P2P+Coordinator Hybrid Testing  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████

Legend: ██ Active work  ░░ Idle/waiting for prior phase
Milestones:
  M1 (Week 6):  Alpha — Rust core complete, single-tab, in-memory coordinator
  M2 (Week 10): Alpha+ — WASM, browser, multi-tab
  M3 (Week 16): Beta — All coordinators, TypeScript SDK, React hooks
  M4 (Week 21): Beta+ — CLI, observability, all tests passing
  M5 (Week 26): Stable — Security audit, optimized, documented, released
  M6 (Week 32): Post-stable — P2P, mesh, Go/Swift/Kotlin SDKs
```

---

## 16. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| **Yrs integration issues** (API changes, performance bugs) | Medium | High | Pin Yrs version. Run Yrs test suite as part of VaultSync CI. Contribute upstream fixes |
| **WASM performance too slow for real-time sync** | Medium | High | Optimize JS↔WASM boundary. Use shared memory for large data. Fallback: native via Tauri |
| **Browser SharedArrayBuffer blocked by COOP/COEP** | Medium | Medium | Detect missing headers. Fallback: BroadcastChannel + IndexedDB polling (slower but works) |
| **E2EE key management complexity** | Medium | Medium | Simplify to a single key per namespace. Use OS keychain for key storage. Provide clear recovery docs |
| **Multi-tab leader election race conditions** | Low | Critical | Formal verification of election protocol. Extensive chaos testing. Fuzz election timings |
| **Postgres coordinator LISTEN/NOTIFY reliability** | Low | Medium | NOTIFY delivery is not guaranteed on connection loss. Implement polling fallback |
| **Community adoption slow** | High | Medium | Focus on unique differentiators (E2EE, CRDT-native, infra-agnostic). Excellent documentation. Comparison benchmarks |
| **Security audit finds critical issues** | Medium | Extreme | Budget extra 2 weeks for fixes. Engage auditor early (Week 18 not Week 22) |
| **Key developer leaves project** | Low | High | Document all architectural decisions. Code review by at least 2 people. Open source community building from day 1 |

---

*VaultSync — End-to-End Implementation Plan*
*32 weeks from scaffolding to stable release. Every phase builds on the prior. Every component is tested, benchmarked, and documented.*
