# Engine V2 — Complete Architecture Plan

> **Status:** Architecture complete (9.6/10). Ready for Phase 1 implementation.
> **Branch:** `engine-v2`
> **Target:** Production-grade local-first sync engine with single Runtime Owner, mutation streaming, and sub-100ms follower startup.

---

## Table of Contents

1. [Architecture Philosophy](#1-architecture-philosophy)
2. [Runtime Architecture](#2-runtime-architecture)
3. [Subsystem Reference](#3-subsystem-reference)
4. [Data Structures](#4-data-structures)
5. [BC Protocol Specification](#5-bc-protocol-specification)
6. [Phase 1 — Manifest + Generic PageIndex](#6-phase-1--manifest--generic-pageindex)
7. [Phase 2 — Runtime-First Architecture](#7-phase-2--runtime-first-architecture)
8. [Phase 3 — Leader/Follower Ownership Split](#8-phase-3--leaderfollower-ownership-split)
9. [Phase 4 — Runtime Bus (Mutation Streaming)](#9-phase-4--runtime-bus-mutation-streaming)
10. [Phase 5 — Reactive Query Engine](#10-phase-5--reactive-query-engine)
11. [Phase 6 — WAL + Specialized Indexes](#11-phase-6--wal--specialized-indexes)
12. [Phase 7 — Stateful Promotion + Failover](#12-phase-7--stateful-promotion--failover)
13. [Invariants](#13-invariants)
14. [Performance Targets](#14-performance-targets)
15. [Risk Analysis](#15-risk-analysis)
16. [Implementation Order Rationale](#16-implementation-order-rationale)

---

## 1. Architecture Philosophy

### Core principle

> The browser has exactly one Runtime Owner. Followers are proxies with a cache.

Not thirteen independent engines. Not "every tab is a full engine that happens to defer writes." One authoritative runtime that owns persistence, upload, download, compaction, and coordination. Everything else is a cache+proxy attached to it.

### Five invariants

1. **One Persistence Owner** — Only the leader writes OPFS. Followers never touch storage.
2. **One Upload Pipeline** — Only the leader pushes to the coordinator. No duplicate uploads.
3. **One Compactor** — Only the leader compacts, snapshots, GCs. No races.
4. **Optimistic UI Everywhere** — Followers update UI instantly, before the leader acknowledges. The mutation is broadcast, not awaited.
5. **Runtime is the Source of Truth** — Not storage. Storage is a persistence implementation detail. The Runtime holds the authoritative in-memory state.

### How this differs from the current architecture

| Aspect | Current | V2 |
|--------|---------|----|
| Per-tab initialization | 18 OPFS enumerations | Read manifest (1 file) |
| Follower storage | Full OPFS access | None |
| Cross-tab sync | "Invalidate → re-read" | Mutation streaming |
| Query resolution | Storage scan → deserialize → React | Runtime cache → React |
| Compaction | Every tab could compact (race) | Leader only |
| Upload | Every tab could upload (duplicate) | Leader only |
| Leader death | Full re-init (~6s) | Stateful promotion (~200ms) |

---

## 2. Runtime Architecture

### 2.1 Deployment topology

```
┌──────────────────────────────────────────────────────────────┐
│                      Browser Tab 1                            │
│  ┌────────────────────────────────────────────────────────┐   │
│  │               Leader Runtime Owner                      │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────┐      │   │
│  │  │  State   │ │ Persist  │ │   Network        │      │   │
│  │  │  Cache   │ │ Engine   │ │  Upload/Download │      │   │
│  │  └────┬─────┘ └────┬─────┘ └────────┬─────────┘      │   │
│  │       │            │                │                │   │
│  │  ┌────▼────────────▼────────────────▼────────────┐   │   │
│  │  │              Runtime Bus                       │   │   │
│  │  │  (BC: SNAPSHOT, MUTATION, HEARTBEAT, ACK)     │   │   │
│  │  └────────────────┬───────────────────────────────┘   │   │
│  └───────────────────┼───────────────────────────────────┘   │
└──────────────────────┼───────────────────────────────────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         ▼             ▼             ▼
┌────────────────┐ ┌────────────┐ ┌────────────┐
│  Tab 2         │ │  Tab 3     │ │  Tab N     │
│  FollowerProxy │ │  Follower  │ │  Follower  │
│                │ │            │ │            │
│  RuntimeCache  │ │  Runtime   │ │  Runtime   │
│  BC Listener   │ │  BC Listen │ │  BC Listen │
│  RPC Client    │ │  RPC Cli   │ │  RPC Cli   │
└────────────────┘ └────────────┘ └────────────┘
```

### 2.2 Leader internal structure

```
Runtime
├── State
│   ├── Documents: HashMap<String, HashMap<String, HashMap<String, CrdtValue>>>
│   ├── Pending: Vec<Mutation>
│   ├── Presence: HashMap<String, PresenceRecord>
│   └── Metadata: { generation, cursor, status }
│
├── Scheduler (prioritized task dispatcher)
│   ├── Upload
│   ├── Download
│   ├── Compaction
│   ├── GC
│   ├── Heartbeat
│   └── Broadcast
│
├── PersistenceEngine
│   ├── ManifestStore (merged manifest + ContentIndex per store)
│   ├── PageStore (6 stores backing OPFS)
│   └── WAL (in-memory + OPFS for crash recovery)
│
├── NetworkEngine
│   ├── UploadPipeline
│   ├── DownloadWorker
│   └── CoordinatorHandle
│
├── RuntimeBus (BC protocol owner)
│   ├── Snapshot provider (serializes metadata on request)
│   ├── Mutation broadcaster (after persist)
│   ├── Heartbeat sender (1s interval)
│   ├── Follower registry (attached tabs)
│   └── Backpressure controller (batch + chunk)
│
├── QueryEngine
│   ├── Runtime store (JS-side, framework-agnostic)
│   ├── Subscription registry
│   └── Dependency graph (query → document mappings)
│
├── Metrics (counters + histograms)
│   ├── cache_hit_ratio
│   ├── page_reads
│   ├── mutation_latency_us
│   ├── bc_latency_us
│   ├── leader_promotions
│   ├── hydration_time_ms
│   └── queue_sizes
│
├── MemoryManager
│   ├── RuntimeCache LRU eviction
│   ├── Page cache eviction
│   ├── Index memory budget
│   └── Heartbeat pressure detection
│
└── CapabilityManager
    ├── Leader (full engine)
    ├── Follower (cache + BC + RPC)
    ├── ReadonlyWarm (no writes, cache from broadcast)
    └── Offline (leader with no coordinator)
```

### 2.3 Follower internal structure

```
FollowerProxy
├── RuntimeCache
│   ├── documents: LruCache<String, LruCache<String, HashMap<String, CrdtValue>>>
│   ├── generation: RuntimeGeneration
│   ├── pending: FollowerPendingMutations
│   └── metadata: { cursor, last_heartbeat }
│
├── BC Listener (vaultsync-ipc-{ns})
│   ├── Handles SNAPSHOT_METADATA → hydrate doc list (no content)
│   ├── Handles SNAPSHOT_DOCUMENT → hydrate single doc on demand
│   ├── Handles MUTATION → apply + fire subscription
│   ├── Handles HEARTBEAT → refresh timer
│   ├── Handles LEADER_TRANSFER → initiate promotion
│   └── Handles ACK → reconcile optimistic mutation
│
├── BC Sender (vaultsync-ipc-{ns})
│   ├── FOLLOWER_ATTACH on startup
│   ├── MUTATION on user edit
│   ├── REQUEST_DOCUMENT for lazy fetch
│   └── FOLLOWER_DETACH on unload
│
├── RPC Client (fallback — MessageChannel or WebRTC)
│   └── Reserved for future cross-tab communication
│
└── UI Adapter (JS-side, framework-agnostic)
    ├── get → from cache
    ├── query → from cache
    ├── insert/update/delete → optimistic + BC mutation
    └── subscribe → register with QueryEngine
```

### 2.4 Runtime Generations

```rust
/// Logical epoch of the runtime. Incremented only when the
/// runtime owner changes (leader election, promotion).
/// NOT incremented on compaction or snapshot — those are
/// storage-layer operations that don't change logical state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeGeneration(u64);

/// Physical generation of storage. Incremented on any
/// storage-layer change (write, compaction, GC).
/// Used for cache invalidation, NOT for mutation ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StorageGeneration(u64);
```

**Key rule**: Mutations carry `RuntimeGeneration`. Followers reject mutations with stale `RuntimeGeneration` (leader has changed). Compaction increments `StorageGeneration` only — followers don't discard their cache on compaction.

---

## 3. Subsystem Reference

### 3.1 RuntimeScheduler

Coordinates all background work with explicit priorities:

| Task | Priority | Max Frequency | Notes |
|------|----------|---------------|-------|
| Broadcast mutation | P0 | Immediate | Never delay UI sync |
| Upload | P1 | 50ms batch window | Coalesces mutations |
| Heartbeat | P2 | 1s interval | BC keepalive |
| Download | P2 | 100ms poll | Pull from coordinator |
| Snapshot | P3 | 30s interval | Background warm cache |
| GC | P4 | 60s interval | Tombstone cleanup |
| Compaction | P5 | 300s interval | Background |
| Prefetch | P6 | Idle | Only when nothing else pending |

```rust
trait ScheduledTask: Send {
    fn priority(&self) -> u8;  // 0 = highest
    fn interval(&self) -> Duration;
    fn name(&self) -> &'static str;
    async fn execute(&self, runtime: &Runtime) -> Result<(), Error>;
}
```

### 3.2 RuntimeMetrics

Counter-based metrics with WASM exposure:

```rust
#[derive(Debug, Default)]
struct RuntimeMetrics {
    // Cache
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,

    // Storage
    page_reads: AtomicU64,
    page_writes: AtomicU64,
    page_enumerations: AtomicU64,  // Should be 0 after Phase 1

    // Bus
    mutations_sent: AtomicU64,
    mutations_received: AtomicU64,
    snapshots_sent: AtomicU64,
    snapshots_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    bus_messages_dropped: AtomicU64,

    // Lifecycle
    leader_promotions: AtomicU64,
    leader_demotions: AtomicU64,
    hydration_time_ms: AtomicU64,
    generation: AtomicU64,

    // Latency histograms (μs, stored as running stats)
    mutation_latency_us: RunningStats,
    bc_latency_us: RunningStats,
    page_read_latency_us: RunningStats,
}
```

Exposed to JS via:
```typescript
interface MetricsSnapshot {
    cacheHitRatio: number;
    pageReads: number;
    mutationLatencyAvgUs: number;
    bcLatencyAvgUs: number;
    leaderPromotions: number;
    hydrationTimeMs: number;
    busMessagesDropped: number;
}
```

### 3.3 MemoryManager

```rust
struct MemoryManager {
    // Runtime cache limits
    max_documents: usize,        // Default: 10,000
    max_bytes: usize,            // Default: 50MB
    eviction_policy: EvictionPolicy,  // LRU or Clock

    // Page cache
    page_cache_max_entries: usize,    // Default: 500
    page_cache_max_bytes: usize,      // Default: 20MB

    // Index
    index_max_entries: usize,         // Default: 50,000
}

enum EvictionPolicy {
    Lru,
    Clock(usize),  // Clock with hand position
}

impl MemoryManager {
    fn should_evict(&self, current_usage: &MemoryUsage) -> bool;
    fn evict_candidates(&self, usage: &MemoryUsage) -> Vec<EvictionTarget>;
}
```

### 3.4 CapabilityManager

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RuntimeCapability {
    /// Full engine: persistence, network, compaction, scheduler
    Leader,
    /// Cache + BC + RPC client. No persistence, no network.
    Follower,
    /// Like follower but no writes. Broadcast-only receive.
    ReadonlyWarm,
    /// Leader with no coordinator. Local-first only.
    Offline,
}
```

Used for:
- Protocol negotiation on FOLLOWER_ATTACH
- Feature detection in the bus
- Limiting what operations are available

### 3.5 BC Protocol Versioning

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BusEnvelope {
    version: u16,            // Current: 1
    message_type: BusMessageType,
    payload: Vec<u8>,        // Postcard-encoded BusMessage
    // Future: extensions can add fields here
}
```

On FOLLOWER_ATTACH, leader checks the follower's protocol version. If incompatible, leader either:
- Rejects the attachment (follower must re-init as independent engine)
- Or offers a downgrade path (if backward compatible)

### 3.6 ACK levels

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum AckLevel {
    /// Mutation has been persisted to OPFS
    Persisted,
    /// Mutation has been sent to coordinator
    Uploaded,
    /// Mutation has been broadcast to all followers
    Replicated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MutationAck {
    seq: u64,
    level: AckLevel,
    error: Option<String>,
}
```

Followers can track which of their optimistic mutations have reached each level. UI can show different indicators: "Saved" (Persisted), "Syncing" (Uploaded), "Synced" (Replicated).

---

## 4. Data Structures

### 4.1 ContentIndex (Phase 1)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
enum IndexKey {
    Document { doc_id: String, record_id: String },
    Sequence(u64),
    Namespace(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContentIndex {
    generation: StorageGeneration,
    entries: HashMap<IndexKey, PageId>,
    live_pages: BTreeSet<PageId>,
}

impl ContentIndex {
    fn lookup(&self, key: &IndexKey) -> Option<PageId>;
    fn insert(&mut self, key: IndexKey, page_id: PageId);
    fn remove(&mut self, key: &IndexKey);
    fn rebuild_from_scan(dir: &FileSystemDirectoryHandle) -> Result<Self>;
}
```

### 4.2 Merged StoreManifest (Phase 1, Q5)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreManifest {
    version: u8,
    generation: StorageGeneration,  // incremented on any storage mutation

    // Metadata
    highest_page_id: PageId,
    current_page: PageId,
    last_sequence: u64,
    pending_count: usize,
    live_pages: usize,
    tombstoned_pages: usize,
    created_at: u64,
    updated_at: u64,

    // Content index (merged, not separate)
    content_index: ContentIndex,

    // Checksum covers everything including content_index
    checksum: u32,
}
```

### 4.3 Mutation structures (Phase 3/4)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
enum MutationAction {
    SetField {
        doc_id: String,
        record_id: String,
        field: String,
        value: CrdtValue,
    },
    DeleteField {
        doc_id: String,
        record_id: String,
        field: String,
    },
    DeleteDocument {
        doc_id: String,
        record_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Mutation {
    action: MutationAction,
    hlc: u64,
    runtime_gen: RuntimeGeneration,
    replica_id: String,
    origin_tab: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptedMutationOperation {
    ciphertext: Vec<u8>,  // Encrypted MutationAction
    hlc: u64,
    runtime_gen: RuntimeGeneration,
    replica_id: String,
    key_version: u32,
}
```

### 4.4 Bus messages (Phase 3/4)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum BusMessage {
    FOLLOWER_ATTACH {
        tab_id: String,
        protocol_version: u16,
        capabilities: Vec<RuntimeCapability>,
    },
    FOLLOWER_DETACH {
        tab_id: String,
    },
    SNAPSHOT_METADATA {
        runtime_gen: RuntimeGeneration,
        storage_gen: StorageGeneration,
        document_ids: Vec<(String, String, u64)>,  // (doc_id, record_id, hash)
        cursor: u64,
        pending_count: usize,
    },
    SNAPSHOT_DOCUMENT {
        doc_id: String,
        record_id: String,
        fields: HashMap<String, CrdtValue>,
    },
    MUTATION {
        mutation: EncryptedMutationOperation,
        seq: u64,
    },
    MUTATION_BATCH {
        mutations: Vec<EncryptedMutationOperation>,
        from_seq: u64,
        to_seq: u64,
    },
    HEARTBEAT {
        runtime_gen: RuntimeGeneration,
        storage_gen: StorageGeneration,
        cursor: u64,
        pending_count: usize,
    },
    LEADER_TRANSFER {
        runtime_gen: RuntimeGeneration,
        storage_gen: StorageGeneration,
        cursor: u64,
        pending_count: usize,
    },
    ACK {
        seq: u64,
        level: AckLevel,
        error: Option<String>,
    },
    REQUEST_SYNC {
        from_seq: u64,
    },
    REQUEST_DOCUMENT {
        doc_id: String,
        record_id: String,
    },
}
```

### 4.5 RuntimeCache with eviction (follower)

```rust
struct RuntimeCache {
    /// LRU cache of documents. Evicts oldest-accessed when over limit.
    documents: LruCache<String, LruCache<String, HashMap<String, CrdtValue>>>,
    /// Current runtime generation (from leader's heartbeat)
    runtime_gen: RuntimeGeneration,
    /// Optimistic mutations not yet acknowledged
    pending: FollowerPendingMutations,
    /// Last leader heartbeat timestamp
    last_heartbeat: Instant,
    /// Cursor from leader
    cursor: u64,
}

struct LruCache<K, V> {
    max_entries: usize,
    entries: HashMap<K, Entry<K, V>>,
    order: VecDeque<K>,
}

impl RuntimeCache {
    fn get(&mut self, doc_id: &str, record_id: &str) -> Option<&HashMap<String, CrdtValue>>;
    fn query(&mut self, doc_id: &str) -> Vec<&HashMap<String, CrdtValue>>;
    fn apply_mutation(&mut self, mutation: &Mutation);
    fn evict_oldest(&mut self);
}
```

### 4.6 RuntimeSnapshot (lazy — Phase 3)

```rust
/// Sent on FOLLOWER_ATTACH. Contains only metadata.
/// Actual document contents are fetched lazily via REQUEST_DOCUMENT.
struct SnapshotMetadata {
    runtime_gen: RuntimeGeneration,
    storage_gen: StorageGeneration,
    /// (doc_id, record_id, content_hash) for every document
    document_ids: Vec<(String, String, u64)>,
    cursor: u64,
    pending_count: usize,
    taken_at: u64,
}
```

Follower attach flow:
```
1. FOLLOWER_ATTACH
2. ← SNAPSHOT_METADATA (doc list only, no content)
3. Follower creates empty cache entries for all doc IDs
4. Follower marks as ready (UI can show placeholder/skeleton)
5. As user accesses documents, REQUEST_DOCUMENT → SNAPSHOT_DOCUMENT
6. Background: leader pushes hot documents proactively
```

This replaces the single large SNAPSHOT message. No 100MB BC message.

### 4.7 FollowerPendingMutations

```rust
struct FollowerPendingMutations {
    /// Mutations optimistically applied but awaiting ACK
    pending: VecDeque<PendingMutationRecord>,
    max_size: usize,
}

struct PendingMutationRecord {
    doc_id: String,
    record_id: String,
    sent_at: Instant,
    retries: u8,
    last_ack: Option<AckLevel>,
}
```

---

## 5. BC Protocol Specification

### 5.1 Channel names

| Channel | Purpose |
|---------|---------|
| `vaultsync-ipc-{ns}` | Leader ↔ Follower (bus messages) |
| `vaultsync-presence-{ns}` | Presence/heartbeat (existing) |

### 5.2 Envelope format

Every BC message is wrapped in:

```rust
struct BusEnvelope {
    version: u16,        // Protocol version (current: 1)
    message: BusMessage, // Postcard-encoded
}
```

Encoding: postcard binary for all messages (small, fast, no JSON parsing). The `BusMessage` enum discriminant serves as the message type tag.

### 5.3 Message flow diagrams

**Follower Attach (startup):**
```
Follower                         Leader
   │                               │
   │── FOLLOWER_ATTACH ───────────▶│  (tab_id, version, capabilities)
   │                               │
   │◀── SNAPSHOT_METADATA ────────│  (doc IDs, no content)
   │                               │
   │  [creates empty cache entries]│
   │                               │
   │◀── HEARTBEAT (1s interval) ──│  (ongoing)
   │◀── MUTATION (as they happen)─│  (ongoing)
   │                               │
   │── REQUEST_DOCUMENT ──────────▶│  (user accesses a doc, lazy)
   │◀── SNAPSHOT_DOCUMENT ────────│  (doc content)
```

**Follower Edit:**
```
Follower                         Leader                    Other Followers
   │                               │                           │
   │  [optimistic UI update]       │                           │
   │── MUTATION ──────────────────▶│                           │
   │                               │  [CRDT merge + persist]   │
   │◀── ACK (Persisted) ──────────│                           │
   │                               │── MUTATION ──────────────▶│
   │◀── MUTATION ─────────────────│                           │  (from leader)
   │  [reconcile, should match]   │                           │
   │◀── ACK (Replicated) ────────▶│                           │
```

**Leader Death & Promotion:**
```
Follower 1 (promoting)          Follower 2
   │                               │
   │  [no HEARTBEAT for 2s]        │  [no HEARTBEAT for 2s]
   │                               │
   │  [acquire Web Lock]           │  [acquire Web Lock → fail]
   │  [load manifest]              │
   │  [rehydrate runtime]          │
   │  [increment RuntimeGeneration]│
   │                               │
   │── LEADER_TRANSFER ───────────▶│
   │  (new runtime_gen, storage_gen, cursor)
   │                               │
   │── SNAPSHOT_METADATA ─────────▶│
   │  (doc IDs, no content)        │
   │                               │
   │  [resume as leader]           │  [continue as follower]
```

### 5.4 Backpressure and flow control

For large mutations (e.g., 50MB JSON paste):

1. **Split**: Leader splits the payload into chunks (max 64KB per chunk)
2. **Sequence**: Each chunk carries `{ batch_id, chunk_index, total_chunks }`
3. **Batch**: All chunks for one mutation share a `batch_id`
4. **Reassembly**: Follower reassembles before applying
5. **Throttle**: If pending_batches > 3, leader sends `FLOW_CONTROL { backoff_ms }` to the source tab

```rust
enum ChunkedMutation {
    Chunk {
        batch_id: u64,
        chunk_index: u16,
        total_chunks: u16,
        data: Vec<u8>,
    },
    Complete {
        batch_id: u64,
    },
}

enum FlowControl {
    Throttle { backoff_ms: u64 },
    Resume,
}
```

### 5.5 Gap recovery

If a follower misses a MUTATION (tab backgrounded, event loop blocked):

1. Follower detects gap via sequence number on HEARTBEAT
2. Follower sends `REQUEST_SYNC { from_seq }`
3. Leader sends `MUTATION_BATCH { mutations: [...], from_seq, to_seq }`
4. Follower applies all missed mutations in order

### 5.6 Encryption

All MUTATION payloads are encrypted with the same E2EE key as OPFS pages. Followers have the keyring (loaded once during FollowerProxy construction — the only storage access followers ever do).

SNAPSHOT_DOCUMENT responses are also encrypted. The leader encrypts with the follower's key or the shared namespace key.

Plaintext messages (FOLLOWER_ATTACH, HEARTBEAT, ACK) are sent unencrypted — they carry no document data.

---

## 6. Phase 1 — Manifest + Generic PageIndex

### 6.1 Goal

Eliminate all OPFS directory enumerations. Replace with a persisted content index that maps `key → PageId` for every store.

### 6.2 Changes

**page_store.rs:**
- Add `ContentIndex` struct with `HashMap<IndexKey, PageId>`
- Merge into `StoreManifest` — single file, single checksum, single read
- `open()`: read manifest, validate checksum. If missing/corrupt, `rebuild_manifest()` (extended to also build `ContentIndex`)
- `list_page_ids(caller)` → `content_index.live_pages.clone()` — no OPFS enumeration
- `write_page_raw()` → after write, update index entry
- `tombstone_page()` → after tombstone, remove from index
- `allocate_page_id()` → no index change (page is allocated but not yet written)

**storage.rs (OpfsStorage):**
- `get_document(doc_id, record_id)` → index lookup → single `read_page()`
- `tombstone_existing_doc_pages(doc_id, record_id)` → index lookup → single `tombstone_page()`
- `list_documents(doc_id)` → iterate index entries matching `IndexKey::Document { doc_id, .. }` prefix

**All 6 PageStores:**
| Store | Key type |
|-------|----------|
| `doc_data` | `IndexKey::Document { doc_id, record_id }` |
| `oplog` | `IndexKey::Sequence(u64)` |
| `sync_states` | `IndexKey::Namespace(String)` |
| `schemas` | `IndexKey::Schema(String)` |
| `migrations` | `IndexKey::Migration(String)` |
| `keys` | `IndexKey::Namespace(String)` |

### 6.3 Backward compatibility

On first startup with Phase 1:
1. `open()` reads manifest
2. If manifest is missing `content_index` (old format) or checksum mismatches:
   - Run `rebuild_manifest()` (existing OPFS scan)
   - Extended to build `ContentIndex` from scanned data
   - Write merged manifest
3. Next startup: reads manifest directly — no scan

### 6.4 Tests
- Index round-trip: build → persist → read → match
- Index update on write: write → index has new entry
- Index removed on tombstone: tombstone → entry gone
- Index rebuild on corruption: corrupt manifest → first startup rebuilds
- Old-format migration: no index → rebuilds once
- Metrics: `list_page_ids` reports 0 OPFS enumerations after warmup

### 6.5 Files touched
- `crates/vaultsync-wasm/src/page_store.rs` — ContentIndex, merged manifest, list_page_ids rewrite
- `crates/vaultsync-wasm/src/storage.rs` — all doc_data/oplog methods use index

### 6.6 Estimated effort: 4-5 days

---

## 7. Phase 2 — Runtime-First Architecture

### 7.1 Goal

Shift the architecture from "storage is the source of truth" to "Runtime (in-memory state) is the source of truth." Storage becomes a persistence component.

### 7.2 New Runtime subsystems

```
Runtime
├── State (in-memory)
├── Scheduler (prioritized tasks)
├── PersistenceEngine (manifest + pages)
├── NetworkEngine (upload/download/coordinator)
├── RuntimeBus (BC)
├── QueryEngine (JS-side subscriptions)
├── Metrics (counters)
├── MemoryManager (eviction limits)
└── CapabilityManager (leader/follower/offline)
```

### 7.3 Write path

```
insert(doc_id, record_id, fields)
  │
  ▼
Runtime::insert()
  │
  ├── CRDT merge into State.Documents (in-memory)
  ├── Create Mutation with HLC
  ├── Scheduler.schedule(Upload) with mutation
  │
  ├── PersistenceEngine::write(mutation)
  │     ├── PageStore.write_page → update ContentIndex
  │     └── Persist manifest
  │
  └── RuntimeBus::broadcast(MUTATION) to followers
```

### 7.4 Read path

```
get(doc_id, record_id)
  │
  └── State.Documents[doc_id][record_id]
        │
        └── Returns immediately — no storage read
```

Cold start: `Runtime::hydrate()` reads manifest, loads pages via ContentIndex, rebuilds State.Documents.

### 7.5 VaultSyncClient compatibility

The existing core `VaultSyncClient` continues to exist but wraps `Runtime`. The WASM `WasmVaultSyncClient` delegates to `Runtime`.

### 7.6 Tests
- Write → state updated + persistence called + bus broadcast called + upload scheduled
- Read → returns from state (zero storage calls)
- Cold start hydrate → state matches persistence
- Concurrent writes → HLC ordering preserved

### 7.7 Files touched
- `crates/vaultsync-wasm/src/runtime.rs` — NEW file
- `crates/vaultsync-wasm/src/client.rs` — refactored to build Runtime
- `crates/vaultsync-wasm/src/scheduler.rs` — NEW file
- `crates/vaultsync-wasm/src/metrics.rs` — NEW file
- `crates/vaultsync-wasm/src/memory.rs` — NEW file

### 7.8 Estimated effort: 5-7 days

---

## 8. Phase 3 — Leader/Follower Ownership Split

### 8.1 Goal

Followers become proxies. No storage, no upload, no download, no coordinator, no compaction. Just a RuntimeCache + BC Listener + RPC client.

### 8.2 Startup paths

**Leader startup** (first tab or election win):
```
1. Open storage (OPFS)
2. Read manifest + ContentIndex
3. Hydrate State.Documents from persistence
4. Start all subsystems (Scheduler, Network, Bus, etc.)
5. Start BC heartbeat (1s interval)
6. Mark as Leader in CapabilityManager
7. Ready (~1-2s)
```

**Follower startup** (subsequent tabs):
```
1. Open BC channel (no OPFS)
2. Send FOLLOWER_ATTACH with protocol_version and [Follower] capability
3. Receive SNAPSHOT_METADATA (doc IDs only, no content)
4. Create empty LRU cache entries for all docs
5. Subscribe to BC MUTATION stream
6. Mark as Follower in CapabilityManager
7. Ready (~50-100ms)
```

### 8.3 Follower writes (Option D — Optimistic Runtime Proxy)

```
1. User edits on follower tab
2. Follower creates Mutation with local HLC
3. Follower applies optimistically to RuntimeCache
4. Follower fires local subscription → React updates immediately
5. Follower adds mutation to FollowerPendingMutations
6. Follower sends MUTATION via BC to leader
7. Leader receives MUTATION
8. Leader applies to RuntimeState.Documents (CRDT merge)
9. Leader persists to OPFS
10. Leader sends ACK (Persisted) to originating follower
11. Leader broadcasts MUTATION to all followers
12. All followers apply (should match optimistic)
13. Leader sends ACK (Replicated) to originating follower
14. Follower reconciles pending mutations
```

### 8.4 Follower reads

Always from RuntimeCache — never touches OPFS.

```
get(doc_id, record_id):
  └── RuntimeCache.documents.get(doc_id)?.get(record_id)
      └── O(1) LRU cache lookup
```

If cache miss (doc not yet fetched): send `REQUEST_DOCUMENT` to leader, receive `SNAPSHOT_DOCUMENT`, hydrate cache, return.

### 8.5 No storage for followers — absolute invariant

Follower code must never reference:
- `PageStore` (any method)
- `OpfsStorage`
- `StoreManifest`
- `ContentIndex`
- Any `Storage` trait method

The `FollowerProxy` struct contains zero references to storage types.

### 8.6 Compaction ownership

Only the leader compacts. Compaction increments `StorageGeneration` but NOT `RuntimeGeneration`. Followers do not discard their cache on compaction.

Pages changed by compaction (tombstone old, write new) update the ContentIndex and manifest. Followers don't need to know — their cache contains the same logical state.

### 8.7 Tests
- Follower attaches → receives metadata → cache created with empty entries
- Lazy document fetch → REQUEST_DOCUMENT → SNAPSHOT_DOCUMENT → cache populated
- Follower write → optimistic → BC → leader persists → broadcast → follower reconciled
- Two followers write simultaneously → both optimistic → leader serializes
- Follower receives old RuntimeGeneration mutation → rejected
- Leader restarts → followers reattach to new leader

### 8.8 Files touched
- `crates/vaultsync-wasm/src/follower.rs` — NEW file
- `crates/vaultsync-wasm/src/runtime.rs` — add bus, snapshot provider, follower registry
- `crates/vaultsync-wasm/src/client.rs` — split init into leader/follower paths
- `crates/vaultsync-wasm/src/ipc.rs` — BusMessage enum, BusEnvelope, all message handlers
- `crates/vaultsync-wasm/src/capability.rs` — NEW file

### 8.9 Estimated effort: 6-8 days

---

## 9. Phase 4 — Runtime Bus (Mutation Streaming)

### 9.1 Goal

Replace `broadcast_invalidation("go re-read storage")` with direct mutation streaming. Followers apply mutations to cache without touching OPFS.

### 9.2 Leader side

```rust
// In Runtime::commit_mutation
async fn commit_mutation(&self, mutation: OplogEntry) -> Result<()> {
    // 1. CRDT merge into State (already done by caller)
    // 2. Persist to OPFS
    self.persistence.write(&mutation).await?;
    // 3. Schedule upload
    self.scheduler.schedule(Task::Upload(mutation.clone()));
    // 4. Broadcast to followers
    self.bus.broadcast(BusMessage::MUTATION {
        mutation: self.encrypt_for_bus(&mutation),
        seq: mutation.sequence,
    }).await;
    Ok(())
}
```

### 9.3 Follower side

```rust
// In FollowerProxy::handle_mutation
fn handle_mutation(&self, msg: BusMessage) {
    match msg {
        BusMessage::MUTATION { mutation, seq } => {
            // 1. Verify RuntimeGeneration
            if mutation.runtime_gen != self.cache.runtime_gen {
                return; // Stale generation, discard
            }
            // 2. Decrypt
            let decrypted = self.keyring.decrypt(&mutation.ciphertext)?;
            // 3. Apply to cache
            self.cache.apply_mutation(&decrypted);
            // 4. Fire subscription (via QueryEngine)
            self.query_engine.notify(&decrypted.doc_id, &decrypted.record_id);
            // 5. If this was our own mutation, reconcile pending
            if mutation.origin_tab == self.tab_id {
                self.cache.pending.ack(seq, AckLevel::Replicated);
            }
        }
        BusMessage::ACK { seq, level, error } => {
            self.cache.pending.ack(seq, level);
        }
        // ...
    }
}
```

### 9.4 Gap recovery

HEARTBEAT includes `cursor`. If follower's `last_seq < leader_cursor`:
```
Follower → REQUEST_SYNC { from_seq }
Leader   → MUTATION_BATCH { mutations: [...], from_seq, to_seq }
Follower → applies all missed mutations
```

### 9.5 Backpressure

For large payloads:
1. ChunkedMutation with `batch_id, chunk_index, total_chunks`
2. Follower reassembles before applying
3. If pending_batches > 3, FLOW_CONTROL with backoff

### 9.6 Tests
- Mutation broadcast → follower cache updated (zero storage reads verified via metrics)
- Gap detection → REQUEST_SYNC → MUTATION_BATCH → catch up
- Chunked mutation → follower reassembles → applies
- Large payload → backpressure triggers → chunking works
- E2EE mutation → follower decrypts correctly
- 1000 mutations → leader and all followers consistent

### 9.7 Estimated effort: 4-5 days

---

## 10. Phase 5 — Reactive Query Engine

### 10.1 Goal

Framework-agnostic runtime store in JS with dependency-graph subscription tracking. React, Vue, Svelte all consume the same store.

### 10.2 Design

```typescript
// sdk/packages/core/src/store.ts
type DocKey = string;
type FieldMap = Record<string, CrdtValue>;

class RuntimeStore {
    // LRU-limited document store
    private cache: Map<string, Map<string, FieldMap>>;

    // Dependency graph: doc_id → Set<subscriber_id>
    //                  doc_id::record_id → Set<subscriber_id>
    private docDeps: Map<string, Set<number>>;
    private recDeps: Map<string, Set<number>>;

    // subscriber_id → callback
    private subs: Map<number, () => void>;
    private nextId: number = 1;

    // Eviction
    private maxEntries: number;
    private accessOrder: string[];  // LRU tracking

    get(docId: string, recordId: string): FieldMap | null;
    query(docId: string): FieldMap[];

    // Called by WASM bridge when mutation arrives
    applySetField(docId: string, recordId: string, field: string, value: CrdtValue): void;
    applyDeleteField(docId: string, recordId: string, field: string): void;
    applyDeleteDocument(docId: string, recordId: string): void;

    subscribe(docId: string, recordId: string | null, cb: () => void): () => void;
    private notify(docId: string, recordId?: string): void;
    private touchKey(key: string): void;  // LRU promotion
    private evict(): void;
}
```

### 10.3 React integration

```typescript
// sdk/packages/react/src/useQuery.ts
function useQuery(docId: string) {
    const store = useRuntimeStore();
    const [data, setData] = useState(() => store.query(docId));

    useEffect(() => {
        return store.subscribe(docId, null, () => {
            setData(store.query(docId));
        });
    }, [docId]);

    return data;
}
```

No WASM calls during rendering. No OPFS reads. Pure JS cache.

### 10.4 Fine-grained dependencies

- `subscribe("note", "abc")` → notified only when `note::abc` changes
- `subscribe("note", null)` → notified when any record in "note" changes
- `subscribe(null, null)` → notified on any change anywhere

### 10.5 Stale-while-revalidate

1. Return cached data immediately (may be empty on cold start)
2. Background: call WASM `find()` or `get()` to verify freshness
3. If stale, update cache + notify subscribers
4. Future updates come via mutation streaming

### 10.6 Files touched
- `sdk/packages/core/src/store.ts` — NEW RuntimeStore
- `sdk/packages/core/src/index.ts` — export RuntimeStore
- `sdk/packages/react/src/useQuery.ts` — use RuntimeStore
- `sdk/packages/react/src/useVaultSyncOne.ts` — use RuntimeStore
- WASM bridge — expose `applySetField` etc.

### 10.7 Estimated effort: 4-5 days

---

## 11. Phase 6 — WAL + Specialized Indexes

### 11.1 Goal

Extend ContentIndex from Phase 1 to enable O(1) lookups for oplog and KV stores.

### 11.2 Oplog index

`mark_synced(seq)` becomes:
1. `content_index.lookup(&IndexKey::Sequence(seq))` → `Some(page_id)`
2. `page_store.read_page(page_id)` → single page
3. Filter out the synced entry
4. Rewrite remaining entries
5. Update index for affected entries

**Approach**: Per-entry index (map EVERY sequence number). Max size = pending mutations (compacted/pruned). With 10,000 pending mutations ≈ 800KB index.

### 11.3 KV store indexes

- `read_sync_state(ns)` → `content_index.lookup(&IndexKey::Namespace(ns))` → single page
- `read_schema(doc_id)` → `content_index.lookup(&IndexKey::Schema(doc_id))` → single page

No scan for any KV operation.

### 11.4 WAL

Write-Ahead Log for crash recovery:
- Before any page write, append mutation to `_pages/{store}/_wal`
- After successful write + index update, remove from WAL
- On crash recovery: replay WAL entries

Only the leader writes to WAL. Followers never touch it.

### 11.5 Tests
- `mark_synced` reads exactly 1 page (metrics verification)
- All KV lookups return from index (no scan)
- WAL append → crash → replay → state consistent
- WAL cleanup after successful write

### 11.6 Estimated effort: 3-4 days

---

## 12. Phase 7 — Stateful Promotion + Failover

### 12.1 Goal

When the leader dies, promote a follower to leader without reinitializing from scratch.

### 12.2 Detection

Follower detects leader death:
1. No HEARTBEAT for `LEADER_TIMEOUT_MS` (2000ms)
2. Follower transitions to `Suspect` state
3. After timeout, attempts promotion

### 12.3 Promotion process

```
1. Detect leader timeout (no HEARTBEAT for 2s)
2. Acquire Web Lock (same lock as current leader election)
3. If lock fails → another tab is promoting → back off
4. Load manifest + ContentIndex from OPFS (O(1) reads)
5. Hydrate State.Documents from persistence via ContentIndex
6. Start Scheduler, NetworkEngine, CompactionEngine
7. Increment RuntimeGeneration (StorageGeneration stays)
8. Start BC heartbeat
9. Broadcast LEADER_TRANSFER with new generation
10. Send SNAPSHOT_METADATA to all attached followers
11. Resume as leader
```

### 12.4 Hydration time

Estimated: O(documents) page reads, not O(pages) directory scan.
- 1000 documents × 1 page read = ~1-2s
- No scan, no coordinator init (starts after hydration)

### 12.5 Reattachment of surviving followers

After LEADER_TRANSFER:
1. New leader sends SNAPSHOT_METADATA (doc IDs only)
2. Followers update their RuntimeGeneration
3. Followers keep their existing cache (same StorageGeneration + same doc IDs)
4. If follower was missing a doc → REQUEST_DOCUMENT on access
5. Old-generation optimistic mutations from old leader → discarded (generation mismatch)
6. User re-applies stale edits (draft recovery)

### 12.6 Tests
- Leader dies → follower promotes → new leader serves reads/writes
- Two followers detect simultaneously → only one acquires lock
- Old-generation mutations rejected after promotion
- Follower cache survives promotion (same StorageGeneration)
- Promote → demote → promote (flapping) → stable after backoff

### 12.7 Estimated effort: 4-5 days

---

## 13. Invariants

These must hold at all times.

### Storage
- **I1**: Only the leader writes OPFS. Followers never write to storage.
- **I2**: Only the leader compacts, GCs, or snapshots. No races.
- **I3**: The manifest (including ContentIndex) is always consistent with pages on disk.
- **I4**: After any write, ContentIndex is updated before broadcast.
- **I5**: StorageGeneration increments on any storage mutation. RuntimeGeneration does NOT increment on storage-only changes (compaction, GC).

### Broadcast
- **I6**: Every MUTATION broadcast by the leader has already been persisted to OPFS.
- **I7**: Every MUTATION carries a RuntimeGeneration. Followers discard mutations from stale generations.
- **I8**: HEARTBEAT is sent at least every 1s while the leader is healthy.

### Follower
- **I9**: Followers never read OPFS during normal operation.
- **I10**: Followers' optimistic mutations are always reconciled with the leader's committed state.
- **I11**: Followers' RuntimeCache is eventually consistent with the leader's RuntimeState.
- **I12**: Followers that detect a gap (via cursor on HEARTBEAT) request a sync before continuing.
- **I13**: RuntimeCache evicts oldest entries when memory limit is reached.

### Mutation Ordering
- **I14**: Mutations from the same origin tab are applied in order.
- **I15**: HLC timestamps are monotonic across all tabs.
- **I16**: CRDT merge is deterministic — all tabs eventually converge.

### Lifecycle
- **I17**: Exactly one leader exists at any time. (Web Locks guarantee mutual exclusion.)
- **I18**: On leader promotion, RuntimeGeneration is incremented. StorageGeneration is NOT.
- **I19**: On leader promotion, no OPFS writes happen until the new leader has rehydrated.
- **I20**: Followers only attach to leaders with protocol_version they support.

---

## 14. Performance Targets

| Scenario | Current | Target | How |
|----------|---------|--------|-----|
| Cold start (leader) | ~6s | ~1-2s | Manifest+Index (Phase 1) |
| Cold start (follower) | ~6s | ~50-100ms | FollowerProxy (Phase 3) |
| Follower write → UI | ~300ms | <5ms | Optimistic cache (Phase 3) |
| Cross-tab sync | ~300ms (BC→reread) | ~10ms (BC→cache) | Mutation streaming (Phase 4) |
| Query with 1000 docs | ~500ms (OPFS scan) | <1ms (RuntimeCache) | Phase 2 + Phase 5 |
| Compaction | ~500ms | ~300ms | Leader only (Phase 3) |
| Leader failover | ~6s (full re-init) | ~2s (stateful promotion) | Phase 7 |
| Memory per follower | ~50MB+ | <10MB | LRU eviction (Phase 3) |

---

## 15. Risk Analysis

### R1 — BC message loss
**Risk**: BC is best-effort. Messages can be lost.
**Mitigation**: HEARTBEAT includes cursor. Follower detects gap → REQUEST_SYNC → MUTATION_BATCH

### R2 — BC message size limit (~1MB)
**Risk**: SNAPSHOT or large document exceeds limit.
**Mitigation**: SNAPSHOT_METADATA (doc IDs only, no content). Large documents chunked via ChunkedMutation. Documents > 100KB fetched lazily.

### R3 — Leader compaction blocks UI
**Risk**: Compaction is I/O heavy. Mutations queue up.
**Mitigation**: Scheduler prioritizes mutation broadcast (P0) over compaction (P5). Compaction yields to mutation queue.

### R4 — Follower optimistic write conflicts
**Risk**: Two followers write the same field. CRDT picks a winner. Losing follower's optimistic update was wrong.
**Mitigation**: Leader's merge is authoritative. Follower reconciles on receiving committed MUTATION. UI may flash briefly — acceptable for CRDT.

### R5 — Leader tab closes during follower write
**Risk**: Follower has optimistic write in-flight. Leader dies before persisting.
**Mitigation**: Mutation is on BC only. New leader has higher RuntimeGeneration → follower discards old-gen mutation. User re-applies from textarea (draft recovery).

### R6 — Web Locks starvation
**Risk**: Frozen tab holds the lock, doesn't heartbeat.
**Mitigation**: HEARTBEAT timeout (2s). If missing, followers assume leader death and promote. `beforeunload` releases lock.

### R7 — Index corruption
**Risk**: Manifest + index file corrupted.
**Mitigation**: Checksum on every read. If fails → rebuild from directory scan (safe, just slow).

### R8 — BC protocol version mismatch
**Risk**: New runtime code broadcasts format old followers can't parse.
**Mitigation**: BusEnvelope.version. On attach, version negotiation. If incompatible, follower re-inits as independent engine.

---

## 16. Implementation Order Rationale

### Dependency chain

```
1 (Manifest+Index) → 2 (Runtime) → 3 (Leader/Follower) → 4 (Bus) → 5 (Query)
                                    │                      │
                                    └── 6 (WAL) ←─────────┘
                                    │
                                    └── 7 (Failover)
```

### Phase sequence

| Phase | What | Depends on | Impact |
|-------|------|------------|--------|
| **1** | Manifest + ContentIndex | None | Eliminates OPFS scans immediately |
| **2** | Runtime-first architecture | Phase 1 | Runtime becomes source of truth |
| **3** | Leader/Follower split | Phase 2 | Followers detach from storage |
| **4** | Runtime Bus (mutation streaming) | Phase 3 | Followers stop re-reading storage |
| **5** | Reactive Query Engine | Phase 4 | React stops calling WASM for data |
| **6** | WAL + specialized indexes | Phase 1 | O(1) oplog/KV lookups |
| **7** | Stateful failover | Phase 2, 3 | Sub-2s leader promotion |

### Why this order

1. **Phase 1 first** — Storage is the deepest layer. Without fixing the index, every benchmark is distorted by scanning. Additive and safe.

2. **Phase 2 second** — With O(1) reads from Phase 1, refactoring to runtime-centric is safe. Storage latency is predictable.

3. **Phase 3 third** — Depends on Runtime existing. The follower proxy is just "Runtime minus persistence/network."

4. **Phase 4 fourth** — Depends on FollowerProxy existing. Can't stream mutations to followers that don't exist.

5. **Phase 5 fifth** — JS-layer optimization. Independent of storage concerns.

6. **Phase 6 sixth** — Refinement of Phase 1's index. Adds oplog and KV coverage.

7. **Phase 7 seventh** — Capstone. Depends on Phase 1 (manifest), Phase 2 (runtime), Phase 3 (follower), Phase 4 (bus).

### Total estimated effort: 30-39 days

| Phase | Days |
|-------|------|
| Phase 1 — Manifest + PageIndex | 4-5 |
| Phase 2 — Runtime-first | 5-7 |
| Phase 3 — Leader/Follower split | 6-8 |
| Phase 4 — Runtime Bus | 4-5 |
| Phase 5 — Query Engine | 4-5 |
| Phase 6 — WAL + Indexes | 3-4 |
| Phase 7 — Failover | 4-5 |
| **Total** | **30-39** |

---

## Complete file change list

| File | Phase | Change |
|------|-------|--------|
| `page_store.rs` | 1, 6 | ContentIndex, merged manifest, list_page_ids rewrite, WAL |
| `storage.rs` | 1, 6 | All operations use ContentIndex |
| `runtime.rs` | 2, 3 | NEW — Runtime struct, subsystems |
| `scheduler.rs` | 2 | NEW — RuntimeScheduler with priorities |
| `metrics.rs` | 2 | NEW — RuntimeMetrics |
| `memory.rs` | 2, 3 | NEW — MemoryManager, LruCache |
| `capability.rs` | 3 | NEW — CapabilityManager |
| `follower.rs` | 3, 4 | NEW — FollowerProxy |
| `ipc.rs` | 3, 4 | BusMessage enum, BusEnvelope, protocol versioning |
| `client.rs` | 2, 3 | Refactor to build Runtime; split leader/follower init |
| `store.ts` | 5 | NEW — RuntimeStore in JS SDK |
| `useQuery.ts` | 5 | Rewrite to subscribe to RuntimeStore |
| `useVaultSyncOne.ts` | 5 | Rewrite to subscribe to RuntimeStore |

---

## Decision record

| Question | Decision | Rationale |
|----------|----------|-----------|
| **Q1** — Transition strategy | **Option A** (additive Phase 1 first) | Bisectability. Isolate storage change from runtime change. |
| **Q2** — Follower writes | **Option D** (optimistic runtime proxy) | Instant UI + single persistence owner. No storage races. |
| **Q3** — Generic index | **Option C** (enum-based IndexKey) | Type-safe, single implementation, no string parsing. |
| **Q4** — BC payload | **Option D** (encrypted mutation operations) | Preserves E2EE. Enables operation replay. Not document snapshots. |
| **Q5** — Manifest vs Index | **Merged** into single StoreManifest | Single atomic metadata source. Simpler recovery. One checksum. |
| **Q6** — Follower fallback | **Option D** (stateful promotion) | Promote warm follower instead of reinitializing. O(1) manifest read. |
| — RuntimeGeneration | **Separate from StorageGeneration** | Compaction doesn't invalidate follower cache. |
| — RuntimeSnapshot | **Metadata + lazy documents** | No 100MB BC message. Works at 100,000 docs. |
| — RuntimeCache eviction | **LRU eviction** | Prevents unbounded memory growth. |
| — Bus flow control | **Chunking + backpressure** | Handles 50MB pastes without flooding BC. |
| — ACK levels | **Persisted/Uploaded/Replicated** | Enables UI indicators. Debugging. |
| — Protocol version | **BusEnvelope.version: u16** | Forward compatibility. Safe upgrades. |

---

*End of plan. Architecture is 9.6/10 ready. Begin Phase 1 implementation.*
