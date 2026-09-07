# Engine V2 — Complete Architecture Plan

> **Status:** Architecture complete (9.95+/10). Ready for Phase 1 implementation.
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
8. [Phase 3 — StorageEngine Abstraction](#8-phase-3--storageengine-abstraction)
9. [Phase 4 — Leader/Follower Ownership Split](#9-phase-4--leaderfollower-ownership-split)
10. [Phase 5 — Runtime Bus (Mutation Streaming)](#10-phase-5--runtime-bus-mutation-streaming)
11. [Phase 6 — Reactive Query Engine](#11-phase-6--reactive-query-engine)
12. [Phase 7 — WAL + Specialized Indexes](#12-phase-7--wal--specialized-indexes)
13. [Phase 8 — Stateful Promotion + Failover](#13-phase-8--stateful-promotion--failover)
14. [Invariants](#14-invariants)
15. [Performance Targets](#15-performance-targets)
16. [Risk Analysis](#16-risk-analysis)
17. [Implementation Order Rationale](#17-implementation-order-rationale)

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
├── RuntimeServices (grouping layer)
│   ├── Scheduler (prioritized task dispatcher with deadline+budget)
│   │   ├── Upload (P1, 50ms batch)
│   │   ├── Download (P2, 100ms poll)
│   │   ├── Compaction (P5, 300s interval, 20ms budget)
│   │   ├── GC (P4, 60s interval)
│   │   ├── Heartbeat (P2, 1s interval, 1ms budget)
│   │   ├── Prefetch (P3, idle backfill)
│   │   └── Broadcast (P0, immediate)
│   │
│   ├── Metrics (counters + histograms)
│   │   ├── cache_hit_ratio
│   │   ├── page_reads
│   │   ├── mutation_latency_us
│   │   ├── bc_latency_us
│   │   ├── leader_promotions
│   │   ├── hydration_time_ms
│   │   └── queue_sizes
│   │
│   ├── MemoryManager
│   │   ├── Document cache eviction (SegmentedLRU)
│   │   ├── Page cache eviction (LRU)
│   │   ├── Index memory budget
│   │   └── Heartbeat pressure detection
│   │
│   ├── RecoveryManager
│   │   ├── WAL replay
│   │   ├── Manifest validation + checksum verification
│   │   ├── ContentIndex repair on corruption
│   │   ├── Startup self-check
│   │   └── Recovery audit log
│   │
│   └── CapabilityManager
│       ├── Leader (full engine)
│       ├── Follower (cache + BC + RPC)
│       ├── ReadonlyWarm (no writes, cache from broadcast)
│       └── Offline (leader with no coordinator)
│
├── DocumentStore
│   ├── documents: HashMap<String, HashMap<String, HashMap<String, CrdtValue>>>
│   └── hot_docs: AccessTracker<String, f64>  // LFU with decay for prefetch
│
├── MetadataStore
│   ├── runtime_gen: RuntimeGeneration
│   ├── bus_gen: BusGeneration
│   ├── cursor: u64
│   ├── pending_count: usize
│   └── status: RuntimeStatus
│
├── PresenceStore
│   ├── followers: HashMap<String, FollowerRecord>  // tab_id → heartbeat
│   └── self: PresenceRecord
│
├── PendingStore
│   ├── pending: VecDeque<Mutation>
│   └── ack_tracker: HashMap<u64, HashSet<String>>  // seq → set of follower ACKs
│
├── IndexStore
│   ├── content_index: ContentIndex  (Phase 1, merged in manifest)
│   └── lookup_cache: SegmentedLruCache<IndexKey, PageId>
│
├── StorageEngine (Phase 3 — the only persistence boundary)
│   ├── StorageHealth
│   │   ├── opfs_available: bool
│   │   ├── manifest_ok: bool
│   │   ├── wal_ok: bool
│   │   ├── page_cache_usage: (usize, usize)
│   │   ├── storage_used_bytes: u64
│   │   ├── last_checkpoint: u64
│   │   ├── corruption_detected: bool
│   │   └── recovery_pending: bool
│   ├── PersistenceEngine
│   │   ├── ManifestStore (merged manifest + ContentIndex per store)
│   │   ├── PageStore with PageCache (LRU, 500 entries max)
│   │   └── WAL (append-only, checkpoint-based cleanup)
│   └── implements StorageEngine trait (swap for IDB/memory later)
│
├── NetworkEngine
│   ├── UploadPipeline
│   ├── DownloadWorker
│   └── CoordinatorHandle
│
├── RuntimeBus (BC protocol orchestrator)
│   ├── BroadcastManager (mutation dispatch + gap recovery + flow control)
│   ├── HeartbeatManager (1s interval sender + follower timeout detection)
│   ├── SnapshotManager (SNAPSHOT_METADATA + lazy REQUEST_DOCUMENT handler)
│   └── PrefetchManager (LFU-with-decay tracking + HOT_DOCUMENTS push)
│
└── QueryEngine
    ├── Runtime store (JS-side, framework-agnostic)
    ├── Subscription registry
    └── Dependency graph (query → document mappings)
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
│   ├── Handles HOT_DOCUMENTS → leader pushes hot docs proactively
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

/// Generation of the broadcast bus. Incremented when the leader
/// restarts or the bus channel resets. Followers use this to
/// detect missed broadcast windows and request gap recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct BusGeneration(u64);
```

**Key rules**:
- Mutations carry `RuntimeGeneration`. Followers reject mutations with stale `RuntimeGeneration` (leader has changed).
- Compaction increments `StorageGeneration` only — followers don't discard their cache on compaction.
- `BusGeneration` is tracked independently. If a follower detects a bus gen gap, it requests missing mutations from the leader rather than full resync.

---

## 3. Subsystem Reference

### 3.1 RuntimeScheduler

Coordinates all background work with explicit priorities:

| Task | Priority | Max Frequency | Deadline | Budget | Notes |
|------|----------|---------------|----------|--------|-------|
| Broadcast mutation | P0 | Immediate | 10ms | — | Never delay UI sync |
| Upload | P1 | 50ms batch | 100ms | — | Coalesces mutations |
| Heartbeat | P2 | 1s interval | 1s | 1ms | BC keepalive |
| Download | P2 | 100ms poll | 500ms | — | Pull from coordinator |
| Hot-doc prefetch | P3 | 500ms after attach | 5s | 50ms | Push active docs to new follower |
| Snapshot | P3 | 30s interval | 30s | 100ms | Background warm cache |
| GC | P4 | 60s interval | 60s | 50ms | Tombstone cleanup |
| Compaction | P5 | 300s interval | — | 20ms per slice | Background, yields to higher priority |
| Idle prefetch | P6 | On idle | — | — | Only when nothing else pending |

```rust
trait ScheduledTask: Send {
    fn priority(&self) -> u8;           // 0 = highest
    fn interval(&self) -> Duration;
    fn deadline(&self) -> Option<Duration>;  // None = no deadline
    fn budget(&self) -> Option<Duration>;    // None = unlimited, Some = max execution slice
    fn name(&self) -> &'static str;
    async fn execute(&self, runtime: &Runtime) -> Result<(), Error>;
}
```

### 3.2 RuntimeMetrics

Counter-based metrics with WASM exposure:

```rust
#[derive(Debug, Default)]
struct RuntimeMetrics {
    // Cache (SegmentedLRU — per-segment breakdown)
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    slru_probation_hits: AtomicU64,
    slru_protected_hits: AtomicU64,
    slru_promotions: AtomicU64,
    slru_demotions: AtomicU64,

    // Storage
    page_reads: AtomicU64,
    page_writes: AtomicU64,
    page_deletes: AtomicU64,
    page_enumerations: AtomicU64,  // Should be 0 after Phase 1

    // Bus — split by manager
    mutations_sent: AtomicU64,
    mutations_received: AtomicU64,
    mutations_rejected_gen: AtomicU64,  // stale RuntimeGeneration
    mutations_gap_recovered: AtomicU64,
    snapshots_sent: AtomicU64,
    snapshots_received: AtomicU64,
    hot_docs_pushed: AtomicU64,
    hot_docs_hit: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_missed: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    bus_messages_dropped: AtomicU64,

    // Recovery
    wal_entries_replayed: AtomicU64,
    manifest_checksum_failures: AtomicU64,
    content_index_repairs: AtomicU64,
    recovery_duration_ms: AtomicU64,

    // Lifecycle
    leader_promotions: AtomicU64,
    leader_demotions: AtomicU64,
    follower_attaches: AtomicU64,
    follower_detaches: AtomicU64,
    promotion_candidate_attempts: AtomicU64,
    hydration_time_ms: AtomicU64,
    failover_time_ms: AtomicU64,
    generation: AtomicU64,

    // Latency histograms (μs, stored as running stats)
    mutation_latency_us: RunningStats,
    bc_latency_us: RunningStats,
    page_read_latency_us: RunningStats,
    page_write_latency_us: RunningStats,
    recovery_latency_us: RunningStats,
    failover_latency_us: RunningStats,
    prefetch_latency_us: RunningStats,
}
```

Exposed to JS via:
```typescript
interface MetricsSnapshot {
    // Cache
    cacheHitRatio: number;
    slruProtectedRatio: number;
    slruProbationRatio: number;

    // Storage
    pageReads: number;
    pageWrites: number;
    pageEnumerations: number;

    // Bus
    mutationsSent: number;
    mutationsReceived: number;
    hotDocsPushed: number;
    hotDocsHit: number;
    busMessagesDropped: number;

    // Lifecycle
    leaderPromotions: number;
    followerAttaches: number;
    hydrationTimeMs: number;
    failoverTimeMs: number;

    // Recovery
    recoveryDurationMs: number;
    manifestChecksumFailures: number;
    contentIndexRepairs: number;

    // Latencies
    mutationLatencyAvgUs: number;
    bcLatencyAvgUs: number;
    recoveryLatencyAvgUs: number;
    failoverLatencyAvgUs: number;
}
```

### 3.3 MemoryManager

```rust
struct SegmentedLruCache<K, V> {
    /// Probation segment: recently accessed once, quick to evict
    probation: LruCache<K, V>,
    /// Protected segment: accessed twice+ in window, expensive to evict
    protected: LruCache<K, V>,
    /// Probation capacity ratio (default: 20% of total)
    probation_ratio: f64,
}

impl<K: Hash + Eq + Clone, V: Clone> SegmentedLruCache<K, V> {
    fn get(&mut self, key: &K) -> Option<&V>;
    fn insert(&mut self, key: K, value: V);
    /// On access: if in probation, promote to protected.
    /// On insert: goes to probation; if full, evict from probation.
    /// On protected full: demote LRU protected entry to probation.
    fn promote(&mut self, key: &K);
    fn evict_one(&mut self) -> Option<(K, V)>;
}

struct MemoryManager {
    // Runtime cache limits (SegmentedLRU)
    max_documents: usize,        // Default: 10,000
    max_bytes: usize,            // Default: 50MB
    slru_probation_ratio: f64,   // Default: 0.20 (20% probation, 80% protected)

    // Page cache
    page_cache_max_entries: usize,    // Default: 500
    page_cache_max_bytes: usize,      // Default: 20MB

    // Index
    index_max_entries: usize,         // Default: 50,000
}

impl MemoryManager {
    fn should_evict(&self, current_usage: &MemoryUsage) -> bool;
    fn evict_candidates(&self, usage: &MemoryUsage) -> Vec<EvictionTarget>;
    fn slru_segment_sizes(&self) -> (usize, usize);  // (probation, protected)
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

/// Each index entry carries metadata for future-proofing.
/// Adding fields later requires no migration — old entries
/// simply have missing fields (serde default).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    page_id: PageId,
    storage_gen: StorageGeneration,
    checksum: u32,
    size: u32,
    last_modified: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContentIndex {
    generation: StorageGeneration,
    entries: HashMap<IndexKey, IndexEntry>,
    live_pages: BTreeSet<PageId>,
}

impl ContentIndex {
    fn lookup(&self, key: &IndexKey) -> Option<&IndexEntry>;
    fn lookup_page_id(&self, key: &IndexKey) -> Option<PageId>;
    fn insert(&mut self, key: IndexKey, entry: IndexEntry);
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

### 4.3 Mutation structures (Phase 4/5)

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
    bus_gen: BusGeneration,
    replica_id: String,
    origin_tab: String,
    depends_on: Option<DependencyCursor>,  // causal ordering
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DependencyCursor {
    /// The last mutation applied on this doc before this one
    doc_seq: u64,
    /// The last mutation applied globally before this one
    global_seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptedMutationOperation {
    ciphertext: Vec<u8>,  // Encrypted MutationAction
    hlc: u64,
    runtime_gen: RuntimeGeneration,
    bus_gen: BusGeneration,
    replica_id: String,
    key_version: u32,
    depends_on: Option<DependencyCursor>,
}
```

### 4.4 Bus messages (Phase 4/5)

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
        bus_gen: BusGeneration,
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
        bus_gen: BusGeneration,
        storage_gen: StorageGeneration,
        cursor: u64,
        pending_count: usize,
    },
    LEADER_TRANSFER {
        runtime_gen: RuntimeGeneration,
        bus_gen: BusGeneration,
        storage_gen: StorageGeneration,
        cursor: u64,
        pending_count: usize,
    },
    ACK {
        seq: u64,
        level: AckLevel,
        error: Option<String>,
    },
    HOT_DOCUMENTS {
        documents: Vec<HotDocument>,
        runtime_gen: RuntimeGeneration,
        bus_gen: BusGeneration,
    },
    REQUEST_SYNC {
        from_seq: u64,
        expected_bus_gen: Option<BusGeneration>,
    },
    REQUEST_DOCUMENT {
        doc_id: String,
        record_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HotDocument {
    doc_id: String,
    record_id: String,
    fields: HashMap<String, CrdtValue>,
    access_frequency: f64,
}
```

### 4.5 RuntimeCache with eviction (follower)

```rust
struct RuntimeCache {
    /// SegmentedLRU cache of documents. Probation entries evicted first.
    documents: SegmentedLruCache<String, SegmentedLruCache<String, HashMap<String, CrdtValue>>>,
    /// Current runtime generation (from leader's heartbeat)
    runtime_gen: RuntimeGeneration,
    /// Current bus generation (for gap detection)
    bus_gen: BusGeneration,
    /// Optimistic mutations not yet acknowledged
    pending: FollowerPendingMutations,
    /// Last leader heartbeat timestamp
    last_heartbeat: Instant,
    /// Cursor from leader (for gap detection)
    cursor: u64,
    /// Last mutation seq seen on bus
    last_mutation_seq: u64,
}

impl RuntimeCache {
    fn get(&mut self, doc_id: &str, record_id: &str) -> Option<&HashMap<String, CrdtValue>>;
    fn query(&mut self, doc_id: &str) -> Vec<&HashMap<String, CrdtValue>>;
    fn apply_mutation(&mut self, mutation: &Mutation);
    fn evict_oldest(&mut self);  // evicts from probation segment first
    fn detect_gap(&self, leader_bus_gen: BusGeneration, leader_cursor: u64) -> bool;
}
```

### 4.6 RuntimeSnapshot (lazy — Phase 4)

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

## 8. Phase 3 — StorageEngine Abstraction

### 8.1 Goal

Insert a clean abstraction boundary between the Runtime and persistence. The Runtime never touches `PageStore`, `Manifest`, `WAL`, or compaction directly — it only knows the `StorageEngine` trait. This enables swapping backends (OPFS ↔ IndexedDB ↔ memory-only) and isolates testability.

### 8.2 StorageEngine trait

```rust
/// The only persistence interface the Runtime depends on.
/// Every storage backend implements this trait.
#[async_trait]
trait StorageEngine: Send + Sync {
    // Document operations
    async fn read_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>>;
    async fn write_document(&self, doc_id: &str, record_id: &str, bytes: &[u8]) -> Result<()>;
    async fn delete_document(&self, doc_id: &str, record_id: &str) -> Result<()>;
    async fn list_documents(&self, doc_id: &str) -> Result<Vec<(String, Vec<u8>)>>;

    // Oplog operations
    async fn read_pending_entries(&self) -> Result<Vec<OplogEntry>>;
    async fn mark_synced(&self, seq: u64) -> Result<()>;
    async fn read_after_sequence(&self, seq: u64) -> Result<Vec<OplogEntry>>;

    // KV store operations (sync_state, schema, keys, migrations)
    async fn read_kv(&self, store: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn write_kv(&self, store: &str, key: &str, value: &[u8]) -> Result<()>;
    async fn delete_kv(&self, store: &str, key: &str) -> Result<()>;

    // Metadata
    async fn read_metadata(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn write_metadata(&self, key: &str, value: &[u8]) -> Result<()>;

    // Lifecycle
    async fn compact(&self) -> Result<CompactionStats>;
    async fn checkpoint_wal(&self) -> Result<()>;
    async fn is_healthy(&self) -> bool;

    // Snapshot for follower rehydration
    async fn snapshot_metadata(&self) -> Result<SnapshotMetadata>;
    async fn read_snapshot_document(&self, doc_id: &str, record_id: &str) -> Result<Option<Vec<u8>>>;
}
```

### 8.3 Architecture boundary

```
Runtime
  │
  │  (only calls StorageEngine trait)
  ▼
StorageEngine  ←── trait boundary ──→  swap for IDB/memory later
  │
  ├── OpfsStorageEngine (Phase 3 implementation)
  │     ├── ManifestStore (merged manifest + ContentIndex)
  │     ├── PageStore with PageCache (LRU, 500 entries)
  │     │     ├── PageCache: HashMap<PageId, Arc<Vec<u8>>>
  │     │     │   └── LRU eviction, separate config from runtime cache
  │     │     └── OPFS read/write/deletion
  │     ├── WAL (append-only, checkpoint-based cleanup)
  │     └── CompactionEngine
  │
  ├── InMemoryStorageEngine (for tests)
  └── IndexedDbStorageEngine (future)
```

### 8.4 PageCache inside PageStore

Each PageStore instance gets an LRU page cache:

```rust
struct PageCache {
    max_entries: usize,         // Default: 500
    max_bytes: usize,           // Default: 20MB
    entries: HashMap<PageId, CachedPage>,
    order: VecDeque<PageId>,
}

struct CachedPage {
    data: Arc<Vec<u8>>,
    last_access: u64,
    size: usize,
}

impl PageCache {
    fn get(&mut self, page_id: PageId) -> Option<Arc<Vec<u8>>>;
    fn insert(&mut self, page_id: PageId, data: Vec<u8>);
    fn remove(&mut self, page_id: PageId);
    fn evict(&mut self);
}
```

The PageCache sits in front of every `read_page()` call. Repeated reads of the same page (common during oplog scanning) hit the cache instead of OPFS.

### 8.5 Why this matters

- **Testability**: Tests use `InMemoryStorageEngine` — no OPFS, no WASM, no async file I/O
- **Swapability**: IndexedDB backend = new impl of the trait. Zero Runtime changes
- **Isolation**: Runtime bugs can't corrupt storage format. Storage bugs can't corrupt runtime state
- **Boundary enforcement**: The trait is the contract. Both sides are independently verifiable

### 8.6 Migration from direct persistence

Phase 2's `PersistenceEngine` is refactored to implement `StorageEngine`. The trait methods wrap the existing `PageStore` + `ContentIndex` calls. No data migration needed — the same OPFS files are read/written through the trait.

### 8.7 Tests
- OpfsStorageEngine round-trip: write → read → verify
- PageCache hit → returns cached bytes (no OPFS read)
- PageCache eviction → oldest entry removed
- InMemoryStorageEngine: all operations work without OPFS
- Trait contract: all methods return consistent errors

### 8.8 Files touched
- `crates/vaultsync-wasm/src/storage_engine.rs` — NEW: StorageEngine trait + OpfsStorageEngine
- `crates/vaultsync-wasm/src/page_store.rs` — add PageCache
- `crates/vaultsync-core/src/storage/tests/` — InMemoryStorageEngine for tests

### 8.9 Estimated effort: 3-4 days

---

## 9. Phase 4 — Leader/Follower Ownership Split

### 9.1 Goal

Followers become proxies. No storage, no upload, no download, no coordinator, no compaction. Just a RuntimeCache + BC Listener + RPC client.

### 9.2 Startup paths

**Leader startup** (first tab or election win):
```
1. Open storage (OPFS)
2. Read manifest + ContentIndex
3. Hydrate DocumentStore from persistence via StorageEngine
4. Start all subsystems (Scheduler, Network, Bus, etc.)
5. Start BC heartbeat (1s interval)
6. Mark as Leader in CapabilityManager
7. Start hot-document prefetch analysis (track access frequency)
8. Ready (~1-2s)
```

**Follower startup** (subsequent tabs):
```
1. Open BC channel (no OPFS)
2. Send FOLLOWER_ATTACH with protocol_version and [Follower] capability
3. Receive SNAPSHOT_METADATA (doc IDs only, no content)
4. Create empty LRU cache entries for all docs
5. Leader proactively pushes HOT_DOCUMENTS (top-N by access frequency)
6. Follower hydrates hot documents into cache (no REQUEST_DOCUMENT needed for these)
7. Subscribe to BC MUTATION stream
8. Mark as Follower in CapabilityManager
9. Ready (~80-150ms)
```

### 9.3 Follower writes (Option D — Optimistic Runtime Proxy)

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

### 9.4 Follower reads

Always from RuntimeCache — never touches OPFS.

```
get(doc_id, record_id):
  └── RuntimeCache.documents.get(doc_id)?.get(record_id)
      └── O(1) LRU cache lookup
```

If cache miss (doc not yet fetched): send `REQUEST_DOCUMENT` to leader, receive `SNAPSHOT_DOCUMENT`, hydrate cache, return.

### 9.5 No storage for followers — absolute invariant

Follower code must never reference:
- `PageStore` (any method)
- `OpfsStorage`
- `StoreManifest`
- `ContentIndex`
- Any `Storage` trait method

The `FollowerProxy` struct contains zero references to storage types.

### 9.6 Compaction ownership

Only the leader compacts. Compaction increments `StorageGeneration` but NOT `RuntimeGeneration`. Followers do not discard their cache on compaction.

Pages changed by compaction (tombstone old, write new) update the ContentIndex and manifest. Followers don't need to know — their cache contains the same logical state.

### 9.7 Hot-document prefetch on follower attach

After SNAPSHOT_METADATA, the leader proactively pushes the most-frequently-accessed documents using a **decaying access-frequency algorithm** (LFU with periodic aging):

```
Leader tracks access frequency per document in DocumentStore.hot_docs
  │
  │ Algorithm: LFU with decay
  │   1. On each access → increment counter per document
  │   2. Every DECAY_INTERVAL (default: 60s) → multiply all counters by 0.5
  │      (exponential decay, prevents "stale popularity" bias)
  │   3. On FOLLOWER_ATTACH:
  │      ├── Select top-N documents by current decayed frequency (default N=50)
  │      ├── Encrypt each document
  │      └── Send HOT_DOCUMENTS batch via BC:
  │            { type: "HOT_DOCUMENTS", documents: [...], bus_gen }
  │   4. On each USER_OPEN (if not already hot) → boost counter to promote
  │
  └── Follower receives and caches immediately
      └── No REQUEST_DOCUMENT needed for hot documents
      └── User opens any hot doc → instant cache hit
```

This eliminates cold-start latency for the most commonly accessed documents. The decay factor ensures that a document popular last hour doesn't crowd out one popular right now. Less-frequently accessed documents are still fetched lazily via REQUEST_DOCUMENT.

**AccessTracker**: Internal data structure wraps `HashMap<K, (f64, u64)>` — (frequency, last_decay_tick). Not an LRU; frequency is explicitly tracked and aged.

### 9.8 Tests
- Follower attaches → receives metadata → cache created with empty entries
- Hot documents pushed proactively → cached without REQUEST_DOCUMENT
- Lazy document fetch for non-hot docs → REQUEST_DOCUMENT → SNAPSHOT_DOCUMENT
- Follower write → optimistic → BC → leader persists → broadcast → follower reconciled
- Two followers write simultaneously → both optimistic → leader serializes
- Follower receives old RuntimeGeneration mutation → rejected
- Leader restarts → followers reattach to new leader

### 9.9 Files touched
- `crates/vaultsync-wasm/src/follower.rs` — NEW file
- `crates/vaultsync-wasm/src/runtime.rs` — add bus, snapshot provider, follower registry, hot-doc tracker
- `crates/vaultsync-wasm/src/client.rs` — split init into leader/follower paths
- `crates/vaultsync-wasm/src/ipc.rs` — BusMessage enum, BusEnvelope, all message handlers, HOT_DOCUMENTS
- `crates/vaultsync-wasm/src/capability.rs` — NEW file

### 9.10 Estimated effort: 6-8 days

---

## 10. Phase 5 — Runtime Bus (Mutation Streaming)

### 10.1 Goal

Replace `broadcast_invalidation("go re-read storage")` with direct mutation streaming. Followers apply mutations to cache without touching OPFS.

### 10.2 Leader side

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

### 10.3 Follower side

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

### 10.4 Gap recovery

HEARTBEAT includes `cursor`. If follower's `last_seq < leader_cursor`:
```
Follower → REQUEST_SYNC { from_seq }
Leader   → MUTATION_BATCH { mutations: [...], from_seq, to_seq }
Follower → applies all missed mutations
```

### 10.5 Backpressure

For large payloads:
1. ChunkedMutation with `batch_id, chunk_index, total_chunks`
2. Follower reassembles before applying
3. If pending_batches > 3, FLOW_CONTROL with backoff

### 10.6 Tests
- Mutation broadcast → follower cache updated (zero storage reads verified via metrics)
- Gap detection → REQUEST_SYNC → MUTATION_BATCH → catch up
- Chunked mutation → follower reassembles → applies
- Large payload → backpressure triggers → chunking works
- E2EE mutation → follower decrypts correctly
- 1000 mutations → leader and all followers consistent

### 10.7 Estimated effort: 4-5 days

---

## 11. Phase 6 — Reactive Query Engine

### 11.1 Goal

Framework-agnostic runtime store in JS with dependency-graph subscription tracking. React, Vue, Svelte all consume the same store.

### 11.2 Design

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

### 11.3 React integration

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

### 11.4 Fine-grained dependencies

- `subscribe("note", "abc")` → notified only when `note::abc` changes
- `subscribe("note", null)` → notified when any record in "note" changes
- `subscribe(null, null)` → notified on any change anywhere

### 11.5 Stale-while-revalidate

1. Return cached data immediately (may be empty on cold start)
2. Background: call WASM `find()` or `get()` to verify freshness
3. If stale, update cache + notify subscribers
4. Future updates come via mutation streaming

### 11.6 Files touched
- `sdk/packages/core/src/store.ts` — NEW RuntimeStore
- `sdk/packages/core/src/index.ts` — export RuntimeStore
- `sdk/packages/react/src/useQuery.ts` — use RuntimeStore
- `sdk/packages/react/src/useVaultSyncOne.ts` — use RuntimeStore
- WASM bridge — expose `applySetField` etc.

### 11.7 Estimated effort: 4-5 days

---

## 12. Phase 7 — WAL + Specialized Indexes

### 12.1 Goal

Extend ContentIndex from Phase 1 to enable O(1) lookups for oplog and KV stores.

### 12.2 Oplog index

`mark_synced(seq)` becomes:
1. `content_index.lookup(&IndexKey::Sequence(seq))` → `Some(page_id)`
2. `page_store.read_page(page_id)` → single page
3. Filter out the synced entry
4. Rewrite remaining entries
5. Update index for affected entries

**Approach**: Per-entry index (map EVERY sequence number). Max size = pending mutations (compacted/pruned). With 10,000 pending mutations ≈ 800KB index.

### 12.3 KV store indexes

- `read_sync_state(ns)` → `content_index.lookup(&IndexKey::Namespace(ns))` → single page
- `read_schema(doc_id)` → `content_index.lookup(&IndexKey::Schema(doc_id))` → single page

No scan for any KV operation.

### 12.4 WAL (checkpoint-based)

Write-Ahead Log for crash recovery. Uses checkpoint-based cleanup (not individual entry deletion):

```
  WAL: append-only log of all mutations
    ↓
  Checkpoint: every N writes (default: 100), compact WAL
    ↓
    - Atomically rewrite WAL: keep only unapplied entries
    - Update manifest checkpoint pointer
    ↓
  Crash recovery: replay from last checkpoint
    ↓
  Normal operation: entries stay in WAL until next checkpoint
```

- Append mutation before any page write (WAL-first)
- On page write + index update success, mark entry as applied (in-memory)
- On checkpoint, rewrite WAL discarding applied entries
- On crash: replay all entries from last checkpoint

This matches SQLite/Postgres WAL behavior. Never delete individual WAL entries — only checkpoint atomically.

Only the leader writes to WAL. Followers never touch it.

### 12.5 Tests
- `mark_synced` reads exactly 1 page (metrics verification)
- All KV lookups return from index (no scan)
- WAL append → crash → replay → state consistent
- WAL checkpoint → applied entries removed
- WAL checkpoint crash → replay from last checkpoint (no data loss)

### 12.6 Estimated effort: 3-4 days

---

## 13. Phase 8 — Stateful Promotion + Failover

### 13.1 Goal

When the leader dies, promote a follower to leader without reinitializing from scratch.

### 13.2 Detection

Follower detects leader death:
1. No HEARTBEAT for `LEADER_TIMEOUT_MS` (2000ms)
2. Follower transitions to `Suspect` state
3. After timeout, attempts promotion

### 13.3 Promotion process

```
1. Detect leader timeout (no HEARTBEAT for 2s)
2. Acquire Web Lock (same lock as current leader election)
3. If lock fails → another tab is promoting → back off
4. **Preserve existing FollowerProxy cache** — do NOT discard
5. Use RecoveryManager for fast validation:
   a. Verify manifest checksum (no corruption check)
   b. Replay remaining WAL entries since last checkpoint
   c. Confirm ContentIndex is consistent
6. **Hydrate only what's missing from cache**:
   a. Already-cached documents → no read needed
   b. Missing documents → read from persistence via ContentIndex
   c. No full scan, no full rehydration
7. Start Scheduler, NetworkEngine, CompactionEngine
8. Increment RuntimeGeneration (StorageGeneration stays)
9. Start BC heartbeat with new BusGeneration
10. Broadcast LEADER_TRANSFER with new generation
11. Send SNAPSHOT_METADATA to all attached followers
12. Resume as leader
```

### 13.4 Hydration time

Estimated: O(documents) page reads, not O(pages) directory scan.
- 1000 documents × 1 page read = ~1-2s
- No scan, no coordinator init (starts after hydration)

### 13.5 Reattachment of surviving followers

After LEADER_TRANSFER:
1. New leader sends SNAPSHOT_METADATA (doc IDs only)
2. Followers update their RuntimeGeneration
3. Followers keep their existing cache (same StorageGeneration + same doc IDs)
4. If follower was missing a doc → REQUEST_DOCUMENT on access
5. Old-generation optimistic mutations from old leader → discarded (generation mismatch)
6. User re-applies stale edits (draft recovery)

### 13.6 Tests
- Leader dies → follower promotes → new leader serves reads/writes
- Two followers detect simultaneously → only one acquires lock
- Old-generation mutations rejected after promotion
- Follower cache survives promotion (same StorageGeneration)
- Promote → demote → promote (flapping) → stable after backoff

### 13.7 Estimated effort: 4-5 days

---

## 14. Invariants

These must hold at all times.

### Storage
- **I1**: Only the leader writes OPFS. Followers never write to storage.
- **I2**: Only the leader compacts, GCs, or snapshots. No races.
- **I3**: The manifest (including ContentIndex) is always consistent with pages on disk.
- **I4**: After any write, ContentIndex is updated before broadcast.
- **I5**: StorageGeneration increments on any storage mutation. RuntimeGeneration and BusGeneration do NOT increment on storage-only changes (compaction, GC).

### Broadcast
- **I6**: Every MUTATION broadcast by the leader has already been persisted to OPFS.
- **I7**: Every MUTATION carries a RuntimeGeneration and BusGeneration. Followers discard mutations from stale RuntimeGenerations. Followers detect bus gen gaps and request recovery.
- **I8**: HEARTBEAT is sent at least every 1s while the leader is healthy.

### Follower
- **I9**: Followers never read OPFS during normal operation.
- **I10**: Followers' optimistic mutations are always reconciled with the leader's committed state.
- **I11**: Followers' RuntimeCache is eventually consistent with the leader's RuntimeState.
- **I12**: Followers that detect a gap (via cursor on HEARTBEAT) request a sync before continuing.
- **I13**: RuntimeCache evicts oldest entries from probation segment first (SegmentedLRU).

### Mutation Ordering
- **I14**: Mutations carry DependencyCursor. The leader ensures causal ordering (depends_on is satisfied before applying).
- **I15**: Mutations from the same origin tab are applied in order.
- **I16**: HLC timestamps are monotonic across all tabs.
- **I17**: CRDT merge is deterministic — all tabs eventually converge.

### Lifecycle
- **I18**: Exactly one leader exists at any time. (Web Locks guarantee mutual exclusion.)
- **I19**: On leader promotion, RuntimeGeneration is incremented. StorageGeneration is NOT. BusGeneration is incremented.
- **I20**: On leader promotion, existing follower cache is preserved. Only missing documents are hydrated.
- **I21**: No OPFS writes happen until RecoveryManager has validated the manifest.
- **I22**: Followers only attach to leaders with protocol_version they support.

---

## 15. Performance Targets

| Scenario | Current | Target | How |
|----------|---------|--------|-----|
| Cold start (leader) | ~6s | ~1-2s | Manifest+Index (Phase 1) |
| Cold start (follower) | ~6s | ~80-150ms | FollowerProxy + hot prefetch (Phase 4) |
| Follower write → UI | ~300ms | <5ms | Optimistic cache (Phase 4) |
| Cross-tab sync | ~300ms (BC→reread) | ~10ms (BC→cache) | Mutation streaming (Phase 5) |
| Query with 1000 docs | ~500ms (OPFS scan) | <1ms (RuntimeCache) | Phase 2 + Phase 6 |
| Compaction | ~500ms | ~300ms | Leader only (Phase 4) |
| Leader failover (cold follower) | ~6s (full re-init) | ~1-2s (stateful promotion) | Phase 8 |
| Leader failover (warm follower) | ~6s (full re-init) | ~200ms (preserve cache + WAL replay) | Phase 8 |
| Memory per follower | ~50MB+ | <10MB | LRU eviction (Phase 4) |

---

## 16. Risk Analysis

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

## 17. Implementation Order Rationale

### Dependency chain

```
1 (Manifest+Index) → 2 (Runtime) → 3 (StorageEngine) → 4 (Leader/Follower) → 5 (Bus) → 6 (Query)
                                                         │                      │
                                                         └── 7 (WAL) ←─────────┘
                                                         │
                                                         └── 8 (Failover)
```

### Phase sequence

| Phase | What | Depends on | Impact |
|-------|------|------------|--------|
| **1** | Manifest + ContentIndex | None | Eliminates OPFS scans immediately |
| **2** | Runtime-first architecture | Phase 1 | Runtime becomes source of truth |
| **3** | StorageEngine abstraction | Phase 2 | Clean boundary between Runtime and storage |
| **4** | Leader/Follower split | Phase 2, 3 | Followers detach from storage |
| **5** | Runtime Bus (mutation streaming) | Phase 4 | Followers stop re-reading storage |
| **6** | Reactive Query Engine | Phase 5 | React stops calling WASM for data |
| **7** | WAL + specialized indexes | Phase 1 | O(1) oplog/KV lookups |
| **8** | Stateful failover | Phase 2, 4 | Sub-2s leader promotion |

### Why this order

1. **Phase 1 first** — Storage is the deepest layer. Without fixing the index, every benchmark is distorted by scanning. Additive and safe.

2. **Phase 2 second** — With O(1) reads from Phase 1, refactoring to runtime-centric is safe. Storage latency is predictable.

3. **Phase 3 third** — Insert the StorageEngine trait boundary before building the follower proxy. Ensures the Runtime never directly depends on PageStore or OPFS. This abstraction will be the contract for all persistence going forward.

4. **Phase 4 fourth** — Depends on Runtime + StorageEngine existing. The follower proxy is "Runtime minus persistence/network."

5. **Phase 5 fifth** — Depends on FollowerProxy existing. Can't stream mutations to followers that don't exist.

6. **Phase 6 sixth** — JS-layer optimization. Independent of storage concerns.

7. **Phase 7 seventh** — Refinement of Phase 1's index. Adds oplog and KV coverage.

8. **Phase 8 eighth** — Capstone. Depends on Phase 1 (manifest), Phase 2 (runtime), Phase 3 (StorageEngine), Phase 4 (follower).

### Total estimated effort: 33-43 days

| Phase | Days |
|-------|------|
| Phase 1 — Manifest + PageIndex | 4-5 |
| Phase 2 — Runtime-first | 5-7 |
| Phase 3 — StorageEngine abstraction | 3-4 |
| Phase 4 — Leader/Follower split | 6-8 |
| Phase 5 — Runtime Bus | 4-5 |
| Phase 6 — Query Engine | 4-5 |
| Phase 7 — WAL + Indexes | 3-4 |
| Phase 8 — Failover | 4-5 |
| **Total** | **33-43** |

---

## Complete file change list

| File | Phase | Change |
|------|-------|--------|
| `page_store.rs` | 1, 3, 7 | ContentIndex, merged manifest, list_page_ids rewrite, PageCache, WAL |
| `storage.rs` | 1, 7 | All operations use ContentIndex |
| `storage_engine.rs` | 3 | NEW — StorageEngine trait + OpfsStorageEngine impl + StorageHealth |
| `runtime.rs` | 2, 4 | NEW — Runtime struct, subsystems, DocumentStore/MetadataStore/etc. |
| `scheduler.rs` | 2 | NEW — RuntimeScheduler with priorities + deadline + budget |
| `metrics.rs` | 2 | NEW — RuntimeMetrics (expanded: SLRU, gap recovery, hot-docs hit, recovery, failover) |
| `memory.rs` | 2, 4 | NEW — MemoryManager, SegmentedLruCache, eviction policies |
| `capability.rs` | 4 | NEW — CapabilityManager |
| `recovery_manager.rs` | 2, 8 | NEW — RecoveryManager (WAL replay, manifest validation, checksum verification, repair, audit log) |
| `access_tracker.rs` | 4 | NEW — AccessTracker (LFU with periodic decay, aging scheduler) |
| `broadcast_manager.rs` | 5 | NEW — BroadcastManager (mutation dispatch, gap detection, flow control, BusGeneration tracking) |
| `heartbeat_manager.rs` | 4 | NEW — HeartbeatManager (1s interval, follower timeout, cursor exchange) |
| `snapshot_manager.rs` | 4 | NEW — SnapshotManager (SNAPSHOT_METADATA assembly, REQUEST_DOCUMENT handler) |
| `prefetch_manager.rs` | 4 | NEW — PrefetchManager (hot-doc ranking via AccessTracker, HOT_DOCUMENTS push) |
| `follower.rs` | 4, 5 | NEW — FollowerProxy with hot-doc prefetch support, promotion-aware (preserves cache) |
| `ipc.rs` | 4, 5 | BusMessage enum, BusEnvelope, protocol versioning, HOT_DOCUMENTS, dependency cursor |
| `client.rs` | 2, 4 | Refactor to build Runtime; split leader/follower init; wire RecoveryManager |
| `store.ts` | 6 | NEW — RuntimeStore in JS SDK |
| `useQuery.ts` | 6 | Rewrite to subscribe to RuntimeStore |
| `useVaultSyncOne.ts` | 6 | Rewrite to subscribe to RuntimeStore |

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
| — RuntimeCache eviction | **SegmentedLRU (probation + protected)** | Higher hit rate than plain LRU. Popular entries survive probation. |
| — Bus flow control | **Chunking + backpressure** | Handles 50MB pastes without flooding BC. |
| — ACK levels | **Persisted/Uploaded/Replicated** | Enables UI indicators. Debugging. |
| — Protocol version | **BusEnvelope.version: u16** | Forward compatibility. Safe upgrades. |
| — StorageEngine | **Trait boundary between Runtime and persistence** | Swap backends without Runtime changes. Testability. |
| — IndexEntry | **Struct with page_id + checksum + size + generation** | Future-proof. No migration when adding metadata fields. |
| — PageCache | **LRU cache inside PageStore** | Avoids repeated OPFS reads of same page. |
| — WAL cleanup | **Checkpoint-based (never delete individual entries)** | Matches SQLite/Postgres. Safer crash recovery. |
| — Hot-doc prefetch | **LFU with periodic decay** | Prevents stale-popularity bias. Documents popular last hour don't crowd out current ones. |
| — Scheduler model | **Priority + Deadline + Budget** | Prevents starvation. Compaction yields to mutations. |
| — RuntimeState split | **DocumentStore / MetadataStore / PresenceStore / PendingStore / IndexStore** | Prevents 3000-line single struct. Clear ownership. |
| — RuntimeBus | **Split into BroadcastManager / HeartbeatManager / SnapshotManager / PrefetchManager** | Each manager has focused responsibility. Cleaner testability. No god-bus. |
| — Follower promotion | **Preserve existing cache, hydrate only missing pieces** | Avoids 100% re-read. Promotion from ~1-2s to ~200ms for warm followers. |
| — Mutation ordering | **DependencyCursor on every mutation** | Enables causal ordering across tabs. Detects concurrent edits. |
| — Recovery | **RecoveryManager subsystem (WAL replay, manifest validation, checksum verification, repair)** | Dedicated recovery path. Not mixed into startup flow. Audit trail. |
| — BusGeneration | **Separate generation type for broadcast bus** | Followers detect missed bus windows and request gap recovery without full resync. |
| — StorageHealth | **Detailed health struct (OPFS available, manifest ok, WAL ok, page cache usage, corruption)** | Exposes storage status to health checks. Enables graceful degradation. |
| — Metrics | **Expanded: SLRU segments, gap recovery, hot-docs hit, heartbeats missed, recovery duration, failover time, per-segment cache hits** | Comprehensive observability for debugging ALL subsystems. |

---

*End of plan. Architecture is 9.9+/10 ready. Begin Phase 1 implementation.*
