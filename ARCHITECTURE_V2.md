# VaultSync Architecture V2

> An adaptive local-first data platform.

---

## Mission

VaultSync is not a sync engine. It is not a CRDT library. It is not a Replicache alternative.

VaultSync is an **adaptive local-first data platform**.

Synchronization is one subsystem. The center of the system is **local data management** — managing a device's working set of data the way an operating system manages memory: adaptively, by usage, with hot/warm/cold/archived tiers, and with startup time and memory usage that are independent of the total dataset size.

---

## North Star

A VaultSync application should feel exactly like an offline desktop application.

- Network availability must never determine UI responsiveness.
- Synchronization is asynchronous — the local database is always the source of truth for the user.
- The coordinator is a replication service, not a query service.
- Every feature is measured against this principle.

---

## Replication Model

VaultSync uses **permission-driven replication**, not query-driven caching.

This is the single most important architectural decision in the platform.

### The Three Models

| Model | Behavior | Offline Reliability | Scalability |
|-------|----------|-------------------|-------------|
| Lazy Network Fetch | Query → fetch → cache | ❌ Fails if never opened | ✅ Minimal storage |
| Full DB Replication | Replicate entire database | ✅ Everything works | ❌ HR downloads finance data |
| **Permission Workspace** | Replicate user's authorized scope | ✅ Everything in scope works | ✅ By role, not by database |

VaultSync follows the **Permission Workspace** model.

### How It Works

**Step 1 — Registration (online):**
The coordinator computes the user's authorized scope from permissions and namespace membership:

```text
HR Namespace
  ✓ Employees (full access)
  ✓ Attendance (full access)
  ✓ Leave (full access)

Finance Namespace
  ✗ No access

Projects Namespace
  ✓ Project A (member)
  ✓ Project B (member)
```

**Step 2 — Initial replication:**
VaultSync downloads **everything inside that authorized scope** to local storage. Not because it has been queried. Because the user owns it. The local database is now complete for that workspace.

**Step 3 — Offline:**
Every query inside the replicated scope works entirely locally. No coordinator required. Data outside the user's scope was never local, and its unavailability offline is expected — the same data would be unavailable online too.

### Implications

- **Offline reliability** — users continue working within their authorized data even without a network. Prior access is irrelevant; authorization determines availability.
- **Low latency** — reads and writes are local-first. The write path never waits for the coordinator.
- **Predictable behavior** — no surprises where a screen works online but fails offline simply because it was never opened before.
- **Scalability by role** — HR does not replicate finance. Finance does not replicate inventory. Each device's storage scales with the user's workspace, not the database.

The coordinator's job is not to answer every query. Its job is to **replicate the right subset** of the database to each client based on authorization. Once replicated, VaultSync becomes the user's local database, and the coordinator keeps it synchronized in the background.

### What About Startup?

Startup loads only metadata:
```text
Meta
  → Namespace permissions
  → Sync state
  → Keys
  → Ready
```

Not documents. Not mutations. Not snapshots. Those remain on disk until queried.

---

## Guiding Invariants

Every architectural decision must satisfy these seven invariants. If a proposed feature violates one, it must be redesigned — not patched.

1. **Startup time is independent of database size.**
2. **Memory usage is proportional to the active working set, not the replicated dataset.**
3. **Every query is served locally if the required data has been replicated.**
4. **Replication is permission-aware and workload-aware.**
5. **Storage responsibilities are separated (metadata, documents, snapshots, mutations, blobs).**
6. **Background work (compaction, cleanup, prefetch, eviction) never blocks foreground operations.**
7. **No operation should scale linearly with total replicated data unless explicitly requested.**

Invariant 7 is the broadest. It applies not just to startup, but to compaction, queries, sync acknowledgements, eviction, schema changes, and checkpoints. Any algorithm that touches all data is a red flag.

---

## Architectural Guarantees

Measurable targets that every implementation must meet:

| Guarantee | Target | What Violates It |
|-----------|--------|------------------|
| Cold startup | <20ms regardless of database size | Any file read beyond `meta.json` |
| Foreground query blocking | <5ms max latency added | Background compaction holding a write lock |
| Crash recovery | Zero data loss on interrupted write | Missing verify-before-rename pattern |
| Memory growth | Proportional to working set only | LRU cache not bounded by memory budget |
| Consistency | Causal+ per namespace | Stale read after locally-confirmed write |
| Self-maintenance | No developer calls `compact()`/`vacuum()` | Exposed maintenance knobs required for health |
| Offline completeness | Every query within user's authorized scope served locally | Data missing that is in scope |

---

## Three-Layer Architecture

```
                 Applications
                       │
               React / Vue / Svelte / etc.
                       │
            ─────────────────────────────
            VaultSync Query Engine
            ─────────────────────────────
                       │
          ┌─────────────────────────┐
          │     Query Planner       │
          └─────────────────────────┘
                       │
            ─────────────────────────────
            Logical Database
            ─────────────────────────────
                       │
          ┌─────────────────────────┐
          │  get / scan / query     │
          │  transaction / watch    │
          └─────────────────────────┘
                       │
            ─────────────────────────────
            Storage Manager
            ─────────────────────────────
                       │
          ┌─────────────────────────┐
          │  Placement / Eviction   │
          │  Compaction / Lifecycle │
          │    Sync Coordinator     │
          └─────────────────────────┘
                       │
            ─────────────────────────────
            Storage Engine
            ─────────────────────────────
                       │
      OPFS │ IndexedDB │ SQLite │ Native
```

**Synchronization is not the center.** The center is local data management.

The **Query Engine** asks "give me document X" — it doesn't know where X lives, or what storage tier it's in.

The **Logical Database** exposes a database-oriented API (`get`, `scan`, `query`, `transaction`, `watch`) and internally resolves through the Storage Manager tiers. The Query Engine never knows about storage tiers — it only knows about the Logical Database.

The **Storage Manager** is the sole authority for all storage access. No subsystem — download worker, upload pipeline, reconciler, scheduler, memory manager — touches storage directly. All I/O goes through the Storage Manager, which enforces placement, eviction, compaction, and lifecycle policy.

The **Storage Engine** is a thin adapter that translates page-level operations to the backend (OPFS file, SQLite row, IndexedDB record). It has no policy logic.

**Workspace boundary:** The Storage Manager manages a single user's authorized scope. Everything in that scope exists on disk (at minimum at the Recoverable tier). Nothing outside the scope exists locally. The Logical Database and Memory Manager operate within this already-replicated workspace — they never decide what should or shouldn't be on disk. That decision belongs to the replication model.

---

## Generation Roadmap

### Generation 1 — Correctness (95% done)

**Goal:** Can it sync? Is it offline? Can it recover? Is it deterministic?

- OPFS race → fixed
- Replay storm → fixed
- Leader election → fixed
- Snapshot recovery → fixed
- Coordinator restart → fixed
- Offline → fixed
- Cross-tab → fixed

### Generation 2 — Architecture (next)

**Goal:** Replace monolithic storage with a page-oriented, tier-managed storage platform. Make the engine self-maintaining.

| Pillar | Description |
|--------|-------------|
| Storage Manager | Sole authority for all storage access. Enforces placement, lifecycle, and integrity policy |
| Storage Engine V2 | Block-device page abstraction with independent stores, no global index |
| Metadata Engine | Central catalog for permissions, schemas, indexes, namespace membership, and replication state |
| Resource Manager | Single source of truth for CPU, battery, memory, disk, network, and idle state |
| Memory Manager | Scoring-based memory budget across Resident / Recoverable / Archived tiers |
| Scheduler | Central traffic controller — all async work submits through its priority queue. Self-optimizing Maintenance Window |
| Query Planner | Cache-aware resolution within workspace: Resident → Recoverable → disk. Future: cost-based optimizer |
| Replication Planner | Decides what should be replicated or removed based on permissions and policy |
| Adaptive Replication | (Postponed) Timeliness hints for push priority within already-replicated scope |

### Generation 3 — Optimization

**Goal:** Performance without changing architecture.

| Feature | Description |
|---------|-------------|
| Intelligent Compaction | CPU/battery/disk/memory-aware compaction strategy selection |
| Predictive Prefetch | Predict likely-needed data and fetch before requested |
| Multi-Level Cache | Resident CRDT → Recoverable cache → Disk pages → Archived segments → Coordinator |
| Adaptive Compression | Rarely-changed data (invoices) → aggressive; frequently-changed (chat) → minimal |

### Generation 4 — Distributed Query Engine

**Goal:** Make VaultSync a database, not just a sync layer.

| Feature | Description |
|---------|-------------|
| Distributed Query Engine | Run queries without deserializing everything — indexes + query planner + snapshot = result |
| Incremental Materialization | Load only the fields needed: title, not full document; comments, not attachments |
| Workload Prediction | Observe usage patterns and adapt storage policy automatically |

---

## Generation 2 — Deep Dive

### Pillar 1: Storage Manager

#### Problem

Today, storage decisions are scattered: `upload.rs` decides when to mark entries synced, `download.rs` decides when to persist cursors, `client.rs` decides when to compact. There is no single authority for **where data lives** and **when it moves**.

#### Solution: Central Storage Manager

The Storage Manager is the single point of control for all storage policy. Externally it exposes a single unified interface; internally it is modular:

```
Storage Manager (facade)
    │
    ├── Placement Engine
    │     └── Which tier does this data belong in?
    │     └── Where should new segments be written?
    │     └── Should data be promoted or demoted?
    │
    ├── Integrity Engine
    │     └── Verify checksums on read
    │     └── Detect crashes via partially-written files
    │     └── Maintain minimal segment headers
    │
    ├── Lifecycle Engine
    │     └── Active → Sealed (segment reaches size limit)
    │     └── Sealed → Deleted (covered by snapshot compaction)
    │     └── Recoverable → Archived (not accessed in N days)
    │     └── Archived → Deleted (explicit GC policy)
    │
    ├── Segment Manager
    │     └── Active journal sealing
    │     └── Small segment merging
    │     └── Compaction coordination
    │
    └── Page Allocator
          └── allocate_page / read_page / write_page / free_page
          └── Page-level operations, no file awareness
```

**No subsystem touches storage directly.** The download worker, upload pipeline, reconciler, scheduler, and memory manager all go through the Storage Manager. The Storage Manager is the sole authority for:
- **Access control** — every read and write is gated through the manager
- **Tier placement** — the manager decides which tier data belongs in based on access patterns and policy
- **Lifecycle enforcement** — data transitions (active → sealed → deleted) are managed centrally
- **Integrity verification** — checksums, crash recovery, and consistency are enforced in one place

**Interface sketch (intent-based, not policy-based):**
```rust
trait StorageManager: Send + Sync {
    // Document access (transparent tier resolution)
    async fn get_document(&self, ns: &str, doc_id: &str) -> Result<Option<Vec<u8>>>;
    async fn put_document(&self, ns: &str, doc_id: &str, data: &[u8]) -> Result<()>;

    // Mutation log (managed lifecycle)
    async fn append_mutation(&self, ns: &str, entry: &OplogEntry) -> Result<()>;
    async fn ack_mutation(&self, ns: &str, id: &str, seq: u64) -> Result<()>;
    async fn read_pending(&self, ns: &str, limit: usize) -> Result<Vec<OplogEntry>>;

    // Compaction (self-managed, called by Scheduler)
    async fn seal_active_segments(&self) -> Result<CompactionStats>;
    async fn delete_compactable_segments(&self, before_seq: u64) -> Result<usize>;

    // Intent-based tier hints (Storage Manager computes tiers internally)
    async fn touch(&self, ns: &str, doc_id: &str) -> Result<()>;     // accessed, update recency
    async fn pin(&self, ns: &str, doc_id: &str) -> Result<()>;       // keep resident
    async fn release(&self, ns: &str, doc_id: &str) -> Result<()>;   // remove pin
}
```

Note the absence of tier-specific methods like `promote_to_hot` or `demote_to_cold`. Callers express **intent** (`touch`, `pin`, `release`); the Storage Manager computes the tier internally. This prevents policy from leaking outside the manager.

---

### Pillar 2: Storage Engine V2

#### Problem

Current storage is a single monolithic JSON blob (`OpfsIndex` in `_system/index.json`) containing seven domains:

```json
{
    "version": 1,
    "doc_listing": { "doc_id": ["record_id", ...] },
    "oplog": [OplogEntry, ...],
    "sync_states": { "ns": SyncState },
    "schemas": { "doc_id": SchemaMeta },
    "migrations": [MigrationRecord],
    "keys": [KeyRecord]
}
```

Every operation serializes/deserializes the entire dataset. Measured bottlenecks:
- `load_index`: 3048ms to parse 896-entry JSON
- `index_transaction`: 1108ms to re-serialize + write 896 entries
- These grow linearly with dataset size — violates Invariant 1.

#### Philosophy: Block Device Abstraction

The Storage Engine presents a **block device** interface to the Storage Manager — not files, not directories, not paths. The Storage Manager never sees filenames or directories. It only sees:

```rust
trait BlockDevice: Send + Sync {
    async fn allocate_page(&self) -> Result<PageId>;
    async fn read_page(&self, id: PageId) -> Result<Option<Vec<u8>>>;
    async fn write_page(&self, id: PageId, data: &[u8]) -> Result<()>;
    async fn free_page(&self, id: PageId) -> Result<()>;
}
```

- A document may span multiple pages
- Multiple small records may share a single page
- The **Storage Manager** decides page placement, compaction, and lifecycle via the block device interface
- Each backend maps pages to its native storage: OPFS files, SQLite database pages, IndexedDB records, native disk blocks
- The **page size** is defined by the backend (OPFS uses a configured page size, SQLite uses its own)

This is how mature storage engines are designed: the higher layers think in abstract pages, the backend translates to concrete storage.

**The document store does not know about oplogs. The mutation store does not know about documents.** Each is a separate page namespace managed independently.

#### Solution: Separated Page Stores

```
{db_name}/
  _system/
    meta/page_0        ← { version, generation_id } ONLY startup load

  meta/
    keys                ← KeyRecord[]
    migrations          ← MigrationRecord[]
    schemas             ← SchemaMeta[]

  sync/
    {namespace}/
      cursor            ← { value: u64 }
      generation        ← { id: String }
      state             ← { last_push_seq, last_pull_seq, ... }

  docs/
    {namespace}/
      {page_id}         ← CRDT document data (one or more pages per doc)

  oplog/
    {namespace}/
      active/
        {page_id}       ← append-only journal of pending mutations
      synced/
        {page_id}       ← sealed Postcard page (seq N to N+SEGMENT_SIZE-1)
```

**Key properties:**
- **No global index.** Every store is independent.
- **Minimal segment manifest.** Each sealed segment page begins with a fixed-size header:
  ```
  magic:      [u8; 4]    // identifies file type
  version:    u8         // format version
  checksum:   [u8; 32]   // blake3 hash of payload
  count:      u32        // number of entries
  min_seq:    u64        // first sequence in file
  max_seq:    u64        // last sequence in file
  ```
  This header enables crash detection, integrity verification, fast range queries, and versioning — without loading the full page. Segment filenames remain derived from sequence numbers (`page_1483`), so there is no external manifest file.
- **No read-modify-write cycle.** Every write targets exactly one page.
- **Startup reads exactly one tiny page** (`meta/page_0`, ~80 bytes).

#### Active Segment Journal

Pending mutations use the same page abstraction as synced data — not one file per mutation.

Active journal is an append-only page of events:

```text
Event::Add(OplogEntry)     // new pending mutation
Event::Ack(id, Seq(u64))   // server confirmed, mark Synced
```

On seal (when journal reaches `SEGMENT_SIZE` entries or page size limit): replay journal, resolve all Acks, write finalized Postcard page to `synced/page_NNNNN`, delete journal.

Upload pipeline:
1. Load active journal
2. Replay to compute pending entries
3. Send pending to coordinator
4. On ack, append `Event::Ack(id, seq)` to journal
5. If journal reaches threshold → seal

#### Storage Trait Mapping

The existing `Storage` trait at `core/src/storage/traits.rs` is preserved as the Storage Engine interface. The OPFS implementation is rewritten to use the new page-oriented layout.

| Trait Method | New Implementation |
|---|---|
| `append_oplog(entry)` | Append `Event::Add(entry)` to active journal page |
| `mark_synced(id, seq)` | Append `Event::Ack(id, seq)` to active journal page |
| `read_pending_oplog(ns, limit)` | Replay active journal, collect Pending entries |
| `read_oplog_after_sequence(ns, seq)` | Compute page range, load and filter via segment headers |
| `read_sync_state(ns)` | Read `sync/{ns}/state` page |
| `write_sync_state(state)` | Write `sync/{ns}/state` page |
| `insert_document(...)` | Write to `docs/{ns}/` page namespace |
| `get_document(...)` | Read from `docs/{ns}/` page namespace |
| `list_documents(doc_id)` | Enumerate `docs/{ns}/` page namespace |
| `list_active_documents(ns)` | Enumerate `docs/{ns}/` page namespace (no separate listing) |
| `read_schema(doc_id)` | Read from `meta/schemas` page |
| `write_schema(meta)` | Write to `meta/schemas` page |
| `delete_synced_before(ns, cutoff)` | Scan segment headers' timestamps, delete matching pages |
| `reset_stale_pending(ns, older_than)` | Replay active journal, find stale entries |

The `index_transaction()` method — which loads → mutates → writes the entire blob — is eliminated entirely.

#### Migration Path

1. Detect `_system/index.json` exists
2. Read the old index
3. Write each domain to its new page location
4. Verify by reading back `meta/page_0`
5. Rename `index.json` → `_system/index.backup`
6. On next restart, detect backup file, delete silently

If migration crashes mid-way, the original `index.json` is intact. Data is never lost.

---

### Pillar 3: Metadata Engine

#### Problem

Today, metadata is scattered across the codebase: permissions live in the coordinator, schemas are embedded in the storage index, namespace membership is inferred from sync state, and there is no central catalog for what exists in the local database. As the platform grows to support ERP-scale data, metadata becomes as important as the data itself.

#### Solution: Central Metadata Engine

The Metadata Engine owns all metadata about the local database:

```
Metadata Engine
    │
    ├── Namespace Catalog
    │     └── Which namespaces are replicated locally?
    │     └── What is the sync state per namespace?
    │     └── Replication scope (authorized documents)
    │
    ├── Permission Index
    │     └── Namespace-level permissions (replicated from coordinator)
    │     └── Document-level access control
    │     └── Input to the Replication Planner
    │
    ├── Schema Registry
    │     └── Versioned document schemas
    │     └── Schema migration history
    │     └── Schema-document mapping
    │
    ├── Index Definitions
    │     └── Which fields are indexed?
    │     └── Index metadata (type, cardinality, storage location)
    │
    ├── Generation IDs
    │     └── Per-namespace generation tracking
    │     └── Coordinator generation for recovery
    │
    └── Replication Catalog
          └── What is being replicated and at what priority?
          └── Snapshot metadata (timestamps, sequence ranges)
          └── Segment index (seq → segment file mapping)
```

**Key properties:**
- All metadata is stored in the page-oriented format (same as documents and mutations)
- Only the metadata root page is loaded at startup (~one page read)
- The Metadata Engine is read by the Storage Manager, Memory Manager, Scheduler, and Query Planner
- It is written by the Replication Planner and Sync Coordinator

The Metadata Engine is **not** a global index. It is a collection of independently paged indexes, each scoped to a specific metadata domain.

---

### Pillar 4: Resource Manager

#### Problem

Today, resource awareness is scattered: the Scheduler guesses CPU load, the Memory Manager guesses available RAM, the Maintenance Window guesses battery state. Each subsystem probes resources independently, leading to inconsistent decisions and duplicated logic.

#### Solution: Central Resource Manager

The Resource Manager is the single source of truth for system resource state:

```
Resource Manager
    │
    ├── CPU
    │     └── Current utilization (idle / moderate / busy)
    │     └── Core count and capacity
    │
    ├── Memory
    │     └── Available RAM
    │     └── Memory pressure level
    │
    ├── Battery
    │     └── Charge level
    │     └── Charging state (charging / discharging / full)
    │
    ├── Disk
    │     └── Available space
    │     └── I/O pressure
    │
    ├── Network
    │     └── Connectivity (online / offline / metered / unmetered)
    │     └── Bandwidth estimate
    │
    └── Idle
          └── User activity state (active / idle / locked)
          └── Idle duration
```

**Interface sketch:**
```rust
trait ResourceManager: Send + Sync {
    fn current_state(&self) -> ResourceState;
    fn can_schedule(&self, priority: Priority) -> bool;
    fn is_maintenance_window(&self) -> bool;
}
```

**Consumers:**
- **Scheduler** — queries `can_schedule()` before dispatching background jobs
- **Memory Manager** — queries memory pressure to adjust eviction aggressiveness
- **Maintenance Window** — queries all resources to determine if maintenance is safe
- **Replication Planner** — queries network state to decide prefetch timing

The Resource Manager is lightweight and passive — it exposes state collected by the system rather than actively managing resources. Its value is in centralizing the resource model so that every subsystem asks the same source of truth.

---

### Pillar 5: Memory Manager

#### Problem

Today, everything that's replicated is "loaded" in the sense that it's accessible. There's no concept of what should be in memory vs. on disk vs. not yet fetched. This violates Invariant 2.

#### Scope

The Memory Manager manages **in-memory tiers only** — not disk residency. Everything in the user's authorized scope is already on disk (Recoverable at minimum) thanks to permission-driven replication. The memory manager answers: "what should be in memory right now?" not "what should exist locally?"

A document falling below the Recoverable threshold does not get deleted. It simply leaves the working set and remains on disk until re-accessed.

#### Solution: Scoring-Based Tier Management

Tiers are not descriptive labels. They are the output of a deterministic scoring function:

```rust
struct AccessScore {
    recency: f64,     // 1.0 = accessed just now, approaches 0.0 over time
    frequency: f64,   // accesses per second in the last hour
    size_penalty: f64,// 1.0 / log2(size_in_bytes) — smaller docs score higher
    predicted: f64,   // workload predictor output (0.0–1.0)
    pinned: f64,      // 100.0 if explicitly pinned, else 1.0
}

fn compute_score(doc: &Document, ctx: &Context) -> f64 {
    let recency = (1.0 / (now - doc.last_access).as_secs_f64().max(1.0)).min(1.0);
    let frequency = (doc.access_count_last_hour as f64 / 3600.0).min(1.0);
    let size_penalty = 1.0 / (doc.size_bytes.max(1) as f64).log2().max(1.0);
    let predicted = ctx.predictor.predict(doc.doc_id);  // 0.0–1.0

    (recency * 0.40 + frequency * 0.30 + size_penalty * 0.10 + predicted * 0.20) * doc.pinned_factor()
}
```

**Tiers describe guarantees, not implementation labels:**

| Guarantee | Tier | Behavior |
|-----------|------|----------|
| Resident | L0 — Active | Currently editing, pinned, never evicted. Full document in WASM Yrs document. |
| Resident | L1 — Hot | Full document in deserialized form. LRU within memory budget. |
| Recoverable | L2 — Warm | Resolvable from metadata + recent segments. <50ms load — stays on disk until accessed. |
| Recoverable | L3 — Cold | On disk within replicated scope. <50ms load from disk. Accessible offline. |
| Archived | L4 — Archived | Compressed segments. Never loaded unless explicitly requested. Data outside authorized scope is never stored locally. |

#### Memory Budget (not soft tiers)

The engine operates on a fixed memory budget, not descriptive tiers:

```
Memory Budget: 512 MB (configurable)

  Active CRDTs (Resident):  150 MB hard limit
  Hot docs (Resident):      100 MB LRU within budget
  Warm docs (Recoverable):  150 MB LRU within budget
  Snapshot cache:            50 MB
  Indexes:                   30 MB
  Scratch:                   32 MB
```

Rules:
- **Promotion:** On access, doc immediately enters Resident (L1). If Resident is full, the lowest-scoring Resident doc is demoted to Recoverable (L2) to make room.
- **Demotion:** Every 60 seconds, a background sweep recomputes scores. Docs that fell below the Resident threshold move to Recoverable. Recoverable docs below threshold are evicted from memory (data stays on disk).
- **Eviction:** Evicted Recoverable docs are not deleted — they simply leave the working set. The Storage Manager knows where their pages are and can reload them on next access.
- **Pinning:** The developer can pin a document (e.g., "currently open invoice"), giving it a score multiplier of 100x. Pinned docs stay in Resident regardless of usage.

**Key property:** The total memory used is always ≤ budget, regardless of total replicated data. A device with 10,000 replicated documents uses the same memory as one with 10 — only the disk footprint differs.

---

### Pillar 6: Scheduler (Traffic Controller)

#### Problem

Today everything is event-driven. Upload pipeline, compaction, cleanup, prefetch, and eviction are ad-hoc systems running independently. They compete with foreground operations unpredictably and have no central coordination.

#### Solution: Central Traffic Controller

The Scheduler becomes the operating system of VaultSync — it is the single point of execution for **all** asynchronous work. Nothing spawns itself. No subsystem creates its own timer, interval, or background task. Every unit of work goes through `Scheduler.submit()`:

```text
Upload Worker
     │
     └── submits job → Scheduler
                          │
Download Worker                 │
     │                         │
     └── submits job → Scheduler
                          │
Leader Election                 │
     │                         │
     └── submits job → Scheduler
                          │
Reconnect Timer                 │
     │                         │
     └── submits job → Scheduler
                          │
Snapshot                        │
     │                         │
     └── submits job → Scheduler
                          │
Compaction                      │
     │                         │
     └── submits job → Scheduler
                          │
Prefetch                        │
     │                         │
     └── submits job → Scheduler
                          │
Cleanup                         │
     │                         │
     └── submits job → Scheduler
                          │
                          ▼
               ┌───────────────────┐
               │ Resource Manager  │
               │   current_state() │
               └────────┬──────────┘
                        │
                        ▼
                   Priority Queue
                        │
               ┌────────┴────────┐
               │ Foreground(0-1) │
               │ Background(2-5) │
               └─────────────────┘
                        │
                        ▼
                   Coordinator
                   Storage Manager
                   Metadata Engine
```

The Scheduler itself does not probe resources — it asks the [Resource Manager](#pillar-4-resource-manager):

```rust
impl Scheduler {
    async fn submit(&self, job: Job) {
        if !self.resource_manager.can_schedule(job.priority()) {
            self.deferred_queue.push_back(job);
            return;
        }
        self.priority_queue.push(job);
    }

    async fn tick(&self) {
        while let Some(job) = self.priority_queue.pop() {
            if self.resource_manager.can_schedule(job.priority()) {
                job.run().await;
            } else {
                self.deferred_queue.push_back(job);
                break;  // yield until next tick
            }
        }
    }
}
```

**Priority Queue:**

| Priority | Category | Description |
|----------|----------|-------------|
| 0 | Editing | User-document writes, must be instant |
| 1 | Incoming sync | Coordinator push, must be responsive |
| 2 | Snapshot | State checkpoint, important for recovery |
| 3 | Compaction | Data organization, can wait |
| 4 | Prefetch | Proactive loading, background only |
| 5 | Cleanup | GC, tombstone removal, idle-only |

Implementation:
- Single scheduler worker with priority queue + deferred queue
- Foreground operations (priority 0–1) preempt background immediately
- Background operations (priority 2–5) yield on backpressure or resource constraints via Resource Manager
- The Resource Manager provides `current_state()` — the Scheduler never probes resources directly

**Maintenance Window (self-optimization):**

When the device is idle, charging, on WiFi, and not under memory pressure, the Maintenance Window opens:

```
Maintenance Window conditions:
  ✓ No user activity for N minutes
  ✓ Device charging (or battery > 50%)
  ✓ On unmetered network
  ✓ No memory pressure
  ✓ No active sync in progress

┌────────────────────────────────────────┐
│          Maintenance Window            │
│                                        │
│  1. Analyze workload                   │
│     → Which docs are hot?             │
│     → Which namespaces are active?    │
│     → Update prediction model         │
│                                        │
│  2. Compact                            │
│     → Seal active segments            │
│     → Merge small segments            │
│     → Delete segments covered by      │
│       snapshots                        │
│                                        │
│  3. Re-index                           │
│     → Refresh access scores           │
│     → Rebuild query indexes           │
│                                        │
│  4. Archive                            │
│     → Demote cold data to compressed  │
│     → Compact archive segments        │
│                                        │
│  5. Predict                            │
│     → Compute next session's hot set  │
│     → Prefetch if on WiFi             │
│                                        │
│  6. Report                             │
│     → Log health metrics              │
│     → Budget utilization              │
│     → Compaction stats                │
└────────────────────────────────────────┘
```

**The developer never calls `compact()`, `vacuum()`, or `cleanup()`.** The engine maintains itself autonomously.

---

### Pillar 7: Query Planner

#### Problem

Today `getDoc(docId)` always reads from OPFS (or cache). There's no query planning — no awareness of what's cached, what's materialized, what can be served from snapshots, or what needs the coordinator.

#### Scope

The Query Planner operates **within the user's authorized workspace**. Every document within scope is already on disk (Recoverable at minimum). The planner should never need a network fetch for data within the workspace — all resolution paths are local. Data outside the workspace is never stored locally and cannot be served regardless of connectivity.

#### Solution: Cache-Aware Query Resolution

```text
getDocument("invoice-123")
    │
    ├── Resident cache hit?     → return immediately (0ms)
    ├── Recoverable cache hit?  → materialize from metadata + delta (<10ms)
    ├── Recoverable disk hit?   → fetch from disk page (<50ms)
    └── Archived or missing?    → data is outside user's authorized scope → cannot be served
                                  (same behavior online and offline — expected)
```

Each level answers "do I have the data to resolve this document's state?" without loading unnecessary data. The query planner never scans the oplog to resolve a document.

Note: There is no network fallback in the query path. Within the workspace, disk is the deepest tier. If the planner cannot find a document, it means the document is outside the user's authorized scope — a permission issue, not a caching issue.

For queries (not single doc):

```text
getDocuments({ namespace, filter, sort, limit })
    │
    ├── Have index for this filter? → use index, load matched docs from working set
    ├── Have metadata snapshot for this namespace? → filter snapshot, apply deltas
    └── No local data? → data in namespace has not been replicated (check permissions)
```

The query planner works with the Storage Manager to determine the fastest resolution path. It never deserializes more data than needed.

**Future evolution:** As query complexity grows beyond single-doc lookups (filters, joins, aggregations), the Query Planner will evolve into a **Cost-Based Optimizer** — similar to SQLite's planner. It will estimate the cost of each resolution path (index scan vs. snapshot vs. disk) and choose the cheapest plan. Foundation: the Metadata Engine's index definitions and the Memory Manager's tier residency information.

---

### Pillar 8: Replication Planner

#### Problem

Today, replication decisions are scattered: the coordinator decides what to push, permissions decide what is allowed, and the download worker decides what to pull. There is no single authority for **what should be replicated** and **what should be removed** when circumstances change.

#### Solution: Central Replication Planner

The Replication Planner determines **what the local database should contain**. It is separate from the Coordinator — the Coordinator executes the plan, the Planner determines it.

```
Replication Planner
    │
    ├── On registration
    │     └── Query coordinator for authorized scope
    │     └── Compute initial replication set
    │     └── Tell Storage Manager: replicate these namespaces
    │
    ├── On permission change
    │     └── Receive permission update from coordinator
    │     └── Compute delta: what should be added or removed
    │     └── Tell Storage Manager: add these, remove these
    │
    ├── On namespace deletion
    │     └── Receive deletion signal from coordinator
    │     └── Decide: delete, archive, or keep encrypted
    │     └── Tell Storage Manager: execute policy
    │
    └── On scope expansion
          └── Compute new documents to replicate
          └── Prioritize by urgency (active vs. background)
          └── Tell Storage Manager: fetch new scope
```

**Key property:** The Replication Planner makes decisions based on permissions and policy. The Coordinator and Storage Manager execute those decisions. This separation ensures that replication policy is enforced in one place, not scattered across the sync protocol.

The Replication Planner reads from the Metadata Engine (permission index, namespace catalog) and writes to it (replication catalog). It submits replication jobs to the Scheduler for execution.

---

### Pillar 9: Adaptive Replication (Postponed)

#### Problem

Today replication pushes all mutations for a namespace at the same urgency. Within a namespace, some document types are hot (actively used) while others are cold (replicated but rarely accessed). The coordinator has no way to prioritize push for the documents the user is actually working on.

#### What Adaptive Replication Is NOT

Adaptive replication does **not** decide what to replicate. That decision is made by permissions at registration time — the user's entire authorized scope is replicated. Adaptive replication only decides **how urgently** mutations are pushed within that scope.

#### Why Postponed

Adaptive replication requires coordinator intelligence — usage hints, tiered push policies, prefetch scheduling. This is a protocol-level change that depends on the Storage Manager, Memory Manager, and Scheduler being in place first.

The client-side hint computation depends on the memory manager knowing which docs are hot. The coordinator-side tiered push depends on the scheduler managing background work. Building adaptive replication before those foundations leads to ad-hoc design.

#### Future Direction

Once the local platform is mature, the SUBSCRIBE protocol extends with **timeliness hints** (not scope hints):

```
SUBSCRIBE {
    namespace: "erp",
    after: 1483,
    timeliness_hints: {
        realtime: ["invoices", "employees"],    // push mutations immediately
        batched:  ["payroll", "leave"],          // push every 5 minutes
        passive:  ["audit_log", "archive"],      // push only on reconnect or request
    }
}
```

Coordinator response:
- Push mutations for `realtime` docs eagerly as they occur
- Push mutations for `batched` docs in periodic batches
- Track cursor for `passive` docs but only push on reconnection or explicit request

The memory manager feeds `realtime` and `batched` hints from the access scoring model. If HR opens Payroll every morning, after a week the predictor promotes Payroll from `passive` → `batched` → `realtime` proactively.

---

## Beyond Storage — The Full Platform

Storage is the foundation of Generation 2, but VaultSync as a data platform will eventually need first-class designs for additional subsystems. These are deferred to Generations 3+ but the storage architecture must not preclude them:

| Subsystem | Why It Matters | Deferred To |
|-----------|----------------|-------------|
| **Indexing** | Document-level and field-level indexes for fast queries without full scans | Gen 3 |
| **Query Execution** | A query planner that can optimize across cache, snapshot, and disk tiers | Gen 3 |
| **Transaction Manager** | Atomic multi-document commit (invoice + stock + ledger + audit log preserved together) | Gen 3 |
| **Schema Evolution** | Versioned schemas with automatic migration | Gen 3 |
| **Permissions** | Row-level and field-level access control — the input to the replication model | Gen 2 (design), Gen 3 (implementation) |
| **Conflict Resolution** | Customizable policies per document type | Gen 3 |
| **Observability** | Metrics, tracing, health diagnostics for production operation | Gen 2 (basic), Gen 3 (comprehensive) |
| **Cost-Based Optimizer** | Evolve Query Planner from cache-aware resolution to SQLite-style cost-based execution planning | Gen 4 |

**Permissions deserve early design attention** because they are the input to the replication model. The coordinator computes the user's authorized scope from permissions at registration time. The storage directory tree (`docs/{namespace}/{doc_id}/`) maps naturally to namespace-level permissions, but field-level and record-level permissions will need a more granular model.

**Terminology evolution:** This document uses "document" as the unit of data, which reflects current CRDT-centric architecture. As VaultSync evolves to handle table-oriented ERP data (invoices, payroll, inventory), the terminology will shift toward **Entity** or **Collection**. The page-oriented storage model supports this shift naturally — an entity is just a page or set of pages with a defined schema. The Metadata Engine's schema registry and permission model are designed for this evolution.

The page-oriented storage, Storage Manager abstraction, and tier architecture must not preclude any of these future subsystems.

---

## Phase Plan

### Phase A — Storage Engine Rewrite (2-3 weeks)

**Scope:** Implement Storage Engine V2. Page-oriented stores. No global index. Postcard segments with minimal headers. Migration from old format.

**Files:**
- `wasm/src/storage.rs` — Rewrite OpfsStorage: ~500 new / ~300 deleted lines
- `wasm/src/page_store.rs` — New file: page read/write, store abstraction: ~200 lines
- `wasm/src/migration.rs` — New file: old-to-new format migration: ~100 lines
- `core/src/storage/traits.rs` — Minimal additions (~10 lines)

**Verification:**
- `cargo check --workspace`
- `wasm-pack build --target web`
- `npm run build` (SDK)
- Startup timing: `load_index` should drop from 3048ms to <5ms

### Phase B — Storage Manager (4-6 weeks)

**Scope:** Central authority for placement, compaction, eviction, lifecycle. All storage access goes through Storage Manager. Storage Engine becomes a thin backend adapter.

**New components:**
- `core/src/storage/manager.rs` — StorageManager trait + implementation
- `core/src/storage/compaction.rs` — Compaction policy engine
- `core/src/storage/lifecycle.rs` — Segment lifecycle (active → sealed → deleted)
- `wasm/src/storage_manager.rs` — WASM OPFS-backed StorageManager

### Phase C — Metadata Engine (4-6 weeks)

**Scope:** Central metadata catalog for permissions, schemas, indexes, namespace membership, generation IDs, and replication state.

**New components:**
- `core/src/metadata/mod.rs` — Metadata Engine implementations
- `core/src/metadata/permissions.rs` — Permission index storage and queries
- `core/src/metadata/schemas.rs` — Schema registry
- `core/src/metadata/catalog.rs` — Namespace and replication catalog

### Phase D — Scheduler + Resource Manager (6-8 weeks)

**Scope:** Central traffic controller with priority queue. Resource Manager for CPU/battery/memory/network/idle state. Maintenance Window self-optimization cycle.

**New components:**
- `core/src/scheduler/mod.rs` — Priority queue, worker pool, job submission
- `core/src/scheduler/resource.rs` — Resource Manager (CPU, battery, memory, network, idle)
- `core/src/scheduler/optimizer.rs` — Maintenance Window self-optimization orchestrator
- `core/src/scheduler/prefetch.rs` — Predictive prefetch job

### Phase E — Memory Manager (4-6 weeks)

**Scope:** Scoring algorithm, memory budget, Resident/Recoverable/Archived tier management, access tracking, pinning.

**New components:**
- `core/src/memory/mod.rs` — Scoring, tier resolution, budget enforcement
- `core/src/memory/cache.rs` — In-memory document cache with LRU eviction
- `core/src/memory/tracker.rs` — Access frequency and recency tracking

### Phase F — Logical Database (4-6 weeks)

**Scope:** Database-oriented API layer between Query Engine and Storage Manager. Exposes `get`, `scan`, `query`, `transaction`, `watch` — resolves through Storage Manager internally.

**New components:**
- `core/src/database/mod.rs` — Logical Database API
- `core/src/database/resolver.rs` — Tier resolution (delegates to Storage Manager + Memory Manager)

### Phase G — Query Planner (4-6 weeks)

**Scope:** Cache-aware query resolution. Resident/Recoverable/disk-aware `getDocument`. Local indexes for filter patterns. Foundation for future cost-based optimizer.

**New components:**
- `core/src/query/planner.rs` — Query planner, tier resolver
- `core/src/query/index.rs` — Local indexes for common filter patterns

### Phase H — Replication Planner (6-8 weeks)

**Scope:** Replication decision engine. Handles registration, permission changes, scope expansion, and namespace deletion.

**New components:**
- `core/src/replication/planner.rs` — Replication decision engine
- `core/src/replication/scope.rs` — Workspace scope computation

### Phase I — Adaptive Replication (6-8 weeks)

**Scope:** Protocol extensions for timeliness hints. Coordinated proactive push. Usage-driven subscription updates.

**Protocol changes:**
- Extend `SUBSCRIBE` with `realtime`/`batched`/`passive` doc hints
- Extend coordinator to tier mutations by doc type
- Client-side hint computation from the Memory Manager

### Phase J — Performance (ongoing)

**Scope:** Postcard optimization. Adaptive compression. Multi-level cache tuning. Benchmark suite.

---

## What We Will NOT Build (Yet)

These are optimization layers intentionally deferred until the architecture is proven:

- ❌ Custom binary protocol before established serialization is validated
- ❌ Fancy compression before we measure actual storage savings needed
- ❌ SIMD before we identify specific hot loops
- ❌ GPU before we identify parallel workloads
- ❌ Lock-free everything before we measure contention
- ❌ Exotic CRDTs before we prove Yrs is the bottleneck

---

## Anti-Patterns to Avoid

1. **Global index** — Never store everything in one serialized blob again. Each store is independent.
2. **Synchronous migration** — All migrations must be crash-safe with rollback (verify → rename → delete).
3. **Flat pending storage** — Never one file per pending mutation. Pending and synced use the same segment abstraction.
4. **External manifest files** — Don't maintain a separate manifest.json that lists all segments. Segment metadata (checksum, count, range) lives in the segment page headers.
5. **Storage coupled to sync** — The document store doesn't know about oplogs. The mutation store doesn't know about queries.
6. **Event-driven background work** — Background work must be scheduled with priorities, not scattered as ad-hoc timers.
7. **Developer-maintained health** — The engine must self-optimize. No `compact()`, `vacuum()`, or `cleanup()` in the public API.

---

## Design Checklist

Before implementing any new storage feature, ask:

- Does this violate one of the 7 invariants?
- Does this require rewriting a whole page for a single entry change?
- Does this create a global bottleneck that grows with dataset size?
- Does this couple two independent concerns?
- Does this have a crash-safe migration path?
- Does this create background work that can block foreground operations?
- Does this require developer intervention to maintain health?

If the answer to any is "yes", redesign before implementing.
