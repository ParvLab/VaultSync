# VaultSync Architecture V2

> An adaptive local-first data platform.

---

## Mission

VaultSync is not a sync engine. It is not a CRDT library. It is not a Replicache alternative.

VaultSync is an **adaptive local-first data platform**.

Synchronization is one subsystem. The center of the system is **local data management** — managing a device's working set of data the way an operating system manages memory: adaptively, by usage, with hot/warm/cold/archived tiers, and with startup time and memory usage that are independent of the total dataset size.

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
          │   Working Set Manager   │
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

The **Query Engine** asks "give me document X" — it doesn't know where X lives.

The **Storage Manager** decides which tier X is in, whether to promote or evict it, when to compact, and what to prefetch. It is the central authority for all placement decisions.

The **Storage Engine** is a thin adapter that translates page-level operations to the backend (OPFS file, SQLite row, IndexedDB record). It has no policy logic.

**Workspace boundary:** The Storage Manager manages a single user's authorized scope. Everything in that scope exists on disk (at minimum at L3/cold). Nothing outside the scope exists locally. The Working Set Engine only manages **memory tiers** (L0–L2) within this already-replicated workspace — it never decides what should or shouldn't be on disk. That decision belongs to the replication model.

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
| Storage Manager | Central authority for placement, compaction, eviction, lifecycle |
| Storage Engine V2 | Page-oriented storage with independent stores, no global index |
| Scheduler | Priority-driven background work + Maintenance Window self-optimization |
| Working Set Engine | Scoring-based tier management with memory budget |
| Query Planner | Cache-aware resolution: L0 → L1 → L2 → L3 → disk |

### Generation 3 — Optimization

**Goal:** Performance without changing architecture.

| Feature | Description |
|---------|-------------|
| Intelligent Compaction | CPU/battery/disk/memory-aware compaction strategy selection |
| Predictive Prefetch | Predict likely-needed data and fetch before requested |
| Multi-Level Cache | L0 Active CRDT → L1 Working Set → L2 Snapshots → L3 Segments → Coordinator |
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

The Storage Manager is the single point of control for all storage policy:

```
Storage Manager
    │
    ├── Placement
    │     └── Which tier does this document belong in?
    │     └── Where should this new segment be written?
    │     └── Should this data be promoted or demoted?
    │
    ├── Compaction
    │     └── When should active journals be sealed?
    │     └── When should small segments be merged?
    │     └── When should segments covered by snapshots be deleted?
    │
    ├── Eviction
    │     └── Which documents must leave the working set?
    │     └── What must be persisted before eviction?
    │     └── What can be discarded without persistence?
    │
    ├── Lifecycle
    │     └── Active → Sealed (segment reaches size limit)
    │     └── Sealed → Deleted (covered by snapshot compaction)
    │     └── Cold → Archived (not accessed in N days)
    │     └── Archived → Deleted (explicit GC policy)
    │
    └── Integrity
          └── Verify checksums on read
          └── Detect crashes via partially-written files
          └── Maintain minimal segment manifests
```

All other subsystems — download worker, upload pipeline, reconciler, scheduler — talk to the Storage Manager. None of them write to storage directly.

**Interface sketch:**
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

    // Tier hints (called by Working Set Engine)
    async fn promote_to_hot(&self, ns: &str, doc_ids: &[&str]) -> Result<()>;
    async fn demote_from_hot(&self, ns: &str, doc_ids: &[&str]) -> Result<()>;
}
```

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

#### Philosophy: Pages, Not Files

Storage engines don't think in terms of files. They think in terms of **pages** — fixed-size blocks that hold objects.

- A document may span multiple pages
- Multiple small records may share a single page
- The Storage Engine only knows about pages: read page N, write page N
- The **page size** is fixed per backend (4KB for OPFS, 8KB for SQLite)
- The **Storage Manager** decides page placement, compaction, and lifecycle

The OPFS backend happens to store pages as individual files in a directory tree. The SQLite backend stores them as database pages. The abstraction hides this.

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

### Pillar 3: Working Set Engine

#### Problem

Today, everything that's replicated is "loaded" in the sense that it's accessible. There's no concept of what should be in memory vs. on disk vs. not yet fetched. This violates Invariant 2.

#### Scope

The Working Set Engine manages **in-memory tiers only** — not disk residency. Everything in the user's authorized scope is already on disk (L3/cold at minimum) thanks to permission-driven replication. The working set answers: "what should be in memory right now?" not "what should exist locally?"

A document falling below the L3 threshold does not get deleted. It simply leaves the working set and remains on disk until re-accessed.

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

**Tiers are ranges on the score axis, not manual classifications:**

| Score Range | Tier | Behavior |
|-------------|------|----------|
| >= 0.8 | L0 — Active | Currently editing, pinned, never evicted |
| 0.5 – 0.8 | L1 — Hot | Full document in deserialized form, LRU within budget |
| 0.2 – 0.5 | L2 — Warm | Resolvable from snapshot + recent segments, <50ms load |
| 0.01 – 0.2 | L3 — Cold | On disk only (within replicated scope), <50ms load from disk |
| < 0.01 | L4 — Archive | Compressed segments, never loaded unless explicitly requested. Data outside authorized scope is never stored locally. |

#### Memory Budget (not soft tiers)

The engine operates on a fixed memory budget, not descriptive tiers:

```
Memory Budget: 512 MB (configurable)

  Active CRDTs (L0):  150 MB hard limit
  Hot docs (L1):      100 MB LRU within budget
  Warm docs (L2):     150 MB LRU within budget
  Snapshot cache:      50 MB
  Indexes:             30 MB
  Scratch:             32 MB
```

Rules:
- **Promotion:** On access, doc immediately enters L1 (hot). If L1 is full, the lowest-scoring L1 doc is demoted to L2 (warm) to make room.
- **Demotion:** Every 60 seconds, a background sweep recomputes scores. Docs that fell below the L0 threshold move to L1. L1 docs below threshold move to L2. L2 docs below threshold are evicted from memory (data stays on disk).
- **Eviction:** Evicted L2 docs are not deleted — they simply leave the working set. The Storage Manager knows where their pages are and can reload them on next access.
- **Pinning:** The developer can pin a document (e.g., "currently open invoice"), giving it a score multiplier of 100x. Pinned docs stay in L0 regardless of usage.

**Key property:** The total memory used is always ≤ budget, regardless of total replicated data. A device with 10,000 replicated documents uses the same memory as one with 10 — only the disk footprint differs.

---

### Pillar 4: Query Planner

#### Problem

Today `getDoc(docId)` always reads from OPFS (or cache). There's no query planning — no awareness of what's cached, what's materialized, what can be served from snapshots, or what needs the coordinator.

#### Scope

The Query Planner operates **within the user's authorized workspace**. Every document within scope is already on disk (L3/cold at minimum). The planner should never need a network fetch for data within the workspace — all resolution paths are local. Data outside the workspace is never stored locally and cannot be served regardless of connectivity.

#### Solution: Cache-Aware Query Resolution

```text
getDocument("invoice-123")
    │
    ├── L0 cache hit?   → return immediately (0ms)
    ├── L1 cache hit?   → return immediately (<1ms)
    ├── L2 cache hit?   → materialize from snapshot + delta (<10ms)
    ├── L3 cache hit?   → fetch from disk page (<50ms)
    └── L4 miss?        → data is outside user's authorized scope → cannot be served
                          (same behavior online and offline — expected)
```

Each level answers "do I have the data to resolve this document's state?" without loading unnecessary data. The query planner never scans the oplog to resolve a document.

Note: L4 is not a network fallback. Within the workspace, L3 (disk) is the deepest tier. If the planner reaches L4, it means the document is outside the user's authorized scope — a permission issue, not a caching issue.

For queries (not single doc):

```text
getDocuments({ namespace, filter, sort, limit })
    │
    ├── Have index for this filter? → use index, load matched docs from working set
    ├── Have snapshot for this namespace? → filter snapshot, apply deltas from warm docs
    └── No local data? → data in namespace has not been replicated (check permissions)
```

The query planner works with the Storage Manager to determine the fastest resolution path. It never deserializes more data than needed.

---

### Pillar 5: Scheduler

#### Problem

Today everything is event-driven. Compaction, cleanup, prefetch, and eviction are ad-hoc timers scattered across the codebase. They compete with foreground operations unpredictably.

#### Solution: Priority Queue + Nightly Self-Optimization

**Priority Queue for real-time work:**

| Priority | Category | Description |
|----------|----------|-------------|
| 0 | Editing | User-document writes, must be instant |
| 1 | Incoming sync | Coordinator push, must be responsive |
| 2 | Snapshot | State checkpoint, important for recovery |
| 3 | Compaction | Data organization, can wait |
| 4 | Prefetch | Proactive loading, background only |
| 5 | Cleanup | GC, tombstone removal, idle-only |

Implementation:
- Single scheduler worker with priority queue
- Foreground operations (priority 0–1) preempt background
- Background operations (priority 2–5) yield on timer or backpressure
- System resources (battery, CPU, memory) influence scheduling decisions

**Nightly Self-Optimization cycle:**

```
Idle trigger (no user activity for N minutes)
    │
    ├── 1. Analyze workload
    │       └── Which docs are hot at what times?
    │       └── Which namespaces are active?
    │       └── Update prediction model
    │
    ├── 2. Compact
    │       └── Seal active segments that are ready
    │       └── Merge small segments (below minimum fill ratio)
    │       └── Delete segments fully covered by snapshots
    │
    ├── 3. Re-index
    │       └── Refresh in-memory access scores
    │       └── Rebuild query indexes if needed
    │
    ├── 4. Archive
    │       └── Demote L3→L4 data to compressed storage
    │       └── Compact archive segments
    │
    ├── 5. Predict
    │       └── Compute next session's likely hot set
    │       └── Prefetch if on unmetered connection / WiFi
    │
    └── 6. Report
            └── Log health metrics, budget utilization, compaction stats
```

**The developer never calls `compact()`, `vacuum()`, or `cleanup()`.** The engine maintains itself.

---

### Pillar 6: Adaptive Replication (Postponed)

#### Problem

Today replication pushes all mutations for a namespace at the same urgency. Within a namespace, some document types are hot (actively used) while others are cold (replicated but rarely accessed). The coordinator has no way to prioritize push for the documents the user is actually working on.

#### What Adaptive Replication Is NOT

Adaptive replication does **not** decide what to replicate. That decision is made by permissions at registration time — the user's entire authorized scope is replicated. Adaptive replication only decides **how urgently** mutations are pushed within that scope.

#### Why Postponed

Adaptive replication requires coordinator intelligence — usage hints, tiered push policies, prefetch scheduling. This is a protocol-level change that depends on the Storage Manager, Working Set Engine, and Scheduler being in place first.

The client-side hint computation depends on the working set engine knowing which docs are hot. The coordinator-side tiered push depends on the scheduler managing background work. Building adaptive replication before those foundations leads to ad-hoc design.

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

The working set engine feeds `realtime` and `batched` hints from the access scoring model. If HR opens Payroll every morning, after a week the predictor promotes Payroll from `passive` → `batched` → `realtime` proactively.

---

## Nightly Self-Optimization

See the full cycle under [Pillar 5: Scheduler](#pillar-5-scheduler). This is called out separately because it is the feature that makes VaultSync genuinely different from every other sync engine.

**The engine stays healthy without developer intervention.**

No manual `compact()` calls. No `cleanup()` timers. No `vacuum()` scripts. The Scheduler observes workload patterns and runs maintenance during idle periods with appropriate priority.

---

## Beyond Storage — The Full Platform

Storage is the foundation of Generation 2, but VaultSync as a data platform will eventually need first-class designs for additional subsystems. These are deferred to Generations 3+ but the storage architecture must not preclude them:

| Subsystem | Why It Matters | Deferred To |
|-----------|----------------|-------------|
| **Indexing** | Document-level and field-level indexes for fast queries without full scans | Gen 3 |
| **Query Execution** | A query planner that can optimize across cache, snapshot, and disk tiers | Gen 3 |
| **Transactions** | Atomicity for multi-document write operations | Gen 3 |
| **Schema Evolution** | Versioned schemas with automatic migration | Gen 3 |
| **Permissions** | Row-level and field-level access control — the input to the replication model | Gen 2 (design), Gen 3 (implementation) |
| **Conflict Resolution** | Customizable policies per document type | Gen 3 |
| **Observability** | Metrics, tracing, health diagnostics for production operation | Gen 2 (basic), Gen 3 (comprehensive) |

**Permissions deserve early design attention** because they are the input to the replication model. The coordinator computes the user's authorized scope from permissions at registration time. The storage directory tree (`docs/{namespace}/{doc_id}/`) maps naturally to namespace-level permissions, but field-level and record-level permissions will need a more granular model.

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

### Phase C — Scheduler (4-6 weeks)

**Scope:** Priority queue for background work. Maintenance Window self-optimization cycle. Resource-aware scheduling.

**New components:**
- `core/src/scheduler/mod.rs` — Priority queue, worker pool
- `core/src/scheduler/optimizer.rs` — Maintenance Window self-optimization orchestrator
- `core/src/scheduler/prefetch.rs` — Predictive prefetch job

### Phase D — Working Set Engine (4-6 weeks)

**Scope:** Scoring algorithm, memory budget, LRU tiers, access tracking, pinning.

**New components:**
- `core/src/working_set/mod.rs` — Scoring, tier resolution, budget enforcement
- `core/src/working_set/cache.rs` — In-memory document cache with LRU eviction
- `core/src/working_set/tracker.rs` — Access frequency and recency tracking

### Phase E — Query Planner (4-6 weeks)

**Scope:** Cache-aware query resolution. Tier-aware `getDocument`. Local indexes for filter patterns.

**New components:**
- `core/src/query/planner.rs` — Query planner, tier resolver
- `core/src/query/index.rs` — Local indexes for common filter patterns

### Phase F — Adaptive Replication (6-8 weeks)

**Scope:** Protocol extensions for usage hints. Coordinated proactive push. Working-set-driven subscription updates.

**Protocol changes:**
- Extend `SUBSCRIBE` with hot/warm/cold doc hints
- Extend coordinator to tier mutations by doc type
- Client-side hint computation from working set engine

### Phase G — Performance (ongoing)

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
