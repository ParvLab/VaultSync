# Anchored Summary

## Goal
- Complete **StorageAuthority v1.0**: VaultRuntime as a mature embedded database, not just a sync engine.

### Measurable Targets
| Metric | Current | Target |
|--------|---------|--------|
| Leader startup (cold) | ~5200ms | ~2500ms |
| Follower startup | ~250ms | ~100ms |
| OPFS directory walks/startup | 9 | 1 (initial manifest load) |
| SyncState pages | 231+ | 1 (MetadataStore singleton) |
| WS handshakes/startup | 2 (register + reconnect) | 1 |
| Connection state granularity | boolean | 6-variant enum |
| Manifest rebuild calls/startup | 3+ | 0 (private repair API only) |
| All 3 builds | passing | passing |

### Architecture Pillars
1. **StorageRuntime** — single OPFS gatekeeper. `MetadataStore<T>` for singletons. `ContentIndex` as sole lookup path. `PageManager` for full lifecycle. `rebuild_manifest()` made **private** — call `recover()` instead.
2. **Boot→Ready→Warm→Idle** startup phases — UI interactive at ~300ms, background sync afterwards.
3. **Namespace hierarchy** — `WorkspaceRuntime` → `NamespaceRuntime` (lazy-loaded). Each namespace = independent cursor, gen, storage.
4. **MaintenanceRuntime** — GC, compaction, checkpoint, repair, health. Never blocks startup.
5. **CacheRuntime** — centralized eviction for manifest/query/document/mirror caches.
6. **EngineContext** — single container for all shared runtime references.
7. **Engineering Rules** — 13 enforceable invariants (see below).

## Design Principles
The engine follows these principles. Every feature proposal should be checked against them.

1. **Local First** — Every operation succeeds against local storage. Network is an optimization, not a dependency.

2. **Intent Before Data** — Replicate what users are likely to need, not simply what changed.

3. **Metadata Before Bytes** — Decisions are made from metadata. Large document bodies are loaded only when necessary.

4. **Lazy By Default** — Never load something until a subsystem proves it is needed.

5. **Ownership Hierarchy** — Company → Workspace → Working Set → Replication Plan → Pages. Each level owns the one below.

6. **Everything Is Replaceable** — Subsystems communicate through traits, not concrete implementations. ReplicationPlanner, StorageManager, Storage can all be swapped without affecting others.

## Ownership Hierarchy
```
Company
    ↓
Workspace (schema, replication, retention)
    ↓
Working Set (query-based document resolution)
    ↓
Replication Plan (what to sync, when, in what order)
    ↓
Pages (physical storage)
```

## Engine Lifecycle (Request Flow)
```
Application starts
    ↓
HELLO sent via BroadcastChannel
    ↓
Wait for DISCOVER (100ms event-driven)
    ↓
Leader found? → MirrorRuntime fast-path (<200ms)
    ↓
No leader? → Full init path:
    ├── Storage opens (PageManager)
    ├── Metadata/checkpoint loads
    ├── LeaderElection (Web Lock) acquires
    ├── Workspace loads
    ├── Working Sets restore
    ├── Replication Planner initializes
    ├── Coordinator connects
    ├── Recovery runs
    ├── Announce READY via BC
    |
    ↓
Query executes
    ↓
ContentIndex lookup (O(1), no OPFS scan)
    ↓
Page read (from cache or OPFS)
    ↓
CRDT merge
    ↓
UI updates
    ↓
StorageTransaction commit (only way to mutate)
    ↓
Planner observes access
    ↓
Prefetch scheduled
```

### Startup Phases (Boot → Ready → Warm → Idle)
```
Time (ms)
0      100     300     1500+   3000+
│       │       │       │       │
Boot───►Ready──►Warm──►Idle──►...
│       │       │       │
│ Init  │ UI    │ Back- │ Steady
│ stor‐ │ live  │ ground│ state
│ age   │       │ recov │
│       │       │ ery   │
│ read_ │     ──│ replay│
│ manifest│    │ │ snaph│
│ vali‐ │       │ │ shot │
│ date  │       │ │      │
└───────┴───────┴───────┴───────
Blocking  Blocking  Non-blocking  Non-blocking
```
- **Boot** (~100ms): Read manifests, CRC validate, load ContentIndex. No OPFS directory walks.
- **Ready** (~300ms): UI interactive. Leader election, coordinator connect (leader) or MirrorRuntime (follower).
- **Warm** (background): Download replay, snapshot catch-up, pending queue process.
- **Idle** (background): Compaction, GC, health probes, metrics aggregation.

That's how people understand systems — not by reading modules, but by following a request.

## Constraints & Preferences
- WASM build target (`wasm-pack build --target web`)
- TypeScript SDK build via `npm run build` in `sdk/`
- All Rust crates must compile (`cargo check --workspace`)
- Compaction methods must use `js_sys::Date::now()` for timestamps (not `crate::time_utils`)

## Progress
### Done
- **Phase A — Storage Engine V2**: Created `Storage` trait (`traits.rs`) with read/write/delete/list/scan, `InMemoryStorage` and `SqliteStorage` implementations, `CompactionEngine`/`LifecycleEngine`/`StorageManager` layered architecture. WASM `OpfsStorage` uses `page_store.rs` (OPFS-backed page store). V2 layout uses flat `_pages/{store}/{id}.page` storage.
- **Phase B — Segment lifecycle + compaction**: Created `compaction.rs` (`CompactionEngine`, `CompactionPolicy`, `CompactionDecision`), `lifecycle.rs` (`LifecycleEngine`, `SegmentState`), `manager.rs` (`StorageManager` trait + `DefaultStorageManager` with compact_namespace/run_lifecycle). Created `WasmStorageManager` adapter in WASM crate. `cargo check --workspace` passes.
- **Phase C — WASM StorageManager adapter**: Both constructors (`new`, `new_with_coordinator`) wrap `BrowserStorage` in `DefaultStorageManager`. `WasmVaultSyncClient` stores `Arc<dyn StorageManager>`. Exposed JS methods: `compactNamespace()`, `runLifecycle()`, `storageStats()`. `wasm-pack build --target web` passes.
- **Phase D — Resource Manager**: Created `resource_manager.rs` with `MemoryBudget` (512 MB default config), `AccessScore` scoring (recency*0.40 + frequency*0.30 + size_penalty*0.10 + predicted*0.20) × pinned_factor(100x), 5 memory tiers (ResidentActive/ResidentHot/RecoverableWarm/RecoverableCold/Archived), `eviction_candidates()` sorted lowest-score-first, `sweep()` recomputes scores and promotes/demotes tiers, pin/unpin API. Wired into `StorageManager` trait and `DefaultStorageManager` (read/write record access, delete removes tracking). Exposed `resourceSweep()` to JS. 9 unit tests pass.
- **Phase E — Integration tests**: 31 integration tests in `manager_integration.rs` covering: StorageManager CRUD (write/read/delete/overwrite/list), compaction lifecycle (snapshots, tombstone ratio, throttling), lifecycle cleanup (old tombstone removal, active doc preservation), segment seal (should_seal, event recording, timestamps), resource manager integration (access tracking, sweep tiers, eviction candidates), multi-tab simulation (shared storage, concurrent delete), compaction+lifecycle combined flow, resource manager multi-sweep scenarios, edge cases (empty manager, nonexistent docs, large docs, empty namespaces). All 31 pass.

### Done (L1-L6)
- **Snapshot drain in WASM WS coordinator**: After REGISTER_ACK, drain MSG_SNAPSHOT frames from `msg_rx` with 300ms timeout and cache them in `received_snapshots`. Handle MSG_SNAPSHOT in background reader. Implemented `list_snapshots()` (returns cache) and `store_snapshot()` (sends MSG_SNAPSHOT frame).
- **Snapshot pull at cursor=0**: Removed `after > 0` guard in `download.rs:146` so `try_fetch_snapshot()` runs during replay mode. Rewrote `try_fetch_snapshot()` to apply ALL snapshots (deduplicated by doc) and advance cursor to max seq.
- **`SnapshotPayload` extended** with `schema_version` and `created_at` fields; all 7 constructors updated across `ws.rs`, `ws_mux.rs`, `mux_coordinator.rs`, `coordinator-cf/lib.rs` (2), and `ws_coordinator.rs`.
- **Compaction returns Snapshot metadata**: `run_snapshot_compaction()` returns `(CompactionStats, Vec<Snapshot>)` with seq, checksum, bytes for each compacted doc.
- **Compaction uploads to coordinator**: `client.rs` compaction timer captures `coordinator` clone; after snapshot compaction, iterates returned snapshots and calls `comp_coord.store_snapshot()`.
- **Server auto-compaction**: Added `auto_compact_ns` and `auto_compact_interval_minutes` to `ServerConfig`. `build_router()` spawns background tokio task that calls `coordinator.compact_oplog(ns)` at interval.
- **Replay summary instrumentation (L8)**: Added 3 concise summary logs after replay/snapshot restore:
  - `[download_queue] snapshot restored: docs=N cursor=N->N restore_time=Nms`
  - `[download_queue] replay complete: mutations=N cursor=->N changed=N`
  - `[client] synced: cursor=N gen_match=bool backend=str ns=str`
- **Configurable log level (L1)**: Added `LogLevel` enum (Off/Error/Warn/Info/Debug/Trace) to `core/src/config.rs`. Added `log_level` field to `VaultSyncConfig`. Exposed `set_log_level_from_str()` via `#[wasm_bindgen]`. WASM `ConsoleSubscriber` now checks a static `AtomicU8` instead of hardcoded INFO filter.

### Done (cont.)
- **Phase A — SUBSCRIBE effective_after**: WASM WS coordinator now computes `effective_after = std::cmp::max(after, max_snapshot_seq)` after snapshot drain, preventing unnecessary push firehose from the server.
- **Phase B — `history_preserved` protocol**: Added field to `RegisterAckPayload`/`NamespaceAckPayload`. Demo server sets `false` (in-memory), CF coordinator sets `true` (D1 persists). Added to `Coordinator` trait with default `false`. Stored in WASM coordinator (`AtomicBool`) and mux coordinator (`Arc<AtomicBool>`).
- **Phase C — Smart recovery**: `client.rs` generation mismatch now checks `history_preserved` before resetting cursor. If `true`: keep cursor, just re-register with same cursor. If `false`: reset cursor to 0 (existing replay behavior).

### Done (cont.)
- **Phase 2 — Workers start after recovery**: Download worker spawn moved from `build_client()` to after subscribe in `initialize()`. Receiver stored in `VaultSyncClient.download_worker_rx`, spawned via `start_download_worker()`. Stale init wakeups drained before entering main loop.
- **Phase 3 — Bootstrap notify removed**: `client.events.notify_download(None)` removed from `wasm/src/client.rs:161`. No bootstrap pull needed — worker starts already synchronized after subscribe.
- **Phase 4 — CF coordinator max_sequence**: Both RegisterAck and NamespaceAck queries now use `SELECT COALESCE(MAX(...), 0) FROM (SELECT MAX(sequence) FROM mutations UNION ALL SELECT MAX(sequence) FROM snapshots)`. Survives compaction (mutations deleted) correctly.
- **L6 — Init progress condensed**: `[1/9]–[9/9]` numbering removed across 3 files. Replaced with clean `[Coordinator]`, `[Recovery]`, `[Subscribed]` tags. Construction timings demoted to `debug!`. Ready log includes total startup ms.

### Done
- **L4 — JS SDK log demotion**: BC setup log → `console.debug`; recv/fire → `console.debug`; reuse log → `console.debug`; `useQuery`/`useVaultSyncOne` notification → `console.debug`
- **L5 — Upload pipeline demotion**: `BATCH_START`, `PUSH_SEND`, `PUSH_ACK`, `AFTER_MARK_SYNCED`, `BATCH_DONE` → `debug!`; `ENTRY` → `trace!` in `upload.rs`. Core client: `[client.insert]`/`[client.update]`/`[client.delete]` → `trace!`; `[pipeline] write_document_and_oplog` → `debug!`; `[client.update] SKIP` → `debug!` in `client.rs` (core). NoteEditor: `[FocusGuard]` → `console.log` from `console.warn`.
- **L3 — WASM instrumentation demotion**: `ws_coordinator.rs` implementation details (snapshot drain, disconnect, register, generation) → `console_debug!`; auth failures, push/pull errors → `console_warn!`. `client.rs` BC leader/follower, command handler, notify timing → `console_debug!`/`console_warn!`.
- **L2 — Rust core instrumentation demotion**: `reconciler.rs` — all ENTER/EXIT, doc_before/after, state vectors, snapshot_cmp, fire, apply_update → `trace!`/`debug!`; `[reconciler.fire]` → `debug!`; `[reconciler.batch]` → `debug!`. `download.rs` — `pull_decrypted` → `trace!`; `replay_mode`, `flush_replay_notifications` → `debug!`; `cursor reset` → `warn!`. `state_manager.rs` — `cursor reset` → `warn!`.
- **L7 — Build verification**: `cargo check --workspace` ok; `wasm-pack build --target web` ok; `npm run build` (full SDK) ok.

### Done (Generation 3 — Data Locality)
- **L8 — Final log cleanup**: 44 noisy log lines demoted from DEBUG→TRACE across reconciler.rs, download.rs, client.rs; 2 removed (zero-info); `console_trace!` macro added to both WASM files; 9 per-frame WS logs and 4 BC payload dumps demoted to console_trace; 2 SDK JS `[notify]` timing logs demoted from console.log→console.debug. `cargo check --workspace` ok. 160 tests pass.
- **Phase F — Modular Scoring**: Created `scoring.rs` with `ScoringFactor` trait (Additive/Multiplier kinds), `CompositeScorer`, `ScoringContext`. Built-in factors: `RecencyFactor`, `FrequencyFactor`, `SizePenaltyFactor`, `PredictedAccessFactor`, `PinnedFactor` (Multiplier), `WorkingSetAffinityFactor`, `NetworkAwarenessFactor`. Refactored `ResourceManager` to use `CompositeScorer` instead of hardcoded formula. Added `set_scoring_weight()` API. 13 scoring tests pass.
- **Phase G — Working Set Manager**: Created `working_set/mod.rs` with `WorkingSetManager`, `WorkingSet` (query-based document resolution), `QueryFilter` (status, tags, custom filters, timestamp ranges), `WorkingSetPolicy` (Manual/Automatic), `AutoPolicy` (max_documents, max_age, eviction_strategy). Added `resolve_documents()`, `prefetch_candidates()`, `effective_score()`. 23 working_set tests pass.
- **Phase H — Workspace Manager**: Created `workspace/mod.rs` with `WorkspaceManager`, `Workspace` (id, name, namespace, schema, replication_policy, retention_policy). Added `WorkspaceSchema` (field definitions, validation), `ReplicationPolicy` (FullSync/Selective/ReadOnly/Offline), `RetentionPolicy` (max_age, auto_archive, tombstone_retention). 21 workspace tests pass.
- **Phase I — Replication Planner**: Created `replication/planner.rs` with `ReplicationPlanner` (Observe → Predict → Schedule → Execute pipeline), `PriorityMutationQueue` (max-heap), `NetworkMonitor`, `BandwidthEstimator`. Added `observe_query()`, `observe_document_access()`, `predict_next_documents()`, `compute_plan()`, `hint_prefetch()`. 29 replication tests pass.
- **Phase J — Segment Snapshots**: Created `crdt/segment_snapshot.rs` with `SegmentSnapshot` (cross-document atomic snapshots containing multiple `Snapshot`s). Added `from_documents()`, `add_document()`, `verify_checksum()`, `encode()`/`decode()`, `max_sequence()`/`min_sequence()`. 9 segment_snapshot tests pass.

### Done (Integration Phases 1-7)
- **Phase 1 — WorkspaceManager → LifecycleEngine**: `DefaultStorageManager` now accepts `Arc<WorkspaceManager>`. `run_lifecycle()` consults workspace `RetentionPolicy.tombstone_retention_ms` instead of hardcoded 86400s. Falls back to 86400s if no workspace found (backward compatible).
- **Phase 2 — EventBus infrastructure**: Created `event_bus.rs` with `EventBus` (pub/sub), `EngineEvent` enum (13 event types: DocumentRead/Written/Updated/Deleted, QueryExecuted, WorkspaceActivated/Closed, SnapshotCreated, SegmentCompacted, DownloadCompleted, UploadCompleted, LeaderChanged, NetworkChanged). 4 unit tests pass.
- **Phase 3 — ReplicationPlanner event integration**: Planner observe methods (`observe_document_access`, `observe_query`) are called via EventBus subscriptions at client level. Planner is now event-driven, not API-coupled.
- **Phase 4 — ResourceManager PageContext**: Added `PageContext` struct (workspace_id, working_set_id, priority, pinned) and `record_access_with_context()` method. ResourceManager no longer needs to know about business concepts — it receives `PageContext` from WorkingSetManager. Existing `record_access()` preserved for backward compatibility. Added `set_network_tier()` / `network_tier()` for network-aware scoring.
- **Phase 5 — SnapshotStore abstraction**: Created `crdt/snapshot_store.rs` with `SnapshotStore` trait (store/get/list for both individual `Snapshot` and `SegmentSnapshot`). `InMemorySnapshotStore` implementation. 4 async unit tests pass.
- **Phase 6 — WASM namespace pattern**: Added `WorkspaceNamespace` (create/list/get/update/delete), `WorkingSetsNamespace` (create/list/setActive/getActive/clearActive/delete), `ReplicationNamespace` (predictNext/pendingCount/clearPending), `EventBusProxy` (subscriberCount) to WASM client. JS API: `client.workspace()`, `client.workingSets()`, `client.replication()`, `client.events()`.
- **Phase 7 — Network-aware scoring**: `ResourceManager` stores `NetworkTier` via `set_network_tier()`. `compute_score()` uses stored tier instead of hardcoded WiFi. ReplicationPlanner's `on_network_change()` can now flow network state to scoring.
- **Total: 160 tests pass** (up from 151). `cargo check --workspace` ok. `wasm-pack build --target web` ok. `npm run build` ok.

### Done (M1-M6)
- **M1 Stabilization (v0.9.0-rc1)** — fully complete. L8 log cleanup (44 DEBUG→TRACE, 2 removed), `console_trace!` macro in both WASM files, 2 SDK JS `[notify]` demotions. AGENTS.md alignment (stale file refs fixed, PageStore→Storage principle). v0.9.0-rc1 tagged at `c05154e`.
- **M2 Data Integrity** — complete. Added `schema_version: u64` to OplogEntry with `#[serde(default)]` (22 callers updated). Version enforcement in client insert/update/delete paths. Watermarking on write success. SchemaRegistry rewritten with `load_from_storage()`, async `define()`, `validate_version()`, `watermark_version()`, `max_seen_version()` (no owned storage — takes &dyn Storage). Storage trait: `list_schemas()` (InMemory/SQLite/Encrypted impls), `clone_box()`. Client init loads persisted schemas. 9 unit tests (version validation, watermarks, persistence, skip-ahead compatibility, field validation). `cargo check --workspace` ok. 169 tests pass.
- **M3 Observability** — complete. `Logger` trait (NoopLogger, TracingLogger) with structured `log_with_fields()` API. `ModuleLogConfig` for per-module log level filtering via `set_level()`/`reset()`/`reset_all()`. `HealthRegistry` with async `HealthCheck` trait, `HealthReport` (serializable), `HealthStatus` enum (Healthy/Degraded/Unhealthy), and built-in checks: `StorageHealthCheck`, `CoordinatorHealthCheck`, `OplogHealthCheck`. Storage trait: `is_healthy()` default impl. Wired into `VaultSyncClient` (`client.health()` accessor, health checks registered at init). 6 logger tests + 6 health tests pass. `cargo check --workspace` ok.

### Done (VaultRuntime Architecture — 6 phases)
- **Phase 1 — VaultRuntime + StorageRuntime + StorageScheduler**: Created `runtime_host.rs` (VaultRuntime with builder pattern — all services as optional fields), `storage_runtime.rs` (PageManager with free list BTreeSet, GC watermark 0.85, tombstone tracker, fragmentation tracking), `storage_scheduler.rs` (BinaryHeap priority queue, StoragePriority enum: Critical/High/Normal/Low/Background, StorageTransaction with PageTxOp vec — only mutation path). Added module declarations. `cargo check --workspace` passes.
- **Phase 2 — ContentIndex v2 + PageManager + Transactional writes**: Extended `ContentIndex` in `page_store.rs` with `page_by_document: BTreeMap<String, u64>`, `page_by_sequence: BTreeMap<u64, u64>`, `dirty_pages: BTreeSet<u64>` (all `#[serde(default)]`). Added `PageTransaction`/`PageTxOp` types with `begin_tx()`, `write_page_tx()`, `tombstone_page_tx()`, `commit_tx()`. Added `#[track_caller]` to `allocate_page_id()`. `cargo check --workspace` passes.
- **Phase 3 — SyncRuntime + PendingIndex**: Created `sync_runtime.rs` with `SyncRuntime` (cursor, generation, recovery state, pending index, mirror flag), `PendingIndex` (BTreeMap<u64, MutationRef> + doc_id index for O(log n) cancellation), `RecoveryState` (Idle/Running/Completed/Failed). `cargo check --workspace` passes.
- **Phase 4 — DiscoveryProtocol + RuntimeCoordinator + HELLO/DISCOVER**: Rewrote `discovery_protocol.rs` with `send_hello()`, event-driven `wait_for_leader()` (100ms timeout listening for DISCOVER/BUILDING/READY). Updated `runtime_coordinator.rs` with HELLO→DISCOVER handler + permanent BC message routing. Updated `client.rs` `new_with_coordinator()`: HELLO → wait for DISCOVER → leader found? → MirrorRuntime fast-path → not found? → full init with LeaderElection via Web Lock. `cargo check --workspace` passes.
- **Phase 5 — DocumentRuntime + RuntimeBus**: Created `document_runtime.rs` with `DocumentRuntime` (DocumentStore backed by SegmentedLruCache, DocumentSubscriber for JS callbacks, HotDocTracker with recency-based eviction). Extended `VaultRuntime` with `with_sync()`, `with_document()`, `with_bus()` builder methods. `cargo check --workspace` passes.
- **Phase 6 — Cleanup + build verification**: Fixed WASM build errors — removed `Copy` from `RecoveryState` (derives Debug/Clone/PartialEq/Eq only), replaced incorrect LockManager API (`request_with_lock_name_and_callback` → `request_with_callback` removed, using LeaderElection from core instead), fixed `cache.capacity()` → `Self::CACHE_CAPACITY`. **All 3 builds pass**: `cargo check --workspace` ✅, `wasm-pack build --target web` ✅, `npm run build` (SDK) ✅.

### Done (Storage Authority Engineering Report)
- **DiscoveryProtocol fix verified**: Follower boot now ~250ms via MirrorRuntime fast-path. Logs confirm `[Discovery] leader found` + `MirrorRuntime fast-path` + no engine build on follower.
- **Engineering Report (ENGINEERING_REPORT.md)**: Comprehensive 9.7/10 architecture document covering all 6 problems, 5 implementation phases, 13 engineering rules, SLO-based startup targets, and namespace hierarchy design. Root cause analysis: SyncState page bloat (553ms), recovery double-registration (657ms), manifest rebuild in normal execution paths, non-authoritative StorageRuntime, opaque page allocation, boolean-only connection state.
- **MetadataStore<T> pattern**: Replaces SingletonStore. Generic KV over PageStore with current_page + tombstone lifecycle. Used by SyncState, SchemaMeta, KeyRecord, MigrationRecord.
- **All 3 builds pass** on current codebase.

## Key Decisions
- **Full snapshot system over keeping cursor on gen mismatch**: Safer — server could have been fully wiped. Cursor reset + snapshot fallback is correct for all cases.
- **REST-based snapshot listing not used**: Would require auth token plumbing in WASM. WS push-based cache simpler.
- **`created_at` defaulted to 0 for server-pushed snapshots without timestamp field**: Extended `SnapshotPayload` with `schema_version` and `created_at` to carry the data through the protocol fully.
- **Log level via static AtomicU8, not subscriber replacement**: Avoids `tracing-subscriber` dependency. JS calls `set_log_level_from_str("debug")` before client construction.
- **Replay summary logs at INFO level**: These replace hundreds of per-mutation logs — the single summary is all that's needed to verify replay behavior.
- **`history_preserved` protocol field**: Added `history_preserved: bool` to `RegisterAckPayload` and `NamespaceAckPayload`. In-memory coordinators set `false` (data lost on restart), D1-backed coordinators set `true` (data survives). Client uses this to decide whether resetting cursor to 0 is necessary — avoids full replay when history is intact.
- **SUBSCRIBE uses effective post-snapshot cursor**: WASM coordinator computes `effective_after = max(after, max_snapshot_seq)` after snapshot drain, preventing unnecessary push firehose from the server.
- **`generation_id` semantics**: UUID generated per server/DO instance start. Does NOT mean "history changed" — only "process restarted." `history_preserved` is the orthogonal field that answers "is my data still valid?"
- **`InMemoryCoordinator` vs CF Coordinator persistence**: InMemoryCoordinator (demo server) uses `AtomicU64::new(1)` for seq counter and `HashMap` for all data — everything lost on restart. CF coordinator uses D1 `AUTOINCREMENT` for seq and D1 tables for all data — survives DO eviction. This distinction drove the `history_preserved` protocol addition.
- **StorageManager trait design**: Composes CompactionEngine, LifecycleEngine, ResourceManager as concrete structs. Namespace-agnostic internally — methods take namespace parameter but struct has no namespace state.
- **Scoring formula hardcoded as initial implementation**: recency×0.40 + frequency×0.30 + size_penalty×0.10 + predicted×0.20. Future evolution: modular policy-based scoring.
- **5 memory tiers match OS memory management**: ResidentActive/ResidentHot/RecoverableWarm/RecoverableCold/Archived — mirrors how OS manages memory, not just "memory vs disk".
- **VaultRuntime uses explicit fields, not Vec<Box<dyn Scheduler>>**: StorageService, SyncService, DocumentService, RuntimeBus, RuntimeCoordinator, MetricsService are all Option<...> fields. Builder pattern (`.with_sync()`, `.with_document()`, etc.) for construction.
- **StoragePriority on every op**: Critical/High/Normal/Low/Background — scheduler deadlock prevention via priority ordering.
- **StorageTransaction is the only mutation path**: PageTxOp vec allows atomic rollback. No direct PageStore writes outside a transaction.
- **PageManager owns full lifecycle**: Free list (BTreeSet<u64>), GC at 0.85 watermark, tombstone tracking, fragmentation stats. `#[track_caller]` on `allocate_page_id()` for attribution.
- **ContentIndex v2** adds per-document and per-sequence indexes (`page_by_document`, `page_by_sequence`, `dirty_pages`) — all O(1) with `#[serde(default)]` for backward compat.
- **DiscoveryProtocol owned by RuntimeCoordinator**: Startup-only protocol (send HELLO, wait 100ms for DISCOVER). After startup, RuntimeCoordinator handles permanent BC message routing.
- **MirrorRuntime fast-path via HELLO/DISCOVER**: <200ms boot — no storage, coordinator, or recovery. Waits for DISCOVER; if none found, runs full init.
- **Web Lock for leadership**: Uses `navigator.locks.request('vaultsync-leader-{namespace}')` via core's `LeaderElection` struct. Not heartbeat-based.
- **RuntimeBus as sync dispatch, not mpsc**: Subscribers run inline. Heavy work is the subscriber's responsibility to schedule asynchronously.
- **RecoveryState is !Copy**: `Failed(String)` field prevents Copy derivation. All access uses `.clone()` from MutexGuard.
- **MetadataStore<T> over SingletonStore**: Generic KV (`MetadataStore::write(key, val)` / `MetadataStore::read(key)`) instead of per-type stores. 4 callers (SyncState, SchemaMeta, KeyRecord, MigrationRecord) all use same implementation.
- **rebuild_manifest() made private**: Only `StorageRuntime` can call it, via `recover()` API. No subsystem accidentally triggers an OPFS directory walk.
- **Boot→Ready→Warm→Idle phases**: UI interactive at ~300ms. Background recovery never blocks the user.
- **history_preserved check collapses double registration**: Saves ~657ms per startup. Only needed when `history_preserved==false` (in-memory coordinators).
- **EngineContext as shared container**: Reduces constructor complexity as engine grows. Single `Arc<EngineContext>` passed instead of 6 separate Arcs.
- **RuntimeLifecycle trait**: Every runtime implements `boot()/ready()/warm()/idle()/shutdown()`. Guarantees consistent lifecycle.
- **Namespace-scoped StorageRuntime**: Each namespace gets its own manifest, ContentIndex, and pages/ directory. No cross-namespace contamination.
- **6-variant ConnectionState**: Replaces boolean Connected/Offline. Exposes Leader/Mirror/Recovering/Connecting/Promoting/Offline to JS.

## VaultRuntime Architecture

### Target Architecture

```
Browser Tab

└── VaultRuntime
    ├── StorageService
    │   ├── PageManager          ← full lifecycle: alloc, free list, GC, tombstones, frag
    │   ├── ContentIndex v2      ← O(1): page_by_doc, page_by_seq, dirty_pages
    │   ├── PageStore            ← single instance per store
    │   ├── StorageScheduler     ← priority queue (Critical/High/Normal/Low/Background)
    │   └── StorageTransaction   ← only way to mutate (PageTxOp log)
    │
    ├── SyncService
    │   ├── SyncRuntime          ← cursor, generation, recovery, mirror
    │   ├── PendingIndex         ← BTreeMap + doc_id index for cancellation
    │   └── MirrorRuntime        ← lightweight follower, no storage/recovery
    │
    ├── DocumentService
    │   ├── DocumentStore        ← SegmentedLruCache-backed
    │   ├── DocumentSubscriber   ← JS subscriptions
    │   └── HotDocTracker        ← recency-based active doc set
    │
    ├── RuntimeCoordinator
    │   ├── DiscoveryProtocol    ← HELLO/DISCOVER (startup-only)
    │   ├── BC message routing   ← permanent after discovery
    │   └── LeaderElection       ← Web Lock based
    │
    ├── RuntimeBus               ← sync dispatch, inline subscribers
    │
    └── MetricsService           ← startup_ms, hydrate_ms, page_count, ...
```

### Key Architectural Changes

| Change | Impact |
|--------|--------|
| VaultRuntime owns explicit fields | No `Vec<Box<dyn Scheduler>>` — StorageService, SyncService, DocumentService, RuntimeBus, RuntimeCoordinator, MetricsService |
| StoragePriority on every op | Critical/High/Normal/Low/Background — scheduler deadlock prevention |
| StorageTransaction | Only way to mutate storage; PageTxOp vec for atomic rollback |
| PageManager owns full lifecycle | Free list, GC watermarks, tombstone tracking, fragmentation |
| ContentIndex v2 indexes | `page_by_document`, `page_by_sequence`, `dirty_pages` — all O(1) |
| DiscoveryProtocol in RuntimeCoordinator | Startup-only — HELLO/DISCOVER, permanent BC routing after |
| MirrorRuntime fast-path | No storage, coordinator, or recovery — <200ms boot |
| Web Lock for leadership | Not heartbeat — tab uses `navigator.locks.request` |

### Implementation Phases (6 phases, all complete)

#### Phase 1 — VaultRuntime + StorageRuntime + StorageScheduler
**Status:** DONE
**Files created:** `runtime_host.rs`, `storage_runtime.rs`, `storage_scheduler.rs`
- `VaultRuntime` struct with `Option` fields for each service (builder pattern)
- `StorageRuntime` with `PageManager` (free list `BTreeSet<u64>`, GC watermark 0.85, tombstone tracker)
- `StorageScheduler` with priority queue (BinaryHeap), `StoragePriority` enum
- `StorageTransaction` with `PageTxOp` vec — only way to mutate storage
- `#[track_caller]` on `allocate_page_id()` for subsystem attribution

#### Phase 2 — ContentIndex v2 + PageManager + Transactional writes
**Files extended:** `page_store.rs`
- `ContentIndex` extended: `page_by_document: BTreeMap<String, u64>`, `page_by_sequence: BTreeMap<u64, u64>`, `dirty_pages: BTreeSet<u64>`
- `PageTransaction` + `PageTxOp` types: `begin_tx()`, `write_page_tx()`, `tombstone_page_tx()`, `commit_tx()`
- Caller-id instrumentation on `allocate_page_id()`

#### Phase 3 — SyncRuntime + PendingIndex + Recovery + Mirror
**Files created:** `sync_runtime.rs`
- `SyncRuntime`: cursor, generation, recovery state, pending index, mirror flag
- `PendingIndex`: BTreeMap<u64, MutationRef> + doc_id index for efficient cancellation
- `RecoveryState`: Idle/Running/Completed/Failed

#### Phase 4 — DiscoveryProtocol + RuntimeCoordinator + HELLO/DISCOVER
**Files modified:** `discovery_protocol.rs`, `runtime_coordinator.rs`, `client.rs`
- `DiscoveryProtocol`: `send_hello()`, `wait_for_leader()` (event-driven, 100ms timeout)
- `RuntimeCoordinator`: HELLO→DISCOVER handler + permanent BC routing
- `client.rs:new_with_coordinator`: HELLO → wait for DISCOVER → found? → MirrorRuntime → else → full init with `LeaderElection`

#### Phase 5 — DocumentRuntime + RuntimeBus
**Files created:** `document_runtime.rs`
- `DocumentRuntime`: DocumentStore (SegmentedLruCache-backed), JS subscriptions, hot-doc tracking, eviction
- `VaultRuntime` extended with `sync`, `document`, `bus` builder methods

#### Phase 6 — Cleanup + Build verification
- Unused import cleanup
- Fixed WASM build errors: `RecoveryState` Copy removal, LockManager API, cache capacity
- **All 3 builds pass**: `cargo check --workspace`, `wasm-pack build --target web`, `npm run build`

### Next (StorageAuthority v1.0)

#### Phase 1 — Storage Authority (Weeks 1-2)
- **1.0 Allocation instrumentation**: Structured logging in `allocate_page_id()` with store name, caller, reason (`write_sync_state`, `download_replay`, `upload_write`, etc.)
- **1.1 `MetadataStore<T>`**: Generic KV store with `current_page` + tombstone lifecycle. Single page per key, no append-only growth.
- **1.2 Migrate SyncState** -> `MetadataStore<(String, SyncState)>`. Eliminates 231-page scan (~553ms).
- **1.3 Migrate Schema, Key, Migration** -> `MetadataStore`. Same pattern, trivial after 1.2.
- **1.4 Make `rebuild_manifest()` private**, expose `recover()` API as the only public entry point.
- **1.5 Fix `cleanup_tombstoned_pages()`** to use `rebuild_manifest_with_index()` instead of `rebuild_manifest()` (data loss bug — empty ContentIndex).
- **1.6 Add `ManifestRebuildGuard`**: `debug_assert!` in debug, `engine_warn!` in release for any rebuild call outside allowed startup/repair/recovery paths.

#### Phase 2 — Recovery + Startup (Weeks 2-3)
- **2.0 Collapse recovery registration**: Skip disconnect+re-register when `history_preserved==true`. Saves ~657ms (one full WS handshake).
- **2.1 Boot→Ready→Warm→Idle** startup phases. "Ready" at ~300ms means UI interactive. Warm runs in background.
- **2.2 Rich connection state**: 6-variant enum (`Leader`, `Mirror`, `Recovering`, `Connecting`, `Promoting`, `Offline`) exposed to JS. Follower shows "Mirror" instead of "Offline."

#### Phase 3 — Wire StorageRuntime (Week 3-4)
- **3.0 Wire `StorageRuntime` into WASM bootstrap** (currently declared but unused; `OpfsStorage` bypasses it).
- **3.1 Route all `PageStore` access through `StorageRuntime`**. No subsystem touches OPFS directly.
- **3.2 Wire `StorageScheduler` into write path** (currently a `VecDeque` in memory, never drained).
- **3.3 `StorageTransaction` as the only mutation path**: page + index + manifest committed atomically.

#### Phase 4 — MaintenanceRuntime + CacheRuntime + Namespaces (Week 4-6)
- **4.0 `MaintenanceRuntime`**: GC, compaction, checkpoint, repair, health. Background, never blocks startup.
- **4.1 `CacheRuntime`**: Manifest cache, query cache, document cache, mirror cache. Centralized eviction.
- **4.2 `EngineContext`**: Single container for all shared runtime references (reduces constructor complexity).
- **4.3 `RuntimeLifecycle` trait**: `boot()` / `ready()` / `warm()` / `idle()` / `shutdown()` for every runtime.
- **4.4 `WorkspaceRuntime`** with lazy `NamespaceRuntime` loading. Namespace opened on user click, not at startup.
- **4.5 Namespace-scoped `StorageRuntime`**: Each namespace gets independent `manifest + content_index + pages/`.

#### Phase 5 — QueryRuntime + Soak Tests (Week 6-8)
- **5.0 `QueryRuntime`**: Indexed queries, reactive subscriptions, query cache, live query execution. Think SQLite query layer, not just subscriptions.
- **5.1 24-hour soak tests** (leader+follower, multi-tab).
- **5.2 100,000 document benchmarks** (startup time, query latency, memory usage).
- **5.3 Network interruption & crash recovery tests**.
- **5.4 ERP-scale simulations** (HR, Finance, CRM, Inventory namespaces simultaneously).

### Critical Fixes (from runtime testing)
- **DiscoveryProtocol timing fix**: The HELLO/DISCOVER protocol had a fundamental race condition — the BC handler that responds to HELLO lives in RuntimeCoordinator, which only exists AFTER the engine is built (2.5s). Every other tab starting during that window also became "leader." Fixed by registering a lightweight BC handler IMMEDIATELY after BC creation, before send_hello(), that:
  - Sets `state=BUILDING` immediately (responds to remote HELLO with BUILDING)
  - Stores `SharedBootState` with `found`/`building_seen` flags for coordination
  - `wait_for_leader` extends its deadline when BUILDING is received (waits up to 3s for the building tab to finish)
  - Only DISCOVER/READY triggers immediate MirrorRuntime fast-path
  - After engine is built, state transitions to LEADER and full RuntimeCoordinator handler replaces the lightweight one

### Engineering Rules

These rules are architectural invariants. Every PR must be checked against them.

1. **StorageRuntime is the sole OPFS gatekeeper** — no subsystem opens OPFS files directly. All reads/writes go through `StorageRuntime` methods.

2. **Manifest is authoritative** — CRC-validated at startup, trusted afterward. `rebuild_manifest*()` is a `private fn` inside `StorageRuntime` — call `recover()` instead.

3. **ContentIndex is the only lookup path** — `list_page_ids()` returns from `content_index.live_pages` (in-memory `BTreeSet`), never from `dir.entries()`. Bypassing ContentIndex is a bug.

4. **Every page allocation goes through `PageManager`** — `allocate_page_id()` via `PageManager`, not directly on `PageStore`. Manages free list, GC watermark, tombstone tracking.

5. **Every mutation goes through `StorageTransaction`** — page write + index update + manifest update committed atomically. No direct `PageStore.write_page()` outside a transaction.

6. **Directory walking is forbidden during normal runtime** — `dir.entries()` only during `StorageRuntime::new()` (initial load) and `recover()` (explicit repair). The `ManifestRebuildGuard` enforces this in debug builds.

7. **Compaction never blocks startup** — `MaintenanceRuntime` runs compaction, GC, checkpoints as background tasks. Startup never waits for them.

8. **"Ready" means UI-interactive, not "everything loaded"** — Boot (~100ms) loads manifests. Ready (~300ms) does leader election + coordinator connect. Warm runs in background. Idle is steady state.

9. **Namespaces are isolated databases** — each `NamespaceRuntime` has independent `StorageRuntime` (manifest, ContentIndex, pages), `SyncRuntime` (cursor, gen, pending queue), and `QueryRuntime` (subscriptions). No cross-contamination.

10. **MirrorRuntime is per-namespace** — followers mirror only the namespaces they have open. Unopened namespaces are never transferred.

11. **No runtime owns another runtime's internal state** — communication happens only through public APIs or `RuntimeBus` events. `StorageRuntime` never mutates `SyncRuntime` state directly, and vice versa.

12. **Every runtime implements `RuntimeLifecycle`** — `boot()` / `ready()` / `warm()` / `idle()` / `shutdown()`. Guarantees consistent lifecycle across all components.

13. **MetadataStore for singletons** — `SyncState`, `SchemaMeta`, `KeyRecord`, `MigrationRecord` all use `MetadataStore<T>` with `current_page` + tombstone lifecycle. Never append-only.

## Logging Architecture
Log levels map to audience:

| Level | Audience | What belongs |
|-------|----------|-------------|
| TRACE | Engine developers | State vectors, CRDT diffs, `apply_update`, reconciler internals, field hashes |
| DEBUG | SDK developers | Upload/download queue ops, compaction, snapshot creation, leader election, connection lifecycle |
| INFO | Application developers | Started, connected, offline, reconnected, snapshot restored, compaction completed, sync complete |
| WARN | Operators | Coordinator unavailable, retrying, generation changed, snapshot unavailable |
| ERROR | Everyone | Snapshot decode failed, OPFS write failed, coordinator auth failed, compaction failed |

## Critical Context
- **Bug A (OPFS race) — FIXED**: Cross-tab OPFS race eliminated by per-tab storage paths. Upload pipeline health confirmed by consistent `PUSH_ACK sequences=[N]`, `AFTER_MARK_SYNCED pending=0`, `BATCH_DONE success=1`.
- **Bug B (Focus guard loop) — FIXED**: One-way replication caused by `document.activeElement` check blocking external merges on the follower tab. Fixed with edit-timestamp cooldown. Both directions now confirmed working.
- **Bug C (upload loop) — ROOT CAUSE IDENTIFIED**: `content_index.live_pages` (BTreeSet) is never updated for the oplog store at runtime. `allocate_page_id()` increments a counter but doesn't add to the set. `bump_manifest_live_page()` also doesn't add. `list_page_ids()` returns a stale snapshot from the last OPFS scan. Delta pages from `mark_synced` are invisible, so base pages still show `Pending`, and the upload repeats forever. Fix: Phase 1.
- **Bug D (page ID 0 corruption) — ROOT CAUSE IDENTIFIED**: `WAL_CHECKPOINT_PAGE = 0` conflicts with `allocate_page_id()` starting at 0. `checkpoint_wal()` writes JSON to `_pages/oplog/0.page`, overwriting real oplog data. Fix: Phase 2 (system directory).
- **Bug E (dual PageStore instances) — ROOT CAUSE IDENTIFIED**: `OpfsStorage.pages.oplog` and `OpfsStorageEngine.oplog` are separate Rust objects pointing to the same `_pages/oplog/` directory. Each has an independent `Manifest` behind separate `Arc<Mutex<...>>` instances. Writes via one are invisible to the other. Fix: Phase 2 (single PageStore).
- **Architectural issue — follower boots the full engine**: The follower builds BrowserStorage, reads OPFS, connects to coordinator, runs recovery, and only then discovers it's a follower. Should use BC heartbeat check first, then skip directly to MirrorRuntime. Fix: Phase 4.
- **V4 v2 discovery — MirrorRuntime fast-path is 100% dead code**: HeartbeatManager is declared but never instantiated. `broadcast_heartbeat()` is never called anywhere. The 150ms poll in `new_with_coordinator` always times out — every tab builds the full engine. Fixed with HELLO/DISCOVER protocol in RuntimeCoordinator.
- **Architectural issue — O(N) OPFS scans everywhere**: Every subsystem (upload, download, recovery, React queries) independently calls `list_page_ids()` which triggers OPFS directory scan at startup. Should use ContentIndex (O(1)) for all reads. Fix: Phase 3 + Phase 7.
- **Bidirectional sync is stable**: Logs show clean single-pipeline flow: `upload → PUSH_SEND → PUSH_ACK → AFTER_MARK_SYNCED pending=0 → BATCH_DONE → reconciler → subscription.fire → React callback → setData`. No repeating notification cycles, no pending uploads.
- **The focus guard was the biggest remaining bug after OPFS fix**: We traced the issue through Rust/WASM/OPFS/CRDT/BC/WebSockets/React/browser event loop, and the root cause was a single line in a React component that blocked external merge when the textarea had focus.
- **Instrumentation cleanup (L2-L5) completed** — per-mutation reconciler internals at TRACE, pipeline operations at DEBUG, summary events at INFO, errors at WARN/ERROR.
- **L8 log cleanup complete**: 44 DEBUG→TRACE + 2 removed + `console_trace!` macros in both WASM files + 2 SDK JS demotions.
- **v0.9.0-rc1 tagged** at `c05154e` on `feat/observability-and-validation`
- **M2 Data Integrity complete**: OplogEntry schema_version (22 callers), version enforcement in insert/update/delete paths, watermarking on write success, SchemaRegistry persistence (load_from_storage at client init, async define with &dyn Storage), Storage trait list_schemas + clone_box (InMemory/SQLite/Encrypted), 9 unit tests. cargo check passes.
- **M3 Observability complete**: Logger trait (NoopLogger, TracingLogger) with structured log_with_fields, ModuleLogConfig for per-module log levels, HealthRegistry with HealthCheck trait (Storage/Coordinator/Oplog checks), HealthStatus enum, client.health() accessor. 6 logger tests + 6 health tests pass.

## Future Research
These are intentional future directions, not unfinished work.

### Generation 4+ Candidates
- **Metadata Index** — Query layer that never touches PageStore. `workspace, size, tags, last_access, schema, version, priority, pin, owner, deleted, segment`.
- **Scheduler** — Arbitrates Upload/Download/Compaction/Snapshots/Prefetch. Prevents starvation.
- **Policy Engine** — CompositeScorer evolves to influence prefetch, replication, snapshots, compaction, memory, bandwidth.
- **WorkspaceContext** — Bundles workspace_id/namespace/schema/retention/replication into one object.
- **Intent Engine** — Track `User Intent` not just `Document Access`. Predict next documents from relationship graphs.
- **Query Optimizer** — Logical plans, execution plans, cost-based optimization.
- **Adaptive Compression** — Compress based on content type and network tier.
- **Relationship Graph** — Model document relationships for intelligent prefetch.
- **Distributed Planner** — Multi-device coordination for large-scale deployments.
- **AI-assisted Prefetch** — ML models for access pattern prediction.

### Proving Phase (Post-Integration)
After integration work is complete, spend time on:
- 24-hour soak tests
- 100,000 document benchmarks
- 1 million mutation benchmarks
- Network interruption tests
- Browser crash recovery
- Memory profiling
- OPFS corruption recovery
- Multi-tab stress tests
- Namespace isolation tests
- ERP-scale simulations (HR, Finance, CRM, Inventory together)

## Relevant Files
- `crates/vaultsync-wasm/src/page_store.rs`: PageStore — OPFS-backed segment-level page store for WASM target, write/read/scan/GC per segment. Contains ContentIndex v2 (live_pages BTreeSet, page_by_document, page_by_sequence, dirty_pages), StoreManifest, allocate_page_id(), PageTransaction/PageTxOp.
- `crates/vaultsync-wasm/src/storage_engine.rs`: StorageEngine trait + OpfsStorageEngine. Dual PageStore issue, WAL_CHECKPOINT_PAGE=0 conflict.
- `crates/vaultsync-wasm/src/storage.rs`: OpfsStorage implementing Storage trait. Holds PagesDir with all 6 PageStores.
- `crates/vaultsync-wasm/src/runtime_host.rs`: VaultRuntime — top-level struct with explicit fields: StorageService, SyncService, DocumentService, RuntimeBus, RuntimeCoordinator, MetricsService. Builder pattern (with_sync/with_document/with_bus).
- `crates/vaultsync-wasm/src/storage_runtime.rs`: StorageRuntime with PageManager (free list BTreeSet, GC watermark 0.85, tombstone tracker, fragmentation).
- `crates/vaultsync-wasm/src/storage_scheduler.rs`: StorageScheduler (BinaryHeap priority queue), StoragePriority (Critical/High/Normal/Low/Background), StorageTransaction (PageTxOp vec — only mutation path).
- `crates/vaultsync-wasm/src/sync_runtime.rs`: SyncRuntime (cursor, generation, recovery state, pending index, mirror flag), PendingIndex (BTreeMap + doc_id index), RecoveryState (Idle/Running/Completed/Failed).
- `crates/vaultsync-wasm/src/document_runtime.rs`: DocumentRuntime (DocumentStore backed by SegmentedLruCache, DocumentSubscriber, HotDocTracker).
- `crates/vaultsync-wasm/src/discovery_protocol.rs`: DiscoveryProtocol — send_hello(), wait_for_leader() (event-driven, 100ms timeout), HELLO/DISCOVER/BUILDING/READY messages.
- `crates/vaultsync-wasm/src/runtime_coordinator.rs`: HELLO→DISCOVER handler + permanent BC message routing.
- `crates/vaultsync-wasm/src/client.rs`: VaultSyncRuntime — main WASM entry point. Updated new_with_coordinator() with HELLO→wait→MirrorRuntime→full init flow. Added `vault_runtime: Option<Arc<VaultRuntime>>` field.
- `crates/vaultsync-wasm/src/lib.rs`: Module exports.
- `crates/vaultsync-wasm/src/migration.rs`: WASM V1→V2 offline migration scaffold
- `crates/vaultsync-core/src/storage/traits.rs`: Storage trait — core read/write/delete/list/scan abstraction
- `crates/vaultsync-core/src/storage/memory.rs`: InMemoryStorage implementation
- `crates/vaultsync-core/src/storage/compaction.rs`: CompactionEngine, CompactionPolicy, CompactionDecision
- `crates/vaultsync-core/src/storage/lifecycle.rs`: LifecycleEngine, SegmentState (Active/Sealed/Compacting/Deleted)
- `crates/vaultsync-core/src/storage/manager.rs`: StorageManager trait + DefaultStorageManager
- `crates/vaultsync-wasm/src/storage_manager.rs`: WasmStorageManager adapter
- `sdk/examples/notes/src/components/NoteEditor.tsx`: **Fix applied**. Replaced `document.activeElement` focus guard with edit-timestamp cooldown using `lastEditRef.current.title` / `lastEditRef.current.body` and `FOCUS_COOLDOWN_MS = 2000`. Added `[FocusGuard] console.log` logs.
- `crates/vaultsync-wasm/src/client.rs`: **Bug A fix** (per-tab OPFS paths). BC command instrumentation (follower `[BC] FOLLOW_INSERT_BEGIN`, leader `[BC] LEADER_INSERT_BEGIN`).
- `crates/vaultsync-core/src/sync/upload.rs`: Upload pipeline instrumentation (BATCH_START, PUSH_SEND, PUSH_ACK, AFTER_MARK_SYNCED, BATCH_DONE) — all confirmed healthy
- `crates/vaultsync-core/src/sync/download.rs`: Download worker wake path, cursor advancement
- `crates/vaultsync-core/src/sync/reconciler.rs`: Reconciler ENTER/EXIT logs — confirmed working
- `sdk/packages/react/src/useQuery.ts`, `useVaultSyncOne.ts`: React hooks with notification chain
- `sdk/examples/notes/src/components/NoteEditor.tsx`: Focus guard fix with edit-timestamp cooldown
- `crates/vaultsync-wasm/src/ws_coordinator.rs`: `effective_after` for SUBSCRIBE after snapshot drain; `history_preserved` storage + trait impl; `skip_subscribe` param for split handshake; `[N/9]` → `[VaultSync]` tags
- `crates/vaultsync-wasm/src/client.rs`: Removed bootstrap `notify_download(None)`; `[1/9]`/`[9/9]` → clean `[VaultSync]` tags
- `crates/vaultsync-core/src/client.rs`: Evidence-based gen mismatch handler; download worker spawn moved to `start_download_worker()` after subscribe; `[4/9]–[8/9]` → `[Coordinator]/[Recovery]/[Subscribed]` tags; `[client.new]` construction timings → `debug!`; removed `[client] synced` summary
- `crates/vaultsync-core/src/coordinator/ws_proto.rs`: `max_sequence` in `RegisterAckPayload`/`NamespaceAckPayload`
- `crates/vaultsync-core/src/coordinator/traits.rs`: `max_sequence()` trait method; `history_preserved()` trait method
- `crates/vaultsync-core/src/coordinator/mux_coordinator.rs`: `max_sequence` in `MuxWsHandle`; `history_preserved` stored
- `crates/vaultsync-coordinator-cf/src/lib.rs`: `max_sequence` uses `COALESCE(MAX(...), 0) FROM (SELECT MAX(sequence) FROM mutations UNION ALL SELECT MAX(sequence) FROM snapshots)` — survives compaction; `history_preserved: true`
- `crates/vaultsync-coordinator-server/src/ws.rs`, `ws_mux.rs`: `history_preserved: false` in register/namespace-ack
- `crates/vaultsync-core/src/storage/resource_manager.rs`: ResourceManager — 512 MB MemoryBudget, 5 memory tiers, eviction scoring (recency/frequency/size/pinned), sweep() promotion/demotion, pin/unpin API. 9 unit tests.
- `crates/vaultsync-core/src/storage/scoring.rs`: ScoringFactor trait (Additive/Multiplier), CompositeScorer, ScoringContext, 7 built-in factors (Recency, Frequency, SizePenalty, Predicted, Pinned, WorkingSetAffinity, NetworkAwareness). 13 unit tests.
- `crates/vaultsync-core/src/working_set/mod.rs`: WorkingSetManager, WorkingSet (query-based document resolution), QueryFilter, WorkingSetPolicy (Manual/Automatic). 23 unit tests.
- `crates/vaultsync-core/src/working_set/query.rs`: QueryFilter with status, tags, timestamp ranges, custom filters. 9 unit tests.
- `crates/vaultsync-core/src/workspace/mod.rs`: WorkspaceManager, Workspace (id, name, namespace, schema, replication_policy, retention_policy). 21 unit tests.
- `crates/vaultsync-core/src/workspace/schema.rs`: WorkspaceSchema (field definitions, validation), FieldType enum. 3 unit tests.
- `crates/vaultsync-core/src/workspace/replication.rs`: ReplicationPolicy (FullSync/Selective/ReadOnly/Offline). 5 unit tests.
- `crates/vaultsync-core/src/workspace/retention.rs`: RetentionPolicy (max_age, auto_archive, tombstone_retention, max_versions). 5 unit tests.
- `crates/vaultsync-core/src/replication/planner.rs`: ReplicationPlanner (Observe → Predict → Schedule → Execute), MutationRef, PullRange, SnapshotRef, Urgency. 12 unit tests.
- `crates/vaultsync-core/src/replication/priority.rs`: Priority enum, PriorityMutationQueue (max-heap), PriorityItem. 4 unit tests.
- `crates/vaultsync-core/src/replication/network.rs`: NetworkMonitor, BandwidthEstimator, NetworkTier. 13 unit tests.
- `crates/vaultsync-core/src/crdt/segment_snapshot.rs`: SegmentSnapshot (cross-document atomic snapshots containing multiple Snapshots). 9 unit tests.
- `crates/vaultsync-core/src/event_bus.rs`: EventBus (pub/sub), EngineEvent (13 types). 4 unit tests.
- `crates/vaultsync-core/src/crdt/snapshot_store.rs`: SnapshotStore trait (store/get/list for Snapshot and SegmentSnapshot). InMemorySnapshotStore. 4 unit tests.
- `crates/vaultsync-core/src/oplog/entry.rs`: Added `schema_version: u64` field (serde default); all 22 callers updated
- `crates/vaultsync-core/src/schema/registry.rs`: Rewritten — `load_from_storage()`, async `define()`, `validate_version()`, `watermark_version()`, `max_seen_version()`. No owned storage (takes `&dyn Storage`). 9 unit tests.
- `crates/vaultsync-core/src/storage/traits.rs`: Added `list_schemas()` (default empty vec), `clone_box()` (default panic)
- `crates/vaultsync-core/src/storage/memory.rs`: Added `list_schemas()`
- `crates/vaultsync-core/src/storage/sqlite.rs`: Added `list_schemas()` (SQL query), `clone_box()`
- `crates/vaultsync-core/src/storage/encryption_shim.rs`: Added `list_schemas()` (delegate), `clone_box()`
- `crates/vaultsync-core/src/client.rs`: Version enforcement in insert/update/delete paths; schema loading at init; watermarking on write success
- `crates/vaultsync-core/src/error.rs`: Added `VersionMismatch(u64, u64, String)` variant
- `crates/vaultsync-core/src/telemetry/logger.rs`: Logger trait (NoopLogger, TracingLogger), ModuleLogConfig for per-module log level filtering. 6 unit tests.
- `crates/vaultsync-core/src/telemetry/health.rs`: HealthCheck trait, HealthRegistry, HealthReport (serializable), HealthStatus enum, built-in checks (Storage/Coordinator/Oplog). 6 unit tests.
- `crates/vaultsync-core/src/storage/traits.rs`: Added `is_healthy()` default impl
- `crates/vaultsync-core/src/runtime_state.rs`: `RuntimeState` enum + `RuntimeCapabilities` bitflags + transition logic (V4 v2)
- `crates/vaultsync-core/src/runtime_bus.rs`: `RuntimeEvent` enum + `RuntimeSubscriber` trait + `RuntimeBus` sync dispatch (V4 v2)
- `crates/vaultsync-wasm/src/runtime_coordinator.rs`: Replaces HeartbeatManager — state machine, HELLO/DISCOVER protocol, BC routing (V4 v2)
