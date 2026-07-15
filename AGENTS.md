# Anchored Summary

## Goal
- Complete **StorageAuthority v1.0**: VaultRuntime as a mature embedded database, not just a sync engine.

### Measurable Targets
| Metric | Current | Target |
|--------|---------|--------|
| Leader startup (cold) | ~5200ms | ~2500ms |
| Follower startup | ~16ms (confirmed in production logs) | ~100ms |
| OPFS directory walks/startup | 9 | 1 (initial manifest load) |
| SyncState pages | 231+ | 1 (MetadataStore singleton) |
| WS handshakes/startup | 2 (register + reconnect) | 1 |
| Connection state granularity | boolean | 6-variant enum |
| Manifest rebuild calls/startup | 3+ | 0 (private repair API only) |
| All 3 builds | passing | passing |

### Architecture Pillars
1. **StorageRuntime** — single OPFS gatekeeper. `MetadataRuntime` for singleton metadata. `ContentIndex` (forward + reverse) as sole lookup path. `PageManager` for full lifecycle. `StorageTransaction` as the only mutation path. `rebuild_manifest()` made **private** — call `recover()` instead.
2. **Boot→Ready→Warm→Idle** startup phases — UI interactive at ~300ms, background sync afterwards. Boot classifies `StorageHealth` before deciding expensive work.
3. **NamespaceManager** — owns `HashMap<NamespaceId, NamespaceRuntime>`. Each namespace is an isolated mini-database with independent storage, sync, metrics, and compaction.
4. **MaintenanceRuntime** — GC, compaction, checkpoint, repair, health monitoring. Never blocks startup. StorageRuntime exposes `compact()`, `cleanup()`, `verify()` — MaintenanceRuntime schedules them.
5. **SchedulerRuntime** — unified priority system over `StorageQueue`, `UploadQueue`, `DownloadQueue`, `MaintenanceQueue`, `RetryQueue`.
6. **MetadataRuntime** — mini metadata database over `MetadataStore<T>`. Owns `SyncState`, `Keys`, `SchemaMeta`, `MigrationRecord`, plus future metadata (presence, workspace, encryption, replication).
7. **Engineering Rules** — 16 enforceable invariants (see below). Every runtime communicates through interfaces, never concrete implementations.

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
Boot → Storage → Health → Discovery (Web Lock ifAvailable)
    ↓
Lock acquired? → I'm leader
    │                 ├── Storage opens (PageManager)
    │                 ├── Metadata/checkpoint loads
    │                 ├── Workspace loads
    │                 ├── Working Sets restore
    │                 ├── Coordinator connects
    │                 ├── Recovery runs
    │                 ├── Announce LEADER_READY via BC
    │                 └── Ready
    │
    └── Lock held? → MirrorRuntime
                        ├── Listen for LEADER_READY
                        ├── Request initial sync via SYNC
                        ├── Receive MUTATION stream → DONE
                        └── Ready
    ↓
Query executes
    ↓
React → QueryCache → WASM → WorkingSet → ContentIndex lookup (O(1))
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

### Startup Phases (Boot → Storage → Health → Discovery → Leadership → Sync → Ready → Warm → Maintenance)
```
Phase          Entry              Exit              Timeout   Metrics
─────          ─────              ────              ───────   ───────
Boot           Init begins        Storage open      N/A       boot_ms
Storage        OPFS manifest      CRC validated     500ms     storage_ms
Health         Classify health    Healthy/Needs*    200ms     health_status
Discovery      Web Lock request   Leader/Mirror     200ms     discovery_ms
Leadership     Acquire/confirm    Leading/Follower  500ms     leader_ms
Sync           Pull mutations     Cursor matched    5s        sync_ms
Ready          UI interactive     Background tasks  300ms     ready_ms
Warm           Replay/buffer      Queue drained     30s       warm_ms
Maintenance    GC/compaction      Tick complete     N/A       maintenance_ms
```
- **Boot**: WASM init, create BC channel, instantiate storage root handle.
- **Storage**: Read manifests for all stores, CRC validate, load ContentIndex into memory.
- **Health**: Classify as Healthy/Degraded/NeedsRepair/ReadOnly/Corrupt. Skip expensive work if Healthy.
- **Discovery**: `navigator.locks.request(name, {ifAvailable: true})` — immediate. If lock acquired → leader. If null → mirror.
- **Leadership** (leader): Leader election confirmed, coordinator connect begins.
- **Sync** (leader): Register with coordinator, subscribe, pull missed mutations.
- **Ready**: UI interactive at ~300ms.
- **Warm** (background): Download replay, snapshot catch-up, pending queue process.
- **Maintenance** (background): Compaction, GC, health probes, metrics aggregation.

That's how people understand systems — not by reading modules, but by following a request.

## Constraints & Preferences
- WASM build target (`wasm-pack build --target web`)
- TypeScript SDK build via `npm run build` in `sdk/`
- All Rust crates must compile (`cargo check --workspace`)
- Compaction methods must use `js_sys::Date::now()` for timestamps (not `crate::time_utils`)

## Progress
### Done (Session 3 — FSM + StorageHealth + Sprint A)
- **Sprint A — Correctness (Storage Transaction Model)**: Fixed Bug C root cause — every page write now atomically updates ALL ContentIndex indexes including `page_by_id`. `write_page_raw` is now a pure OPFS writer (no manifest updates). `commit_page_write()` replaces `bump_manifest_live_page()` and updates `live_pages` + `page_by_id` + (optionally) `entries`/`page_by_document`/`page_by_sequence`. `allocate_page_id()` and `commit_allocated_page_id()` add stub `page_by_id` entries so `repair_orphans()` no longer tombstones legitimate delta pages. `remove_by_page_id()` handles stub entries correctly. `split_page()` uses `write_page_tx` + `commit_tx` for atomic ContentIndex updates. `commit_tx()` Write ops with `None` key properly track pages in all indexes instead of tombstoning them.
- All 3 builds pass (`cargo check --workspace`, `wasm-pack build --target web`, `npx tsc --noEmit`).

### Done (Session 3 — FSM + StorageHealth)
- **Phase 5a — Demote mirror recreation**: `demote()` now async, fully recreates MirrorRuntime + BC handler + LeaderElection(Waitt) instead of setting a flag. Fixes limbo state where demoted tab had `Capability=Follower` but `mirror=None`.
- **Phase 5b — LeaderElection identity fix**: `promote()` releases old Wait-mode instance before creating new client. Rust BC handler's synchronous `cm.set(Follower)` removed — only JS async `demote()` path fires, ensuring full mirror recreation runs.
- **Phase 5c — Broadcast ordering**: `BC_MSG_SEQ` static counter in `session_protocol.rs`. 6-field MUTATION format (`MUTATION|bc_seq|tab|doc_id|record_id|payload`) with backward-compatible parser. bc_seq included in MUTATION, SYNC_BEGIN, SYNC_DONE, LEADER_READY messages.
- **Phase 6 — FSM**: `transition_to_leader()` (24 params + cm) and `transition_to_follower()` (mirror + leader_election) encapsulate all unsafe field swaps, capability/coordinator state set, `log_state()`, and 4 `debug_assert!` invariants. `promote()`/`demote()` refactored to use them. All 3 constructors have invariant assertions.
- **StorageHealth threshold fix**: Raised Degraded to `> 10` and NeedsRepair to `> 50` (was `> 0`/`> 10`). Normal GC lag (1-10 orphans) is now Healthy, skipping unnecessary startup repair. `storage_runtime.rs:240-248`.
- All 3 builds pass (`cargo check --workspace`, `wasm-pack build --target web`, `npx tsc --noEmit`).

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
- **SyncStateStore trait**: Replaces Arc<dyn Storage> dependency in DownloadQueue. Cursor and generation live in memory after boot, never read from OPFS on mutations.
- **MetadataRuntime as metadata database**: SyncState, SchemaMeta, KeyRecord, MigrationRecord live behind RwLock in memory. Persisted via MetadataStore<T> on checkpoint, never at read time.
- **StorageHealth classification**: 5 variants determined at boot — Healthy, Degraded, NeedsRepair, ReadOnly, Corrupt. Healthy boot skips ALL cleanup/rebuild/repair. No OPFS scans beyond manifest load.
- **MaintenanceRuntime owns scheduling**: StorageRuntime exposes compact/cleanup/verify. MaintenanceRuntime runs them as background ticks. No maintenance in boot path.
- **SchedulerRuntime unifies queues**: Single priority system over Storage/Upload/Download/Maintenance/Retry queues. One ordering, no starvation.
- **NamespaceManager as VaultRuntime child**: HashMap<NamespaceId, NamespaceRuntime> with lazy loading, LRU eviction. VaultRuntime delegates namespace lifecycle to NamespaceManager.
- **RuntimeStatus for UI**: Single object with mode + health + namespace + lag_ms + leader_id. UI reads one source of truth instead of WebSocket readyState.

## VaultRuntime Architecture

### Target Architecture

```
Browser Tab
    │
    └── React → QueryCache (JS, ephemeral view cache)
                     │
                     └── WASM VaultRuntime
                              │
                              ├── NamespaceManager
                              │      │
                              │      ├── NamespaceRuntime (HR)
                              │      │      ├── WorkingSet        ← loaded docs, pinned, LRU, dirty, subscribed
                              │      │      ├── SubscriptionIndex ← HashMap<doc_id, subscriber_ids>
                              │      │      ├── SyncRuntime
                              │      │      │      ├── CursorStore/SyncStateStore
                              │      │      │      ├── PendingIndex
                              │      │      │      └── MirrorRuntime
                              │      │      │
                              │      │      └── StorageRuntime    ← database engine per namespace
                              │      │             ├── TransactionManager  ← ONLY mutation path
                              │      │             ├── ContentIndex        ← ALL lookups (see below)
                              │      │             ├── ManifestManager     ← authoritative after startup
                              │      │             ├── MetadataRuntime     ← in-memory singletons
                              │      │             ├── PageManager         ← free list, GC watermark
                              │      │             ├── CompactionManager   ← sealed segment lifecycle
                              │      │             └── OPFS               ← persistence layer only
                              │      │
                              │      ├── NamespaceRuntime (Projects)
                              │      ├── NamespaceRuntime (CRM)
                              │      └── NamespaceRuntime (...)
                              │
                              ├── SchedulerRuntime
                              │      ├── StorageQueue
                              │      ├── UploadQueue
                              │      ├── DownloadQueue
                              │      ├── MaintenanceQueue
                              │      └── RetryQueue
                              │
                              ├── MaintenanceRuntime
                              │      ├── GC
                              │      ├── Compaction
                              │      ├── Checkpoints
                              │      ├── Repair
                              │      └── Health monitoring
                              │
                              ├── RuntimeCoordinator
                              │      ├── DiscoveryProtocol  ← PROTO v1 (Web Lock ifAvailable + BC)
                              │      ├── BC message routing ← 6 message types, stable
                              │      └── LeaderElection     ← Web Lock lifecycle
                              │
                              ├── RuntimeBus
                              │
                              └── MetricsService
```

### ContentIndex (authoritative lookup, expanded)

```
ContentIndex
│
├── page_by_document:   HashMap<(doc_id, record_id), PageId>    ← O(1) forward lookup
├── page_by_id:         HashMap<PageId, IndexKey>               ← O(1) reverse lookup
├── live_pages:         BTreeSet<PageId>                        ← all currently live pages
├── tombstoned_pages:   BTreeSet<PageId>                        ← garbage-collectable pages
├── document_versions:  HashMap<IndexKey, u64>                  ← version per document
├── namespace_versions: BTreeMap<u64, HashSet<PageId>>          ← version→pages for sync
├── generation:         u64                                     ← manifest generation counter
├── last_compaction:    u64                                     ← timestamp of last compaction
└── crc:                u64                                     ← integrity check
```

All lookups go through ContentIndex. OPFS is a persistence layer only.

### SubscriptionIndex (beside WorkingSet)

```
SubscriptionIndex
│
├── by_document: HashMap<DocumentId, Vec<HandleId>>           ← O(1) doc→subscribers
├── by_handle:   HashMap<HandleId, (DocumentId, Callback)>    ← O(1) handle→subscription
└── by_namespace: HashMap<NamespaceId, Vec<HandleId>>         ← O(1) namespace→subscribers
```

Mutation dispatch: `by_document.get(doc_id)` → iterate only listeners for that doc.

### Namespace Lifecycle

```
Closed
    ↓
Opening ──► Loading ──► Ready ──► Idle ──► Evicting ──► Closed
    │                                                    ↑
    └──────────────── Error ─────────────────────────────┘
```

- **Closed**: Not loaded, no memory, no OPFS handles.
- **Opening**: Allocating resources, loading manifest.
- **Loading**: ContentIndex hydrate, metadata load.
- **Ready**: Accepting queries.
- **Idle**: No active subscribers; eligible for eviction.
- **Evicting**: Flush dirty pages, release handles.
- **Error**: Failed to open; resources cleaned up.

NamespaceManager owns: `HashMap<NamespaceId, NamespaceRuntime>` with LRU eviction, lazy loading.

### PROTO v1 — BroadcastChannel Protocol

Stable 6-message protocol. Never add new message types without bumping version.

| Message | Direction | Payload | Purpose |
|---------|-----------|---------|---------|
| `HELLO\|replica_id` | Follower → BC | replica_id | Follower announces presence |
| `LEADER\|{tab_id,boot_id,gen,cursor}` | Leader → BC | JSON | Leader announces readiness |
| `MUTATION\|gen\|doc_id\|record_id\|fields_json` | Leader → BC | pipe-delimited | Real-time mutation broadcast |
| `SYNC\|cursor` | Follower → BC | cursor | Follower requests catch-up sync |
| `LEFT\|tab_id` | Any → BC | tab_id | Tab is closing |
| `PING\|replica_id` | Any → BC | replica_id | Liveness check (optional) |

No `HEARTBEAT`, no `BUILDING`, no `DISCOVER`, no `RUNTIME_SNAPSHOT`, no `LEADER_TRANSFER`. Six messages, stable.

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
| ContentIndex reverse lookup | `page_by_id: HashMap<PageId, IndexKey>` — O(1) GC, repair, compaction |
| StorageTransaction mandatory | No direct `PageStore.write()` — every mutation goes through `tx.commit()` |
| MetadataRuntime | In-memory metadata DB over MetadataStore<T> — no OPFS reads for cursor/gen |
| StorageHealth classification | Boot decides Healthy/Degraded/NeedsRepair/ReadOnly/Corrupt before expensive work |
| MaintenanceRuntime scheduler | StorageRuntime exposes compact/cleanup/verify; MaintenanceRuntime schedules them |
| SchedulerRuntime unified | One priority system over Storage/Upload/Download/Maintenance/Retry queues |
| NamespaceManager | HashMap<NamespaceId, NamespaceRuntime> — lazy loading, LRU eviction |
| RuntimeStatus object | mode + health + namespace + lag_ms + leader_id — UI never asks multiple questions |
| Subsystems communicate through traits | SyncStateStore, CursorStore, Scheduler — never concrete implementations |

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

### Next (StorageAuthority v1.0) — 4-Sprint Roadmap (replaces old 6-sprint)

#### Sprint A — Correctness (Storage Transaction Model)
*Enforce: "A document can never have >1 live page after a committed transaction."*

1. **Complete ContentIndex** with forward (`page_by_document`) + reverse (`page_by_id`) indexes, `live_pages`, `tombstoned_pages`, `document_versions`, `namespace_versions`, `generation`, `last_compaction`, `crc`. All fields updated atomically on every write.
2. **StorageTransaction as the only write API**. Remove public `PageStore.write()`, `PageStore.allocate()`, `PageStore.tombstone()`. Expose only `StorageRuntime.begin_transaction()` with `tx.write_document()`, `tx.write_metadata()`, `tx.delete()`, `tx.commit()`. No bypass paths.
3. **Replace O(n) doc scan** — `tombstone_existing_doc_pages()` uses `page_by_document.get()` (O(1)) instead of iterating all live pages and reading every header.
4. **Atomic document writes** — tombstone old page + allocate new page + write new page + update both ContentIndex indexes + bump manifest, all inside a single `StorageTransaction`.
5. **ManifestRebuildGuard** — `debug_assert!` in debug, `engine_warn!` in release if rebuild finds >1 live page per key.
6. **Invariant**: Every storage mutation produces exactly one index mutation. They happen together or not at all.
7. **Fix MUTATION parsing in `handle_mirror_bc_message`**: `parts[1/2/3]` → `parts[2/3/4]`.
8. **RuntimeStore → view cache over WASM**: `useQuery()` calls WASM `find()` as fallback when RuntimeStore is empty. RuntimeStore becomes write-through + read-cache, not source of truth.

#### Sprint B — Ownership (Runtime Separation)
*Enforce: "DownloadQueue never accesses storage metadata directly."*

1. **SyncStateStore trait** in core crate: `cursor()`, `set_cursor()`, `generation()`, `set_generation()`, plus future fields (`last_snapshot`, `pending`, `replica_id`).
2. **Implementors**: `SyncRuntime` (WASM) delegates to `AtomicU64` + `RwLock<String>`. `AtomicSyncStateStore` (native). `MockSyncStateStore` (tests).
3. **DownloadQueue decoupled**: Accepts `Arc<dyn SyncStateStore>` instead of `Arc<dyn Storage>`. All `read_sync_state()` calls become in-memory reads. No OPFS for cursor/generation after boot.
4. **MetadataRuntime** — mini metadata database over `MetadataStore<T>`. Owns `SyncState`, `Keys`, `SchemaMeta`, `MigrationRecord` as in-memory values behind `RwLock`, persisted via `MetadataStore<T>`.
5. **StorageRuntime authority**: Only `StorageRuntime` touches OPFS. All `PageStore` access goes through it.
6. **Follower initial sync via replication**: New protocol: HELLO → LEADER_READY → SYNC(cursor) → MUTATION stream → DONE. Leader reads from storage, streams mutations to follower. No RuntimeSnapshot serialization.
7. **Leader lifecycle events**: Leader sends `LEADER_READY` at startup, `LEFT` via `beforeunload`. Follower tracks via these events, not periodic heartbeats.

#### Sprint C — Startup (Health Classification + Web Lock Discovery)
*Enforce: "Boot never performs repair work. It classifies health, then delegates."*

1. **Web Lock `ifAvailable` discovery** — replace `wait_for_leader(2500)` with `navigator.locks.request(name, {ifAvailable: true})`. Lock acquired → leader. Null → mirror. Short timer fallback (200ms) for browser edge cases.
2. **StorageHealth** (5 variants): `Healthy` | `Degraded` | `NeedsRepair` | `ReadOnly` | `Corrupt`. Determined at boot before any expensive operations.
3. **Boot path when Healthy**: Load manifests → CRC validate → `Health::Healthy` → skip all cleanup/rebuild/repair → go to Ready (~100ms).
4. **MaintenanceRuntime** — owns GC, compaction, checkpoints, manifest repair, index verification, health monitoring. `StorageRuntime` exposes `compact()`, `cleanup()`, `verify()` — MaintenanceRuntime schedules them. Never blocks startup.
5. **SchedulerRuntime** — unified priority system over `StorageQueue`, `UploadQueue`, `DownloadQueue`, `MaintenanceQueue`, `RetryQueue`.
6. **Deferred cleanup**: `cleanup_tombstoned_pages()` moved to Warm/Idle phase via `MaintenanceRuntime` tick.

#### Sprint D — Namespace Architecture + UX
*Enforce: "Each namespace is an isolated database. Unopened namespaces are never loaded."*

1. **NamespaceManager** — `HashMap<NamespaceId, NamespaceRuntime>` with LRU eviction, lazy loading, namespace lifecycle management. Namespace lifecycle: Closed → Opening → Loading → Ready → Idle → Evicting → Closed.
2. **NamespaceRuntime** — owns `WorkingSet`, `SubscriptionIndex`, `StorageRuntime`, `SyncRuntime`, `Metrics`. Each namespace is an isolated mini-database.
3. **WorkingSet** — first-class runtime: loaded docs, pinned docs, LRU eviction, dirty docs, subscribed docs. Only 20/500k docs in memory.
4. **SubscriptionIndex** — `HashMap<doc_id, Vec<handle_id>>` beside WorkingSet. Mutation dispatches only to docs that have listeners.
5. **RuntimeStatus** — `{ mode: Leader|Mirror|Promoting|Recovering|Offline, health, namespace, lag_ms, leader_id }`. UI reads this instead of WebSocket `readyState`.
6. **Batch BC notifications** — `RuntimeStore.applySetFieldBatch()` applies all fields silently, notifies once. No per-field re-render cascade.
7. **Leader handoff via Web Lock waiter** — When leader tab closes, browser releases Web Lock. Follower's pending lock request fires → promotion. No polling, no setInterval.

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

14. **Subsystems communicate through traits, never concrete implementations** — `SyncStateStore`, `CursorStore`, `Scheduler` are traits. `DownloadQueue` accepts `Arc<dyn SyncStateStore>`, not `Arc<SyncRuntime>`.

15. **Every storage mutation produces exactly one index mutation** — write + index update + manifest bump happen atomically in a single `StorageTransaction.commit()`. No deferred index updates.

16. **Boot classifies health, then delegates** — `StorageHealth` determined before any cleanup/repair/rebuild. Healthy boot skips all expensive work. Repair is the responsibility of `MaintenanceRuntime`, never boot.

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
- **Bug C (upload loop) — FIXED**: Every page write now atomically updates ALL ContentIndex indexes including `page_by_id`. `write_page_raw` is a pure OPFS writer. `commit_page_write()`, `commit_tx()`, `allocate_page_id()`, and `commit_allocated_page_id()` all add `page_by_id` stub entries so `repair_orphans()` no longer tombstones legitimate delta pages from `mark_synced` on restart.
- **Bug D (page ID 0 corruption) — ROOT CAUSE IDENTIFIED**: `WAL_CHECKPOINT_PAGE = 0` conflicts with `allocate_page_id()` starting at 0. `checkpoint_wal()` writes JSON to `_pages/oplog/0.page`, overwriting real oplog data. Fix: Phase 2 (system directory).
- **Bug E (dual PageStore instances) — ROOT CAUSE IDENTIFIED**: `OpfsStorage.pages.oplog` and `OpfsStorageEngine.oplog` are separate Rust objects pointing to the same `_pages/oplog/` directory. Each has an independent `Manifest` behind separate `Arc<Mutex<...>>` instances. Writes via one are invisible to the other. Fix: Phase 2 (single PageStore).
- **Architectural issue — follower boots the full engine**: The follower builds BrowserStorage, reads OPFS, connects to coordinator, runs recovery, and only then discovers it's a follower. Should use BC heartbeat check first, then skip directly to MirrorRuntime. Fix: Phase 4.
- **V4 v2 discovery — MirrorRuntime fast-path is 100% dead code**: HeartbeatManager is declared but never instantiated. `broadcast_heartbeat()` is never called anywhere. The 150ms poll in `new_with_coordinator` always times out — every tab builds the full engine. Fixed with HELLO/DISCOVER protocol in RuntimeCoordinator.
- **Architectural issue — O(N) OPFS scans everywhere**: Every subsystem (upload, download, recovery, React queries) independently calls `list_page_ids()` which triggers OPFS directory scan at startup. Should use ContentIndex (O(1)) for all reads. Fix: Sprint 1 (ContentIndex authority).
- **Logs confirm StorageTransaction gap**: `get_document: found 2 live pages` then `insert_document: tombstoned 2 live pages (expected 1)` appears repeatedly. ContentIndex `page_by_document` is never populated at runtime for doc_data — `tombstone_existing_doc_pages()` does O(n) scan reading every live page header. Fix: Sprint 1 (populate ContentIndex at write time, O(1) lookup).
- **Logs confirm read_sync_state=None flood**: Every `process_push_mutation()` call triggers `read_sync_state()` returning `None`, re-reads cursor from OPFS on every mutation. DownloadQueue holds `Arc<dyn Storage>` and reads SyncState through it; SyncRuntime is disconnected. Fix: Sprint 2 (SyncStateStore trait, DownloadQueue uses in-memory cursor).
- **Logs confirm hidden startup cost**: ~1500ms between "startup" and "storage_manager ready" despite all init steps taking <10ms each. Caused by `PagesDir::open()` (6× OPFS manifest reads) + `cleanup_tombstoned_pages()` (6× full OPFS directory walks). Fix: Sprint 3 (StorageHealth classification → skip cleanup when Healthy, deferred to Warm phase).
- **Logs confirm follower shows Offline**: Follower intentionally skips coordinator init (no WebSocket), but UI still derives status from WS `readyState`. Need `RuntimeStatus.mode: Mirror` instead. Fix: Sprint 5 (RuntimeStatus to JS).
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

## Known Issues

### StorageHealth Degraded at startup
**Status**: Tracked, not blocking today

Runtime logs show `StorageHealth=Degraded` on every startup due to orphan page repair (logs: `CRC mismatch`, `orphans repaired`). The orphan pages are from normal operation (tombstoned pages that GC hasn't cleaned yet), not corruption. The Degraded classification triggers unnecessary startup overhead.

**Root cause**: `StorageRuntime::new()` at `storage_runtime.rs:216-232` runs `repair_orphans()` unconditionally and classifies any orphan count > 0 as Degraded. Orphans are expected between compaction cycles — a zero-orphan threshold is too strict for normal steady-state operation.

**Fix**: Raised Degraded threshold to `> 10` and NeedsRepair threshold to `> 50` at `storage_runtime.rs:240-248`. Orphans 0-10 → Healthy, 11-50 → Degraded, >50 → NeedsRepair. Startup now skips expensive repair for normal steady-state orphan counts.

### Bug 1 — Leader bootstrap incomplete cache (FIXED)
WASM `find()` at `client.rs:1338-1356` previously short-circuited to the Runtime in-memory document_store when it had ANY entries. If BC MUTATION messages populated even 2 records during startup, `find()` returned only those 2 instead of falling through to OPFS (authoritative source with all 14+ records).

**Fix**: Removed the Runtime cache shortcut on the leader path. WASM `find()` now always delegates to `self.client.find()` (OPFS) when a client is available. Runtime cache is only used as fallback when no client exists (mirror/follower modes already handled by separate mirror path).

### Bug 5 — Duplicate React keys from id/record_id mismatch (FIXED)
`useQuery.ts` used `fields.id` as the RuntimeStore key, while Core CRDT stores records under `record_id` internally. When the same logical record arrived through both a `find()` result and a BC MUTATION with different key fields, RuntimeStore created two separate Map entries — one keyed by `id` and another by `record_id`. React rendered both, causing duplicate key warnings.

**Fix**: 
1. `useQuery.ts` now uses `fields.record_id ?? fields.id` consistently for the storage key
2. `RuntimeStore.applySetFieldBatch` normalizes `fields.id` to the effective `record_id` and merges duplicate entries
3. `RuntimeStore.applySetField` migrates records when the `id` field is set to a value different from the current key

