# Anchored Summary

## Goal
- Build Storage Engine V2: page-store abstraction, segment lifecycle, compaction engine, resource manager, memory manager
- Eliminate redundant mutation replay on coordinator restart via snapshot-based reconnection
- Build proper logging architecture with configurable log levels
- Build Generation 3: Data Locality Architecture (Working Sets, Workspaces, Replication Planner, Segment Snapshots)

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
Storage opens
    ↓
Metadata loads
    ↓
Workspace loads
    ↓
Working Sets restore
    ↓
Replication Planner initializes
    ↓
Query executes
    ↓
Metadata lookup
    ↓
Page lookup
    ↓
Storage read
    ↓
CRDT merge
    ↓
UI updates
    ↓
Planner observes access
    ↓
Prefetch scheduled
```

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
- **Generation 2 architecture is complete**: Next evolution (Generation 3) should focus on Working Set Manager, Dataset Manager, Replication Planner — not more storage features.

## Next Steps
1. **M1 Stabilization (v0.9.0-rc1)**: L8 log cleanup done. AGENTS.md alignment done. Tag v0.9.0-rc1.
2. **M2 Data Integrity**: Document version watermarking → version enforcement on writes → register concrete migration → SchemaRegistry persistence → compatibility tests (v1→v4 skip-ahead)
3. **M3 Observability**: Logger trait → module-level log config → Metrics API → Health API → replace ad-hoc log macros → optional OpenTelemetry integration
4. **M4 Production Validation**: Benchmarks (Metadata Ready, First Query, Interactive, Background Sync), Reliability (8h/24h/72h soak), Resilience (crash/corruption/network/multi-tab failure injection), Scalability (10-10000 users)
5. **M5 Security Review**: Encryption correctness, key rotation, replay protection, malformed snapshot handling, namespace isolation, DOS resistance
6. **v1.0.0** after proven under realistic workloads
7. **M6 Developer Experience**: VaultSync Inspector, ERP Validation Suite, CLI tool, telemetry dashboard, documentation

### Logging Architecture
Log levels map to audience:

| Level | Audience | What belongs |
|-------|----------|-------------|
| TRACE | Engine developers | State vectors, CRDT diffs, `apply_update`, reconciler internals, field hashes |
| DEBUG | SDK developers | Upload/download queue ops, compaction, snapshot creation, leader election, connection lifecycle |
| INFO | Application developers | Started, connected, offline, reconnected, snapshot restored, compaction completed, sync complete |
| WARN | Operators | Coordinator unavailable, retrying, generation changed, snapshot unavailable |
| ERROR | Everyone | Snapshot decode failed, OPFS write failed, coordinator auth failed, compaction failed |

### Next Steps Plan (Detailed)
1. **Production soak test**: 8–12 hour run with new Storage Engine V2.


## Critical Context
- **Bug A (OPFS race) — FIXED**: Cross-tab OPFS race eliminated by per-tab storage paths. Upload pipeline health confirmed by consistent `PUSH_ACK sequences=[N]`, `AFTER_MARK_SYNCED pending=0`, `BATCH_DONE success=1`.
- **Bug B (Focus guard loop) — FIXED**: One-way replication caused by `document.activeElement` check blocking external merges on the follower tab. Fixed with edit-timestamp cooldown. Both directions now confirmed working.
- **Bidirectional sync is stable**: Logs show clean single-pipeline flow: `upload → PUSH_SEND → PUSH_ACK → AFTER_MARK_SYNCED pending=0 → BATCH_DONE → reconciler → subscription.fire → React callback → setData`. No repeating notification cycles, no pending uploads.
- **The focus guard was the biggest remaining bug after OPFS fix**: We traced the issue through Rust/WASM/OPFS/CRDT/BC/WebSockets/React/browser event loop, and the root cause was a single line in a React component that blocked external merge when the textarea had focus.
- **Instrumentation cleanup (L2-L5) completed** — per-mutation reconciler internals at TRACE, pipeline operations at DEBUG, summary events at INFO, errors at WARN/ERROR.

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
- `crates/vaultsync-wasm/src/page_store.rs`: PageStore — OPFS-backed segment-level page store for WASM target, write/read/scan/GC per segment
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
