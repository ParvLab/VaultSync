# Anchored Summary

## Goal
- Eliminate redundant mutation replay on coordinator restart via snapshot-based reconnection
- Build proper logging architecture with configurable log levels

## Constraints & Preferences
- WASM build target (`wasm-pack build --target web`)
- TypeScript SDK build via `npm run build` in `sdk/`
- All Rust crates must compile (`cargo check --workspace`)
- Compaction methods must use `js_sys::Date::now()` for timestamps (not `crate::time_utils`)

## Progress
### Done
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

### In Progress
- L6: Clean up init progress `[1/9]–[9/9]` → condensed

### Blocked
- Log demotion (L2-L5) blocked until snapshot verification confirms replay is functional

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

## Next Steps
1. **Verify snapshot restore with compaction**: Start server, POST /namespace/notes-demo/compact, restart coordinator, connect client. Confirm `[download_queue] snapshot restored: docs=N ...` and no full replay.
2. **Log demotion (after verification)**: Demote ~130 log lines to `trace!`/`debug!` per the audience model below.
3. **Tag release** once stable.

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
When ready, the remaining work splits into these phases:

| Phase | Scope | Files | Est. Changes |
|-------|-------|-------|-------------|
| L2 | Demote Rust core instrumentation (`reconciler.rs`, `download.rs` internals) | `core/src/sync/reconciler.rs`, `download.rs`, `state_manager.rs` | ~90 lines |
| L3 | Demote WASM instrumentation (`client.rs`, `ws_coordinator.rs`, `storage.rs`) | `wasm/src/client.rs`, `ws_coordinator.rs`, `storage.rs` | ~60 lines |
| L4 | Demote notify pipeline + JS SDK logging | `sdk/web/src/index.ts`, `sdk/react/src/useQuery.ts`, `useVaultSyncOne.ts` | ~12 lines |
| L5 | Upgrade operational events + demote FocusGuard | `upload.rs`, `client.rs` (core + wasm), `NoteEditor.tsx` | ~30 lines |
| L6 | Clean up init progress `[1/9]–[9/9]` → condensed | `wasm/src/client.rs`, `ws_coordinator.rs` | ~15 lines |
| L7 | Verify: `cargo check`, `wasm-pack build`, `npm run build`, smoke test | — | — |

## Critical Context
- **Bug A (OPFS race) — FIXED**: Cross-tab OPFS race eliminated by per-tab storage paths. Upload pipeline health confirmed by consistent `PUSH_ACK sequences=[N]`, `AFTER_MARK_SYNCED pending=0`, `BATCH_DONE success=1`.
- **Bug B (Focus guard loop) — FIXED**: One-way replication caused by `document.activeElement` check blocking external merges on the follower tab. Fixed with edit-timestamp cooldown. Both directions now confirmed working.
- **Bidirectional sync is stable**: Logs show clean single-pipeline flow: `upload → PUSH_SEND → PUSH_ACK → AFTER_MARK_SYNCED pending=0 → BATCH_DONE → reconciler → subscription.fire → React callback → setData`. No repeating notification cycles, no pending uploads.
- **The focus guard was the biggest remaining bug after OPFS fix**: We traced the issue through Rust/WASM/OPFS/CRDT/BC/WebSockets/React/browser event loop, and the root cause was a single line in a React component that blocked external merge when the textarea had focus.
- **Instrumentation should be kept until stress tests pass**, then cleaned up for release.

## Relevant Files
- `sdk/examples/notes/src/components/NoteEditor.tsx`: **Fix applied**. Replaced `document.activeElement` focus guard with edit-timestamp cooldown using `lastEditRef.current.title` / `lastEditRef.current.body` and `FOCUS_COOLDOWN_MS = 2000`. Added `[FocusGuard] console.warn` logs.
- `crates/vaultsync-wasm/src/client.rs`: **Bug A fix** (per-tab OPFS paths). BC command instrumentation (follower `[BC] FOLLOW_INSERT_BEGIN`, leader `[BC] LEADER_INSERT_BEGIN`).
- `crates/vaultsync-core/src/sync/upload.rs`: Upload pipeline instrumentation (BATCH_START, PUSH_SEND, PUSH_ACK, AFTER_MARK_SYNCED, BATCH_DONE) — all confirmed healthy
- `crates/vaultsync-core/src/sync/download.rs`: Download worker wake path, cursor advancement
- `crates/vaultsync-core/src/sync/reconciler.rs`: Reconciler ENTER/EXIT logs — confirmed working
- `sdk/packages/react/src/useQuery.ts`, `useVaultSyncOne.ts`: React hooks with notification chain
- `sdk/examples/notes/src/components/NoteEditor.tsx`: Focus guard fix with edit-timestamp cooldown
- `crates/vaultsync-wasm/src/ws_coordinator.rs`: `effective_after` for SUBSCRIBE after snapshot drain; `history_preserved` storage + trait impl
- `crates/vaultsync-core/src/client.rs`: Smart recovery — keep cursor on gen mismatch when `history_preserved=true`
- `crates/vaultsync-core/src/coordinator/ws_proto.rs`: `history_preserved` in `RegisterAckPayload`/`NamespaceAckPayload`
- `crates/vaultsync-core/src/coordinator/traits.rs`: `history_preserved()` trait method
- `crates/vaultsync-core/src/coordinator/mux_coordinator.rs`: `history_preserved` stored in `MuxWsHandle` + exposed via `NamespacedCoordinator`
- `crates/vaultsync-coordinator-cf/src/lib.rs`: `history_preserved: true` in register/namespace-ack
- `crates/vaultsync-coordinator-server/src/ws.rs`, `ws_mux.rs`: `history_preserved: false` in register/namespace-ack
