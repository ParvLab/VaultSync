# Anchored Summary

## Goal
Stop the `RefCell already borrowed` panic at `queue.rs:42` and restore leader/follower state convergence by fixing the mutation echo cycle and all downstream bugs.

## Constraints & Preferences
- Never patch the symptom (`js-sys::queue.rs`) until all architecture fixes fail.
- Never suppress subscription events (`fire()` re-entrancy guard, `try_send` drop).
- Provenance: every mutation carries `who`, `how`, and `already seen?` from creation to consumption.
- Server must not echo mutations to origin; client must not re-merge its own mutations.
- BroadcastChannel is IPC only, never part of the mutation processing loop.

## Progress

### Phase 0 — Crash Fixes (Done ✓)
- `FinalizationRegistry` double-free — `defer_drop()` → `forget()` everywhere.
- `closure invoked recursively` — `Closure::once` → `Closure::wrap` everywhere.
- leader election key config — `config.storage = Wasm`.
- WS old callbacks cleared on reconnect — verified correct.
- IndexedDB future drops immediate — verified correct.

### Phase 1 — Provenance & Self-Filter (Done ✓)
- Added `replica_id: String` with `#[serde(default)]` to `PendingMutation` (20+ sites, 12 files).
- **Client self-filter** in `download.rs`: `process_batch()`, `process_p2p_mutation()` skip own `replica_id`.
- **WASM WS self-filter** in `ws_coordinator.rs`: background processor skips own `replica_id`.
- **Server socket filter** in `ws.rs`: subscription fan-out skips origin socket.
- **Dedup cache** in `reconciler.rs`: 10k-entry LRU-ish `(HashSet, VecDeque)`.
- Did NOT stop the crash — the real loop was BC self-echo + double instances, not WS echo.

### Phase 2 — Double-Instance Fix (Done ✓, stopped the crash)
- **BC posting removed** from Rust `global_listener` in `crates/vaultsync-wasm/src/client.rs`.
- **Singleton cache** in `sdk/packages/web/src/index.ts` — `Map<string, Promise<VaultSyncClient>>` keyed by namespace.
- **Provider cleanup** in `sdk/packages/react/src/context.tsx` — no `shutdown()` on StrictMode cleanup, only `cancelled` flag.
- **Result**: `RefCell already borrowed` panic is gone. Mutations flow to seq=200+ without crash.

### Phase 3 — State Divergence Diagnosis (Done ✓)
Confirmed two critical bugs and three safety issues that prevent leader/follower convergence:

1. **Dedup-before-storage-write** (`reconciler.rs:27-40`) — `check_dedup()` inserted into seen set BEFORE `write_batch_reconciliation()`. If write failed, retry skipped all entries (dedup'd) and cursor advanced past lost data.

2. **Cursor advances before storage persist** (`download.rs:146-165`) — in-memory cursor advanced BEFORE `write_sync_state()` to storage. If persist failed, in-memory cursor was ahead of storage — page reload regressed cursor.

3. **`onclose` kills active WebSocket** (`ws_coordinator.rs:106`) — unconditional `*inner.ws = None` in `onclose` closure. If WS1 fired `onclose` after WS2 was stored, WS2 was killed.

4. **Zombie tasks on reconnect** — `connect_and_handshake` spawned 5 `spawn_local` tasks. Old tasks never cancelled. Background processor (#1) leaked forever (leaked `msg_tx` sender in `forget()`-ed closure kept `msg_rx` alive).

5. **WebSocket send without ready_state guard** — `send_with_u8_array` called without checking `ws.ready_state() == OPEN`. Explains "CLOSING or CLOSED" console errors.

### Phase 4 — Fixes Applied (Done ✓)
All P0 fixes deployed across 3 files (~80 lines changed):

| Fix | File | Lines | What Changed |
|-----|------|-------|-------------|
| Dedup after write | `reconciler.rs` | 27-40, 42-67, 69-98, 100-157 | Split `check_dedup` into `is_deduped()` (read-only) + `mark_dedup()` (after write). All three apply methods stage IDs then commit dedup only after storage persists. |
| Cursor after persist | `download.rs` | 144-175 | Moved `last_sequence.store()` to AFTER `write_sync_state()`. Cursor survives page reload. |
| Generation counter | `ws_coordinator.rs` | `WasmWsCoordinatorInner` | Added `ws_gen: AtomicU64`. Incremented before creating WS. `onclose` closure captures its generation and only nullifies `ws` if `current_gen == my_gen`. |
| Zombie task cancellation | `ws_coordinator.rs` | `connect_and_handshake` | Added `cancel_flag: Arc<AtomicBool>`. Old flag set to true before creating new tasks. Background processor uses `select!` between `msg_rx.next()` and polling cancel flag. Heartbeat, discovery loops check flag. |
| ready_state guard | `ws_coordinator.rs` | `send_request`, `subscribe`, `register`, `heartbeat`, signal handler | All `send_with_u8_array` calls now check `ws.ready_state() == 1` first. |
| Coordinator UUID tracing | `ws_coordinator.rs` | `connect_and_handshake`, all spawned tasks | Each connection logs `[coordinator=<uuid>]` prefix in all task lifecycle messages. |

### Files Modified in Phase 4
- `crates/vaultsync-core/src/sync/reconciler.rs` — dedup ordering fix
- `crates/vaultsync-core/src/sync/download.rs` — cursor ordering fix
- `crates/vaultsync-wasm/src/ws_coordinator.rs` — generation counter, cancel flag, ready_state guards, UUID tracing

## Next Steps
1. **Build and deploy** — compile with `wasm-pack`, deploy to CF/serve, test.
2. **Verify leader/follower convergence** — open two tabs, type in leader, confirm follower converges within 1-2 sync cycles.
3. **Verify `RefCell` panic stays gone** — long-running test with rapid edits.
4. **Verify OPFS files appear** — after convergence is stable.
5. **Phase 5 (if needed):** Make PUSH useful — call `coordinator.subscribe()` in sync loop, integrate subscription stream with DownloadQueue cursor.
6. **Phase 6 (if needed):** IndexedDB schema wipe — last resort if protocol fixes don't unstick persistent state.

## Key Decisions
- **The `RefCell` crash was caused by StrictMode double-mount → 2 clients → BC self-echo → executor overload.** Phase 1's WS-focused filters couldn't stop a BC loop.
- **Singleton cache by namespace is correct** for StrictMode. Different tabs have independent JS contexts (no cross-tab singleton), so per-tab replica_ids differ naturally.
- **`replica_id` self-filter is correct** (tabs have different IDs, so cross-tab mutations pass through).
- **Dedup must mark after storage write**, not before. Otherwise a single storage failure causes permanent mutation loss for the session.
- **In-memory cursor must advance only after storage persist.** Otherwise a crash after in-memory advance but before storage persist regresses cursor on reload.
- **Generation counter** is the cleanest fix for the `onclose` race — no complex locking, just a check at the start of the callback.
- **`select!` over cancel flag** is the only reliable way to kill a task blocked on `mpsc::Receiver::next()` when the sender is leaked inside a `forget()`-ed Closure.
- OPFS investigation is deferred until protocol stability is confirmed.

## Relevant Files
- `crates/vaultsync-core/src/sync/reconciler.rs` — `is_deduped()` + `mark_dedup()` split. Entry IDs only committed to dedup set after `write_batch_reconciliation()` succeeds.
- `crates/vaultsync-core/src/sync/download.rs` — Cursor `last_sequence.store()` moved to AFTER `write_sync_state()`.
- `crates/vaultsync-wasm/src/ws_coordinator.rs` — `ws_gen: AtomicU64` with generation-gated `onclose`; `cancel_flag: Arc<AtomicBool>` for zombie task cancellation; `ready_state()` guards on all sends; `[coordinator=<uuid>]` tracing.
- `crates/vaultsync-wasm/src/client.rs` — BC posting removed (Phase 2).
- `sdk/packages/web/src/index.ts` — Singleton promise cache (Phase 2).
- `sdk/packages/react/src/context.tsx` — No shutdown on cleanup (Phase 2).
