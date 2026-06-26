# Anchored Summary

## Goal
Eliminate timer-based polling from the download worker, making it fully event-driven. The worker now only wakes on push mutations or bootstrap notifications — no 30s safety timer, no redundant `pull(after=N)` returning 0.

## Constraints & Preferences
- Keep OPFS write synchronous; only WS upload becomes async (no phantom edits on page close).
- Generation ID uses UUID, sent in RegisterAckPayload with `#[serde(default)]`, stored in SyncState.
- Cursor reset eager (on register), not lazy (in process_batch).
- Phase order: 0 → A → A5 → B → C → D → E → F → G → H.
- TransportRouter uses `remotes: Vec<Arc<dyn Transport>>` not `Option<Arc<dyn Transport>>` for future-proofing.

## Progress

### Completed (All compile clean)

| Phase | Component | Key Changes |
|-------|-----------|-------------|
| 0 | WASM build | `wasm-pack` verified |
| A | Transport trait | `crates/vaultsync-core/src/transport/traits.rs` — `Transport`, `InboundMutation`, `TransportSource`, `TransportError` |
| A | BC Transport | `vaultsync-wasm/src/transport/broadcast_channel.rs` — plaintext, same-origin |
| A | WS Transport | `vaultsync-wasm/src/transport/ws_transport.rs` — refactored from `ws_coordinator.rs` transport layer |
| A | TransportRouter | `vaultsync-wasm/src/transport/router.rs` — merged, deduped, self-filtered stream via channels |
| A | WASM crate compiles | old `transport.rs` removed, replaced by `transport/` dir |
| A5 | SyncState.generation_id | `#[serde(default)]` on `generation_id: String` |
| A5 | RegisterAckPayload.generation_id | `#[serde(default)]` on `generation_id: String` |
| A5 | Coordinator trait | `async fn generation_id(&self) -> String { String::new() }` default method |
| A5 | SyncStateManager | `crates/vaultsync-core/src/sync/state_manager.rs` — `current_cursor()`, `current_generation()`, `advance_cursor()`, `reset_cursor()`, `check_generation()` |
| A5-1 | WasmWsCoordinator.generation_id | Parsed from RegisterAck in `connect_and_handshake`, stored in `inner.generation_id: Mutex<String>`, exposed via `Coordinator::generation_id()` |
| A5-2 | Client init generation check | In `initialize()` after `register()`: reads `coordinator.generation_id()`, compares with stored `SyncState.generation_id`, resets cursor to 0 if mismatch |
| A5-3 | Server generation_id | `AppState.generation_id: String` — `uuid::Uuid::new_v4()` at startup, included in RegisterAckPayload |
| A5-4 | CF worker generation_id | `static GEN: OnceLock<String>` — UUID at WASM module init, included in RegisterAckPayload |
| A5-5 | SQLite generation_id | `COALESCE(generation_id, '')` in SELECT; `generation_id` in INSERT OR REPLACE; new column backward-compatible via `#[serde(default)]` |
| A5-6 | All SyncState literals fixed | `client.rs`, `test_utils.rs`, `sqlite.rs`, `download.rs`, `conformance.rs` — all include `generation_id` |
| B | Push-Primary | `download_notify` from `()` to `Option<PendingMutation>`; `subscribe()` wired in download worker; bridge stream → mpsc channel; `process_push_mutation()` on DownloadQueue |
| C | HLC | `clock.rs` — `HybridLogicalClock`, `HlcTimestamp`; clock integrated into `VaultSyncClient`; OplogEntry timestamps use HLC walls |
| D | Snapshots | `try_fetch_snapshot()` in DownloadQueue; `snapshot_threshold` in DownloadConfig; auto catch-up via `list_snapshots` |
| E | MutationStore | `mutation_store.rs` — OPFS-backed queue; `push`, `pop_batch`, `ack`, `nack` methods; registered in WASM crate |
| F | Optimistic Writes | `SyncStatus::Optimistic` variant; all local writes use `Optimistic` + HLC timestamp; crash-recovery scans optimistic entries |
| G | Presence | `presence.rs` — `PresenceManager` with join/leave/heartbeat; `beforeunload` hook; peer tracking via BC |
| H | Metrics | `push_mutations_received`, `snapshots_applied`, `optimistic_writes`, `hlc_logical_wraps`, `active_peers` in `MetricsSnapshot` + both telemetry/no-telemetry impls |

### In Progress
- **Phase C: Follower push → reconciler gap** — Fixed via cursor gate. Two root causes converged: (1) `process_push_mutation()` called reconciler BEFORE checking cursor, so deduped pushes from a previous session bypassed `seen_ids` on a new session, hit `changed=false` inside reconciler, and never fired subscriptions. (2) Pull-path pushes (gen mismatch reconnect) were also deduped by `seen_ids` but cursor still advanced, causing the same symptom on next session.
- **Fix**: Moved cursor check (`seq <= last_sequence`) to the TOP of `process_push_mutation()`, before the reconciler call. Stale pushes rejected early. Post-reconciler logic simplified: always advance + persist, since gate guarantees `new_seq > cursor_before`.
- **Instrumentation**: Decision-point `info!` logs at every stage of `apply_remote_update()` (ENTER, dedup, doc_load, apply_update, snapshot_cmp, fire, EXIT).

### Blocked
- (none currently)

## Current Session — Phases 1-4 end-to-end

### Key Findings from Logs

**1. Generation mismatch on every refresh** (root cause):
- `read_sync_state cursor=58` then `generation mismatch: local=148bd163... server=61162831...` → cursor resets to 0
- Server gen `61162831...` is stable across sessions, so the `148bd163...` stored locally comes from a non-server source
- NOT from BroadcastChannel — gen check runs (step [5/9]) before BC setup (step after [9/9])
- Need to trace EVERY `write_sync_state` call to find who writes the stale gen

**2. Subscribe runs BEFORE generation check** (confirmed ordering bug):
- `do_connect()` issues subscribe INSIDE `register()`, using cursor before generation check
- `initialize()` runs generation check AFTER `register()` returns
- If mismatch detected, cursor is reset to 0 — but subscribe was already issued with old cursor
- Server pushes seq 1…N as historical replay (thinks client is behind)
- `process_push_mutation(seq=1)` compares `new_seq(1) > state.last_synced_sequence(58)` → FALSE → skips advance → cursor stays stale

**3. PushOutcome is NOT the right fix for stale pushes** (user vetoed):
- Changing `PushOutcome::Contiguous` to `GapDetected` for stale (new_seq <= cursor) pushes would cause a pull on EVERY duplicate during normal reconnect
- Correct fix: stop the replays from arriving at all (fix the subscribe timing)

**4. React subscription race** (independent of transport):
- `reconciler firing 0 subscription notifications` while storage has data
- `fetchInitial found 1 record` — React can read storage but subscriptions aren't registered yet
- Pure startup ordering bug: React subscribes after `new_with_coordinator()` resolves, missing the initial batch of subscription firings

**5. UI "3 pending" vs Rust "0 pending"** — stale React state cache, not a Rust bug

### Changes This Session

| Phase | Area | Change |
|-------|------|--------|
| 1 | `initialize()` | Added `[4.5/9] register returned cursor={}` after register() completes |
| 1 | `initialize()` gen check | Added `cursor=` to `[5/9]` log; `[5.5/9] cursor after gen check= (subscribe was with after=)` |
| 1 | `initialize()` gen OK path | Added `[sync_state] generation OK gen=` when no mismatch |
| 1 | `initialize()` cursor reset log | Changed to include `(was {cursor_before})` |
| 1 | Download worker | Added `t=` to `started` and `iter=N wake=Y` logs |
| 2 | `Coordinator` trait | Added `async fn disconnect()` default method (returns Ok(())) |
| 2 | `WasmWsCoordinator` | Implements `disconnect()` — closes WS, clears handlers, bumps conn_gen to kill old readers |
| 2 | `initialize()` reconnect | After generation mismatch: `coordinator.disconnect()` + `register(..., cursor=0)` with `[6/9]` and `[6.5/9]` logs |
| 2 | `ReplicaInfo` | Extracted once at top of `initialize()` to reuse across both register() calls |
| 3 | `process_push_mutation()` | Upgraded stale-push skip from `debug!` to `warn!` with "should not happen after Phase 2 fix" |
| 4 | `useQuery.ts` | Swapped order: `client.subscribe()` before `fetchInitial()` |
| 4 | `useVaultSyncOne.ts` | Swapped order: `client.subscribe()` before `fetchInitial()` |

### Completed Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | **Instrumentation** — [4.5/9], cursor before/after, t= timestamps | ✅ Done |
| 2 | **Generation reset fix** — disconnect + re-register with cursor=0 on mismatch | ✅ Done |
| 3 | **Stale push detection** — warn-level log to verify Phase 2 eliminates replays | ✅ Done |
| 4 | **React subscription race** — register subscribe before fetchInitial | ✅ Done |
| 5 | **Remove 30s polling timer** — download worker blocked on `download_rx.next().await` with bare `match` (no `select`), wakes only on push or bootstrap | ✅ Done |
| 6 | **Logging cleanup** — demoted 40+ `info!`/`console_log!` calls to `debug!`/`trace!`/`console_debug!`; added `console_debug!`/`console_warn!` macros for WASM; protocol chatter now invisible at default levels | ✅ Done |
| 7 | **Notification tracing** — `[notify] seq=N record=... phase=... t=...` logs at every stage (WASM callback → spawn_local → JS callback → microtask → React setData); `NOTIFY_SEQ` counter in WASM bridge correlates across boundary | ✅ Done |
| 8 | **DownloadQueue gen check** — converted from read-write (mutates cursor) to read-only verify (logs mismatch, does NOT reset cursor); authority lives in `initialize()` only | ✅ Done |
| 9 | **Clock skew removed from pull/push** — pull path (`process_batch`) and push path (`process_push_mutation`) no longer reject coordinator-validated mutations; P2P path retains strict check | ✅ Done |
| | **WASM build** | ✅ `wasm-pack build --target web` succeeded, pkg at `crates/vaultsync-wasm/pkg/` |

### Generation ID Instability

The root cause of WHY the local gen changes from `61162831...` to `148bd163...` between page refreshes is still unknown. The gen check runs at [5/9] before BC setup, so BroadcastChannel is not the culprit. Action: search every call to `write_sync_state()` / `save_sync_state()` and trace who writes `generation_id` to storage. This is a separate investigation from the subscribe timing fix.

### Expected Log Flow After All Fixes

```
[4/9] coordinator register cursor=58
[7/9] register
[8/9] subscribe after=58
[4.5/9] register returned cursor=58
[5/9] generation check server_gen=61162831... cursor=58
[sync_state] generation mismatch: local=148bd163... server=61162831... -> resetting cursor
[sync_state] cursor reset to 0 due to server generation change (was 58)
[5.5/9] cursor after gen check=0 (subscribe was with after=58)
[6/9] generation changed, reconnecting with cursor=0
[WasmWs] disconnect requested
[WasmWs] existing connection closed
[WasmWs] disconnect complete
[7/9] register                         ← second register()
[8/9] subscribe after=0                ← subscribe with cursor=0 THIS TIME
[6.5/9] re-register complete cursor=0
```

After reconnect, server pushes from seq 0 and all historical pushes arrive with `new_seq > cursor`, so `process_push_mutation` advances the cursor correctly.

## Key Decisions
- Generation ID in RegisterAckPayload (not separate message) — no extra RTT, backward compatible.
- SyncStateManager centralizes cursor + generation (replaces scattered cursor logic in DownloadQueue, client init, storage).
- `check_generation()` returns `bool` (true if reset happened). Server with empty gen = no generation tracking.
- TransportRouter uses internal unbounded channels + `wasm_bindgen_futures::spawn_local` for WASM-compatible stream merging.
- BC transport sends PendingMutation (plaintext, no encryption). WS transport sends via MSG_PUSH frame with encryption.
- CF worker generation_id: module-level `OnceLock<String>` (persists for worker isolate lifetime). Changes on restart, which is acceptable — D1 DB reset is rare enough that false positives on DO eviction are tolerable.
- Server generation_id: `AppState` field, UUID at startup. Restart = new gen = all clients reset cursor and re-download. Safe because mutations are durable in SQLite.
- HLC NOT added as a field on mutation structs (96+ breakage sites avoided). Instead, `HybridLogicalClock` lives on `VaultSyncClient` and its `.now().wall` replaces all client-side timestamp generation.
- Subscribe stream bridged through `futures::channel::mpsc::unbounded()` channel instead of directly using `Pin<Box<dyn Stream>>` in `select!` — avoids complex pinning lifetime issues in WASM-compatible code.
- Snapshot fetch is an optimization in `process_batch()` (before pull when cursor is behind), not a replacement for mutation pull. Falls back to normal pull if no snapshot available.
- Optimistic writes use `SyncStatus::Optimistic` variant instead of new bool field — minimal diff, backward-compatible serialization.
- Presence uses its own BroadcastChannel listeners rather than sharing the Transport BC — avoids message type confusion and keeps transport layer focused on mutations.
- Cursor reset during generation check is EAGER (in `initialize()`, before spawning workers), not lazy.
- **Subscribe-after-gen-check ordering is the bug, not subscribe's location.** Subscribe should be reissued if generation check resets cursor. Approach: reconnect (Option B), not move subscribe out of `do_connect()`.
- **Generation mismatch on refresh is NOT from BroadcastChannel.** Gen check runs at [5/9], BC is set up after [9/9]. The stale gen `148bd163...` was written by an earlier code path — must find every `write_sync_state()` call.
- **PushOutcome should NOT return GapDetected for stale pushes.** That would cause a pull on every duplicate during normal reconnect. The correct fix is to prevent stale pushes from arriving in the first place (fix the subscribe timing with gen check).
- **Download worker is now fully event-driven.** The `select(notify_fut, timer_30s)` replaced with bare `download_rx.next().await`. Worker blocks on the channel indefinitely — only wakes on `Some(Some(m))` (push) or `Some(None)` (bootstrap). No timer wake, no redundant pulls.
- **Heartbeat + compaction + discovery remain timer-based.** These are housekeeping tasks, not sync operations. Changing them would add complexity with no benefit.
- **Logging levels: `trace!` for protocol chatter, `debug!` for routine operations, `info!` for identity/state changes, `warn!` for anomalies, `error!` for failures.** Added `console_debug!` and `console_warn!` macros for WASM to match.
- **Notification tracing uses `[notify]` prefix with correlation `seq=N` at each stage.** The `NOTIFY_SEQ` atomic in the WASM bridge (`client.rs:308`) increments per-fire; correlated with Rust `[subscription] fire seq=N` log.
- **Clock skew: trust the coordinator, not the wall clock.** Mutations the coordinator accepted and serialized should not be rejected by the download worker. P2P retains strict checking because there is no central authority.
- **DownloadQueue::check_generation() is now read-only.** It logs a warning if generation mismatches but does NOT reset cursor. Authority is in `initialize()` only.
- **SyncStateManager is defined but NOT yet wired in.** It was designed as the single authority for cursor+gen management, but `initialize()` and `DownloadQueue` still read/write storage directly. Future work: wire SyncStateManager into the client struct and route all gen/cursor operations through it.

## Relevant Files
- `crates/vaultsync-core/src/transport/traits.rs` — `Transport` trait, `InboundMutation`, `TransportSource`, `TransportError`
- `crates/vaultsync-core/src/transport/mod.rs` — Core transport module re-export
- `crates/vaultsync-wasm/src/transport/broadcast_channel.rs` — `BroadcastChannelTransport` impl
- `crates/vaultsync-wasm/src/transport/ws_transport.rs` — `WasmWsTransport` impl
- `crates/vaultsync-wasm/src/transport/router.rs` — `TransportRouter` with merged incoming stream + channel-based dedup
- `crates/vaultsync-wasm/src/transport/mod.rs` — WASM transport module declaration
- `crates/vaultsync-core/src/sync/state.rs` — `SyncState` with `generation_id`
- `crates/vaultsync-core/src/sync/state_manager.rs` — `SyncStateManager` — centralized cursor + generation
- `crates/vaultsync-core/src/sync/events.rs` — `SyncEvents.download_notify` now `UnboundedSender<Option<PendingMutation>>`
- `crates/vaultsync-core/src/sync/download.rs` — `process_push_mutation()`, `try_fetch_snapshot()`, `DownloadConfig.snapshot_threshold`
- `crates/vaultsync-core/src/clock.rs` — `HlcTimestamp`, `HybridLogicalClock` — CAS-based `now()` and `update_with_received()`
- `crates/vaultsync-core/src/client.rs` — HLC clock, download worker with subscribe bridge + select loop, generation check, `SyncStatus::Optimistic`
- `crates/vaultsync-core/src/oplog/entry.rs` — `SyncStatus::Optimistic` variant
- `crates/vaultsync-core/src/telemetry/metrics.rs` — `MetricsSnapshot` with push/snapshot/optimistic/hlc/presence fields
- `crates/vaultsync-core/src/coordinator/ws_proto.rs` — `RegisterAckPayload` with `generation_id`
- `crates/vaultsync-core/src/coordinator/traits.rs` — `Coordinator` trait with `generation_id()` default method
- `crates/vaultsync-wasm/src/ws_coordinator.rs` — `generation_id()` impl, RegisterAck parsing, subscription + push notify
- `crates/vaultsync-wasm/src/mutation_store.rs` — `MutationStore` with OPFS-backed `push()`, `pop_batch()`, `ack()`, `nack()`
- `crates/vaultsync-wasm/src/presence.rs` — `PresenceManager` with join/leave/heartbeat over BroadcastChannel
- `crates/vaultsync-wasm/src/client.rs` — `notify_download(None)` bootstrap call
- `crates/vaultsync-coordinator-server/src/state.rs` — `AppState.generation_id`
- `crates/vaultsync-coordinator-server/src/ws.rs` — RegisterAck includes `state.generation_id`
- `crates/vaultsync-coordinator-server/src/main.rs` — UUID generation at startup
- `crates/vaultsync-coordinator-cf/src/lib.rs` — `server_generation_id()` static + RegisterAck payload
- `crates/vaultsync-core/src/storage/sqlite.rs` — SELECT/INSERT with `generation_id` column
- `crates/vaultsync-core/src/subscription/engine.rs` — `fire()` logs `seq=N` for correlation
- `crates/vaultsync-wasm/src/client.rs` — `NOTIFY_SEQ` atomic + `[notify]` logs at wasm_callback/spawn_local phases
- `sdk/packages/web/src/index.ts` — `[notify]` logs at js_callback/microtask_exec phases
- `sdk/packages/react/src/useQuery.ts` — `[notify]` logs at react_callback/react_setData phases
- `sdk/packages/react/src/useVaultSyncOne.ts` — `[notify]` logs at react_callback_one/react_setData_one phases
