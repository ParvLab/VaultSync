# Anchored Summary

## Goal
Same-browser CRDT sync via BroadcastChannel invalidation + shared OPFS, with deterministic offline mode that fully decouples tab-to-tab sync from the coordinator server.

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
- **Cross-browser test (Chrome ↔ Firefox)** — only path where BC doesn't apply; push path must carry the full sync load. Test all four directions in the matrix.
- **Log noise reduction** — `INDEX LOAD`, `FLUSH_BEFORE`, `PENDING_READ`, `MARK_SYNCED`, `listeners=3` — demote to `trace!` for cleaner signal.

### Blocked
- (none currently)

## Current Session — Same-Browser Sync + Offline Mode

### Key Findings from Logs

**1. Same-browser sync breaks because shared OPFS makes server push redundant:**
- Tab A writes → OPFS → server receives push → Tab B gets server push notification
- Tab B calls `process_push_mutation()` → reconciler reads OPFS → Tab A's data already visible (shared OPFS)
- `apply_update()` sees `redundant=true snapshot_changed=false` → no React re-render
- Fix not in the reconciler — fix is to notify via BC before server push even arrives

**2. `broadcast_p2p` is a no-op on WASM** (`client.rs:1458`):
- `#[cfg(target_arch = "wasm32")] async fn broadcast_p2p(...) {}` — empty function
- No P2P mutation transport exists for same-browser tabs
- Confirmed: zero `postMessage` calls in the entire codebase for `vaultsync-ipc-*` channels

**3. JS BroadcastChannel listener already exists and works:**
- `index.ts:46-70` — `setupBroadcastChannel()` creates `vaultsync-ipc-{namespace}` listener
- Parses `{doc_id, record_id}`, calls `fire_subscription()` via `setTimeout(0)`
- `fire_subscription()` does a real OPFS read (`self.client.get().await`), not a cache hit
- Confirmed alive via console logs — but nothing was sending to it

**4. CoordinatorMode::Offline gives deterministic startup:**
- No WS connection attempt, no heartbeat, no reconnect loop
- Register/gen check skipped entirely — clean startup in 2 lines
- Download worker sleeps forever (no pushes, no timer) — harmless
- All reads/writes/subscriptions/BC notifications work without coordinator

### Changes This Session

| Phase | Area | Change |
|-------|------|--------|
| 12 | `sync/state.rs` | Added `SyncState::new(namespace, generation_id)` constructor — makes invalid (empty gen) construction impossible through the typed API |
| 12 | `client.rs` | `read_sync_state` log expanded: `cursor={} gen={} backend={} namespace={} took {} ms` — complete diagnostic on startup |
| 12 | `download.rs` | All 3 `read_sync_state=None` fallback paths now call `fallback_generation_id()` + emit `warn!` log with namespace/caller context; prevents gen wipe that caused stale-gen reconnect loop on every page load |

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
| 10 | **BC notification for same-browser sync** — persistent `cross_tab_channel` on `WasmVaultSyncClient`; `notify_cross_tab()` posts `{doc_id, record_id, tab_id}` after every write; JS-side self-filtering via `tab_id` comparison | ✅ Done |
| 11 | **CoordinatorMode enum** — `CoordinatorMode { Online, Offline }` in `VaultSyncConfig`; deterministic offline init skips register, gen check, WS entirely; JS SDK exposes `mode: 'online' | 'offline'` | ✅ Done |
| 12 | **Generation fallback hardening** — `SyncState::new()` constructor; all 3 `read_sync_state=None` paths in `download.rs` preserve coordinator gen; `read_sync_state` log includes `gen=`/`backend=`/`namespace=` | ✅ Done |
| | **WASM build** | ✅ `wasm-pack build --target web` succeeded, pkg at `crates/vaultsync-wasm/pkg/` |

### Generation ID Instability

The root cause was the 3 `read_sync_state=None` fallback paths in `download.rs` that constructed `SyncState { generation_id: String::new(), .. }` — if `read_sync_state` ever returned `None` (even once, due to an ordering race or OPFS visibility delay), the subsequent `write_sync_state` would persist `generation_id=""`. Next page load would read `gen=""` → mismatch with server gen → reconnect loop.

Fix: all 3 fallback paths now call `self.fallback_generation_id().await` (which reads `coordinator.generation_id()`) instead of hardcoding `String::new()`. Additionally, a `warn!` log fires on every `None` case to detect if the race still occurs. The `SyncState` struct now has a `SyncState::new(namespace, generation_id)` constructor to prevent invalid construction in future code.

The 3 hardened paths:
1. `process_batch` pull (line ~196) — `caller=process_batch`
2. `try_fetch_snapshot` (line ~390) — `caller=try_fetch_snapshot`
3. `process_push_mutation` (line ~518) — `caller=process_push_mutation`

If `[download_queue] read_sync_state=None` logs never appear, the theory was wrong and the race doesn't exist. If they do appear occasionally, the fix prevents data loss while more investigation is needed.

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
- **Same-browser sync uses BC invalidation, not server push.** Shared OPFS means push will always be redundant. BC notification + OPFS re-read is the correct pattern.
- **CoordinatorMode enum (`Online`/`Offline`) instead of `skip_coordinator: bool`.** Enums evolve better than booleans — future modes (P2P, LAN, Testing) slot in without refactoring.
- **Deterministic offline init:** `CoordinatorMode::Offline` skips register, gen check, and WS entirely. No failed connection attempts, no heartbeat timers, no log noise. Not reactive (try-fail-continue) — intentionally offline.
- **BC payload is an invalidation bus, not a data channel.** Only `{doc_id, record_id, tab_id}` — no document data, no CRDT ops, no encryption concerns.

## Relevant Files
- `crates/vaultsync-core/src/sync/state.rs` — `SyncState` with `generation_id` and `SyncState::new()` constructor
- `crates/vaultsync-core/src/sync/download.rs` — 3 fallback paths with `fallback_generation_id()` + warning logs
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
- `crates/vaultsync-core/src/config.rs` — `VaultSyncConfig` with `coordinator_mode: CoordinatorMode`
- `crates/vaultsync-core/src/lib.rs` — re-exports `CoordinatorMode`
- `sdk/packages/web/src/index.ts` — `VaultSyncClient.create()` routes by `mode`, BC self-filter by `tabId`
- `sdk/packages/web/src/types.ts` — `VaultSyncConfig.mode?: 'online' | 'offline'`
- `sdk/packages/react/src/context.tsx` — `VaultSyncProvider` includes `config.mode` in deps
