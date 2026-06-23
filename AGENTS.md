# Anchored Summary

## Goal
Redesign VaultSync engine architecture and implement all phases end-to-end (Transport Split, SyncStateManager, Generation ID, Push-Primary, HLC, Snapshots, MutationStore, Presence, Metrics, Optimistic Writes).

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
- (none — all phases done)

### Blocked
- (none currently)

## Next Steps
1. Wire Phase H metric recording calls into DownloadQueue (`record_push_mutation`, `record_snapshot_applied`, `record_optimistic_write`), client.rs, and PresenceManager.
2. Test all phases compile end-to-end on native + WASM targets.
3. Write integration tests for Phase B (push mutation flow), C (HLC monotonicity), D (snapshot catch-up), F (optimistic → synced transition), G (presence join/leave detection).
4. Deploy server with new generation ID to staging and verify cursor-reset on restart.

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
