# Anchored Summary

## Goal
- Eliminate oplog bloat and notification loop: filter `updatedAt` from CRDT writes, autoset on content change only, implement WASM storage compaction

## Constraints & Preferences
- WASM build target (`wasm-pack build --target web`)
- TypeScript SDK build via `npm run build` in `sdk/`
- All Rust crates must compile (`cargo check --workspace`)
- Compaction methods must use `js_sys::Date::now()` for timestamps (not `crate::time_utils`)

## Progress
### Done
- **`updatedAt` metadata normalization**: `VaultSyncClient::filter_meta_fields()` strips `updatedAt`/`updated_at` from user-provided fields before CRDT apply in both `insert()` and `update()`. Engine auto-sets `updatedAt` to HLC wall clock only when content fields actually change. No-op saves (no content change) skip mutation entirely (`[client.update] SKIP no-op mutation`).
- **OPFS compaction implemented**: 5 methods (`delete_synced_oplog_older_than`, `list_tombstoned_documents`, `list_active_documents`, `read_synced_oplog_for_document`, `delete_synced_oplog_before_timestamp`) fully implemented for both `OpfsStorage` and `IndexedDbStorage` backends.
- **Aggressive compaction defaults**: Changed from 7-day/500-entry to 24h age cutoff, 4h tombstone grace, 100-entry snapshot threshold (`CompactionConfig` defaults).
- **Compaction runs on all tabs**: Age-based cleanup runs every 5 min on ALL tabs (not just leader). Snapshot compaction stays leader-only (runs every 15 min).
- **Notes example updated**: Removed `updatedAt` from `App.tsx:insert()` and `NoteEditor.tsx:auto-save` — engine handles it automatically.
- **Build verification**: `cargo check --workspace` (0 new warnings), `wasm-pack build --target web` (0 errors), `npm run build` (all 5 SDK packages + notes Vite bundle).

### In Progress
- (none — all three hardening items addressed)

### Blocked
- (none)

## Key Decisions
- **`updatedAt` filtered at core client level, not just SDK**: Applications that include `updatedAt` in mutations are automatically handled. No app-level changes needed beyond removing the redundant field. The engine uses HLC wall clock (not user-provided value) to prevent mutation uniqueness when content is identical.
- **Compaction uses `js_sys::Date::now()` in WASM**: The `crate::time_utils::system_time_now_ms()` is not available in the WASM crate (it's in vaultsync-core only). `js_sys::Date::now()` returns the same wall-clock milliseconds.
- **Age-based compaction safe on all tabs**: Each tab independently cleans up its own oplog by removing Synced entries past the cutoff. No cross-tab race because `index_transaction`/`tx_lock` serializes writes; concurrent deletions of different entries are idempotent.
- **Snapshot compaction stays leader-only**: Re-writing document snapshots + bulk-deleting synced oplog entries is not idempotent if two tabs run it simultaneously. Leader-only avoids cross-tab races.

## Next Steps
1. **Stress testing**: Run multi-tab scenarios to verify oplog stays bounded and notification loop eliminated
2. **Logging cleanup (after stress tests pass)**: Demote ~200 log lines to `trace!`/`debug!`, install level-filtered WASM tracing subscriber
3. **Tag release** once stable

### Logging Cleanup Plan (Detailed)
When ready to implement, the work splits into these phases:

| Phase | Scope | Files | Est. Changes |
|-------|-------|-------|-------------|
| L1 | Add `LogLevel` to config + WASM tracing subscriber | `core/src/config.rs`, `wasm/src/lib.rs`, `sdk/web/src/types.ts` | 3 files |
| L2 | Demote Rust core instrumentation (`reconciler.rs`, `download.rs` internals) | `core/src/sync/reconciler.rs`, `download.rs`, `state_manager.rs` | ~90 lines |
| L3 | Demote WASM instrumentation (`client.rs`, `ws_coordinator.rs`, `storage.rs`) | `wasm/src/client.rs`, `ws_coordinator.rs`, `storage.rs` | ~60 lines |
| L4 | Demote notify pipeline + JS SDK logging | `sdk/web/src/index.ts`, `sdk/react/src/useQuery.ts`, `useVaultSyncOne.ts`, `sdk/web/src/sync.ts`, `subscription.ts` | ~20 lines |
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
