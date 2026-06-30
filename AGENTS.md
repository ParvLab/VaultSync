# Anchored Summary

## Goal
- Complete bidirectional sync fix: eliminate notification loop caused by stale-focus guard, then stabilize with stress testing

## Constraints & Preferences
- WASM build target (`wasm-pack build --target web`)
- TypeScript SDK build via `npm run build` in `sdk/`
- All Rust crates must compile (`cargo check --workspace`)
- Instrumentation logs must clearly identify each pipeline stage with unique prefixes

## Progress
### Done
- **Bug A — Per-tab OPFS storage path fix**: Changed `crates/vaultsync-wasm/src/client.rs` lines 58 and 119 from `format!("{}_db", namespace)` to `format!("{}_{}_db", namespace, replica_id)`. Each tab now has its own `index.json`. Cross-tab OPFS lost-update race eliminated.
- **Bug B — Focus guard notification loop**: Identified and fixed the root cause of one-way replication in `sdk/examples/notes/src/components/NoteEditor.tsx`. The `document.activeElement` focus guard at line 60 blocked external merges when the textarea merely had focus (even with no active typing), causing the follower to auto-save stale state back to the leader via BC commands, creating an infinite loop.
- **Fix applied**: Replaced binary `document.activeElement` check with edit-timestamp cooldown. Added `lastEditRef.current` tracking per field (`{ title: number, body: number }`) and `FOCUS_COOLDOWN_MS = 2000`. A field is "actively editing" only if it has focus AND the user typed within 2 seconds. Stale focus does not block merge. Added `[FocusGuard] console.warn` logs.
- **BC command instrumentation**: Added `[BC] FOLLOW_INSERT_BEGIN` / `[BC] FOLLOW_UPDATE_BEGIN` logs on the follower side (payload_len + first 500 chars of payload) and `[BC] LEADER_INSERT_BEGIN` / `[BC] LEADER_UPDATE_BEGIN` logs on the leader side (field_count, keys, payload_len + first 300 chars of payload)
- **Build verification**: All Rust crates pass `cargo check --workspace`, WASM binary builds, all 5 SDK packages build clean
- **Phase 4 — Replay batching**: Aggregates stale push skips into single `debug!` line with `swap(0)` counter
- **Phase 5 — Upload worker instrumentation**: WAKE/OFFLINE/RECONNECTED/BATCH markers with stage prefixes
- **vaultsync-cli fix**: Added missing `FireSource::LocalWrite` argument
- **vaultsync-napi fix**: `*const` raw pointers → `usize` for `#[napi]` Send bound
- **Bidirectional sync confirmed**: Both leader→follower and follower→leader work reliably. Logs show clean `PUSH_SEND → PUSH_ACK → AFTER_MARK_SYNCED pending=0 → BATCH_DONE` on every upload. Reconciler fires `subscription.fire` once per mutation. Document content is identical on both tabs across multiple alternating edits.

### In Progress
- **Stress test suite**: 6 scenarios to run before removing instrumentation: (1) 3 tabs simultaneous edits, (2) leader closes while follower editing (leader election), (3) offline → edit → reconnect, (4) refresh one tab during continuous edits, (5) create/delete multiple notes concurrently, (6) 15-minute soak with continuous alternating edits.

### Blocked
- (none)

## Key Decisions
- **Focus guard was the root cause of one-way replication**: The `NoteEditor.tsx` `document.activeElement` check at line 60 prevented the follower from merging external edits when its textarea had focus (even stale, no-active-typing focus). This caused the auto-save to fire with stale local state, sending a BC command to the leader that overwrote the correct data with stale data. The subsequent push then re-triggered the cycle.
- **Edit-timestamp cooldown replaces binary focus check**: Instead of checking `document.activeElement?.id !== 'note-body-textarea'`, the fix checks if the field has focus AND the user has typed within `FOCUS_COOLDOWN_MS` (2 seconds). This protects active editing while allowing external merges when the focus is stale.
- **The fix is a circuit breaker, not a semantic version guard**: The fix uses string equality (via `lastEditRef` timestamps + content hashing) rather than CRDT document revision IDs. A more robust solution would track `lastAppliedRemoteRevision` to prevent re-saving data that arrived from remote.

## Next Steps
1. **Stress test**: Run 3+ tab multi-scenario stress test suite before removing instrumentation
2. **Demote instrumentation logs**: Move `[opfs_flush]`, `[BC fire_subscription]`, `[FocusGuard]`, upload pipeline markers from `info!`/`console.warn` to `trace!`/`console.debug` after stress tests pass
3. **Tag release**: Once stable, tag a release and update AGENTS.md with final status

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
