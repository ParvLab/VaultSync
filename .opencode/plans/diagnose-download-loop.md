# Diagnose Download Worker Wake Loop

## Goal
Identify and fix the root cause of the download worker repeatedly waking up and applying the same mutations (`note-0t5eo7` applied 1300+ times) even though `pull(after=cursor)` returns empty and no new push notifications are arriving.

## Observed
- Repeated `[storage] apply mutation` for same record IDs (e.g. `note-0t5eo7`).
- `pull(after=cursor)` returns empty — cursor is current, no new server mutations.
- `cursor=N but pull returned empty` fires repeatedly — worker wakes without new data.
- `[ws] download notify sent` logs only during initial catch-up burst, not during repetition.
- Leader/follower transition from follower→leader once at startup (normal, expected).

## Known Fixes (coded, not yet deployed to WASM)
- **P0**: `client.rs:504-574` — removed `sub_rx` from download worker select. Select now listens only on `download_notify` + 30s safety timer.
- **P1**: `reconciler.rs:42-168` — `apply_remote_update`, `apply_encrypted_update`, `apply_batch` compare old vs new snapshot bytes before calling `subscriptions.fire()`. No `fire()` when data unchanged.
- **P2**: `SyncBar.tsx` — 1.5s UI debounce for "Syncing" status.
- **Idempotent push**: SQLite `INSERT OR IGNORE` + PG `ON CONFLICT DO NOTHING`.

## Hypotheses (in order of likelihood)
1. **Stale WASM binary** — browser running pre-fix artifact despite rebuild + hard reload. Vite aggressively caches `.wasm`.
2. **Dedup not preventing re-application** — mutation IDs differ between push and pull paths, or dedup cache is scoped per-reconciler instance and multiple reconcilers exist.
3. **Unidentified wake source** — neither `download_notify` nor timer, but something else waking the worker (subscription engine callback, transport router, etc.).
4. **Reconciler unconditionally firing subscriptions** — old code path calls `fire()` for every apply regardless of data change. Creates downstream churn even if not directly looping the worker.

## Instrumentation Plan

| Step | What | Where | Log output |
|------|------|-------|------------|
| 0 | Build identifier | `vaultsync-wasm/src/lib.rs` init | `info!("VaultSync WASM build: 2026-06-24-p0-p1")` |
| 1 | Wake reason | `client.rs` download worker select | `[download_worker] wake=Push\|Timer` |
| 2 | Mutation source | `reconciler.rs` | `[reconciler] source=push seq=N record=R id=ID` / `[reconciler] source=pull seq=N record=R id=ID` |
| 3 | Dedup hit/miss | `reconciler.rs` | `[dedup] id=ID record=R already_seen=true\|false` |
| 4 | Subscription fire count | `subscription/engine.rs` | `[subscription] fire doc=D record=R count=N` |

## Execution Order
1. Rebuild WASM with P0 + P1 → deploy → verify with build ID log.
2. If loop persists: add wake reason logging → retest.
3. If still looping: add mutation source logging → retest.
4. If source identified: add dedup logging to confirm or rule out.
5. If dedup not the issue: inspect subscription engine for cascading fire behavior.

## Files to Modify
- `crates/vaultsync-wasm/src/lib.rs` — add `BUILD_ID` const log at startup
- `crates/vaultsync-core/src/client.rs:504-574` — add `wake=Push|Timer` log in download worker select
- `crates/vaultsync-core/src/sync/reconciler.rs:42,76,114` — add `source=push|pull` log in `apply_remote_update`, `apply_encrypted_update`, `apply_batch`
- `crates/vaultsync-core/src/sync/reconciler.rs:27-40` — add `[dedup]` log in `is_deduped` and `mark_dedup`
- `crates/vaultsync-core/src/subscription/engine.rs` — add fire count log

## Non-Goals
- Do NOT speculate about React causing the loop (no evidence).
- Do NOT investigate leader/follower transition (normal at startup).
- Do NOT redesign architecture — only instrument and fix the wake loop.
