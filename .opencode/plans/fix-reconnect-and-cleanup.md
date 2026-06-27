# Fix Reconnect Notification + Cleanup

## Diagnostics Complete
All instrumentation is deployed (BUILD_ID, wake reason, mutation source, dedup, fire count). Results:

- ✅ **P0/P1 verified running** — BUILD_ID confirmed fresh WASM binary
- ✅ **Download worker healthy** — `wake=Push` on push, `wake=Timer` every 30s
- ✅ **Dedup working** — `already_seen=true` for duplicate IDs
- ✅ **Pull-after-push correct** — cursor is current, 0 new mutations

## Remaining Issues

### P0 — Reconnect doesn't notify workers (CORRECTNESS BUG)
When WebSocket dies and reconnects, `push()`, `pull()`, `subscribe()` all call `connect_and_handshake()`, but no notification fires to wake the upload/download workers. Result: pending uploads stall until next local mutation.

**Fix** (`vaultsync-wasm/src/ws_coordinator.rs`):
1. Add `upload_notify: Mutex<Option<mpsc::UnboundedSender<()>>>` to `WasmWsCoordinatorInner`
2. Add `set_upload_notify(&self, tx: ...)` method
3. In `connect_and_handshake()`, after successful connection (`if result.is_ok()`), send:
   - `upload_notify.unbounded_send(())`
   - `download_notify.unbounded_send(None)`
4. In `WasmClient::new()` (`vaultsync-wasm/src/client.rs`), wire up upload_notify after line 113:
   ```rust
   coordinator.set_upload_notify(client.events.upload_notify.clone());
   ```

### P1 — UI startup state shows incorrect "Follower"
During startup, the UI briefly shows "Follower" before leader election completes. The initial role should be `Unknown` / `Connecting...`.

**Fix** (`sdk/packages/react/src/...`):
- Change initial role from `Follower` to `Unknown`
- Render "Connecting..." in SyncBar when role is Unknown

### P2 — Skip unnecessary pull after push (PERFORMANCE)
After `process_push_mutation(m)`, the worker calls `process_batch()` unconditionally. But if `m.sequence == cursor + 1`, there's no gap — skip the pull.

**Fix** (`crates/vaultsync-core/src/sync/download.rs` or `client.rs`):
- Track `last_push_seq` in the download worker loop
- Skip `process_batch()` inner loop when `m.sequence == last_sequence + 1`

### P3 — Increase safety poll interval
Change from 30s to 60s (or 120s) after stability confirmed.

## Execution Order
1. P0 (reconnect) — only remaining correctness bug
2. P1 (UI startup) — cosmetic but visible
3. P2 (skip pull) — optimization
4. P3 (increase timer) — minor tuning

## Files to Modify (P0 only)
- `crates/vaultsync-wasm/src/ws_coordinator.rs` — add `upload_notify` field + setter + reconnect notification
- `crates/vaultsync-wasm/src/client.rs` — wire `set_upload_notify()` after `set_download_notify()`

## Verification
- Hard-reload browser, kill WS server, wait for disconnect, restart server
- After reconnect, verify "upload worker" processes pending mutations without local edit
- Confirm no extra `wake=Push` logs without actual push messages
