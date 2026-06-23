# VaultSync — Complete Development Roadmap

This document covers the full planned development of VaultSync across all phases.
Each phase builds on the previous. Phase 1 starts immediately.

---

## Architecture Reference

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT SIDE                              │
│                                                                 │
│  Browser          Mobile            Desktop       Server-side   │
│  ─────────        ──────────        ─────────     ────────────  │
│  @v/web           @v/expo           @v/tauri      @v/node       │
│  @v/react         @v/react-native   vaultsync-    @v/next       │
│  @v/vue           vaultsync-swift   rs (native)   @v/svelte(*)  │
│  @v/svelte        vaultsync-kotlin                              │
│  @v/next          vaultsync-dart                                │
│                                                                 │
│  All of these embed the VaultSync CRDT engine (WASM or native)  │
└────────────────────────┬────────────────────────────────────────┘
                         │ WebSocket (VaultSync protocol)
┌────────────────────────▼────────────────────────────────────────┐
│                   COORDINATOR SERVER                            │
│                   (Rust binary — already built)                 │
│                                                                 │
│   Memory ── Postgres ── Redis ── Custom                         │
└────────────────────────┬────────────────────────────────────────┘
                         │ WebSocket (server → client push)
┌────────────────────────▼────────────────────────────────────────┐
│                  BACKEND SDKs (Phase 4)                         │
│  Python  ──  Go  ──  (Java, .NET, Rust-native as needed)        │
│  Backend services that push data INTO VaultSync so it syncs     │
│  to all connected clients automatically                         │
└─────────────────────────────────────────────────────────────────┘
```

### Engine Architecture (already built)

These engine-level components are already implemented and power all client-side sync:

- **TransportRouter** — merges BroadcastChannel (same-origin, unencrypted) and WebSocket (encrypted, remote) transport streams into a single deduplicated mutation stream via internal channels and `wasm_bindgen_futures::spawn_local`
- **MutationStore** — OPFS-backed mutation queue for crash recovery; `push`, `pop_batch`, `ack`, `nack` survive page close without phantom edits
- **HLC Clock** — `HybridLogicalClock` with CAS-based `now()` and `update_with_received()`; all client timestamps use HLC walls for causal ordering
- **Generation ID** — per-server UUID sent in `RegisterAckPayload` (`#[serde(default)]` for backward compat); client compares on init and resets cursor to 0 if mismatch (handles server restart without stale state)

---

## Phase 1 — Example Projects (NOW)

> **Goal:** Delete the broken toy examples, build 3 professional real-world apps that showcase VaultSync as a complete sync engine. Fix CI which is currently broken due to deleted `todo-basic`.

### Critical Fix — CI is currently broken

`sdk/package.json` and `.github/workflows/ci.yml` both reference `examples/todo-basic` which was deleted.

#### [MODIFY] [sdk/package.json](file:///e:/drift/sdk/package.json)
```json
{
  "scripts": {
    "build": "npm run build:wasm && npm -w @vaultsync/web run build",
    "dev": "npm -w examples/notes run dev",
    "e2e": "playwright test --config examples/notes/playwright.config.ts"
  }
}
```

---

### App 1 — `notes` (also E2E harness)

**Package:** `@vaultsync/web` + `@vaultsync/react`  
**Runtime:** Browser  
**What a user sees:** A complete local-first notes app. Write offline — notes survive. Open in 3 tabs — they stay in sync. Encryption badge shows E2EE is active. Sync bar shows connection state and pending writes. This is the "hello world" of local-first.

#### Files
- `examples/notes/package.json` — React 18, Vite, `@vaultsync/web`, `@vaultsync/react`, Playwright
- `examples/notes/vite.config.ts` — dev on port 9876 (E2E compat)
- `examples/notes/tsconfig.json`
- `examples/notes/index.html` — semantic HTML5, meta tags, dark-mode
- `examples/notes/src/main.tsx` — VaultSyncProvider bootstrap (supports presence and metrics out of the box: `useSyncStatus()` returns `activePeers` and metric fields)
- `examples/notes/src/App.tsx` — sidebar + main editor layout
- `examples/notes/src/components/NoteList.tsx` — `useQuery('notes')`, search filter, new button
- `examples/notes/src/components/NoteEditor.tsx` — `useVaultSyncOne`, debounced auto-save
- `examples/notes/src/components/SyncBar.tsx` — `useSyncStatus()`, pending count, leader/follower, active peers via `SyncIndicator.showPeers`, expanded sync status with metrics
- `examples/notes/src/components/EncryptionBadge.tsx` — E2EE indicator, key fingerprint
- `examples/notes/src/style.css` — premium dark-mode, Inter font, micro-animations
- `examples/notes/e2e/offline.spec.ts` — write offline, reconnect, verify sync
- `examples/notes/e2e/multitab.spec.ts` — write in tab A, verify in tab B
- `examples/notes/e2e/leader.spec.ts` — close leader tab, verify new leader elected
- `examples/notes/README.md` — what it shows, how to run, architecture diagram

---

### App 2 — `collab-docs`

**Package:** `@vaultsync/react`  
**Runtime:** Browser  
**What a user sees:** Two users (two sync engines side by side) editing the same document. Both type simultaneously. CRDT merges their changes — no conflicts, no "document locked" messages. A merge log at the bottom shows each merge event in real time.

#### Files
- `examples/collab-docs/package.json`
- `examples/collab-docs/vite.config.ts`
- `examples/collab-docs/tsconfig.json`
- `examples/collab-docs/index.html`
- `examples/collab-docs/src/main.tsx`
- `examples/collab-docs/src/App.tsx` — two `VaultSyncProvider` instances (same namespace, different replicaId)
- `examples/collab-docs/src/components/DocPane.tsx` — editor for one replica, `useVaultSyncOne`
- `examples/collab-docs/src/components/PresenceBar.tsx` — which replicas are connected, uses `PresenceManager` wasm_bindgen export with dedicated BroadcastChannel for join/leave/heartbeat (separate from mutation transport)
- `examples/collab-docs/src/components/MergeLog.tsx` — live CRDT event stream
- `examples/collab-docs/src/style.css` — split-pane layout, dark-mode
- `examples/collab-docs/README.md`

---

### App 3 — `next-kanban`

**Package:** `@vaultsync/next` + `@vaultsync/react`  
**Runtime:** Browser + SSR (Next.js App Router)  
**What a user sees:** A Kanban project board. The server renders the initial board instantly (fast first paint). The client hydrates and goes live — drag cards between columns. Go offline, move cards, reconnect — changes sync automatically. Shows SSR + local-first working together.

#### Files
- `examples/next-kanban/package.json` — Next 14, `@vaultsync/next`, `@vaultsync/react`, `@hello-pangea/dnd`
- `examples/next-kanban/next.config.js`
- `examples/next-kanban/tsconfig.json`
- `examples/next-kanban/app/layout.tsx` — root layout, `VaultSyncHydrationProvider`
- `examples/next-kanban/app/page.tsx` — **Server Component**: fetch initial board, pass as `initialData`
- `examples/next-kanban/app/components/KanbanBoard.tsx` — `'use client'`, `DragDropContext`
- `examples/next-kanban/app/components/KanbanColumn.tsx` — `Droppable`, `useQuery` per column
- `examples/next-kanban/app/components/KanbanCard.tsx` — `Draggable`, `useVaultSyncOne`
- `examples/next-kanban/app/components/SyncStatus.tsx` — header badge, `useSyncStatus()`
- `examples/next-kanban/app/api/vaultsync/route.ts` — WebSocket upgrade handler
- `examples/next-kanban/app/globals.css`
- `examples/next-kanban/README.md`

---

### Phase 1 Verification
- `npm run build --prefix sdk` — all packages compile
- `npm run e2e --prefix sdk` — Playwright E2E tests pass (coordinator on port 9876)
- Each example runs with `npm run dev`

---

## Phase 2 — New Framework Packages

> **Goal:** Extend VaultSync to Vue 3 and SvelteKit. Developers using these frameworks should have the same first-class hooks/composables experience as React users.

### `sdk/packages/vue/` — `@vaultsync/vue`

**API surface mirrors `@vaultsync/react` but uses Vue 3 composables:**

- `VaultSyncPlugin` — Vue plugin (`app.use(VaultSyncPlugin, config)`)
- `useQuery(docId)` → `{ data: Ref<RecordFields[]>, loading: Ref<boolean> }`
- `useVaultSyncOne(docId, recordId)` → `{ record: Ref<RecordFields | null> }`
- `useSyncStatus()` → `{ connected: Ref<boolean>, pendingMutations: Ref<number>, activePeers: Ref<number>, metrics: Ref<MetricsSnapshot> }`
- `useVaultSyncMutations(docId)` → `{ insert, update, delete }`
- `SyncIndicator.vue` — drop-in status component

#### Files
- `packages/vue/package.json`
- `packages/vue/tsconfig.json`
- `packages/vue/src/plugin.ts` — Vue plugin, provides client via `inject/provide`
- `packages/vue/src/composables/useQuery.ts`
- `packages/vue/src/composables/useVaultSyncOne.ts`
- `packages/vue/src/composables/useSyncStatus.ts`
- `packages/vue/src/composables/useVaultSyncMutations.ts`
- `packages/vue/src/components/SyncIndicator.vue`
- `packages/vue/src/index.ts`
- `packages/vue/README.md`

### `sdk/packages/svelte/` — `@vaultsync/svelte`

**API surface uses Svelte stores and runes (Svelte 5 compatible):**

- `createVaultSync(config)` — returns a client context
- `queryStore(docId)` → Svelte readable store of `RecordFields[]`
- `syncStatusStore()` → Svelte readable store of `SyncStatus` (includes `activePeers` and `metrics`)
- `mutations(docId)` → `{ insert, update, delete }`
- `SyncIndicator.svelte` — drop-in component

#### Files
- `packages/svelte/package.json`
- `packages/svelte/tsconfig.json`
- `packages/svelte/src/context.ts` — setContext/getContext pattern
- `packages/svelte/src/stores/queryStore.ts`
- `packages/svelte/src/stores/syncStatusStore.ts`
- `packages/svelte/src/mutations.ts`
- `packages/svelte/src/components/SyncIndicator.svelte`
- `packages/svelte/src/index.ts`
- `packages/svelte/README.md`

### Phase 2 Examples
- `examples/vue-notes/` — same notes app concept, built in Vue 3 + Vite
- `examples/svelte-notes/` — same notes app concept, built in SvelteKit

---

## Phase 3 — Mobile Packages

> **Goal:** VaultSync on every platform. This is the biggest gap in the current sync engine ecosystem — Zero, LiveStore, ElectricSQL are all browser-only.

### `@vaultsync/expo` — React Native + Expo

**Approach:** Expo Modules API wraps the Rust `vaultsync-napi`/FFI layer. Uses `expo-sqlite` as local storage backend. JavaScript API identical to `@vaultsync/react`.

- Same hooks: `useQuery`, `useVaultSyncOne`, `useSyncStatus`, `useVaultSyncMutations`
- Offline: survives app backgrounding, uses `AppState` API
- Multi-instance: each device is a replica
- Storage: SQLite via `expo-sqlite`

#### Files
- `sdk/packages/expo/` — Expo module + TS hooks
  - `src/index.ts` — re-exports all hooks
  - `src/VaultSyncProvider.tsx`
  - `src/hooks/` — same API as `@vaultsync/react`
  - `ios/` — Expo native module (Swift bridge)
  - `android/` — Expo native module (Kotlin bridge)
  - `package.json`

#### Example
- `examples/expo-notes/` — React Native notes app, runs on iOS and Android

---

### `@vaultsync/tauri` — Desktop (Windows, macOS, Linux)

**Approach:** Tauri plugin that exposes the Rust VaultSync engine directly (no WASM overhead). Uses Tauri's `invoke` bridge. JavaScript API is identical to `@vaultsync/web`.

- Native performance (no WASM JIT overhead)
- Storage: SQLite or OS file system
- Works with any frontend framework (React, Vue, Svelte)

#### Files
- `sdk/packages/tauri/` — Tauri plugin
  - `src-tauri/src/lib.rs` — Tauri plugin exposing VaultSync commands
  - `src/index.ts` — JS bridge using Tauri `invoke`
  - `src/hooks/` — `useQuery`, `useSyncStatus` etc. for Tauri context
  - `package.json`
  - `Cargo.toml`

#### Example
- `examples/tauri-notes/` — Desktop notes app, shows native filesystem storage

---

### `vaultsync-swift` — iOS / macOS Native

**Approach:** Swift Package Manager package wrapping the Rust FFI via `UniFFI`. Provides Swift-native API with Combine publishers and async/await.

- `VaultSyncClient` Swift class
- `queryPublisher(docId:)` → `AnyPublisher<[Record], Never>`
- `syncStatusPublisher()` → `AnyPublisher<SyncStatus, Never>`
- Offline: persists through app termination
- SwiftUI `@StateObject` integration

#### Deliverables
- `native/swift/` — Swift package
- `VaultSync.xcframework` — pre-built binary for distribution

---

### `vaultsync-kotlin` — Android Native

**Approach:** Kotlin Multiplatform + JNI wrapping the Rust FFI. Provides Kotlin coroutine-based API with StateFlow.

- `VaultSyncClient` Kotlin class
- `query(docId: String): StateFlow<List<Record>>`
- `syncStatus(): StateFlow<SyncStatus>`
- Room-like DAO pattern option

#### Deliverables
- `native/kotlin/` — Kotlin library module
- Maven/Gradle artifact

---

### `vaultsync-dart` — Flutter (iOS, Android, Web, Desktop)

**Approach:** Flutter plugin wrapping Rust FFI via `flutter_rust_bridge`. Provides Dart streams API.

- `VaultSyncClient` Dart class
- `queryStream(docId)` → `Stream<List<Record>>`
- `syncStatusStream()` → `Stream<SyncStatus>`
- Works across all Flutter targets

#### Deliverables
- `native/dart/` — Flutter plugin
- pub.dev package

---

### Phase 3 Verification
- Each mobile package runs in its simulator/emulator
- Offline/online cycle works on real device
- Cross-platform: data written on iOS syncs to Android via coordinator

---

## Phase 4 — Backend SDKs

> **Goal:** Backend services (Python, Go) can push data INTO VaultSync so it syncs to all connected clients automatically. These are lightweight WebSocket clients — they do NOT embed the CRDT engine.

### Why Backend SDKs Exist

```
Stripe webhook → Python backend → vaultsync-python → Coordinator → All clients update instantly
GitHub CI event → Go service    → vaultsync-go    → Coordinator → All clients update instantly
```

Without backend SDKs, the only way to push server-side data to clients is via custom REST endpoints that clients must poll. Backend SDKs eliminate polling entirely.

---

### `vaultsync-python` — PyPI package

**API:**
```python
from vaultsync import VaultSyncBackendClient

async with VaultSyncBackendClient(coordinator_url, namespace) as client:
    await client.insert("orders", order_id, {"status": "shipped", "amount": 99.99})
    await client.update("orders", order_id, {"status": "delivered"})
    records = await client.find("orders")
```

- Async-first (asyncio)
- Sync wrapper available for Django/Flask
- Type hints throughout
- `pip install vaultsync`

#### Files
- `backends/python/` — Python package
  - `vaultsync/client.py`
  - `vaultsync/sync_client.py` — sync wrapper
  - `vaultsync/__init__.py`
  - `pyproject.toml`
  - `README.md`
  - `examples/fastapi_webhook.py` — Stripe webhook example
  - `examples/django_signal.py` — Django post_save signal example

---

### `vaultsync-go` — Go module

**API:**
```go
client, _ := vaultsync.NewBackendClient(coordinatorURL, namespace)
defer client.Close()

client.Insert(ctx, "orders", orderID, map[string]any{"status": "shipped"})
records, _ := client.Find(ctx, "orders")
```

- Standard `context.Context` support
- `go get github.com/parv68/vaultsync-go`

#### Files
- `backends/go/` — Go module
  - `client.go`
  - `types.go`
  - `go.mod`
  - `README.md`
  - `examples/webhook_handler.go`

---

### Phase 4 Verification
- Python: `pytest` suite — insert/update/delete/find all pass
- Go: `go test ./...` — all tests pass
- Integration test: Python backend inserts → browser client receives update

---

## Phase 5 — CLI + Migration Guide

> **Goal:** Zero-overhead project creation. A developer runs one command and has a working VaultSync app. Migration guide helps existing app developers adopt VaultSync without rebuilding.

### `create-vaultsync-app` CLI

```bash
npx create-vaultsync-app my-app
```

**Interactive prompts:**
```
? Project name: my-app

? Choose your framework:
  ❯ React
    Next.js
    Vue
    Svelte
    Vanilla JS (no framework)
    React Native (Expo)
    Tauri (desktop)

? Choose your coordinator backend:
  ❯ Memory     — dev only, zero setup, data lost on restart
    PostgreSQL — production ready, persistent SQL
    Redis      — production ready, pub/sub fanout
    Custom     — implement your own coordinator

? Coordinator URL: ws://localhost:9876

✓ Scaffolded ./my-app with React + PostgreSQL
✓ Run: cd my-app && npm install && npm run dev
```

**What gets generated (React + PostgreSQL example):**
```
my-app/
├── src/
│   ├── main.tsx          ← VaultSyncProvider configured
│   ├── App.tsx           ← example collection with CRUD
│   └── components/
│       └── SyncBar.tsx   ← sync status already wired
├── .env.example          ← DATABASE_URL, VAULTSYNC_*
├── docker-compose.yml    ← Postgres + coordinator server
├── package.json
├── vite.config.ts
└── README.md             ← what was generated, how to extend
```

#### Files
- `sdk/packages/create-vaultsync-app/`
  - `bin/create.ts` — CLI entry, interactive prompts via `@clack/prompts`
  - `templates/react/` — React template (based on `notes` example, cleaned up)
  - `templates/next/` — Next.js template (based on `next-kanban`, cleaned up)
  - `templates/vue/` — Vue template
  - `templates/svelte/` — SvelteKit template
  - `templates/vanilla/` — Plain `@vaultsync/web` + Vite
  - `templates/expo/` — React Native Expo template
  - `templates/tauri/` — Tauri desktop template
  - `package.json` — `bin: { "create-vaultsync-app": "./dist/bin/create.js" }`
  - `README.md`

---

### Migration Guide

`MIGRATING.md` at the repo root — covers how to add VaultSync to an existing app without rebuilding.

**Sections:**
1. **Mental model** — VaultSync replaces your data-fetching layer, not your backend
2. **React migration** — swap `useEffect + fetch` → `useQuery`, step by step
3. **Next.js migration** — add `VaultSyncHydrationProvider` to root layout
4. **Data seeding** — one-time script to push existing DB records into the coordinator
5. **Incremental adoption** — run VaultSync alongside existing REST calls, migrate collection by collection
6. **Schema mapping** — map your existing DB tables to VaultSync collections

---

## Summary — Full Delivery Order

| Phase | Deliverable | What gets built |
|---|---|---|
| **1** | Example Projects | `notes`, `collab-docs`, `next-kanban` + CI fix |
| **2** | Framework Packages | `@vaultsync/vue`, `@vaultsync/svelte` + example apps |
| **3** | Mobile Packages | `@vaultsync/expo`, `@vaultsync/tauri`, Swift, Kotlin, Dart |
| **4** | Backend SDKs | `vaultsync-python`, `vaultsync-go` |
| **5** | CLI + Migration | `create-vaultsync-app`, `MIGRATING.md`, `ROADMAP.md` |
