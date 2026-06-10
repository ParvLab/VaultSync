# 🌌 VaultSync

[![CI Status](https://github.com/parv68/VaultSync/actions/workflows/ci.yml/badge.svg)](https://github.com/parv68/VaultSync/actions)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/vaultsync-core.svg)](https://crates.io/crates/vaultsync-core)
[![npm](https://img.shields.io/npm/v/@vaultsync/web.svg)](https://www.npmjs.com/package/@vaultsync/web)
[![WASM](https://img.shields.io/badge/target-wasm32--unknown--unknown-green)](crates/vaultsync-wasm)

> **Conflict-free, end-to-end encrypted, infrastructure-agnostic synchronization for the local-first era.**

VaultSync is an embedded local-first synchronization and replication runtime with CRDT-native conflict resolution, end-to-end encryption, multi-tab/process safety, and zero-trust coordinator architecture. It makes your application work **instantly offline**, sync **automatically when online**, and feel **0ms fast** — because every read and write hits a local database, never a remote server.

---

## Table of Contents

- [Why VaultSync?](#-why-vaultsync)
- [The Business Case — Real Benefits](#-the-business-case--real-benefits)
- [How It Works — High-Level](#-how-it-works--high-level)
- [Core Architecture](#-core-architecture)
- [CRDT Document Model](#-crdt-document-model)
- [End-to-End Encryption (E2EE)](#-end-to-end-encryption-e2ee)
- [Multi-Tab & Multi-Process Safety](#-multi-tab--multi-process-safety)
- [Offline Support & Durability](#-offline-support--durability)
- [Synchronization Flow](#-synchronization-flow)
- [Reactive Subscriptions](#-reactive-subscriptions)
- [Coordinator Abstraction Layer](#-coordinator-abstraction-layer)
- [Performance Model](#-performance-model)
- [Deployment Modes](#-deployment-modes)
- [SDK Reference — Quickstart](#-sdk-reference--quickstart)
- [Schema System & Migrations](#-schema-system--migrations)
- [Observability & Debuggability](#-observability--debuggability)
- [Security Model](#-security-model)
- [Real-World Use Cases](#-real-world-use-cases)
- [VaultSync vs The World](#-vaultsync-vs-the-world)
- [Repository Structure](#-repository-structure)
- [Contributing](#-contributing)
- [Production Checklist](#-production-checklist)

---

## 🚀 Why VaultSync?

The traditional web architecture forces every user interaction through a server:

```
User clicks button
     ↓
HTTP Request → API Server → Remote Database
     ↓
Wait 100–2000ms for round-trip
     ↓
UI updates
```

This creates a cascade of real problems:

- **Your app is unusable offline.** No server = no data. A 2-second underground subway trip breaks your product.
- **Every interaction has latency.** Every click waits for a network round-trip. On mobile under 4G, that's 200–2000ms.
- **High server costs.** Every read and write is an API call. 1 million users = 1 million round-trips per page load.
- **Realtime collaboration is complex.** You need WebSockets, event buses, conflict resolution logic — all custom.
- **Server outages break everything.** When your backend goes down, every user sees an error screen.
- **Data breaches expose everything.** If your server is compromised, all user data is exposed in plaintext.

---

## 💡 The Business Case — Real Benefits

VaultSync flips the model: **data lives on the device first, syncs to the coordinator in the background.**

### ⚡ Feel 0ms Fast — Instant Reads and Writes

Every read and write goes to a **local embedded database** (SQLite on native, OPFS or IndexedDB in-browser). There is no network round-trip on the critical path.

```
User clicks button
     ↓
Local CRDT write → SQLite/OPFS  ← sub-millisecond
     ↓
UI updates instantly
     ↓ (background, async)
Encrypted mutation uploads to coordinator
     ↓
Other devices receive & merge
```

**Perceived latency: 0ms.** Users feel no lag even on slow networks, because the UI updates before the network is involved.

### 📴 Work Fully Offline — Forever

Users can read and write all day on an airplane with no internet. Every mutation is stored locally as an encrypted CRDT entry. When connectivity returns, VaultSync automatically uploads everything and downloads everything they missed — no manual conflict resolution, no data loss.

A user who makes 47 changes offline lands, reconnects, and every change syncs without them lifting a finger.

### 💸 Dramatically Lower Server Costs

With VaultSync, **reads no longer touch your API servers.** A user browsing through 1000 records creates 0 server requests — all data is served from local storage. Server load drops to only sync operations (uploads and downloads of CRDT mutations), which are batched and efficient.

In real applications, this reduces API call volume by **60–95%**, cutting infrastructure costs proportionally.

### 🤝 Real-Time Collaboration — Built-In

When two users edit the same record at the same time, VaultSync's **CRDT merge algorithm** handles it automatically and deterministically. There are no conflicts to resolve, no "someone else changed this" dialogs, no last-write-wins data loss. Both changes are preserved.

### 🔒 Zero-Trust Security — Built-In E2EE

The sync server (coordinator) **never sees your data**. Every CRDT mutation is encrypted on-device with X25519 + ChaCha20-Poly1305 before leaving. The coordinator stores only encrypted blobs. Even if your sync infrastructure is breached, no user data is exposed.

This means VaultSync is **HIPAA-ready by default** — the coordinator literally cannot read patient records.

### 🗂️ Multi-Tab Safety — Built-In

Open the same app in 3 browser tabs. Close the one that was writing. VaultSync automatically elects a new leader, replays any in-flight mutations, and continues — with **no data loss, no corruption, no stale reads**.

---

## 🔭 How It Works — High-Level

```
┌──────────────────────────────────────────────────────────────────┐
│                    Your Application Process                       │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │   Application Layer                                          │  │
│  │   vaultsync.db.todos.insert({ text: "buy milk" })               │  │
│  │   vaultsync.db.todos.subscribe(callback)                         │  │
│  └─────────────────────────┬───────────────────────────────────┘  │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────────┐  │
│  │   VaultSync Core Engine (Rust + WASM)                            │  │
│  │                                                               │  │
│  │   ┌────────────────────┐  ┌──────────────────────────────┐  │  │
│  │   │  CRDT Layer (Yrs)  │  │  E2EE Encryption             │  │  │
│  │   │  Conflict-free     │  │  X25519 + ChaCha20Poly1305   │  │  │
│  │   │  merge of all ops  │  │  Coordinator sees 0 plaintext│  │  │
│  │   └──────────┬─────────┘  └────────────────┬─────────────┘  │  │
│  │              │                              │                 │  │
│  │   ┌──────────▼──────────────────────────────▼─────────────┐  │  │
│  │   │  Multi-Tab IPC Layer                                   │  │  │
│  │   │  Leader Election + Shared Memory + Crash Recovery      │  │  │
│  │   └────────────────────────┬───────────────────────────────┘  │  │
│  │                            │                                  │  │
│  │   ┌────────────────────────▼───────────────────────────────┐  │  │
│  │   │  Oplog & Sync Engine                                   │  │  │
│  │   │  Append-only encrypted CRDT mutation log               │  │  │
│  │   │  Upload queue + retry + batching                       │  │  │
│  │   │  Download queue + replay + CRDT merge                  │  │  │
│  │   └────────────────────────┬───────────────────────────────┘  │  │
│  │                            │                                  │  │
│  │   ┌────────────────────────▼───────────────────────────────┐  │  │
│  │   │  Storage Abstraction                                   │  │  │
│  │   │  SQLite / OPFS / IndexedDB / RocksDB / In-Memory       │  │  │
│  │   └────────────────────────┬───────────────────────────────┘  │  │
│  │                            │                                  │  │
│  │   ┌────────────────────────▼───────────────────────────────┐  │  │
│  │   │  Transport Abstraction                                 │  │  │
│  │   │  Coordinator (WebSocket) │ P2P (WebRTC) │ Mesh (libp2p)│  │  │
│  │   └────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                                     │
              ┌──────────────────────┴────────────────────────┐
              │   Zero-Trust Coordinator (your choice)          │
              │   Postgres │ Redis │ Cloudflare DO │ Custom     │
              │   (only sees encrypted blobs + routing metadata)│
              └─────────────────────────────────────────────────┘
```

---

## 🏗️ Core Architecture

VaultSync is built on three non-negotiable design principles:

### 1. CRDT-Native From Day One

Concurrent writes **never conflict**. There is no V1 (last-write-wins) → V2 (field merge) → V3 (CRDT) upgrade path. Every operation from the first line of code is a Yrs CRDT mutation. All replicas converge deterministically regardless of write order.

| CRDT Type | Merge Behavior | Use Cases |
|---|---|---|
| **LWW-Register** | Last-write-wins by hybrid logical clock | Text fields, status flags, scalars |
| **PN-Counter** | Increment/decrement sums across all replicas | Likes, votes, inventory counts |
| **OR-Set** | Add/remove with per-element tombstones | Tags, labels, member lists |
| **Yrs Text** | Sequence CRDT (OT-compatible) | Rich text, collaborative documents |
| **Yrs Array** | Ordered sequence with insert/delete/move | To-do lists, ranked items |

### 2. Zero-Trust Coordinator

The sync server is **cryptographically blind** to your data. The coordinator stores:

```json
{
  "id": "mut_abc123",
  "namespace": "workspace:core",
  "replica_id": "laptop-abc",
  "encrypted": "<base64 ciphertext>",
  "timestamp": 1740000000,
  "sequence": 42
}
```

No field names. No field values. No schema info. Even if your coordinator is fully compromised, attackers see only encrypted bytes. Decryption keys never leave the device.

### 3. Infrastructure-Agnostic

The `Coordinator` is a **Rust trait**, not a specific service. Choose or build:

```rust
pub trait Coordinator: Send + Sync + Debug {
    async fn push(mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>>;
    async fn pull(after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>>;
    async fn subscribe(from_sequence: SequenceId) -> Result<Box<dyn Stream<...>>>;
    async fn register(namespace: &str, info: ReplicaInfo) -> Result<()>;
}
```

Swap from Postgres to Redis to Cloudflare DO without changing a single line of application code.

---

## 🔀 CRDT Document Model

### How CRDTs Eliminate Conflicts

Traditional sync engines treat conflicts as exceptional — something to detect and resolve after they happen. This leads to:

- Complex conflict detection logic
- Data loss on last-write-wins
- User-facing "merge conflict" dialogs
- Brittle upgrade paths

CRDTs eliminate the concept of conflict entirely. Every concurrent write produces a **deterministic merge**.

```
Replica A edits: doc.set("text", "buy organic milk")   → Yrs update binary diff
Replica B edits: doc.set("completed", true)             → Yrs update binary diff

Both updates applied to either replica:
→ { text: "buy organic milk", completed: true }

Both replicas converge. Both changes preserved. No conflicts.
```

When two replicas edit the **same field** concurrently:

```
Replica A (timestamp T1): doc.set("text", "version A")
Replica B (timestamp T2): doc.set("text", "version B")

LWW-Register: latest timestamp wins → { text: "version B" }
Deterministic. No data loss for the later write.
```

### CRDT Document Structure

Every table in VaultSync is a Yrs CRDT Map:

```
Document: "todos" → record: "todo:123" (Yrs Map)
├── field: "text"       → LWW-Register("build vaultsync")
├── field: "completed"  → LWW-Register(false)
├── field: "priority"   → PN-Counter(3)
├── field: "tags"       → OR-Set(["backend", "rust"])
└── field: "assignee"   → LWW-Register("alice")
```

Every write produces a **Yrs binary diff** — a small, self-contained update that can be applied to any replica's document to produce the same result, regardless of order.

### Why This Means No Breaking Upgrades

Because every operation has always been a CRDT mutation:

- **No data format migration** when adding features
- **No conflict handler rewrite** ever
- **No schema redefinition** as CRDT types are declared at definition time
- **Additive-only schema changes** — new fields can be added at any time without affecting existing data

---

## 🔐 End-to-End Encryption (E2EE)

### Why E2EE Must Be the Default

Every other sync engine (ElectricSQL, Zero, PowerSync, Replicache) stores **plaintext data** on the coordinator. This means:

- The coordinator operator can read all synced user data
- A coordinator breach exposes every user's data
- HIPAA, GDPR, SOC2 compliance requires additional layers

VaultSync's E2EE ensures the **coordinator is zero-trust**. It stores only encrypted blobs and routing metadata. It cannot read any application data — ever.

### Encryption Flow

```
Local CRDT write
       │
       ▼
Yrs binary diff produced (the mutation delta)
       │
       ▼
Encrypt with namespace symmetric key:
   ciphertext = ChaCha20Poly1305_encrypt(yrs_update, namespace_key)
       │
       ▼
Local oplog stores:
   yrs_update:     plaintext Yrs diff   ← for local fast merge
   encrypted_blob: AEAD ciphertext      ← for upload to coordinator
       │
       ▼
Upload to coordinator:
   { id, replicaId, namespace, encrypted_blob, timestamp }
   ← coordinator never sees yrs_update plaintext
       │
       ▼
Other replica downloads encrypted_blob
       │
       ▼
Decrypt with namespace symmetric key
       │
       ▼
Merge Yrs diff into local CRDT document
       │
       ▼
UI updates automatically via subscription
```

### Key Architecture

- **X25519 keypair** per replica — public key registered with coordinator, private key never leaves device
- **Symmetric namespace key** — shared by all authorized replicas, never sent to coordinator
- **New device onboarding** — trusted replica wraps the namespace key with the new device's public key and delivers it securely
- **Key rotation** — supported at any time; old mutations remain decryptable with old key version stored in local keychain
- **Replay attack prevention** — each mutation has a unique globally-scoped ID; coordinator deduplicates idempotently

### What the Coordinator Can and Cannot See

| Information | Coordinator Sees? |
|---|---|
| Encrypted mutation blob | ✅ (opaque bytes) |
| Replica ID (who sent it) | ✅ (routing) |
| Namespace (which dataset) | ✅ (routing) |
| Sequence number | ✅ (ordering) |
| Field names | ❌ (encrypted) |
| Field values | ❌ (encrypted) |
| Document structure | ❌ (encrypted) |
| CRDT type information | ❌ (encrypted) |

---

## 🖥️ Multi-Tab & Multi-Process Safety

### The Problem Most Sync Engines Ignore

When multiple browser tabs, Electron windows, or OS processes share the same local database (one SQLite file or OPFS directory), concurrent writes cause:

- SQLite locking errors (`SQLITE_BUSY`)
- CRDT document corruption (concurrent Yrs mutations outside a lock)
- Oplog ordering violations
- Subscription double-firing

### VaultSync's Solution: Leader Election

VaultSync uses **leader election** to coordinate write access. One process writes; all others read via shared memory.

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Tab A (Leader) │    │   Tab B (Reader)  │    │   Tab C (Reader)  │
│                  │    │                   │    │                   │
│ Write access: ✅  │    │ Write access: ❌   │    │ Write access: ❌   │
│ Lock: acquired   │    │ Lock: waiting      │    │ Lock: waiting      │
│ Heartbeat: 1s    │    │                   │    │                   │
└────────┬─────────┘    └────────┬──────────┘    └────────┬──────────┘
         │                       │                          │
         └───────────────────────┼──────────────────────────┘
                                 │
                    ┌────────────▼───────────────┐
                    │    Shared Memory Region      │
                    │  CRDT document snapshots    │
                    │  Oplog tail (ring buffer)   │
                    │  Subscription notifications  │
                    └─────────────────────────────┘
                                 │
                    ┌────────────▼───────────────┐
                    │  Local Storage (SQLite/OPFS) │
                    └─────────────────────────────┘
```

### Leader Election by Platform

| Platform | Mechanism |
|---|---|
| **Browser (tabs)** | `BroadcastChannel` + `navigator.locks.request` on `vaultsync-leader` lock |
| **Native (Linux/macOS/Windows)** | `flock()` on SQLite WAL file or `CreateMutex` |
| **Electron** | Browser mechanism + `net.Server` on local socket for cross-window IPC |

### Heartbeat & Crash Recovery

```
Leader writes heartbeat every 1 second:
  { leaderId: "tab-A", timestamp: ..., sequence: 42 }

Reader checks every 500ms:
  if current_time - leader.heartbeat > 3000ms:
    → leader considered dead
    → reader acquires lock → becomes new leader
    → replays uncommitted ops from shared memory ring buffer
    → notifies other readers via BroadcastChannel

No data is lost — shared memory ring buffer preserves last N in-flight mutations.
New leader continues sync from last confirmed sequence.
```

### What Happens Across Tabs in Real Time

When a CRDT merge happens:

1. **Leader** merges mutation, writes to storage, updates shared memory
2. **Leader** fires subscription callbacks in its own process
3. **Leader** sends `{ changedDocs: ["todos/789"] }` via BroadcastChannel
4. **Reader tabs** detect change, read from shared memory
5. **Reader tabs** fire their own subscription callbacks

All tabs see the update in **< 5ms** — even the ones that didn't initiate the write.

---

## 📴 Offline Support & Durability

### What Happens When You Go Offline

```
Network disconnects
       │
       ▼
VaultSync detects disconnect (WebSocket close / heartbeat timeout)
       │
       ▼
Connection status → "disconnected"
       │
       ▼
Application continues working fully:
  ✅ Reads served from local CRDT materialized state (instant)
  ✅ Writes produce CRDT mutations → persist to local DB + oplog (status: pending)
  ✅ All mutations E2EE-encrypted before storage
  ✅ Subscriptions continue firing on local CRDT merges
  ✅ UI remains fully interactive
  ✅ Multi-tab: leader continues serving all tabs
```

Pending mutations accumulate in the local oplog. The coordinator is not involved.

### What Happens on Reconnect

```
Network reconnects
       │
       ▼
VaultSync reconnects to coordinator (exponential backoff: 1s → 2s → 4s → ... → 30s)
       │
       ▼
Step 1: Upload backlog
  → Read all pending mutations from oplog
  → Sort by local timestamp
  → Upload in batches (encrypted)
       │
       ▼
Step 2: Download missed mutations
  → Request all mutations with sequence > last_synced_sequence
  → Decrypt each
  → Merge into local CRDT documents
  → Fire subscriptions for affected documents
       │
       ▼
Connection status → "connected"
       │
       ▼
Resume real-time sync
```

### Durability Guarantees

| Scenario | Result |
|---|---|
| Process crash | Mutations in oplog survive — re-uploaded on restart |
| Tab close | Shared memory ring buffer preserves in-flight mutations; new leader replays them |
| Power failure | Oplog on persistent storage survives; re-uploaded on restart |
| Local storage theft | E2EE oplog encrypted at rest — attacker sees only ciphertext |

CRDT uploads are **idempotent by mutation ID**. Re-uploading a mutation the coordinator has already seen causes a silent deduplicated no-op. Zero duplicates.

---

## 🔄 Synchronization Flow

A complete, annotated trace of a local write propagating to a remote replica:

```
Step 1: Application Write
  vaultsync.db.todos.insert({ id: "todo:123", text: "build vaultsync" })
       │
       ▼ CRDT Engine
  Yrs document "todos/123" receives mutation
  Yrs produces binary diff (yrs_update)
       │
       ▼ E2EE Layer
  encrypted_blob = ChaCha20Poly1305_encrypt(yrs_update, namespace_key)
       │
       ▼ Storage (atomic transaction, Leader only)
  BEGIN TRANSACTION
    INSERT vaultsync_oplog (yrs_update, encrypted_blob, status: "pending")
    UPDATE vaultsync_documents (new Yrs snapshot for fast queries)
  COMMIT
       │
       ▼ UI
  Subscription fires immediately — React re-renders <0.1ms after write

Step 2: Background Upload
  Upload worker reads pending oplog entries
       │
  Batches multiple pending mutations if present
       │
  Sends encrypted_blob to coordinator via WebSocket PUSH frame
  (coordinator never sees yrs_update plaintext)

Step 3: Coordinator Processing
  Validates: replica authorized? schema version compatible?
       │
  Assigns monotonic sequence number (e.g., 44)
       │
  Persists { sequence: 44, replica_id, encrypted_blob, timestamp }
       │
  Fans out MUTATION_PUSH to all other connected replicas in namespace

Step 4: ACK Back to Sender
  Coordinator sends PUSH_ACK { request_id, sequences: [44] }
       │
  Local oplog: sync_status → "synced"
  vaultsync_sync_state: last_synced_sequence = 44

Step 5: Remote Replica Receives Mutation
  Replica B receives MUTATION_PUSH (sequence 44, encrypted_blob)
       │
  Decrypts: yrs_update = decrypt(encrypted_blob, namespace_key)
       │
  CRDT engine: Yrs.merge(yrs_update) into local "todos/123" document
       │
  Subscription fires in Replica B → UI updates
  Total end-to-end: 50–500ms on typical connections
```

### Sync State per Mutation

```
pending   → written locally, not yet uploaded
uploading → currently being sent to coordinator
synced    → coordinator confirmed receipt and assigned sequence
failed    → upload failed; in retry queue (exponential backoff)
```

Note: There is **no "conflict" state**. CRDT mutations never conflict.

### WebSocket Wire Protocol

All coordinator communication uses binary WebSocket frames with a 5-byte header:

```
┌──────────┬────────────┬────────────────────────────────────────┐
│ type: u8 │ length: u32│ payload: JSON bytes (length bytes)      │
│ (1 byte) │ (4 bytes)  │                                         │
└──────────┴────────────┴────────────────────────────────────────┘
```

Key message types: `AUTH`, `REGISTER`, `PUSH`, `PUSH_ACK`, `PULL`, `PULL_RESPONSE`, `MUTATION_PUSH`, `HEARTBEAT`, `KEY_FETCH`, `SCHEMA_SYNC`.

---

## 📡 Reactive Subscriptions

VaultSync subscriptions fire on both local writes **and** incoming remote CRDT merges:

```typescript
// Table subscription — fires on any CRDT merge in the collection
const unsub = vaultsync.db.todos.subscribe((todos) => {
  renderTodoList(todos)
})

// Filtered subscription
const unsub = vaultsync.db.todos.subscribe(
  (todos) => renderTodos(todos),
  { where: { assignee: "alice", completed: false } }
)

// Single record subscription
const unsub = vaultsync.db.todos.subscribeOne("todo:123", (todo) => {
  if (!todo) renderDeleted()
  else renderTodo(todo)
})

// Sync status subscription
vaultsync.subscribe("sync:status", (status) => {
  updateConnectionIndicator(status)
  // status: { connected, pendingUploads, lastSyncAt, leaderStatus }
})
```

### React Hooks (via `@vaultsync/react`)

```tsx
import { useQuery, useVaultSyncOne, useSyncStatus, useVaultSyncMutations } from "@vaultsync/react"

function TodoList() {
  // Reactive live query — re-renders whenever any todo changes locally or remotely
  const { data: todos, loading } = useQuery("todos", {
    where: { completed: false },
    orderBy: { field: "priority", direction: "desc" }
  })

  const { insert } = useVaultSyncMutations("todos")

  return (
    <div>
      <button onClick={() => insert({ text: "new task", completed: false })}>
        Add
      </button>
      {loading ? <Spinner /> : <ul>{todos.map(t => <TodoItem key={t.id} todo={t} />)}</ul>}
    </div>
  )
}

function SyncIndicator() {
  const { connected, pendingUploads } = useSyncStatus()
  return (
    <span className={connected ? "synced" : "pending"}>
      {connected ? "✓ Synced" : `⟳ ${pendingUploads} pending`}
    </span>
  )
}
```

Subscription callbacks fire **once per CRDT merge** — not once per field. A remote mutation that changes 10 fields fires one subscription callback with the full post-merge document state.

---

## 🔌 Coordinator Abstraction Layer

### Why a Trait, Not a Service

Every other sync engine locks you into a specific backend. VaultSync defines `Coordinator` as a **Rust trait**. You choose the implementation. You can swap backends without changing application code.

### Built-in Implementations

| Backend | Package | Real-Time | Best For |
|---|---|---|---|
| **PostgreSQL** | `vaultsync-coordinator-postgres` | `LISTEN/NOTIFY` (5–50ms) | Production self-hosted |
| **Redis** | `vaultsync-coordinator-redis` | Pub/Sub (1–10ms) | High throughput, in-memory |
| **SQLite** | `vaultsync-coordinator-sqlite` | Polling (250ms) | Local dev, single-server |
| **Cloudflare DO** | `vaultsync-coordinator-cloudflare` | DO WebSocket (10–50ms) | Edge-deployed, global |
| **Supabase** | `vaultsync-coordinator-supabase` | Supabase Realtime (10–100ms) | Hosted Postgres |
| **In-Memory** | `vaultsync-coordinator-memory` | Channel (< 1ms) | Testing |

### Switching Coordinators

```typescript
// PostgreSQL
const vaultsync = new VaultSync({
  namespace: "workspace:core",
  storage: "opfs",
  coordinator: new PostgresCoordinator({
    connectionString: "postgres://user:pass@host/db"
  })
})

// Cloudflare Durable Objects
const vaultsync = new VaultSync({
  namespace: "workspace:core",
  storage: "opfs",
  coordinator: new CloudflareCoordinator({
    accountId: "...",
    durableObjectId: "..."
  })
})

// Your own implementation — implement the trait
class MyCoordinator implements Coordinator {
  async push(mutations) { /* ... */ }
  async pull(after, limit) { /* ... */ }
  async subscribe(fromSequence) { /* ... */ }
}
```

### Coordinator Server — Deployable Binary

VaultSync ships a production-ready coordinator server you can self-host:

```bash
# Docker with PostgreSQL backend
docker run -p 8080:8080 \
  -e VAULTSYNC_DB_URL=postgres://user:pass@db:5432/vaultsync \
  -e VAULTSYNC_AUTH_JWKS_URL=https://your-auth.example.com/.well-known/jwks.json \
  vaultsyncsync/coordinator:latest

# Or run the binary
vaultsync-server \
  --backend postgres \
  --db-url postgres://user:pass@localhost/vaultsync \
  --port 8080
```

### New Replica Bootstrap

When a new device joins an existing namespace:

```
1. New device sends REGISTER with last_sequence = 0
2. Coordinator checks: snapshot available?
3a. YES: returns snapshot_url (compressed batch of encrypted mutations)
    → New device downloads, decompresses, decrypts each mutation
    → Applies all Yrs diffs in sequence order to build local CRDT state
    → Fetches remaining mutations since snapshot
3b. NO: Paginates through full mutation history via PULL
4. Normal real-time sync begins
```

Coordinator snapshots are **encrypted compressed mutation batches** — the coordinator never decrypts to create them.

---

## ⚡ Performance Model

### Local Operation Targets (Engine Overhead Only)

| Operation | Rust Native (SQLite WAL) | Browser WASM (OPFS) |
|---|---|---|
| **Single record read** (from materialized CRDT state) | 1–5 µs | 0.5–2 ms |
| **Single record write** (CRDT mutation + WAL commit) | 5–20 µs | 1–5 ms |
| **CRDT merge** (single Yrs update into document) | 1–5 µs | 10–50 µs |
| **E2EE encrypt** (ChaCha20-Poly1305) | 5–15 µs | 50–200 µs |
| **E2EE decrypt** | 5–15 µs | 50–200 µs |
| **Full operation path** (write → encrypt → oplog → subscription fire) | 10–40 µs | 2–10 ms |
| **Cross-tab notification** (shared memory / BroadcastChannel) | 2–10 µs | 1–5 ms |
| **Snapshot load** (1MB Yrs document from mmap) | < 1 ms | 10–50 ms |

All benchmarks run via Criterion.rs. A > 10% regression fails the CI build.

### End-to-End Sync Targets (Engine + Typical Network)

| Operation | Perceived Latency |
|---|---|
| Write → visible on same device | **< 1ms** (local write) |
| Write → coordinator ACK | **50–200ms** (fast network) |
| Write → visible on another device | **100–500ms** (fast network) |
| Reconnect + sync 1000 pending operations | **1–5 seconds** |
| New replica bootstrap (1MB snapshot) | **500–2000ms** |

### Why Local-First Feels 0ms

The UI updates **before** the network is involved. The write is committed to local SQLite/OPFS synchronously. The subscription fires synchronously after the local CRDT merge. The upload to the coordinator happens asynchronously in a background worker thread. Users see their action reflected immediately — network latency is invisible.

---

## 🚀 SDK Reference — Quickstart

### Installation

```bash
# Browser / React
npm install @vaultsync/web @vaultsync/react

# Node.js server
npm install @vaultsync/node

# Next.js
npm install @vaultsync/web @vaultsync/react @vaultsync/next
```

### Rust

```toml
[dependencies]
vaultsync-core = "0.1"
```

### Initialize VaultSync (Browser + React)

```tsx
import { VaultSync } from "@vaultsync/web"
import { VaultSyncProvider } from "@vaultsync/react"
import { PostgresCoordinator } from "@vaultsync/web/coordinator"

// Create the client with CRDT schema
const vaultsync = new VaultSync({
  storage:   "opfs",              // OPFS (SQLite via WASM) for browsers
  namespace: "workspace:core",
  replicaId: getDeviceId(),       // stable, unique device identifier
  coordinator: new PostgresCoordinator({
    connectionString: process.env.COORDINATOR_URL
  }),
  auth: {
    getToken: () => clerk.session?.getToken() // your JWT provider
  },
  sync: {
    reconnect:   { attempts: Infinity, maxDelay: "30s" },
    uploadRetry: { attempts: 10, backoff: "exponential" }
  },
  multiTab: {
    enabled:           true,
    electionTimeout:   3000,
    heartbeatInterval: 1000
  }
})

// Define CRDT-typed schema
vaultsync.schema.define("todos", {
  fields: {
    id:        { type: "string",  crdtType: "lww",     primaryKey: true },
    text:      { type: "string",  crdtType: "lww",     indexed: true    },
    completed: { type: "boolean", crdtType: "lww"                       },
    priority:  { type: "number",  crdtType: "counter"                   },
    tags:      { type: "array",   crdtType: "orset",   indexed: true    },
    assignee:  { type: "string",  crdtType: "lww",     indexed: true    },
    localNote: { type: "string",  crdtType: "lww",     sync: false      } // never synced
  },
  softDelete: true  // DELETE adds tombstone, not hard delete — required for correct sync
})

// Register migrations
vaultsync.migration("v1", async (db) => {
  await db.defineDocument("todos", { /* initial schema */ })
})
vaultsync.migration("v2", async (db) => {
  await db.addField("todos", "priority", { type: "number", crdtType: "counter" })
})

// Initialize — runs migrations, sets up E2EE keys, connects to coordinator
await vaultsync.initialize()

// Wrap your app
export default function App() {
  return (
    <VaultSyncProvider client={vaultsync}>
      <MyApp />
    </VaultSyncProvider>
  )
}
```

### CRUD Operations

```typescript
const db = vaultsync.db

// INSERT — creates a new CRDT document (instant local write)
await db.todos.insert({
  id:        "todo:123",
  text:      "build vaultsync",
  completed: false,
  tags:      ["backend", "rust"],
  priority:  3
})

// READ — from local CRDT materialized state (no network, instant)
const todos    = await db.todos.findAll()
const todo     = await db.todos.findById("todo:123")
const filtered = await db.todos.findAll({
  where:   { completed: false, tags: { contains: "backend" } },
  orderBy: { field: "priority", direction: "desc" },
  limit:   50
})

// UPDATE — produces a Yrs binary CRDT diff, instantly visible in UI
await db.todos.update("todo:123", { text: "build vaultsync runtime" })

// DELETE — soft delete (tombstone) if softDelete: true in schema
await db.todos.delete("todo:123")

// BATCH — multiple CRDT mutations in one atomic transaction
await db.batch([
  db.todos.insert({ id: "todo:456", text: "write docs" }),
  db.todos.update("todo:123", { completed: true })
])
```

### Key Management

```typescript
// Keys are generated automatically on initialize()
// Manual management for advanced use cases:

await vaultsync.keys.rotate()                // Rotate namespace key
const pubKey   = await vaultsync.keys.publicKey()   // Export public key
const keyInfo  = await vaultsync.keys.status()
// { version: 2, createdAt: ..., algorithm: "X25519+ChaCha20-Poly1305" }
```

---

## 📐 Schema System & Migrations

### Schema Definition

```typescript
vaultsync.schema.define("projects", {
  fields: {
    id:          { type: "string",  crdtType: "lww",     primaryKey: true },
    name:        { type: "string",  crdtType: "lww",     indexed: true    },
    description: { type: "string",  crdtType: "text"                      }, // Rich text CRDT
    memberCount: { type: "number",  crdtType: "counter"                   }, // PN-Counter
    tags:        { type: "array",   crdtType: "orset"                     }, // OR-Set
    ownerId:     { type: "string",  crdtType: "lww",     indexed: true    },
    draft:       { type: "boolean", crdtType: "lww",     sync: false      } // Local only
  },
  indexes: [
    { fields: ["ownerId"] },
    { fields: ["name", "memberCount"] }
  ],
  softDelete: true
})
```

### CRDT Type as the Conflict Strategy

| `crdtType` | Merge Behavior | Real-World Use |
|---|---|---|
| `lww` | Last-write-wins by hybrid logical clock | Title, status, any scalar |
| `counter` | All increments/decrements sum globally | Likes, votes, inventory |
| `orset` | Add/remove with tombstone tracking | Tags, members, feature flags |
| `text` | Sequence CRDT (position-based insert/delete) | Rich text bodies |
| `custom` | Developer-defined Yrs extension | Complex domain logic |

### Versioned Migrations

Migrations are **additive only**. Existing CRDT fields can never be removed (tombstones handle deletion). CRDT type of a field cannot be changed (use a new field name). This ensures all replicas on any schema version can accept mutations from other versions without data loss.

```typescript
vaultsync.migration("v1", async (db) => {
  await db.defineDocument("todos", {
    fields: { id: { type: "string", crdtType: "lww", primaryKey: true },
              text: { type: "string", crdtType: "lww" } }
  })
})

vaultsync.migration("v2", async (db) => {
  await db.addField("todos", "priority", { type: "number", crdtType: "counter" })
})

vaultsync.migration("v3", async (db) => {
  await db.addField("todos", "tags", { type: "array", crdtType: "orset" })
})
```

Schema migration checksums are stored in `vaultsync_migrations` and verified on every apply. Tampered migrations are rejected.

---

## 🔭 Observability & Debuggability

### OpenTelemetry Traces — Every Operation

```
Trace: "vaultsync.write" (trace_id: abc123)
├── Span: "crdt.merge"          →  2.1 µs  — Yrs merge into document
├── Span: "e2ee.encrypt"        →  7.3 µs  — ChaCha20-Poly1305 encrypt
├── Span: "oplog.append"        →  1.5 µs  — Write to local oplog
├── Span: "subscription.fire"   →  0.8 µs  — Fire local subscription
└── Span: "transport.send"      → 45 ms    — Upload to coordinator
    ├── Span: "coord.validate"  →  2 ms
    ├── Span: "coord.store"     →  5 ms
    └── Span: "coord.fanout"    → 20 ms    — Push to other replicas
```

Exportable to Jaeger, Datadog, Grafana Tempo, or any OTLP-compatible backend.

### Prometheus Metrics

| Metric | Description |
|---|---|
| `vaultsync_mutations_total` | Total mutations processed (by status, namespace) |
| `vaultsync_mutations_pending` | Current upload queue depth |
| `vaultsync_sync_lag_ms` | Time from local write to coordinator ACK |
| `vaultsync_download_lag_ms` | Time from coordinator sequence to replica apply |
| `vaultsync_encryption_time_us` | E2EE encrypt/decrypt duration |
| `vaultsync_crdt_merge_time_us` | CRDT merge duration per document |
| `vaultsync_connection_status` | 1=connected, 0=disconnected |
| `vaultsync_leader_status` | 1=leader, 0=reader |

### Debug HTTP API

```bash
GET  /debug/vaultsync/state           → Full internal state as JSON
GET  /debug/vaultsync/state/oplog     → Last 1000 oplog entries
GET  /debug/vaultsync/state/documents → CRDT document snapshot metadata
GET  /debug/vaultsync/leader          → Current leader status
GET  /debug/vaultsync/metrics         → Prometheus endpoint
POST /debug/vaultsync/force-sync      → Trigger immediate sync
POST /debug/vaultsync/force-election  → Trigger leader re-election
```

### CLI Tool

```bash
# Attach to a running VaultSync process
vaultsync inspect --pid 1234

# Watch live operation stream
vaultsync inspect --pid 1234 --stream

# Dump current CRDT document state
vaultsync inspect --pid 1234 --state

# Export and replay a trace file for deterministic debugging
vaultsync inspect --pid 1234 --export-trace > trace.json
vaultsync replay trace.json
```

---

## 🛡️ Security Model

### Layers of Security

| Layer | Mechanism |
|---|---|
| **Transport** | All sync traffic over TLS (WSS). Even if TLS is compromised, coordinator cannot read data (E2EE). |
| **Application data** | E2EE via X25519 + ChaCha20-Poly1305. Coordinator stores only encrypted blobs. |
| **Authentication** | Every coordinator connection requires a JWT from your auth provider (Clerk, Auth0, etc.). Expired tokens close the connection. |
| **Namespace isolation** | A replica authorized for `workspace:alpha` cannot receive or push mutations for `workspace:beta`. Enforced at the coordinator. |
| **Local storage** | OS-level sandboxing (OPFS is origin-private). Optional SQLCipher full-database encryption. Private keys stored in OS keychain (macOS Keychain, Windows Credential Manager). |
| **Key pinning** | TOFU (Trust-On-First-Use) by default. Strict mode available: any new replica key requires manual operator approval before sync proceeds. |

### For Regulated Industries

```typescript
const vaultsync = new VaultSync({
  namespace: "healthcare:patient-records",
  e2ee: {
    keyValidation: "strict",           // Manual approval for new replica keys
    onNewKeyDetected: async (replicaId, newKey) => {
      await notifySecurityTeam(replicaId, newKey)
      return "reject" // "accept" | "reject"
    }
  },
  storageEncryption: {
    enabled: true,
    mode: "sqlcipher",                 // Full database encryption
    keyDerivation: "os-keychain"       // AES-256-CBC, key in OS keychain
  }
})
```

---

## 🌍 Real-World Use Cases

### Collaborative Todo / Project Management

```
Alice (laptop) adds todo:123 — instant UI update, CRDT mutation queued
      ↓ (async, encrypted)
Coordinator sequences and distributes encrypted mutation
      ↓ (50–200ms on good connection)
Bob (phone) receives encrypted mutation, decrypts, CRDT merges
Bob sees todo:123 appear without refreshing

Both users edit todo:123 simultaneously:
Alice: UPDATE { text: "updated title" }
Bob:   UPDATE { completed: true }

CRDT merge: { text: "updated title", completed: true }
Both changes preserved — no data loss, no conflict dialog.
```

### Offline-First Mobile App (Airplane Mode)

```
User opens app on airplane (no connectivity):
  → All reads served from local CRDT state — instant, zero latency
  → User makes 47 changes during the flight
  → 47 encrypted CRDT mutations queued in oplog, status: pending
  → Device lost? Oplog encrypted at rest — attacker sees only ciphertext

Plane lands, connectivity returns:
  → VaultSync reconnects, uploads 47 mutations in batches
  → Downloads 12 mutations from other devices
  → CRDT merges all 12 — no data lost, no conflicts
  → App fully synchronized — coordinator never saw plaintext
```

### Multi-Tab Web App

```
User opens same app in 3 browser tabs:
  Tab A (Leader): handles all writes
  Tab B (Reader): reads via shared memory, < 5ms
  Tab C (Reader): reads via shared memory, < 5ms

User closes Tab A (leader crash):
  Tab B detects heartbeat timeout (> 3s)
  Tab B acquires lock → becomes Leader
  Tab B replays in-flight mutations from shared memory
  Tab C detects new leader via BroadcastChannel
  Zero data loss. Zero interruption. Users notice nothing.
```

### Regulated Healthcare / Legal / Finance

```
Patient data synced across clinic devices:
  → E2EE: coordinator (even if breached) cannot read PHI
  → HIPAA compliance: no plaintext in coordinator logs
  → Audit trail: OpenTelemetry traces every CRDT mutation
  → On-prem coordinator: all data stays in clinic's network
  → Key rotation: staff departures trigger namespace key rotation
```

### Developer Tools & AI Applications

```
AI assistant stores context locally as CRDT documents:
  → Reads and writes instant — no latency on AI completions
  → Context syncs across devices when online
  → Works fully offline — context available on airplane
  → No server reads required for inference — cost drops by 90%
```

### P2P LAN Collaboration

```
Two developers on the same office WiFi:
  → Coordinator: Postgres in cloud (persistence, third-party access)
  → P2P transport: WebRTC enabled for same-LAN peers
  → Low-latency edits: P2P path (< 10ms round-trip)
  → Developer leaves office: P2P disconnects, coordinator takes over
  → Transparent failover. No data loss.
```

---

## ⚔️ VaultSync vs The World

| Feature | VaultSync | ElectricSQL | Zero (Rocicorp) | PowerSync | REST + WebSocket |
|---|---|---|---|---|---|
| **Offline first** | ✅ Full | ✅ | ✅ | ✅ | ❌ |
| **Conflict resolution** | ✅ CRDT (no loss) | ⚠️ LWW | ⚠️ LWW | ⚠️ LWW | ❌ Server wins |
| **E2EE** | ✅ Default, zero-trust | ❌ | ❌ | ❌ | ❌ |
| **Multi-tab safe** | ✅ Leader election | ❌ | ❌ | ❌ | ❌ |
| **Coordinator choice** | ✅ Any via trait | ❌ Postgres only | ❌ Proprietary | ❌ MongoDB | N/A |
| **Perceived 0ms writes** | ✅ Local-first | ✅ | ✅ | ✅ | ❌ Network required |
| **Reduce API costs** | ✅ Reads never hit server | ✅ | ✅ | ✅ | ❌ All reads hit server |
| **Open source** | ✅ Apache 2.0 | ✅ | ✅ | ❌ | N/A |
| **Rust core + WASM** | ✅ | ❌ | ❌ | ❌ | N/A |
| **Observability** | ✅ OpenTelemetry | Partial | Partial | ❌ | N/A |

---

## 🚀 Deployment Modes

### Mode 1: Local-Only (No Sync)

```typescript
const vaultsync = new VaultSync({
  storage:   "sqlite",
  namespace: "local",
  sync:      false    // no coordinator — offline-only
})
```

### Mode 2: Embedded + Managed Coordinator

```typescript
const vaultsync = new VaultSync({
  storage:     "opfs",
  namespace:   `user:${userId}`,
  coordinator: new PostgresCoordinator({ connectionString: process.env.DATABASE_URL })
})
```

### Mode 3: Self-Hosted Coordinator Binary

```typescript
const vaultsync = new VaultSync({
  storage:     "sqlite",
  namespace:   "workspace:core",
  coordinator: new CustomCoordinator({ url: "wss://your-coordinator.example.com/vaultsync" })
})
```

### Mode 4: Multi-Instance Server Sync

```
App Server A (VaultSync embedded, namespace: workspace:core)
App Server B (VaultSync embedded, namespace: workspace:core)
App Server C (VaultSync embedded, namespace: workspace:core)
              │
     Shared Coordinator (Postgres)
```

### Mode 5: Hybrid Client + Server + P2P

```typescript
const vaultsync = new VaultSync({
  namespace:  "workspace:core",
  coordinator: new PostgresCoordinator({ ... }),
  transports: {
    coordinator: true,
    p2p: { enabled: true },   // WebRTC for same-LAN peers
    mesh: { enabled: false }  // libp2p mesh (future)
  }
})
```

---

## 🤝 Contributing

We welcome contributions from the community! See [CONTRIBUTING.md](CONTRIBUTING.md) for:

- Local development setup
- Coding standards (Clippy + `rustfmt` required, zero warnings policy)
- [Conventional Commits](https://www.conventionalcommits.org/) format
- Branch naming conventions
- How to add a new coordinator backend
- How to add a new storage backend
- Fuzzing guide

```bash
# Get started
git clone https://github.com/parv68/VaultSync.git
cd vaultsync
./scripts/setup-dev.sh

# Run all tests
./scripts/run-all-tests.sh

# Run Rust tests only
cargo test --workspace --exclude vaultsync-fuzz

# Run SDK tests
npm test -w @vaultsync/web
npm test -w @vaultsync/node
npm test -w @vaultsync/react
npm test -w @vaultsync/next
```

---

## ✅ Production Checklist

Before deploying VaultSync to production:

- [ ] **E2EE keys generated** — verify keys are backed up; export public keys for recovery
- [ ] **Coordinator deployed** — Postgres (recommended), Redis, or self-hosted server
- [ ] **Replica JWT auth configured** — `auth.getToken` returns valid tokens for all sessions
- [ ] **Namespace authorization** — limit which replicas can push/pull per namespace
- [ ] **Soft delete enabled** on all synced tables — prevents sync gaps on delete operations
- [ ] **Schema migrations tracked** — every field addition has a versioned migration
- [ ] **CRDT types chosen per field** — LWW / Counter / OR-Set / Text selected intentionally
- [ ] **Oplog compaction configured** — snapshot interval + tombstone GC threshold set
- [ ] **Multi-tab election tested** — verified 3+ tab crash recovery scenario
- [ ] **Coordinator backups running** — snapshot + mutation log backup to durable storage
- [ ] **OpenTelemetry configured** — traces, metrics, logs exported to your observability backend
- [ ] **Sync lag alerting set up** — alert if `vaultsync_sync_lag_ms` > threshold
- [ ] **Offline replica alerts** — alert if a replica hasn't synced within tombstone retention
- [ ] **Graceful shutdown** — server replicas flush pending uploads before process exit
- [ ] **Chaos tests passed** — network partition, leader crash, clock skew, key rotation mid-sync
- [ ] **Benchmark regression gates green** — no > 10% performance regressions vs main branch

---

## 📄 License

VaultSync is licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE) for the full text.

---

## 📖 Further Reading

- [Complete Technical Specification](docs/vaultsync-spec.md) — 158KB deep dive into every design decision
- [Testing Specification](docs/vaultsync-testing-spec.md) — Conformance tests, chaos scenarios, property tests
- [Implementation Plan](docs/vaultsync-implementation-plan.md) — Internal build reference

---

*VaultSync — Conflict-free, encrypted, infrastructure-agnostic synchronization for the local-first era.*
