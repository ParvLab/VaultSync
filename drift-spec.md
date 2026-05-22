# Drift — Embedded Local-First Sync & Replication Runtime
### Complete Technical Specification & Build Reference (v2 — Production Architecture)

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Core Philosophy](#2-core-philosophy)
3. [What Drift Is (and Is Not)](#3-what-drift-is-and-is-not)
4. [Tech Stack](#4-tech-stack)
5. [System Architecture](#5-system-architecture)
6. [Core Concepts](#6-core-concepts)
7. [CRDT Document Model](#7-crdt-document-model)
8. [Storage Model — CRDT-Native Hybrid](#8-storage-model--crdt-native-hybrid)
9. [Operation Log (Oplog)](#9-operation-log-oplog)
10. [Schema System & Migrations](#10-schema-system--migrations)
11. [End-to-End Encryption](#11-end-to-end-encryption)
12. [Multi-Tab & Multi-Process Architecture](#12-multi-tab--multi-process-architecture)
13. [Synchronization Flow](#13-synchronization-flow)
14. [Offline Support & Durability](#14-offline-support--durability)
15. [Retry & Reconnection System](#15-retry--reconnection-system)
16. [Reactive Subscriptions](#16-reactive-subscriptions)
17. [Coordinator Abstraction Layer](#17-coordinator-abstraction-layer)
18. [Full Database Schema](#18-full-database-schema)
19. [Performance Model](#19-performance-model)
20. [Observability & Debuggability](#20-observability--debuggability)
21. [SDK Reference](#21-sdk-reference)
22. [Deployment Modes](#22-deployment-modes)
23. [Security Model](#23-security-model)
24. [Integrations (Optional Plugins)](#24-integrations-optional-plugins)
25. [Development Roadmap](#25-development-roadmap)
26. [Real-World Use Cases](#26-real-world-use-cases)

---

## 1. Project Summary

**Drift** is an embedded, local-first synchronization and replication runtime with CRDT-native conflict resolution, end-to-end encryption, multi-tab/process safety, and infrastructure-agnostic coordination. It enables applications to operate fully offline, synchronize data across devices and replicas, and handle network failures gracefully — without requiring dedicated sync infrastructure.

Drift is built on three non-negotiable principles:

1. **CRDT-native from day one** — concurrent writes never conflict. There is no "V1 coordinator-order, V3 field-merge, V4 CRDT upgrade path." CRDTs (Conflict-Free Replicated Data Types) are the foundation of every operation. All replicas converge deterministically regardless of write order.

2. **End-to-end encryption by default** — the coordinator never sees plaintext data. Every operation is encrypted with a namespace key before leaving the device. The coordinator stores only encrypted blobs and routing metadata. Zero-trust infrastructure.

3. **Infrastructure-agnostic** — Drift defines a `Coordinator` trait. Users choose or build their own coordinator backend: Postgres, Redis, Cloudflare Durable Objects, Supabase, SQLite, or a custom implementation. No vendor lock-in. No Cloudflare dependency.

Drift also solves a problem no other sync engine addresses: **multi-tab and multi-process safety**. Browser tabs, Electron windows, and OS processes all share the same local database via leader election and shared memory — no split-brain, no corruption, no data loss.

Inspired by the local-first software movement and modern sync systems like ElectricSQL, PowerSync, Replicache, and Zero — but built with an embedded-first philosophy:

> *Synchronization should be conflict-free, encrypted, infrastructure-agnostic, and observable — then scale into distributed coordination only when necessary.*

Drift is designed for:

- Collaborative editors where multiple users edit the same document simultaneously
- Local-first productivity tools that must work offline and sync when reconnected
- AI-native applications with local state that synchronizes across sessions and devices
- Mobile applications requiring reliable offline-first data handling with E2EE
- Realtime dashboards that reflect changes instantly without polling
- Desktop applications synchronizing state across multiple machines
- Multiplayer systems requiring low-latency local state with deterministic convergence
- Regulated industries (healthcare, legal, finance) requiring E2EE for synced data
- Developer tools and IDEs with synchronized project state

### License

MIT — fully open source. No proprietary components, no BSL, no dual license.

---

## 2. Core Philosophy

### Traditional Architecture and Its Problems

```
Client
   ↓  (HTTP request)
REST API
   ↓
Remote Database  ← single source of truth
```

Every read and write goes through the server. Problems this creates:

- The application is unusable offline — no server means no data
- Every user interaction waits for a network round-trip before the UI updates
- Poor mobile experience under intermittent connectivity
- Realtime collaboration requires complex server-side infrastructure
- A slow or unavailable server makes the entire product feel broken
- Server costs scale linearly with read traffic
- Server compromise exposes all user data

### The Drift Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Application                            │
│                                                           │
│    ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│    │  Tab 1   │  │  Tab 2   │  │      Tab 3           │  │
│    │ (Leader) │  │ (Reader) │  │      (Reader)         │  │
│    └────┬─────┘  └────┬─────┘  └──────────┬───────────┘  │
│         │              │                   │               │
│         └──────────────┴───────────────────┘               │
│                          │  Shared Memory (IPC)            │
│    ┌─────────────────────▼─────────────────────────────┐  │
│    │               Drift Core Engine                     │  │
│    │                                                     │  │
│    │  ┌──────────────────┐  ┌────────────────────────┐  │  │
│    │  │ CRDT Document    │  │  E2EE Encryption       │  │  │
│    │  │ Store (Yrs)      │  │  Layer (libsodium)     │  │  │
│    │  └────────┬─────────┘  └────────────┬───────────┘  │  │
│    │           │                          │               │  │
│    │  ┌────────▼──────────────────────────▼───────────┐  │  │
│    │  │          Multi-Tab IPC Layer                   │  │  │
│    │  │  Leader Election │ Shared Mem │ Crash Recovery │  │  │
│    │  └────────────────────────┬───────────────────────┘  │  │
│    │                           │                           │  │
│    │  ┌────────────────────────▼───────────────────────┐  │  │
│    │  │          Storage Abstraction                    │  │  │
│    │  │  SQLite │ OPFS (WASM) │ RocksDB │ In-Memory   │  │  │
│    │  └────────────────────────┬───────────────────────┘  │  │
│    │                           │                           │  │
│    │  ┌────────────────────────▼───────────────────────┐  │  │
│    │  │          Transport Abstraction                  │  │  │
│    │  │  Coordinator │ P2P (WebRTC) │ Mesh (libp2p)    │  │  │
│    │  └────────────────────────┬───────────────────────┘  │  │
│    │                           │                           │  │
│    │  ┌────────────────────────▼───────────────────────┐  │  │
│    │  │          Observability (OpenTelemetry)          │  │  │
│    │  │  Tracing │ Metrics │ Logs │ Debug HTTP API     │  │  │
│    │  └────────────────────────────────────────────────┘  │  │
│    └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Design Principles

| Principle | Description |
|---|---|
| **CRDT-Native** | Every write is a CRDT mutation. Conflicts cannot exist — concurrent writes merge deterministically |
| **Zero-Trust** | Coordinator never sees plaintext data. E2EE is not optional; it is the default |
| **Multi-Process Safe** | Leader election prevents split-brain across tabs, windows, and OS processes |
| **Infrastructure-Agnostic** | Coordinator is a trait. Users choose or build the backend |
| **Observable by Default** | Every operation is traced end-to-end. Internal state is inspectable at runtime |
| **Local-First** | The app functions fully offline; server is not required for reads or writes |
| **Embedded First** | Sync engine runs in-process; no dedicated sync service required |
| **Durable Synchronization** | Operations survive crashes, disconnects, and restarts |
| **Eventual Consistency with CRDT Guarantees** | Replicas always converge to the same state |
| **Reactive State** | UI subscribes to local state; re-renders on change automatically |
| **Open Source (MIT)** | Every line of code is auditable. No black boxes. No proprietary components |

---

## 3. What Drift Is (and Is Not)

### What Drift Manages

- CRDT-native local database state (SQLite, OPFS, IndexedDB, RocksDB)
- CRDT document store and mutation log (append-only oplog of CRDT deltas)
- E2EE key generation, distribution, and rotation per namespace
- Multi-tab leader election and shared-memory state distribution
- Background upload of encrypted CRDT mutations to coordinator
- Download and merge of remote mutations from other replicas
- Offline operation buffering and upload on reconnect
- Reactive subscriptions (subscribe to table/row changes)
- Schema definitions and versioned migrations (stored as CRDT documents)
- Retry and reconnection logic for sync failures
- Device and replica identity management
- OpenTelemetry tracing, metrics, and structured logging
- Coordinator abstraction (swap backends without application changes)

### What Drift Does NOT Manage

- Authentication (use Clerk, Auth0, etc.)
- Authorization (use Aegis or any authz system — optional plugin)
- Background job execution (use Arc or any job system — optional plugin)
- Event pub-sub routing (use FluxBus or any event bus — optional plugin)
- HTTP API routing
- User account management

### Drift vs Traditional Sync Approaches

| Approach | Model | Offline? | Conflicts? | E2EE? | Multi-Tab Safe? | Infra Choice? |
|---|---|---|---|---|---|---|
| REST polling | Server-first | ❌ | Server wins | ❌ | ❌ | ❌ |
| WebSocket push | Server-first | ❌ | Server wins | ❌ | ❌ | ❌ |
| Optimistic UI | Server-first | Partial | LWW | ❌ | ❌ | ❌ |
| **Drift** | **Local-first** | **✅** | **CRDT (none)** | **✅** | **✅** | **✅** |
| LiveStoreJS (ElectricSQL) | Local-first | ✅ | LWW | ❌ | ❌ | ❌ (Postgres) |
| Zero (Rocicorp) | Local-first | ✅ | LWW | ❌ | ❌ | ❌ (Proprietary) |
| PowerSync | Local-first | ✅ | LWW | ❌ | ❌ | ❌ (MongoDB) |

---

## 4. Tech Stack

### Core Engine

| Layer | Technology | Rationale |
|---|---|---|
| Core Engine | **Rust** | Memory safety, WebAssembly compilation target, zero-cost async |
| Async Runtime | **Tokio** | Handles concurrent sync/upload/download loops |
| WebAssembly Target | **wasm-pack** + **wasm-bindgen** | Enables browser/OPFS deployment |
| CRDT Engine | **Yrs** (y-crdt) | Battle-tested CRDT implementation used in production by multiple editors. Supports Text, Map, Array, Counter types with automatic merge |
| Serialization | **serde** + **serde_json** + **bincode** | Fast operation serialization; bincode for internal, JSON for debugging |
| E2EE | **libsodium** (native) / **WebCrypto** (browser) via **aead** crate | X25519 key exchange, ChaCha20-Poly1305 encryption |
| Diff Engine | **Yrs encode_diff_v1** | CRDT-native change detection — no custom diff logic needed |
| Sync Transport | WebSocket (primary), HTTP (fallback), WebRTC (P2P) | Real-time connection with automatic fallback |
| Observability | **OpenTelemetry** (tracing crate + opentelemetry-rust) | Traces, metrics, and logs — industry standard |

### CRDT Types (Yrs)

| CRDT Type | Merge Behavior | Use Case |
|---|---|---|
| **Yrs Map (LWW)** | Last-write-wins per key with hybrid logical clock | Simple scalar fields, status flags |
| **Yrs Text** | Sequence CRDT (operational transform compatible) | Rich text, document bodies |
| **Yrs Array** | Ordered list CRDT | To-do list items, ordered collections |
| **Yrs Counter (PN)** | Increment/decrement that sums across replicas | Likes, votes, inventory counts |
| **Custom Yrs Plugin** | Developer-defined CRDT via Yrs extension | Complex business logic |

### Local Storage Backends

| Backend | Environment | Notes |
|---|---|---|
| **SQLite** (WAL mode) | Server, Desktop, Native Mobile | Default; transactional, embedded, battle-tested. WAL mode enables concurrent readers |
| **OPFS** (Origin Private File System) | Browser | Persistent browser-side SQLite via WebAssembly in Web Worker |
| **IndexedDB** | Browser (fallback) | Wider browser support; slower than OPFS. Limited schema migration |
| **RocksDB** | High-throughput native | LSM-tree; large local datasets |
| **In-Memory** | Testing | Ephemeral, for test suites and low-footprint scenarios |

### Coordinator Backends (Trait Implementations)

| Backend | Use Case | Storage |
|---|---|---|
| **PostgreSQL** | Production self-hosted | Relational with LISTEN/NOTIFY for real-time |
| **Redis** | High-throughput, in-memory | Streams + Pub/Sub for op distribution |
| **SQLite** | Single-server, dev, embedded | Zero-dependency coordinator |
| **Cloudflare Durable Objects** | Managed edge coordinator | Single-writer per namespace via DO |
| **Supabase** | Hosted PostgreSQL + Realtime | Supabase Realtime for WebSocket push |
| **Custom** | Any backend | Implement the Coordinator trait |

### SDKs

| Language | Package | Environment |
|---|---|---|
| **TypeScript (Browser)** | `@drift/web` | Browser-native; OPFS/IndexedDB |
| **TypeScript (Node.js)** | `@drift/node` | Server-side sync; SQLite backend |
| **React** | `@drift/react` | React hooks for reactive queries |
| **Swift** | `drift-swift` | iOS/macOS native |
| **Kotlin** | `drift-android` | Android native |
| **Rust** | `drift` (crate) | Core library |
| **Go** | `drift-go` | Server-side replication |

---

## 5. System Architecture

### Layer Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Application Process                           │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Application Layer                          │   │
│  │   drift.db.todos.insert({ text: "..." })                     │   │
│  │   drift.db.todos.subscribe(callback)                          │   │
│  └──────────────────────────┬───────────────────────────────────┘   │
│                             │                                        │
│  ┌─────────────────────────▼────────────────────────────────────┐   │
│  │                    Drift Core Engine (Rust)                   │   │
│  │                                                               │   │
│  │  ┌──────────────────────────────────────────────────────────┐ │   │
│  │  │              CRDT Document Layer (Yrs)                   │ │   │
│  │  │                                                          │ │   │
│  │  │  - Every table is a Yrs Map/Array document               │ │   │
│  │  │  - Every write produces a Yrs update (diff)              │ │   │
│  │  │  - Merges are deterministic by Yrs CRDT algorithm        │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              E2EE Encryption Layer                        │ │   │
│  │  │                                                          │ │   │
│  │  │  - Encrypt Yrs update with namespace key before upload   │ │   │
│  │  │  - Decrypt on download before merging into local doc     │ │   │
│  │  │  - Coordinator never sees plaintext                      │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              Multi-Tab IPC Layer                          │ │   │
│  │  │                                                          │ │   │
│  │  │  - Leader election (file lock / BroadcastChannel)        │ │   │
│  │  │  - Shared memory ring buffer for state                   │ │   │
│  │  │  - Heartbeat-based crash detection                       │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              Oplog & Sync Engine                          │ │   │
│  │  │                                                          │ │   │
│  │  │  - Append-only operation log of CRDT mutations            │ │   │
│  │  │  - Upload queue with batching and retry                   │ │   │
│  │  │  - Download queue with replay and merge                   │ │   │
│  │  │  - Subscription engine (fires on CRDT merge)             │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              Storage Abstraction                          │ │   │
│  │  │                                                          │ │   │
│  │  │  SQLite / OPFS / IndexedDB / RocksDB / In-Memory         │ │   │
│  │  │  - Materialized CRDT state                               │ │   │
│  │  │  - CRDT mutation log (oplog)                             │ │   │
│  │  │  - Schema metadata                                       │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              Transport Abstraction                        │ │   │
│  │  │                                                          │ │   │
│  │  │  ┌──────────┐  ┌──────────┐  ┌────────────────────────┐ │ │   │
│  │  │  │Coordinator│  │P2P (WRC)│  │  Mesh (libp2p)          │ │ │   │
│  │  │  │ WebSocket│  │WebRTC   │  │  Gossip / Kademlia      │ │ │   │
│  │  │  └──────────┘  └──────────┘  └────────────────────────┘ │ │   │
│  │  └────────────────────────┬─────────────────────────────────┘ │   │
│  │                           │                                     │   │
│  │  ┌────────────────────────▼─────────────────────────────────┐ │   │
│  │  │              Observability (OpenTelemetry)                │ │   │
│  │  │                                                          │ │   │
│  │  │  - Traces: every operation tracked end-to-end            │ │   │
│  │  │  - Metrics: Prometheus-compatible counters/gauges        │ │   │
│  │  │  - Logs: structured JSON via tracing crate               │ │   │
│  │  │  - Debug API: /debug/drift/* on localhost                │ │   │
│  │  └──────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                                                                        │
               ┌──────────┴──────────┐  ┌──────────┴──────────┐
               │   Coordinator (your  │  │  P2P / Mesh (opt.)  │
               │   choice)            │  │  Other Drift peers  │
               │   Postgres / Redis   │  │  via WebRTC / libp2p│
               │   DO / Supabase / .. │  │                     │
               └─────────────────────┘  └──────────────────────┘
```

### Component Responsibilities

**CRDT Document Layer (Yrs)**
Maintains all application state as Yrs CRDT documents. Every table is a Yrs Map document. Every write produces a Yrs binary update (diff) that can be applied to any replica to produce identical state. This eliminates the concept of "conflicts" entirely — concurrent writes always converge deterministically.

**E2EE Layer**
Encrypts every Yrs update before it leaves the process. Uses X25519 key exchange and ChaCha20-Poly1305 (via libsodium or WebCrypto). The coordinator stores only encrypted blobs. Decryption happens locally after download before CRDT merge.

**Multi-Tab IPC Layer**
Coordinates access to local storage across multiple browser tabs, Electron windows, or OS processes. One leader holds the write lock. Reader tabs access state via shared memory (mmap on native, SharedArrayBuffer in browser). Leaders are elected via file lock (native) or BroadcastChannel (browser). Heartbeat detects crashes; new leader elected automatically.

**Oplog & Sync Engine**
Append-only log of encrypted CRDT mutations. The upload queue reads pending mutations, batches them, and sends them to the chosen transport. The download queue receives remote mutations, decrypts them, and merges them into the local CRDT document. The subscription engine fires callbacks whenever a CRDT merge changes application-relevant state.

**Storage Abstraction**
Presents a unified interface for all storage backends. Stores:
- Materialized CRDT state (snapshots)
- CRDT mutation log (oplog)
- Schema metadata and migration history
- Replica identity and sync state

**Transport Abstraction**
Presents a unified `Transport` trait. The primary transport connects to the chosen coordinator via WebSocket. Optional transports (P2P via WebRTC, Mesh via libp2p) can be enabled per namespace or per table.

**Observability**
Every operation is traced with OpenTelemetry. The trace spans cover: CRDT merge → encrypt → write to oplog → transport → coordinator acknowledgment → decrypt → CRDT merge on remote. Debug HTTP API exposes full internal state without requiring external tools.

---

## 6. Core Concepts

### CRDT Document

A CRDT document is the fundamental unit of state in Drift. Every table is a Yrs CRDT document. Documents are:

- **Conflict-free**: concurrent edits from any number of replicas merge deterministically
- **Schema-aware**: fields within a document have CRDT types (LWW, Counter, Text, etc.)
- **Snapshottable**: full document state can be serialized for fast bootstrap
- **Differential**: changes are captured as Yrs binary diffs (mutation deltas), not full-state snapshots

```
Document: "todos" (Yrs Map)
├── field: "text"       → LWW-Register
├── field: "completed"  → LWW-Register
├── field: "tags"       → OR-Set
├── field: "priority"   → PN-Counter
└── field: "assignee"   → LWW-Register
```

### Replica

A replica is any instance of the application that holds a copy of the synchronized data. Each replica has a unique identity.

```
User's laptop    → replica:laptop-abc
User's phone     → replica:phone-xyz
Team member      → replica:laptop-def
Server instance  → replica:server-001
```

All replicas share the same logical dataset and always converge to the same state due to CRDT guarantees.

### Namespace

A namespace is the synchronization boundary. Replicas in the same namespace synchronize with each other. Replicas in different namespaces are isolated.

```
namespace: workspace:core       → all team members' replicas
namespace: user:123-personal    → user's devices only
namespace: project:aegis        → project-scoped replicas
```

Each namespace has its own:
- CRDT document set (tables)
- Encryption keypair (public/private)
- Coordinator routing (different coordinators per namespace)
- Sync settings and transport configuration

### CRDT Mutation (Operation)

A CRDT mutation is the atomic unit of synchronization. It is a Yrs binary diff that, when applied to any replica's CRDT document, produces the same result regardless of application order.

```json
{
  "id":        "mut_abc123",
  "replica":   "replica:laptop-abc",
  "namespace": "workspace:core",
  "docId":     "todos",
  "recordId":  "todo:789",
  "type":      "CRDT_UPDATE",
  "payload":   "<base64-encoded Yrs update binary>",
  "encrypted": "<base64-encoded AEAD ciphertext>",
  "timestamp": 1740000000,
  "sequence":  42,
  "syncStatus": "pending"
}
```

### Leader Election

Drift uses leader election to coordinate write access across multiple tabs/processes sharing the same local storage.

- **Leader**: the single process with write access to the local CRDT document store
- **Readers**: all other processes that read from shared memory (read-only)
- **Election**: via file lock (SQLite WAL lock on native) or `BroadcastChannel` + `navigator.locks` (browser)
- **Heartbeat**: leader writes a timestamp every 1s; readers detect >3s gap → initiate election
- **Recovery**: on leader crash, the new leader replays any in-flight operations from shared memory

### Sync State per Replica

```
last_synced_sequence:  42   ← last confirmed sequence from coordinator
pending_upload_count:  3    ← mutations waiting to upload
last_upload_at:        timestamp
last_download_at:      timestamp
connection_status:     connected | disconnected | syncing
leader_status:         leader | reader | electing
```

---

## 7. CRDT Document Model

### Why CRDTs?

Traditional sync engines treat conflicts as exceptional — something to detect and resolve after they happen. This leads to:

- Complex conflict detection logic
- Data loss on last-write-wins
- User-facing merge UIs
- Brittle upgrade paths (V1→V3→V4)

CRDTs eliminate the concept of conflict entirely. Every concurrent write produces a deterministic merge. The system is simpler, safer, and provably correct.

### Document Structure

Every CRDT document is a Yrs Map that contains fields of various types:

```
Yrs Document: "record:todo:789"
{
  "text":       LWW-Register("build drift"),
  "completed":  LWW-Register(false),
  "priority":   PN-Counter(3),
  "tags":       OR-Set(["urgent", "backend"]),
  "assignee":   LWW-Register("alice")
}
```

### How CRDT Merge Works

When two replicas modify the same document concurrently:

```
Replica A:  doc.set("text", "build drift runtime")
Replica B:  doc.set("completed", true)

Both Yrs updates are captured as binary diffs.

Replica A applies B's diff → { text: "build drift runtime", completed: true }
Replica B applies A's diff → { text: "build drift runtime", completed: true }

Both replicas converge to identical state.
```

Different fields are independent. Same field with concurrent edits:

```
Replica A:  doc.set("text", "version A")  (timestamp: T1)
Replica B:  doc.set("text", "version B")  (timestamp: T2)

Both converge to: { text: "version B" }
(LWW register: latest timestamp wins deterministically)
```

### CRDT Type Reference

| Type | Behavior | Deterministic? | Tombstones? |
|---|---|---|---|
| **LWW-Register** (Yrs Map default) | Last-write-wins by hybrid logical clock | Yes | No |
| **PN-Counter** | Increment/decrement that sums across replicas | Yes | No |
| **OR-Set** | Add/remove with per-element tombstones | Yes | Yes (GC-able) |
| **Text (Sequence)** | Insert/delete with position-based IDs | Yes | Yes (GC-able) |
| **Array** | Ordered sequence with insert/delete/move | Yes | Yes (GC-able) |

### Why This Eliminates the V1→V3→V4 Upgrade Path

The current spec's V1 (coordinator-order) → V3 (field-merge) → V4 (CRDTs) ladder exists because the V1 design doesn't carry enough information for field-level or CRDT merge. By using CRDTs from day one:

- **No data format migration**: every operation from day one is a CRDT mutation
- **No conflict handler rewrite**: the same Yrs merge engine works in Alpha and in v10
- **No schema redefinition**: CRDT types are declared at schema definition time and never need to change
- **Backward compatible**: new CRDT types can be added without changing existing data

---

## 8. Storage Model — CRDT-Native Hybrid

### Why Hybrid?

Two pure approaches exist — and both fail at scale:

**Event-only (pure oplog)**
- Startup requires replaying the entire CRDT mutation history
- Query performance degrades as history grows
- Storage costs grow unboundedly
- Impossible to query "current state" directly without replay

**Snapshot-only (no oplog)**
- Cannot produce a diff of what changed since a replica last synced
- Sync requires sending full document snapshots (expensive)
- No history for debugging, auditing, or rollback
- No incremental synchronization

**Drift's hybrid approach:**

```
┌────────────────────────────────────────────────────────────┐
│                    Local Database                            │
│                                                              │
│   Materialized CRDT State        CRDT Mutation Log          │
│   ──────────────────────         ──────────────────         │
│   todos CRDT document   ←───────  mut: UPDATE todos/789    │
│   (current Yrs state)            mut: INSERT todos/790    │
│                                  mut: DELETE todos/788    │
│                                                              │
│   Fast reads ✓                    Sync source ✓             │
│   Current state ✓                 History ✓                 │
│   Complex queries ✓               Replay ✓                  │
│   No oplog replay needed ✓        CRDT merge always safe ✓  │
└──────────────────────────────────────────────────────────────┘
```

**Key difference from the traditional hybrid**: The materialized CRDT state is a Yrs document snapshot that contains **all current values without replaying history**. The mutation log contains Yrs binary diffs. Unlike the old spec where the materialized state was opaque SQL rows, here it is a first-class CRDT document that can be snapshotted, transferred, and merged.

### CRDT Document Storage

Each CRDT document is stored as a Yrs binary snapshot:

```sql
CREATE TABLE drift_documents (
  doc_id      TEXT    PRIMARY KEY,  -- "todos" or "projects"
  record_id   TEXT    PRIMARY KEY,  -- "todo:789"
  doc_bytes   BLOB   NOT NULL,      -- Yrs binary snapshot
  updated_at  INTEGER NOT NULL,
  field_index JSON                  -- indexes for filtered queries
);
```

The `doc_bytes` column contains the full Yrs document state. This is loaded on startup and all operations are applied in-memory. Periodic snapshots are written to persist the current state.

### Oplog Compaction with CRDTs

CRDT-based compaction is simpler than the old spec's approach because Yrs documents can be garbage-collected:

1. **CRDT merge contains all needed info**: Yrs updates are self-contained. You don't need the full history to compute current state.
2. **Document snapshots replace replay**: Instead of replaying the oplog from the beginning, loading a Yrs document snapshot + applying mutations from the last snapshot onward gives current state.
3. **Tombstone GC**: Yrs supports garbage collection of tombstones (deleted elements). A `yrs::GarbageCollector` pass removes tombstones older than a configurable threshold.

```
Compaction cycle:
  ┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
  │  Full Yrs     │────►│  Apply compacted  │────►│  Yrs doc     │
  │  doc snapshot  │     │  ops from oplog   │     │  (smaller)   │
  └──────────────┘     └──────────────────┘     └──────────────┘
  (stores current      (drops ops already      (new snapshot with
   materialized state)  reflected in snapshot)   GC tombstones)
```

New replicas bootstrap from the latest Yrs document snapshot + mutations after the snapshot — not from full oplog replay.

---

## 9. Operation Log (Oplog)

### Operation Types

| Type | Trigger | Description |
|---|---|---|
| `CRDT_UPDATE` | Any write | Yrs binary diff (update) from a CRDT mutation |
| `CRDT_INSERT` | New record | New Yrs document created + initial state |
| `CRDT_DELETE` | Record removed | Yrs document deleted (tombstone if soft-delete) |
| `CRDT_BATCH` | `db.batch([...])` | Multiple CRDT mutations in one transaction |

There is only one fundamental operation type: **CRDT mutation**. INSERT, UPDATE, DELETE, and BATCH are all CRDT `YrsUpdate` operations with different metadata. This eliminates the complexity of the old spec's 6 operation types.

### Operation Structure

```typescript
interface Mutation {
  id:          string     // "mut_abc123" — globally unique
  replicaId:   string     // which replica produced this
  namespace:   string     // sync namespace
  type:        MutationType  // CRDT_UPDATE | CRDT_INSERT | CRDT_DELETE | CRDT_BATCH
  docId:       string     // table/document name
  recordId:    string     // record identifier
  yrsUpdate:   Uint8Array // Yrs binary diff (encoded CRDT mutation)
  encrypted:   Uint8Array // AEAD ciphertext of yrsUpdate
  timestamp:   number     // local wall clock (Unix ms)
  sequence?:   number     // coordinator-assigned ordering (null until synced)
  syncStatus:  "pending" | "synced" | "failed"
  createdAt:   Date
}
```

### Operation Capture (Inside Transaction)

Operations are captured inside the same database transaction as the CRDT write. This guarantees that no write can occur without a corresponding oplog entry.

```rust
// CRDT write + oplog entry in one transaction
txn.begin();
  let yrs_update = doc.update(txn, |t| {
    t.insert(&mut doc_map, "text", "build drift");
  });
  // yrs_update is the binary diff
  oplog.insert(Mutation {
    id: "mut_abc",
    yrsUpdate: yrs_update,
    encrypted: encrypt(yrs_update, namespace_key),
    syncStatus: "pending",
    ...
  });
txn.commit();
```

If the transaction fails, neither the CRDT write nor the oplog entry is recorded. Consistency is guaranteed at the storage level.

### Oplog Schema

```sql
CREATE TABLE drift_oplog (
  id               TEXT    PRIMARY KEY,
  replica_id       TEXT    NOT NULL,
  namespace        TEXT    NOT NULL,
  mutation_type    TEXT    NOT NULL,   -- CRDT_UPDATE | CRDT_INSERT | CRDT_DELETE | CRDT_BATCH
  doc_id           TEXT    NOT NULL,   -- table/document name
  record_id        TEXT    NOT NULL,
  yrs_update       BLOB   NOT NULL,   -- Yrs binary diff (encrypted in transit, plaintext locally)
  encrypted_blob   BLOB,              -- AEAD ciphertext (for verification)
  timestamp        INTEGER NOT NULL,
  sequence         INTEGER,           -- assigned by coordinator after sync
  sync_status      TEXT    NOT NULL DEFAULT 'pending',
  synced_at        DATETIME,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_oplog_sync_status ON drift_oplog(sync_status, created_at);
CREATE INDEX idx_oplog_namespace   ON drift_oplog(namespace, sequence);
CREATE INDEX idx_oplog_record      ON drift_oplog(doc_id, record_id);
```

### Key Differences from the Old Spec

| Old Spec | New Spec | Why |
|---|---|---|
| 6 operation types (INSERT, UPDATE, DELETE, UPSERT, BATCH, TRUNCATE) | 4 types (CRDT_UPDATE, CRDT_INSERT, CRDT_DELETE, CRDT_BATCH) | CRDT operations are homogeneous; the diff IS the operation |
| Opaque JSON payload | Yrs binary diff | CRDT diff is smaller, self-contained, and merge-safe |
| `previous_payload` field for undo | Not stored (CRDT document snapshot handles undo) | Yrs documents can be rolled back by reloading a prior snapshot |
| Separate conflict_info column | Not needed | CRDTs have no conflicts to record |
| `pending_sync → synced → failed → conflict` statuses | `pending → synced → failed` | CRDTs never enter a "conflict" state |

---

## 10. Schema System & Migrations

### Schema Definition

Every table synchronized by Drift must have an explicit schema. The schema defines field types (CRDT types), indexes, and sync behavior.

```typescript
import { Drift } from "@drift/core"

const drift = new Drift({
  storage:    "sqlite",
  namespace:  "workspace:core"
})

drift.schema.define("todos", {
  fields: {
    id:          { type: "string",  crdtType: "lww",    primaryKey: true },
    text:        { type: "string",  crdtType: "lww",    indexed: true    },
    completed:   { type: "boolean", crdtType: "lww"                      },
    priority:    { type: "number",  crdtType: "counter"                  },
    tags:        { type: "array",   crdtType: "orset",  indexed: true    },
    assignee:    { type: "string",  crdtType: "lww",    indexed: true    },
    createdAt:   { type: "number",  crdtType: "lww"                      },
    updatedAt:   { type: "number",  crdtType: "lww"                      },
    localNote:   { type: "string",  crdtType: "lww",    sync: false      }  // local only
  },
  indexes: [
    { fields: ["assignee"] },
    { fields: ["priority", "completed"] }
  ],
  softDelete: true  // DELETE sets tombstone instead of removing
})
```

### CRDT Type as Conflict Strategy

In the old spec, conflict strategy was a separate concern from field type. In the new spec, **the CRDT type IS the conflict strategy**:

| Schema `crdtType` | Merge Behavior | Replaces Old Strategy |
|---|---|---|
| `lww` | Last-write-wins by hybrid logical clock | `last-write-wins` |
| `counter` | Increment/decrement sums across replicas | `max` / `min` |
| `orset` | Add/remove with tombstone tracking | `append` |
| `text` | Sequence CRDT | N/A (new capability) |
| `custom` | Developer-defined Yrs plugin | `custom` |

### Local-Only Fields

Fields marked `sync: false` are persisted locally but never included in CRDT mutations sent to the coordinator. Useful for UI state, drafts, or device-specific preferences.

### Soft Delete

When `softDelete: true`, DELETE operations add a tombstone marker to the Yrs document (an `isDeleted: true` field) instead of removing the document. This is important for sync correctness — a hard delete cannot be distinguished from "this record was never created" by replicas that haven't seen it yet.

### Versioned Migrations

Schema changes are versioned and tracked. Every replica stores its current schema version and validates compatibility before accepting remote operations.

```typescript
drift.migration("v1", async (db) => {
  // Initial schema — runs on fresh install
  await db.defineDocument("todos", {
    fields: {
      id:        { type: "string", crdtType: "lww", primaryKey: true },
      text:      { type: "string", crdtType: "lww" },
      completed: { type: "boolean", crdtType: "lww" }
    }
  })
})

drift.migration("v2", async (db) => {
  // Add priority field as PN-Counter
  await db.addField("todos", "priority", { type: "number", crdtType: "counter" })
})

drift.migration("v3", async (db) => {
  // Add tags field as OR-Set
  await db.addField("todos", "tags", { type: "array", crdtType: "orset" })
})
```

### Migration Safety Rules

- CRDT fields are **additive only** — new fields can be added at any time without affecting existing data
- Existing fields cannot be removed (CRDT tombstones handle deletion automatically)
- CRDT type of an existing field cannot be changed (a new field with a different name + old field tombstone is the migration path)
- Every replica validates its schema version against the coordinator's current version before syncing
- A replica on an older schema version downloads and runs pending migrations before resuming sync
- Migrations are stored in `drift_migrations` table and are themselves synced as Yrs documents

### Schema Metadata Table

```sql
CREATE TABLE drift_schema_meta (
  doc_id          TEXT    PRIMARY KEY,
  schema_version  INTEGER NOT NULL DEFAULT 1,
  schema_json     JSON    NOT NULL,
  applied_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE drift_migrations (
  version     TEXT    PRIMARY KEY,
  applied_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  checksum    TEXT    NOT NULL,   -- SHA256 of migration code; detects tampering
  rollback    TEXT                -- rollback instructions (if applicable)
);
```

If a migration checksum does not match, Drift **refuses to apply the migration and logs the discrepancy to the observability system**. The operator must resolve the tampered migration before sync continues.

---

## 11. End-to-End Encryption

### Why E2EE Must Be Default

Every other sync engine (LiveStoreJS, Zero, PowerSync, Replicache) stores plaintext data on the coordinator. This means:
- The coordinator operator can read all synced data
- A coordinator breach exposes all application data
- Compliance with HIPAA, GDPR, SOC2 requires additional layers

Drift's E2EE ensures **the coordinator is zero-trust**. It stores only encrypted blobs and routing metadata. It cannot read any application data.

### Key Generation

On namespace creation, Drift generates an X25519 keypair:

```
Namespace "workspace:core"
├── Public Key:  pk_core_abc123...   (shared with coordinator)
└── Private Key: sk_core_xyz789...   (never leaves device; encrypted with device key)
```

- Public key: registered with the coordinator for routing
- Private key: encrypted with the device's local key (OS keychain on native, WebCrypto `subtle.exportKey()` on browser) and stored in local database

### Operation Encryption Flow

```
Local CRDT write
       │
       ▼
Yrs binary diff produced
       │
       ▼
Encrypt with namespace private key:
  ciphertext = crypto_box(yrs_update, namespace_sk, coordinator_pk)
       │
       ▼
Store in oplog:
  - yrs_update: plaintext (local, for fast merge)
  - encrypted_blob: ciphertext (for upload to coordinator)
       │
       ▼
Upload to coordinator:
  Coordinator receives: { id, replicaId, namespace, encrypted_blob, timestamp }
  Coordinator never sees plaintext
       │
       ▼
Coordinator stores encrypted_blob + metadata
       │
       ▼
Other replica downloads:
  Replica receives encrypted_blob
       │
       ▼
Decrypt with namespace private key:
  yrs_update = crypto_box_open(encrypted_blob, namespace_sk, coordinator_pk)
       │
       ▼
Merge into local CRDT document via Yrs
```

### What the Coordinator Sees

The coordinator stores only:

```json
{
  "id":         "mut_abc123",
  "namespace":  "workspace:core",
  "replica_id": "laptop-abc",
  "encrypted":  "<base64 ciphertext>",
  "timestamp":  1740000000,
  "sequence":   42
}
```

No plaintext field names. No plaintext values. No schema information. The coordinator cannot distinguish between a "text" field update and a "completed" field update — it sees only opaque encrypted bytes.

### Key Distribution

1. Each replica generates its own keypair on first join
2. Public key is registered with the coordinator (visible to other replicas in the namespace)
3. Private key never leaves the device
4. When Replica A sends to Replica B, it encrypts with its own private key + Replica B's public key
5. Replica B decrypts with its private key + Replica A's public key

### Key Rotation

Keys can be rotated without data loss:

```
1. Generate new keypair (pk_new, sk_new)
2. Re-encrypt local oplog with new key
3. Upload new public key to coordinator
4. Sign the rotation with old private key (proves ownership)
5. Coordinator rotates: new mutations use new key
6. Old mutations remain decryptable with old key (kept in local keychain)
```

### Replay Attack Prevention

Each mutation includes a unique ID (`mut_abc123`). The coordinator deduplicates by mutation ID. Replayed mutations are silently dropped — idempotent by design.

---

## 12. Multi-Tab & Multi-Process Architecture

### The Problem

When multiple browser tabs, Electron windows, or OS processes share the same local storage (SQLite file or OPFS), concurrent writes cause:

- SQLite locking errors (SQLITE_BUSY)
- CRDT document corruption (concurrent Yrs mutations)
- Oplog ordering violations
- Subscription double-firing

Drift solves this with **leader election**: one process writes, all others read via shared memory.

### Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Tab/Process A   │     │   Tab/Process B   │     │   Tab/Process C   │
│     (Leader)      │     │     (Reader)      │     │     (Reader)      │
├──────────────────┤     ├──────────────────┤     ├──────────────────┤
│ Write access: ✅  │     │ Write access: ❌  │     │ Write access: ❌  │
│ Shared mem: r/w  │     │ Shared mem: r/o   │     │ Shared mem: r/o   │
│ Lock: acquired   │     │ Lock: waiting     │     │ Lock: waiting     │
│ Heartbeat: 1s    │     │                    │     │                    │
└────────┬─────────┘     └────────┬──────────┘     └────────┬──────────┘
         │                        │                         │
         └────────────────────────┼─────────────────────────┘
                                  │
                     ┌────────────▼────────────┐
                     │    Shared Memory Mmap     │
                     │  (latest CRDT doc state   │
                     │   + oplog tail + status)  │
                     └─────────────────────────┘
                                  │
                     ┌────────────▼────────────┐
                     │     Local Storage         │
                     │  (SQLite / OPFS file)     │
                     └─────────────────────────┘
```

### Leader Election Protocol

| Platform | Mechanism | Details |
|---|---|---|
| **Native (Linux/macOS Windows)** | `flock` on SQLite WAL file or `named pipe` / `CreateMutex` | Process acquires exclusive lock. Lock released on process exit (even crash) by OS |
| **Browser (tabs)** | `BroadcastChannel` + `navigator.locks.request` | First tab acquires `drift-leader` lock. All tabs communicate via BroadcastChannel. `SharedArrayBuffer` passes state |
| **Electron** | Same as browser + `net.Server` on local socket for cross-window IPC | Primary window is leader; renderers are readers |

### Heartbeat & Crash Detection

```
Leader writes heartbeat every 1 second to shared memory:
  { leaderId: "tab-A", timestamp: 1740000001000, sequence: 42 }

Reader checks every 500ms:
  if current_time - leader.heartbeat.timestamp > 3000ms:
    → leader considered dead
    → reader attempts to acquire lock
    → if acquired, becomes new leader
    → replays uncommitted ops from shared memory oplog tail
    → notifies other readers via BroadcastChannel
```

### Shared Memory Layout

```
┌──────────────────────────────────────────────────────┐
│                Shared Memory Region                    │
├──────────────────────────────────────────────────────┤
│ Header:                                               │
│   leader_id, leader_timestamp, generation_number      │
├──────────────────────────────────────────────────────┤
│ CRDT Document Cache:                                  │
│   Yrs binary snapshots (latest state per document)    │
├──────────────────────────────────────────────────────┤
│ Oplog Tail (ring buffer):                             │
│   Last N mutations not yet synced                     │
├──────────────────────────────────────────────────────┤
│ Subscription Traffic:                                 │
│   Change notifications for cross-tab subscription     │
└──────────────────────────────────────────────────────┘
```

### What Happens on Leader Crash

```
Leader Tab A crashes (notebook closes, browser tab killed)
         │
         ▼
Reader Tab B detects heartbeat timeout (>3s)
         │
         ▼
Tab B acquires file lock / navigator.locks.request
         │
         ▼
Tab B reads shared memory oplog tail
         │
         ▼
Tab B replays any in-flight mutations into local CRDT documents
         │
         ▼
Tab B reads drift_sync_state from storage to get last confirmed sequence
         │
         ▼
Tab B becomes Leader
         │
         ▼
Tab B continues sync from last confirmed sequence
         │
         ▼
Tab C (another reader) detects new leader via BroadcastChannel
         │
         ▼
Tab C updates shared memory reference to new leader
```

**No data is lost.** The leader crash only loses in-flight mutations that were in memory but not yet written to the oplog. The shared memory ring buffer preserves the last N mutations, and the new leader replays them before taking over.

### Subscription Across Tabs

Subscriptions fire in all tabs — not just the leader. When a CRDT merge happens:

1. **Leader** merges the mutation, writes to storage, updates shared memory
2. **Leader** fires subscription callbacks in its own process
3. **Leader** sends `{ changedDocs: ["todos/789"] }` via BroadcastChannel
4. **Reader tabs** receive notification, read updated CRDT state from shared memory
5. **Reader tabs** fire their own subscription callbacks

---

## 13. Synchronization Flow

### Complete Flow — Local Write to Remote Replica

```
Step 1: Application Write
────────────────────────
drift.db.todos.insert({ id: "todo:123", text: "build drift" })
         │
         ▼
    CRDT: Yrs document "todos/123" receives mutation
         │
         ▼
    Yrs produces binary diff (yrs_update)
         │
         ▼
    E2EE: encrypt(yrs_update, namespace_key) → encrypted_blob
         │
         ▼
    BEGIN TRANSACTION (Leader only; others route to leader via IPC)
      INSERT into drift_oplog (yrs_update, encrypted_blob, status: pending)
      Update CRDT document snapshot in storage
    COMMIT
         │
         ▼
    UI updates immediately (CRDT merge fires subscription locally)


Step 2: Subscription Notification
──────────────────────────────────
    Subscription engine detects CRDT document change
         │
         ▼
    Notifies all subscribers for "todos"
         │
         ▼
    React components re-render instantly
         │
         ▼
    If multi-tab: leader broadcasts change via shared memory
    Reader tabs detect change → fire their subscriptions


Step 3: Upload Queue Processes Oplog
─────────────────────────────────────
    Background upload worker polls oplog
         │
         ▼
    Reads pending operations
         │
         ▼
    Batches if multiple pending (batch CRDT updates = concatenated Yrs diffs)
         │
         ▼
    Uploads encrypted_blob to coordinator via WebSocket
    (Coordinator never sees plaintext yrs_update)


Step 4: Coordinator Processes Mutation
────────────────────────────────────────
    Validates: schema version compatible? Replica authorized?
         │
         ▼
    Assigns monotonic sequence number
         │
         ▼
    Persists encrypted_blob + metadata to coordinator storage
         │
         ▼
    Pushes to all other replicas in namespace


Step 5: Replica Acknowledges
─────────────────────────────
    Coordinator sends ACK with sequence number
         │
         ▼
    Local oplog entry: sync_status → 'synced'
         │
         ▼
    last_synced_sequence updated


Step 6: Other Replicas Receive Mutation
─────────────────────────────────────────
    Replica B subscribed to namespace
         │
         ▼
    Coordinator pushes encrypted_blob to Replica B
         │
         ▼
    Replica B decrypts: yrs_update = decrypt(encrypted_blob, namespace_key)
         │
         ▼
    Replica B's CRDT engine merges yrs_update into local document
         │
         ▼
    CRDT merge fires subscription → Replica B's UI updates
```

### Sync States per Mutation

```
pending       ← written locally, not yet uploaded
uploading     ← currently being sent to coordinator
synced        ← coordinator confirmed receipt and assigned sequence
failed        ← upload failed; in retry queue
```

### Why "conflict" State Is Removed

In the old spec, operations could enter a `conflict` state. In CRDT-native Drift, **mutations never conflict**. Every Yrs update can be applied to any document state and produces a deterministic result. The `conflict` status is eliminated.

---

## 14. Offline Support & Durability

### What Happens Offline

When network connectivity is lost:

```
Network disconnects
         │
         ▼
Drift detects disconnect (WebSocket close / fetch failure)
         │
         ▼
Connection status → "disconnected"
         │
         ▼
Application continues working fully:
  - Reads served from local CRDT materialized state
  - Writes produce CRDT mutations → persist to local DB + oplog (status: pending)
  - Mutations encrypted before storage (E2EE at rest)
  - Subscriptions continue firing on local CRDT merges
  - UI remains fully interactive
  - Multi-tab: leader continues writing for all tabs
         │
         ▼
Pending mutations accumulate in oplog (encrypted at rest)
```

### What Happens on Reconnect

```
Network reconnects
         │
         ▼
Drift detects connectivity (WebSocket reconnect / heartbeat success)
         │
         ▼
Connection status → "syncing"
         │
         ▼
Step 1: Upload backlog
  → Read all pending mutations from oplog
  → Sort by local timestamp
  → Upload in batches to coordinator
         │
         ▼
Step 2: Download missed mutations
  → Request all mutations with sequence > last_synced_sequence
  → Decrypt each mutation
  → Merge into local CRDT documents
  → Fire subscriptions for affected documents
         │
         ▼
Connection status → "connected"
         │
         ▼
Resume real-time sync
```

### Durability Guarantee

CRDT mutations written locally are durable from the moment the local database transaction commits. A process crash, tab close, device sleep, or app kill does not lose pending mutations:

1. **Single-tab case**: Mutations are in the oplog on persistent storage. On restart, Drift reads all `pending` and `failed` mutations and re-uploads them. CRDT updates are idempotent — re-uploading does not cause duplicates.

2. **Multi-tab case**: If leader crashes, the shared memory ring buffer preserves the last N pending mutations. The new leader replays them before taking over. If shared memory is also lost (full power failure), the new leader reads the oplog from persistent storage — same as single-tab recovery.

3. **E2EE at rest**: All Yrs updates stored in the oplog are encrypted with the namespace key. Local storage theft does not expose application data.

### Pending Sync Table

```sql
CREATE TABLE drift_sync_state (
  namespace             TEXT    PRIMARY KEY,
  replica_id            TEXT    NOT NULL,
  last_synced_sequence  INTEGER NOT NULL DEFAULT 0,
  pending_upload_count  INTEGER NOT NULL DEFAULT 0,
  connection_status     TEXT    NOT NULL DEFAULT 'disconnected',
  leader_status         TEXT    NOT NULL DEFAULT 'leader',
  last_connected_at     DATETIME,
  last_sync_at          DATETIME,
  schema_version        INTEGER NOT NULL DEFAULT 1
);
```

---

## 15. Retry & Reconnection System

### Upload Retry Policy

```typescript
new Drift({
  sync: {
    uploadRetry: {
      attempts:     10,
      backoff:      "exponential",
      initialDelay: "500ms",
      maxDelay:     "2m",
      jitter:       true
    },
    reconnect: {
      attempts:     Infinity,    // keep trying indefinitely
      backoff:      "exponential",
      initialDelay: "1s",
      maxDelay:     "30s"
    }
  }
})
```

### Retry Flow

```
Upload attempt fails (network error / coordinator unavailable)
         │
         ▼
Mark operation as: sync_status = 'failed'
         │
         ▼
Retry engine schedules next attempt
(exponential backoff: 500ms → 1s → 2s → 4s → ... → 2m with jitter)
         │
         ▼
Retry attempt
         │
         ▼
If coordinator returns 4xx (bad request, auth failure):
  → Mark as permanent failure
  → Log to observability system
  → Do not retry automatically
         │
         ▼
If coordinator returns 5xx (server error):
  → Continue retry loop
         │
         ▼
If max attempts exceeded:
  → sync_status = 'upload_failed'
  → Alert via observability system
```

### Connection Health

Drift maintains a WebSocket heartbeat. If the heartbeat is missed:

```
Heartbeat missed (3 consecutive)
         │
         ▼
Mark connection as disconnected
         │
         ▼
Pause upload queue
         │
         ▼
Begin reconnect loop with backoff
         │
         ▼
On reconnect:
  → Re-authenticate replica
  → Resume from last_synced_sequence
  → Upload pending mutations
```

---

## 16. Reactive Subscriptions

Subscriptions are the primary way application UIs stay in sync with local state. They fire on both local writes and incoming remote CRDT merges.

### How CRDT-Aware Subscriptions Work

Unlike the old spec where subscriptions watched opaque table rows, Drift subscriptions watch CRDT documents. When a Yrs merge changes a document, the subscription engine compares the document's state before and after the merge and fires callbacks for changed fields.

**Key behavioral difference from the old spec:**
- Subscriptions fire **after** the CRDT merge completes, not after individual row operations
- A single remote mutation that affects 10 fields fires one subscription callback, not 10
- The callback receives the full CRDT document state (materialized), not a "change event"

### Table Subscription

```typescript
// Subscribe to all changes on a table (document collection)
const unsub = drift.db.todos.subscribe((todos) => {
  renderTodoList(todos)
})

// Unsubscribe when component unmounts
unsub()
```

### Filtered Subscription (CRDT Field Index)

```typescript
// Subscribe to a filtered query
const unsub = drift.db.todos.subscribe(
  (todos) => renderTodos(todos),
  { where: { assignee: "alice", completed: false } }
)
```

Filtered subscriptions use CRDT field indexes: the engine maintains a materialized index of LWW-Register fields and only fires subscriptions when indexed fields change matching the filter predicate.

### Single Record Subscription

```typescript
// Subscribe to a specific CRDT document
const unsub = drift.db.todos.subscribeOne(
  "todo:123",
  (todo) => {
    if (!todo) return renderDeleted()
    renderTodo(todo)
  }
)
```

### Sync Status Subscription

```typescript
// Subscribe to sync status changes
drift.subscribe("sync:status", (status) => {
  updateSyncIndicator(status)
  // status: { connected, pendingUploads, lastSyncAt, leaderStatus }
})
```

### React Hooks

```typescript
import { useDrift, useDriftQuery, useDriftOne, useDriftSync } from "@drift/react"

function TodoList() {
  const todos = useDriftQuery(db => db.todos.findAll())
  return <ul>{todos.map(t => <TodoItem key={t.id} todo={t} />)}</ul>
}

function TodoDetail({ id }: { id: string }) {
  const todo = useDriftOne(db => db.todos.findById(id))
  if (!todo) return <NotFound />
  return <div>{todo.text}</div>
}

function ProjectTodos({ projectId }: { projectId: string }) {
  const todos = useDriftQuery(db =>
    db.todos.findAll({ where: { projectId, completed: false } })
  )
  return <TodoList todos={todos} />
}

function SyncIndicator() {
  const { connected, pendingUploads } = useDriftSync()
  return (
    <span>{connected ? "✓ Synced" : `⟳ ${pendingUploads} pending`}</span>
  )
}
```

### Subscription Internals

1. Application calls `db.todos.subscribe(callback, filter?)`
2. Subscription engine registers a **CRDT document watcher** on the `todos` document collection
3. When a Yrs merge modifies any `todos` document, the engine checks:
   - Is this document's collection watched?
   - Does the change match any active filter predicates?
4. If matched, the callback is invoked synchronously after the CRDT merge commits
5. For multi-tab: leader fires subscriptions locally; readers fire subscriptions after receiving shared memory notification

---

## 17. Coordinator Abstraction Layer

### Why a Trait?

The old spec locked Drift to Cloudflare Durable Objects. This violated the principle of infrastructure choice. The Coordinator trait makes the coordinator a **pluggable backend** that users can choose, replace, or self-host without changing application code.

### Coordinator Trait Definition

```rust
#[async_trait]
pub trait Coordinator: Send + Sync + Debug {
    /// Push CRDT mutations to the coordinator for a namespace.
    /// Returns the assigned sequence numbers.
    async fn push(
        &self,
        namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError>;

    /// Pull mutations from the coordinator after the given sequence.
    async fn pull(
        &self,
        namespace: &str,
        after: SequenceId,
        limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError>;

    /// Subscribe to real-time mutation notifications for a namespace.
    async fn subscribe(
        &self,
        namespace: &str,
        from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation>>, CoordinatorError>;

    /// Register a replica in the namespace.
    async fn register(
        &self,
        namespace: &str,
        info: ReplicaInfo,
    ) -> Result<(), CoordinatorError>;

    /// Send a heartbeat to keep the replica connection alive.
    async fn heartbeat(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<(), CoordinatorError>;

    /// Get the current schema version for a namespace.
    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError>;

    /// Report sync state for observability.
    async fn report_metrics(&self, metrics: CoordinatorMetrics) -> Result<(), CoordinatorError>;
}
```

### What the Coordinator Stores

The coordinator stores only what it needs for ordering and routing:

```sql
-- Coordinator-agnostic data model
coordinator_mutations:
  sequence      BIGINT PRIMARY KEY
  namespace     TEXT    NOT NULL
  replica_id    TEXT    NOT NULL
  mutation_id   TEXT    NOT NULL UNIQUE    -- dedup key
  encrypted     BLOB   NOT NULL           -- AEAD ciphertext
  timestamp     BIGINT  NOT NULL
  created_at    TIMESTAMPTZ NOT NULL

coordinator_replicas:
  id            TEXT    PRIMARY KEY
  namespace     TEXT    NOT NULL
  public_key    BLOB   NOT NULL           -- E2EE public key
  last_heartbeat TIMESTAMPTZ
  last_sequence BIGINT  NOT NULL DEFAULT 0
```

### Built-in Coordinator Implementations

| Implementation | Package | Storage | Real-time |
|---|---|---|---|
| **PostgreSQL** | `@drift/coordinator-postgres` | `drift_coordinator_mutations` table | `LISTEN/NOTIFY` |
| **Redis** | `@drift/coordinator-redis` | Redis Streams per namespace | `PUBLISH/SUBSCRIBE` |
| **SQLite** | `@drift/coordinator-sqlite` | Single SQLite file (dev/test) | Polling |
| **Cloudflare DO** | `@drift/coordinator-cloudflare` | Durable Object state | WebSocket per DO |
| **Supabase** | `@drift/coordinator-supabase` | Supabase tables | Supabase Realtime |
| **In-Memory** | `@drift/coordinator-memory` | HashMap (testing) | Channel |

### Switching Coordinators

```typescript
// PostgreSQL
const drift = new Drift({
  storage: "sqlite",
  namespace: "workspace:core",
  coordinator: new PostgresCoordinator({
    connectionString: "postgres://user:pass@host/db"
  })
})

// Cloudflare
const drift = new Drift({
  storage: "opfs",
  namespace: "workspace:core",
  coordinator: new CloudflareCoordinator({
    accountId: "...",
    durableObjectId: "..."
  })
})

// Custom
const drift = new Drift({
  storage: "sqlite",
  namespace: "workspace:core",
  coordinator: new MyCustomCoordinator({
    endpoint: "https://my-coordinator.example.com"
  })
})
```

### Bootstrapping a New Replica

```
New replica connects
         │
         ▼
Sends: { replicaId, namespace, publicKey, schemaVersion, lastSequence: 0 }
         │
         ▼
Coordinator checks if schemaVersion matches current
         │
         ▼
If schema mismatch → send migration instructions
         │
         ▼
Coordinator sends latest CRDT document snapshot (if available) + mutation tail
         │
         ▼
Replica loads CRDT document snapshot into Yrs engine
         │
         ▼
Replica applies all mutations from snapshot sequence → latest
         │
         ▼
Replica marks sync_status = "connected"
         │
         ▼
Normal real-time sync begins
```

---

## 18. Full Database Schema

### Local (Client-Side) Tables

```sql
-- CRDT Document Store
-- Each document is a Yrs binary snapshot of current state
CREATE TABLE drift_documents (
  doc_id      TEXT    NOT NULL,          -- table/document name
  record_id   TEXT    NOT NULL,          -- record identifier
  doc_bytes   BLOB   NOT NULL,          -- Yrs binary snapshot
  updated_at  INTEGER NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at  DATETIME,                 -- soft delete marker
  PRIMARY KEY (doc_id, record_id)
);

-- Operation Log (CRDT mutations)
CREATE TABLE drift_oplog (
  id               TEXT    PRIMARY KEY,
  replica_id       TEXT    NOT NULL,
  namespace        TEXT    NOT NULL,
  mutation_type    TEXT    NOT NULL,     -- CRDT_UPDATE | CRDT_INSERT | CRDT_DELETE | CRDT_BATCH
  doc_id           TEXT    NOT NULL,
  record_id        TEXT    NOT NULL,
  yrs_update       BLOB   NOT NULL,     -- Yrs binary diff (plaintext locally)
  encrypted_blob   BLOB,                -- AEAD ciphertext (for upload verification)
  timestamp        INTEGER NOT NULL,
  sequence         INTEGER,             -- assigned by coordinator
  sync_status      TEXT    NOT NULL DEFAULT 'pending',
  synced_at        DATETIME,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_oplog_sync_status ON drift_oplog(sync_status, created_at);
CREATE INDEX idx_oplog_namespace   ON drift_oplog(namespace, sequence);
CREATE INDEX idx_oplog_record      ON drift_oplog(doc_id, record_id);

-- Sync state per namespace
CREATE TABLE drift_sync_state (
  namespace             TEXT    PRIMARY KEY,
  replica_id            TEXT    NOT NULL,
  last_synced_sequence  INTEGER NOT NULL DEFAULT 0,
  pending_upload_count  INTEGER NOT NULL DEFAULT 0,
  connection_status     TEXT    NOT NULL DEFAULT 'disconnected',
  leader_status         TEXT    NOT NULL DEFAULT 'leader',
  last_connected_at     DATETIME,
  last_sync_at          DATETIME,
  schema_version        INTEGER NOT NULL DEFAULT 1
);

-- Schema metadata
CREATE TABLE drift_schema_meta (
  doc_id          TEXT    PRIMARY KEY,
  schema_version  INTEGER NOT NULL DEFAULT 1,
  schema_json     JSON    NOT NULL,
  applied_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Migration history
CREATE TABLE drift_migrations (
  version     TEXT    PRIMARY KEY,
  applied_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  checksum    TEXT    NOT NULL,
  rollback    TEXT
);

-- Replica identity
CREATE TABLE drift_replica_meta (
  id            TEXT    PRIMARY KEY,
  device_name   TEXT,
  device_type   TEXT,
  namespace     TEXT    NOT NULL,
  public_key    BLOB   NOT NULL,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Encryption key storage
CREATE TABLE drift_keys (
  id            TEXT    PRIMARY KEY,
  namespace     TEXT    NOT NULL,
  key_type      TEXT    NOT NULL,        -- 'namespace_sk' | 'device_sk'
  key_blob      BLOB   NOT NULL,        -- encrypted private key
  key_version   INTEGER NOT NULL DEFAULT 1,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### Coordinator-Side Schema (Postgres Example)

```sql
-- Coordinator mutation log
CREATE TABLE drift_coordinator_mutations (
  sequence      BIGSERIAL  PRIMARY KEY,
  namespace     TEXT       NOT NULL,
  replica_id    TEXT       NOT NULL,
  mutation_id   TEXT       NOT NULL UNIQUE,
  encrypted     BYTEA      NOT NULL,     -- AEAD ciphertext
  timestamp     BIGINT     NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_coord_mutations_namespace_seq
  ON drift_coordinator_mutations(namespace, sequence);
CREATE INDEX idx_coord_mutations_replica
  ON drift_coordinator_mutations(replica_id);
CREATE INDEX idx_coord_mutations_mutation_id
  ON drift_coordinator_mutations(mutation_id);

-- Replica registry
CREATE TABLE drift_coordinator_replicas (
  id              TEXT    PRIMARY KEY,
  namespace       TEXT    NOT NULL,
  public_key      BYTEA   NOT NULL,
  device_info     JSONB,
  last_heartbeat  TIMESTAMPTZ,
  last_sequence   BIGINT  NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Snapshot registry
CREATE TABLE drift_coordinator_snapshots (
  id              TEXT    PRIMARY KEY,
  namespace       TEXT    NOT NULL,
  sequence        BIGINT  NOT NULL,
  snapshot        BYTEA   NOT NULL,      -- compressed Yrs document snapshot
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Key Differences from Old Schema

| Old Table | Change | Reason |
|---|---|---|
| `drift_oplog` — JSON payload | Now `yrs_update BLOB` + `encrypted_blob BLOB` | CRDT binary diffs replace opaque JSON; E2EE adds ciphertext storage |
| `drift_oplog` — `conflict_info` | **Removed** | CRDTs have no conflicts |
| `drift_oplog` — `previous_payload` | **Removed** | CRDT document snapshots handle rollback |
| `drift_oplog` — `sync_status conflict` | **Removed** | CRDT mutations never conflict |
| `drift_sync_state` — `replica_id` PK (old section 11) vs `namespace` PK (old section 16) | **Fixed**: `namespace` PK consistently | Resolved schema contradiction |
| `drift_conflicts` | **Removed** | No conflict log needed |
| `drift_keys` | **New** | E2EE key storage table |
| `drift_documents` — `doc_bytes BLOB` | **New** | CRDT document snapshot storage |

---

## 19. Performance Model

### Honest Performance Targets

Drift does not claim "0ms latency." All latency numbers are measured with Criterion.rs benchmarks in CI, and performance regressions >10% fail the build. Targets are for the **engine overhead** — network latency is excluded and depends on the user's infrastructure.

### Local Operations (Engine Overhead Only)

| Operation | Rust Native (SQLite WAL) | Browser WASM (OPFS) | Measurement Method |
|---|---|---|---|
| **Local Read** (single record, from CRDT materialized state) | **1–5 µs** | **0.5–2 ms** | Criterion warm-up 10K iterations |
| **Local Write** (single CRDT mutation, WAL commit) | **5–20 µs** | **1–5 ms** | Criterion warm-up 10K iterations |
| **CRDT Merge** (single Yrs update into document) | **1–5 µs** | **10–50 µs** | Yrs built-in benchmark harness |
| **E2EE Encrypt** (X25519 + ChaCha20-Poly1305) | **5–15 µs** | **50–200 µs** | libsodium / WebCrypto timer |
| **E2EE Decrypt** (X25519 + ChaCha20-Poly1305) | **5–15 µs** | **50–200 µs** | libsodium / WebCrypto timer |
| **Snapshot Load** (1MB Yrs doc from mmap) | **< 1 ms** | **10–50 ms** | File read + Yrs deserialize |
| **Snapshot Save** (1MB Yrs doc to mmap) | **< 1 ms** | **10–50 ms** | Yrs serialize + file write |
| **Tab→Tab via Shared Memory** (single change notification) | **2–10 µs** | **1–5 ms** | Shared memory / BroadcastChannel roundtrip |
| **Full Local Operation Path** (write → CRDT merge → encrypt → oplog → subscription fire) | **10–40 µs** | **2–10 ms** | End-to-end Criterion trace |
| **Opens 1000 concurrent subscriptions** | **< 100 ms** | **< 500 ms** | Initialization time |
| **Memory per CRDT document** (100 fields, materialized) | **~2 KB** | **~2 KB + WASM overhead** | Yrs document heap size |

### Why WASM Is Slower (And Why That's Okay)

The browser WASM path adds unavoidable overhead:
- **JS → WASM bridge**: calling from JS into WASM and back adds ~0.1–0.5ms per boundary crossing
- **Web Worker postMessage**: OPFS SQLite runs in a Worker; every operation crosses the Worker boundary
- **WebCrypto vs libsodium**: WebCrypto is a browser API with IPC overhead; libsodium native calls are faster

These overheads are **typically invisible to users** because:
- UI operations are measured in 16ms frames (60fps)
- Local writes under 10ms are not perceptible
- The bottleneck is always network latency for cross-device sync

### Sync Operations (Engine + Network)

| Operation | Engine Overhead | Estimated Total (good connection) | Estimated Total (slow connection) |
|---|---|---|---|
| Write → visible on same device | 10–40 µs (native) / 2–10 ms (WASM) | **< 10 ms** | **< 50 ms** |
| Write → coordinator ACK | 20–100 µs | **50–200 ms** | **500–2000 ms** |
| Write → visible on other device | 50–200 µs (both sides) | **100–500 ms** | **1000–5000 ms** |
| Full sync catch-up (1000 pending ops) | 1–5 ms merge total | **1–5 seconds** | **10–60 seconds** |
| Snapshot bootstrap (1MB, new replica) | 1–10 ms | **500–2000 ms** | **5000–30000 ms** |

### Benchmark CI Gates

Every PR runs in CI:

```
Benchmark                                Current  vs Main  Pass/Fail
─────────────────────────────────────────────────────────────────────
local_read_1row                         1.2 µs   +3%     ✅
local_write_1row                        8.7 µs   -2%     ✅
crdt_merge_1op                          2.1 µs   +5%     ✅
e2ee_encrypt_1op                        7.3 µs   +1%     ✅
e2ee_decrypt_1op                        7.1 µs   +0%     ✅
snapshot_load_1mb                       0.4 ms   +8%     ✅
tab_to_tab_latency                      3.2 µs   +4%     ✅
full_operation_path                     22.1 µs  +5%     ✅
```

---

## 20. Observability & Debuggability

### Full Observability — No Black Box

Every Drift component emits OpenTelemetry traces, metrics, and structured logs. All Drift code is MIT-licensed — there are no proprietary components. Any failure can be traced to its exact cause.

### OpenTelemetry Traces

Every operation produces a trace:

```
Trace: "drift.write" (trace_id: abc123)
├── Span: "crdt.merge"          → 2.1 µs  — Yrs merge mutation into document
├── Span: "c2e.encrypt"         → 7.3 µs  — E2EE encrypt Yrs update
├── Span: "oplog.append"        → 1.5 µs  — Write to local oplog
├── Span: "subscription.fire"   → 0.8 µs  — Fire local subscription
├── Span: "transport.send"      → 45 ms   — Upload to coordinator
│   ├── Span: "coord.validate"  → 2 ms    — Coordinator validates mutation
│   ├── Span: "coord.store"     → 5 ms    — Coordinator persists to Postgres
│   └── Span: "coord.distribute"→ 20 ms   — Push to other replicas
└── Span: "crdt.merge_remote"   → 2.5 µs  — Remote replica merges Yrs update
```

Traces are exportable to Jaeger, Datadog, Grafana Tempo, or any OTLP-compatible backend.

### Metrics (Prometheus-Compatible)

| Metric | Type | Description |
|---|---|---|
| `drift_mutations_total` | Counter (labels: status, namespace) | Total mutations processed |
| `drift_mutations_pending` | Gauge (labels: namespace) | Current pending upload queue depth |
| `drift_mutations_failed` | Counter (labels: namespace, reason) | Mutations in failed state |
| `drift_sync_lag_ms` | Histogram (labels: namespace) | Time from local write to coordinator ACK |
| `drift_download_lag_ms` | Histogram (labels: namespace) | Time from coordinator sequence to replica apply |
| `drift_encryption_time_us` | Histogram (labels: operation) | E2EE encrypt/decrypt duration |
| `drift_crdt_merge_time_us` | Histogram (labels: doc_id) | CRDT merge duration |
| `drift_replica_count` | Gauge (labels: namespace) | Active replicas per namespace |
| `drift_connection_status` | Gauge (labels: namespace) | 1=connected, 0=disconnected |
| `drift_leader_status` | Gauge | 1=leader, 0=reader |
| `drift_oplog_size` | Gauge (labels: namespace) | Total oplog entries per namespace |
| `drift_doc_count` | Gauge (labels: doc_id) | Documents per CRDT document collection |

### Structured Logging

All logs are structured JSON via the `tracing` crate:

```json
{
  "timestamp": "2025-01-01T10:00:01.234Z",
  "level": "warn",
  "target": "drift::sync::upload",
  "message": "Upload batch partially failed",
  "fields": {
    "namespace": "workspace:core",
    "batch_size": 10,
    "failed": 2,
    "failed_ids": ["mut_abc", "mut_def"],
    "coordinator_error": "timeout",
    "retry_in": "4s"
  },
  "span": {
    "trace_id": "abc123",
    "span_id": "def456"
  }
}
```

### Debug HTTP API

Every Drift process exposes a debug HTTP endpoint on localhost:9876 (disabled in production unless configured):

```
GET /debug/drift/state           → Full internal state as JSON
GET /debug/drift/state/oplog     → Last 1000 oplog entries
GET /debug/drift/state/documents → All CRDT document snapshots (sizes, doc_ids)
GET /debug/drift/state/keys      → Key metadata (not key material)
GET /debug/drift/state/replicas  → Connected replica info
GET /debug/drift/metrics         → Prometheus metrics
GET /debug/drift/traces          → Recent trace spans
GET /debug/drift/leader          → Current leader status
POST /debug/drift/force-sync     → Trigger immediate sync
POST /debug/drift/force-election → Trigger leader re-election
```

### CLI: `drift inspect`

```bash
# Attach to a running Drift process and watch operations in real-time
$ drift inspect --pid 1234

# Watch live operation stream
$ drift inspect --pid 1234 --stream

# Dump current state
$ drift inspect --pid 1234 --state

# Replay a trace file
$ drift replay trace_abc123.json
```

### Trace Replay Tool

```bash
# Export a trace from a running process
$ drift inspect --pid 1234 --export-trace > trace.json

# Replay the trace in a deterministic test environment
$ drift replay trace.json
# Verifies: all replicas converge, no data loss, all ACKs match
```

---

## 21. SDK Reference

### Installation

```bash
# Browser / React
npm install @drift/web @drift/react

# Node.js
npm install @drift/node

# React Native
npm install @drift/native
```

### Initialization

```typescript
import { Drift } from "@drift/web"
import { DriftProvider } from "@drift/react"

const drift = new Drift({
  storage:    "opfs",           // "sqlite" | "opfs" | "indexeddb"
  namespace:  "workspace:core",
  replicaId:  getDeviceId(),    // stable device identifier
  coordinator: new PostgresCoordinator({
    connectionString: "postgres://user:pass@host/db"
  }),
  sync: {
    reconnect:    { attempts: Infinity, maxDelay: "30s" },
    uploadRetry:  { attempts: 10, backoff: "exponential" }
  }
})

// Define schema with CRDT types
drift.schema.define("todos", {
  fields: {
    id:        { type: "string",  crdtType: "lww",    primaryKey: true },
    text:      { type: "string",  crdtType: "lww"                      },
    completed: { type: "boolean", crdtType: "lww"                      },
    tags:      { type: "array",   crdtType: "orset"                    },
    priority:  { type: "number",  crdtType: "counter"                  }
  }
})

// Register migrations
drift.migration("v1", async (db) => { ... })
drift.migration("v2", async (db) => { ... })

// Initialize (runs migrations, connects to coordinator, E2EE key setup)
await drift.initialize()
```

### CRUD Operations

```typescript
const db = drift.db

// INSERT — creates a new CRDT document
const todo = await db.todos.insert({
  id:        "todo:123",
  text:      "build drift",
  completed: false,
  tags:      ["backend", "rust"],
  priority:  3
})

// READ — from local CRDT materialized state (instant, no network)
const todos      = await db.todos.findAll()
const todo       = await db.todos.findById("todo:123")
const filtered   = await db.todos.findAll({
  where:   { completed: false, tags: { contains: "backend" } },
  orderBy: { field: "priority", direction: "desc" },
  limit:   50
})

// UPDATE — produces a CRDT mutation (Yrs diff)
await db.todos.update("todo:123", {
  text: "build drift runtime"
})

// DELETE — soft delete (tombstone) if configured
await db.todos.delete("todo:123")

// BATCH — multiple CRDT mutations in one transaction
await db.batch([
  db.todos.insert({ id: "todo:456", text: "write docs" }),
  db.todos.update("todo:123", { completed: true })
])
```

### Subscriptions

```typescript
// Table subscription (fires on any CRDT merge in the collection)
const unsub = db.todos.subscribe(todos => renderList(todos))

// Filtered subscription
const unsub = db.todos.subscribe(
  todos => renderList(todos),
  { where: { assignee: "alice" } }
)

// Single record
const unsub = db.todos.subscribeOne("todo:123", todo => render(todo))

// Sync status
drift.subscribe("sync:status", status => updateUI(status))

// Encrypted mutations inspection (debug only)
drift.subscribe("sync:mutations", mutations => {
  // mutations that were just synced
})
```

### E2EE Key Management

```typescript
// Keys are generated automatically on initialize()
// Manual management for advanced use cases:

// Rotate namespace key
await drift.keys.rotate()

// Export public key (for sharing with other replicas)
const pubKey = await drift.keys.publicKey()

// Check key status
const keyInfo = await drift.keys.status()
// { version: 2, createdAt: ..., algorithm: "X25519+ChaCha20-Poly1305" }
```

### Multi-Tab

```typescript
// Drift handles multi-tab automatically
// Configuration options:

const drift = new Drift({
  // ...
  multiTab: {
    enabled: true,
    electionTimeout: 3000,  // ms
    heartbeatInterval: 1000, // ms
    sharedMemorySize: 64 * 1024 * 1024 // 64MB shared memory region
  }
})

// Query current leader status
const status = await drift.leaderStatus()
// { amILeader: true, leaderId: "tab-A", uptime: 120000 }

// Force leader re-election (debug only)
await drift.forceElection()
```

### Coordinator Switching

```typescript
// Switch coordinator at runtime (graceful)
await drift.switchCoordinator(new RedisCoordinator({
  url: "redis://localhost:6379"
}))
// Pending uploads drain to old coordinator, then switch
```

### Oplog & Sync Inspection

```typescript
// Pending mutations count
const pending = await drift.pendingUploads()

// Full mutation history for a record
const history = await drift.mutationLog("todos", "todo:123")

// Force sync (useful for debugging)
await drift.forceSync()

// Sync status
const status = await drift.syncStatus()
// { connected, pendingUploads, lastSyncAt, replicaId, schemaVersion, leaderStatus }
```

### Shutdown

```typescript
// Graceful shutdown — flush pending uploads, close WebSocket, release leader lock
await drift.shutdown()
```

---

## 22. Deployment Modes

### Mode 1: Embedded Local-Only (No Sync)

Local CRDT document store only. No coordinator. No sync. Useful for offline-first apps that don't need multi-device sync.

```typescript
const drift = new Drift({
  storage:   "sqlite",
  namespace: "local",
  sync:      false    // ← no coordinator connection
})
```

### Mode 2: Embedded + Managed Coordinator (Postgres, Redis, etc.)

Application embeds Drift; coordinator is a managed database. Simplest path to multi-device sync.

```typescript
const drift = new Drift({
  storage:    "opfs",
  namespace:  `user:${userId}`,
  coordinator: new PostgresCoordinator({
    connectionString: process.env.DATABASE_URL
  })
})
```

### Mode 3: Embedded + Self-Hosted Coordinator

Application embeds Drift; coordinator is a self-hosted Drift coordination worker.

```typescript
const drift = new Drift({
  storage:   "sqlite",
  namespace: "workspace:core",
  coordinator: new CustomCoordinator({
    url: "wss://your-coordinator.example.com/drift"
  })
})
```

### Mode 4: Multi-Instance Server Sync

Multiple server instances sharing a coordinator. Used when server-side data must sync between instances.

```
App Server A (Drift embedded, namespace: workspace:core)
App Server B (Drift embedded, namespace: workspace:core)
App Server C (Drift embedded, namespace: workspace:core)
                        │
                Shared Coordinator (Postgres)
```

### Mode 5: Hybrid Client + Server + P2P

Both clients (browsers/mobile) and server instances participate in the same namespace. Optional P2P sync for low-latency collaboration between clients on the same LAN.

```typescript
const drift = new Drift({
  storage:   "sqlite",
  namespace: "workspace:core",
  coordinator: new PostgresCoordinator({...}),
  transports: {
    coordinator: true,      // sync via coordinator
    p2p: { enabled: true }, // also sync via WebRTC when peers are on same LAN
    mesh: { enabled: false }
  }
})
```

---

## 23. Security Model

### Transport Security

All sync traffic uses TLS (WSS for WebSocket). CRDT mutations are encrypted with E2EE before transport — even if TLS is compromised, the coordinator cannot read application data.

### Replica Authentication

```typescript
const drift = new Drift({
  storage:   "opfs",
  namespace: "workspace:core",
  auth: {
    getToken: async () => {
      // Return current session JWT from auth provider (Clerk/Auth0)
      return await clerk.session?.getToken()
    }
  }
})
```

The coordinator validates the JWT on every WebSocket connection and operation. Expired tokens cause the connection to close; the client re-authenticates and reconnects.

### Namespace Isolation

Each namespace is a hard sync boundary. A replica authenticated for `namespace:workspace:alpha` cannot receive or submit mutations for `namespace:workspace:beta`. Isolation is enforced at the coordinator level.

### End-to-End Encryption

Already covered in Section 11. Summary:

- Every CRDT mutation is encrypted with X25519 + ChaCha20-Poly1305
- The coordinator stores only encrypted blobs and routing metadata
- Private keys never leave the device
- Key rotation is supported
- Replay attacks are prevented by mutation ID deduplication

### Local Storage Encryption

The `drift_keys` table stores private keys encrypted with the device's local key (OS keychain on native, WebCrypto `subtle.wrapKey` on browser). The `drift_oplog.yrs_update` column stores plaintext CRDT diffs locally (for fast merge) but can be configured for local-only encryption as well.

### Operation Validation

Every incoming mutation is validated by the coordinator:

- Schema conformance (CRDT document structure)
- Record ID format validation
- Encrypted blob size limits
- Authorization via Aegis (optional plugin)
- Sequence ordering sanity checks

Mutations failing validation are rejected with a specific error code. The client logs the rejection and surfaces it as a sync error.

### Replay Attack Prevention

Each mutation includes a client-generated unique ID (`mut_abc123`). The coordinator deduplicates by mutation ID. A replayed mutation is silently dropped — idempotent by design. The E2EE layer also prevents meaningful replay since the coordinator cannot produce new valid ciphertexts without the private key.

---

## 24. Integrations (Optional Plugins)

### Aegis (Authorization)

Every sync operation can be authorized by Aegis before the coordinator applies it. This is an **optional plugin** — not required for Drift to function.

```typescript
// Namespace authorization
await aegis.write({
  subject:  `replica:${deviceId}`,
  relation: "sync",
  object:   "namespace:workspace:core"
})

// Table-level sync permissions
await aegis.write({
  subject:  `user:${userId}`,
  relation: "sync",
  object:   "table:todos"
})

// The coordinator calls aegis.checkSync() before distributing mutations
```

### Arc (Background Jobs)

Arc workflows handle complex sync-related background tasks — operations that are too involved for Drift's built-in retry logic.

```typescript
arc.workflow("drift-conflict-notification", async (ctx) => {
  // CRDTs mean no conflicts, but you may want to notify on certain merge events
  const { docId, recordId, changedFields } = ctx.payload
  await ctx.step("notify", () => notifyUsers(docId, recordId, changedFields))
})
```

### FluxBus (Event Bus)

CRDT mutations can optionally emit FluxBus events for system-wide reactivity.

```typescript
drift.schema.define("todos", {
  fields: { ... },
  emit: {
    onMutation: "todo.changed"
  }
})
```

---

## 25. Development Roadmap

### Alpha — Core Foundation

Deliver a single-tab, single-coordinator (Postgres) system with all foundational features:

- [ ] Rust core engine with Yrs CRDT integration
- [ ] SQLite + OPFS + IndexedDB storage backends
- [ ] CRDT document store (materialized state + oplog)
- [ ] E2EE layer (X25519 + ChaCha20-Poly1305)
- [ ] Single-tab leader mode (no multi-tab yet)
- [ ] Coordinator trait with Postgres implementation
- [ ] Upload queue with retry and batching
- [ ] Download queue with replay and CRDT merge
- [ ] Schema definition system with CRDT type mapping
- [ ] Versioned migration system
- [ ] Reactive table/record subscriptions
- [ ] Offline buffering and reconnect sync
- [ ] OpenTelemetry tracing + metrics + logging
- [ ] Debug HTTP API
- [ ] TypeScript (browser + Node) SDK
- [ ] React hooks (`@drift/react`)
- [ ] WebSocket transport with heartbeat and reconnect
- [ ] Full test suite: unit, integration, E2E, benchmark, CRDT property tests

### Beta — Multi-Process & Scale

- [ ] Multi-tab leader election (file lock / BroadcastChannel)
- [ ] Shared memory ring buffer for cross-tab state
- [ ] Leader heartbeat and crash recovery
- [ ] P2P transport (WebRTC) for browser-to-browser sync
- [ ] Redis coordinator implementation
- [ ] SQLite coordinator implementation (embedded, dev)
- [ ] Cloudflare DO coordinator implementation
- [ ] Supabase coordinator implementation
- [ ] Advanced operation batching and compression
- [ ] Partial sync (sync subset of records per replica)
- [ ] CRDT document snapshot and compaction
- [ ] Go SDK
- [ ] Swift SDK (iOS/macOS)
- [ ] React Native SDK
- [ ] `drift inspect` CLI tool
- [ ] Chaos testing suite

### Stable — Production Hardening

- [ ] Mesh transport (libp2p) for server-to-server sync
- [ ] Coordinator conformance testing (all implementations pass the same suite)
- [ ] Performance optimization pass (target: 2x native, 1.5x WASM from Alpha)
- [ ] E2EE key rotation
- [ ] Coordinator failover and redundancy
- [ ] Snapshot-based replica bootstrapping
- [ ] CRDT tombstone GC
- [ ] Custom CRDT plugin support (developer-defined Yrs extensions)
- [ ] Kotlin SDK (Android)
- [ ] Drift Cloud dashboard (optional managed UI)
- [ ] Security audit (third-party)
- [ ] 100% test coverage on critical paths
- [ ] Fuzz testing (24h continuous)

### Post-Stable — Advanced

- [ ] Advanced CRDTs (text with rich formatting, undo/redo)
- [ ] Advanced observability (conflict dash, sync analytics)
- [ ] Automated coordinator failover
- [ ] Performance benchmarks published and tracked

---

## 26. Real-World Use Cases

### Collaborative Todo / Project Management

```
User A (laptop) adds todo:123
         │ (instant local CRDT update, offline or online)
Drift encrypts and uploads mut: INSERT todos/todo:123
         │
Coordinator sequences and distributes encrypted mutation
         │
User B's app (phone) receives encrypted mutation, decrypts, CRDT merges
         │ (real-time, < 200ms on fast connection)
User B sees todo:123 appear instantly

Both users edit todo:123 simultaneously:
User A: UPDATE { text: "updated title" }   → CRDT mutation
User B: UPDATE { completed: true }         → CRDT mutation

Result: { text: "updated title", completed: true }
Both changes preserved — CRDT merge, not last-write-wins
```

### Offline-First Mobile App with E2EE

```
User opens app on airplane (no connectivity)
  → All reads served from local CRDT document store instantly
  → All writes produce CRDT mutations → encrypted → queued in oplog
  → Even if device is lost, oplog is encrypted at rest

User makes 47 changes during the flight
  → 47 encrypted CRDT mutations in oplog, status: pending

Plane lands, connectivity returns
  → Drift reconnects to coordinator
  → Uploads 47 encrypted mutations (batched)
  → Downloads 12 encrypted mutations from other devices
  → Decrypts and CRDT merges locally
  → App is fully synchronized
  → Coordinator never saw plaintext data
```

### Multi-Tab Collaborative Editor

```
User opens the same app in 3 browser tabs:
  Tab A (Leader): writes document changes
  Tab B (Reader): reads via shared memory, sees changes < 5ms
  Tab C (Reader): same, reads via shared memory

User closes Tab A (leader):
  Tab B detects heartbeat timeout → acquires lock → becomes leader
  Tab B replays any in-flight mutations from shared memory
  Tab C detects new leader → updates reference
  No data lost. No interruption.

User reopens the app later:
  → Single tab becomes leader
  → Reads from persistent SQLite/OPFS storage
  → Syncs with coordinator for any missed mutations
```

### Regulated Healthcare Application

```
Patient data synced across clinic devices:
  → E2EE ensures coordinator (even if breached) cannot read patient records
  → HIPAA compliance: no PHI in coordinator logs or storage
  → Audit trail via OpenTelemetry traces (every CRDT mutation recorded)
  → On-prem coordinator option for clinics that cannot use cloud
  → Self-hosted Postgres coordinator: all data stays in clinic's network
```

### P2P LAN Collaboration

```
Two developers on the same office Wi-Fi:
  → Both connected to coordinator (Postgres in cloud)
  → P2P transport also enabled (WebRTC)
  → For low-latency edits (CRDT document updates), P2P path is used
  → Coordinator is fallback for persistence and third-party access
  → When one developer leaves the office, P2P disconnects
  → Coordinator takes over transparently
```

---

## Appendix: CRDT Type Decision Guide

| Data Type | CRDT Type | Merge Behavior |
|---|---|---|
| Simple scalar (string, boolean, number) | LWW-Register | Last-write-wins by hybrid logical clock |
| Counters (likes, views, inventory) | PN-Counter | Increment/decrement sums across replicas |
| Tags / labels | OR-Set | Add/remove with GC-able tombstones |
| Ordered list (to-do items, rankings) | Yrs Array | Position-based insert/delete/move |
| Rich text | Yrs Text | Sequence CRDT, OT-compatible |
| Financial records | LWW-Register | Last-write-wins (correctness-critical, no auto-merge) |
| AI memory / context | LWW-Register | Last-write-wins with timestamp |

## Appendix: Coordinator Decision Guide

| Scenario | Recommended Coordinator |
|---|---|
| Small team, self-hosted, simple | SQLite |
| Production, self-hosted | PostgreSQL |
| High-throughput, in-memory | Redis |
| Edge-deployed, global | Cloudflare Durable Objects |
| Hosted, managed Postgres | Supabase |
| Testing / development | In-Memory |
| Regulated industry (on-prem) | PostgreSQL (self-hosted) |
| Custom infrastructure | Custom (implement the trait) |

## Appendix: Storage Backend Decision Guide

| Scenario | Recommended Backend |
|---|---|
| Browser app (modern) | OPFS (SQLite via WASM) |
| Browser app (wide compatibility) | IndexedDB |
| Node.js / server | SQLite (WAL mode, persistent volume) |
| Electron / desktop | SQLite (app data directory, WAL mode) |
| React Native / iOS / Android | SQLite (native) |
| High-throughput local writes | RocksDB |
| Testing | In-Memory |

## Appendix: Production Checklist

- [ ] E2EE keys generated and backed up (export public keys for recovery)
- [ ] Coordinator chosen and deployed (Postgres, Redis, etc.)
- [ ] Replica authentication via JWT configured
- [ ] Namespace authorization via Aegis or equivalent
- [ ] Soft delete enabled on synced tables (prevents sync gaps)
- [ ] Schema versions tracked and migration history persisted
- [ ] CRDT type chosen per field (LWW, Counter, OR-Set, Text)
- [ ] Oplog compaction policy configured (snapshot interval, tombstone GC)
- [ ] Multi-tab leader election tested (3+ tabs, leader crash scenario)
- [ ] Coordinator backups configured (snapshot + mutation log)
- [ ] OpenTelemetry exporter configured (traces + metrics + logs)
- [ ] Sync lag alerting configured
- [ ] Offline replica detection alerting configured
- [ ] Graceful shutdown integrated for server replicas
- [ ] Chaos tests passed (network partition, crash recovery, clock skew)
- [ ] Benchmark regression gates passing (no >10% perf drop)
- [ ] Security audit completed (key management, encryption, auth)

---

*Drift — Conflict-free, encrypted, infrastructure-agnostic synchronization for the local-first era.*
*MIT License — Open source. Auditable. No black boxes.*
