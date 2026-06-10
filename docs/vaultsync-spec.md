# VaultSync — Embedded Local-First Sync & Replication Runtime
### Complete Technical Specification & Build Reference (v2 — Production Architecture)

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Core Philosophy](#2-core-philosophy)
3. [What VaultSync Is (and Is Not)](#3-what-vaultsync-is-and-is-not)
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
27. [Multi-Namespace & Multi-Tenant Architecture](#27-multi-namespace--multi-tenant-architecture)

---

## 1. Project Summary

**VaultSync** is an embedded, local-first synchronization and replication runtime with CRDT-native conflict resolution, end-to-end encryption, multi-tab/process safety, and infrastructure-agnostic coordination. It enables applications to operate fully offline, synchronize data across devices and replicas, and handle network failures gracefully — without requiring dedicated sync infrastructure.

VaultSync is built on three non-negotiable principles:

1. **CRDT-native from day one** — concurrent writes never conflict. There is no "V1 coordinator-order, V3 field-merge, V4 CRDT upgrade path." CRDTs (Conflict-Free Replicated Data Types) are the foundation of every operation. All replicas converge deterministically regardless of write order.

2. **End-to-end encryption by default** — the coordinator never sees plaintext data. Every operation is encrypted with a namespace key before leaving the device. The coordinator stores only encrypted blobs and routing metadata. Zero-trust infrastructure.

3. **Infrastructure-agnostic** — VaultSync defines a `Coordinator` trait. Users choose or build their own coordinator backend: Postgres, Redis, Cloudflare Durable Objects, Supabase, SQLite, or a custom implementation. No vendor lock-in. No Cloudflare dependency.

VaultSync also solves a problem no other sync engine addresses: **multi-tab and multi-process safety**. Browser tabs, Electron windows, and OS processes all share the same local database via leader election and shared memory — no split-brain, no corruption, no data loss.

Inspired by the local-first software movement and modern sync systems like ElectricSQL, PowerSync, Replicache, and Zero — but built with an embedded-first philosophy:

> *Synchronization should be conflict-free, encrypted, infrastructure-agnostic, and observable — then scale into distributed coordination only when necessary.*

VaultSync is designed for:

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

### The VaultSync Architecture

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
│    │               VaultSync Core Engine                     │  │
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

## 3. What VaultSync Is (and Is Not)

### What VaultSync Manages

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

### What VaultSync Does NOT Manage

- Authentication (use Clerk, Auth0, etc.)
- Authorization (use Aegis or any authz system — optional plugin)
- Background job execution (use Arc or any job system — optional plugin)
- Event pub-sub routing (use FluxBus or any event bus — optional plugin)
- HTTP API routing
- User account management

### VaultSync vs Traditional Sync Approaches

| Approach | Model | Offline? | Conflicts? | E2EE? | Multi-Tab Safe? | Infra Choice? |
|---|---|---|---|---|---|---|
| REST polling | Server-first | ❌ | Server wins | ❌ | ❌ | ❌ |
| WebSocket push | Server-first | ❌ | Server wins | ❌ | ❌ | ❌ |
| Optimistic UI | Server-first | Partial | LWW | ❌ | ❌ | ❌ |
| **VaultSync** | **Local-first** | **✅** | **CRDT (none)** | **✅** | **✅** | **✅** |
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
| **TypeScript (Browser)** | `@vaultsync/web` | Browser-native; OPFS/IndexedDB |
| **TypeScript (Node.js)** | `@vaultsync/node` | Server-side sync; SQLite backend |
| **React** | `@vaultsync/react` | React hooks for reactive queries |
| **Swift** | `vaultsync-swift` | iOS/macOS native |
| **Kotlin** | `vaultsync-android` | Android native |
| **Rust** | `vaultsync` (crate) | Core library |
| **Go** | `vaultsync-go` | Server-side replication |

---

## 5. System Architecture

### Layer Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Application Process                           │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Application Layer                          │   │
│  │   vaultsync.db.todos.insert({ text: "..." })                     │   │
│  │   vaultsync.db.todos.subscribe(callback)                          │   │
│  └──────────────────────────┬───────────────────────────────────┘   │
│                             │                                        │
│  ┌─────────────────────────▼────────────────────────────────────┐   │
│  │                    VaultSync Core Engine (Rust)                   │   │
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
│  │  │  - Debug API: /debug/vaultsync/* on localhost                │ │   │
│  │  └──────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                                                                        │
               ┌──────────┴──────────┐  ┌──────────┴──────────┐
               │   Coordinator (your  │  │  P2P / Mesh (opt.)  │
               │   choice)            │  │  Other VaultSync peers  │
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

A CRDT document is the fundamental unit of state in VaultSync. Every table is a Yrs CRDT document. Documents are:

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

VaultSync uses leader election to coordinate write access across multiple tabs/processes sharing the same local storage.

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
  "text":       LWW-Register("build vaultsync"),
  "completed":  LWW-Register(false),
  "priority":   PN-Counter(3),
  "tags":       OR-Set(["urgent", "backend"]),
  "assignee":   LWW-Register("alice")
}
```

### How CRDT Merge Works

When two replicas modify the same document concurrently:

```
Replica A:  doc.set("text", "build vaultsync runtime")
Replica B:  doc.set("completed", true)

Both Yrs updates are captured as binary diffs.

Replica A applies B's diff → { text: "build vaultsync runtime", completed: true }
Replica B applies A's diff → { text: "build vaultsync runtime", completed: true }

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

**VaultSync's hybrid approach:**

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
CREATE TABLE vaultsync_documents (
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

### 8.4 IndexedDB Backend Schema

IndexedDB is the browser fallback storage backend when OPFS is unavailable (older browsers, specific privacy modes). Unlike OPFS which uses a single SQLite file, IndexedDB uses a key-value object store model.

#### Object Stores

```
vaultsync_documents
  keyPath: ["doc_id", "record_id"]
  indexes:
    - name: "by_doc_id",    keyPath: "doc_id"
    - name: "by_updated_at", keyPath: "updated_at"

vaultsync_oplog
  keyPath: "id"
  indexes:
    - name: "by_namespace_status", keyPath: ["namespace", "sync_status"]
    - name: "by_namespace_seq",    keyPath: ["namespace", "sequence"]
    - name: "by_doc_record",       keyPath: ["doc_id", "record_id"]

vaultsync_sync_state
  keyPath: "namespace"

vaultsync_schema_meta
  keyPath: "doc_id"

vaultsync_migrations
  keyPath: "version"

vaultsync_keys
  keyPath: ["namespace", "version"]

vaultsync_replica_meta
  keyPath: "id"
```

#### Transaction Strategy

IndexedDB transactions are scoped and short-lived by browser design. VaultSync uses the following rules:

1. **CRDT write + oplog append** — one `readwrite` transaction on `["vaultsync_documents", "vaultsync_oplog"]`. Both stores are opened in the same transaction to maintain atomicity (equivalent to SQLite's `BEGIN ... COMMIT`).
2. **Reads** — use `readonly` transactions; open the minimum required stores.
3. **Sync state update** — separate `readwrite` transaction on `["vaultsync_sync_state"]` after upload/download completes.

#### Version Migration

IndexedDB uses integer database version numbers. Every schema change increments the version, and `onupgradeneeded` applies changes:

```typescript
const DB_VERSION = 1;

function onupgradeneeded(event: IDBVersionChangeEvent) {
  const db = (event.target as IDBOpenDBRequest).result;
  const oldVersion = event.oldVersion;

  if (oldVersion < 1) {
    // Initial schema
    const docs = db.createObjectStore("vaultsync_documents", { keyPath: ["doc_id", "record_id"] });
    docs.createIndex("by_doc_id", "doc_id");
    docs.createIndex("by_updated_at", "updated_at");

    const oplog = db.createObjectStore("vaultsync_oplog", { keyPath: "id" });
    oplog.createIndex("by_namespace_status", ["namespace", "sync_status"]);
    oplog.createIndex("by_namespace_seq", ["namespace", "sequence"]);
    oplog.createIndex("by_doc_record", ["doc_id", "record_id"]);

    db.createObjectStore("vaultsync_sync_state", { keyPath: "namespace" });
    db.createObjectStore("vaultsync_schema_meta", { keyPath: "doc_id" });
    db.createObjectStore("vaultsync_migrations", { keyPath: "version" });
    db.createObjectStore("vaultsync_keys", { keyPath: ["namespace", "version"] });
    db.createObjectStore("vaultsync_replica_meta", { keyPath: "id" });
  }

  // Future versions add migrations here:
  // if (oldVersion < 2) { /* add new object store or index */ }
}
```

VaultSync schema migrations (user-defined field additions) do **not** increment the IndexedDB version. They are recorded in the `vaultsync_migrations` object store, exactly as in the SQLite backend. IndexedDB version upgrades are reserved for internal VaultSync storage schema changes only.

#### Performance Notes

| Operation | OPFS (SQLite) | IndexedDB |
|---|---|---|
| Single record read | 0.5–2 ms | 2–10 ms |
| Single record write | 1–5 ms | 5–20 ms |
| Batch read (100 records) | 1–5 ms | 10–50 ms |
| Oplog append | 1–5 ms | 5–20 ms |

IndexedDB is slower due to browser IPC overhead per transaction. Use OPFS (SQLite via WASM) when available for production performance.

### 8.5 Tombstone GC Safety with Offline Replicas

CRDT tombstone garbage collection (GC) has a well-known safety hazard that must be explicitly managed.

#### The Problem

OR-Set and Array CRDTs use tombstones to mark deletions. When Replica A deletes an element, it writes a tombstone. When Replica B, which added the element, receives the tombstone, the CRDT merges: element is deleted.

If the tombstone is garbage-collected (removed to save space) before Replica C (offline since before the delete) reconnects:

```
Replica A:  OR-Set = { "tag:urgent" }
Replica B:  OR-Set = { "tag:urgent" }

Replica A deletes "tag:urgent" → tombstone written
Both replicas GC the tombstone after 30 days

Replica C (offline for 35 days) reconnects:
  Replica C's state: { "tag:urgent" } — element still present, no tombstone
  CRDT merge: no tombstone to apply → element RESURRECTED ❌
```

This is not a bug in the CRDT algorithm — it is a known, expected behavior. The safety requirement is:

> **The tombstone retention period must be strictly greater than the maximum expected offline duration of any replica in the namespace.**

#### VaultSync's GC Safety Rules

1. **Default tombstone retention: 90 days.** Any replica offline longer than 90 days is considered stale and must perform a full re-bootstrap (receive a fresh coordinator snapshot) rather than incremental merge.

2. **Configurable per namespace:**

```typescript
const vaultsync = new VaultSync({
  namespace: "workspace:core",
  crdt: {
    tombstoneRetentionDays: 180,  // override default 90 days
    maxOfflineDays: 180           // must match tombstone retention
  }
})
```

3. **Coordinator-enforced cutoff:** The coordinator tracks each replica's `last_heartbeat`. If a replica has been offline for longer than `maxOfflineDays`, the coordinator rejects its reconnection with `ReplicaTooStaleError`. The replica must re-bootstrap from snapshot.

```
Replica reconnects after 100 days (default maxOfflineDays = 90)
         │
         ▼
Coordinator: replica.last_heartbeat > maxOfflineDays?
         │
         YES
         ▼
Coordinator returns: { error: "REPLICA_TOO_STALE", bootstrapFrom: latestSnapshotSequence }
         │
         ▼
Replica discards local CRDT state
         │
         ▼
Replica bootstraps from latest coordinator snapshot (Section 17.3)
         │
         ▼
All local pending mutations are re-uploaded
```

4. **GC never runs on unsynced tombstones:** A tombstone is only eligible for GC if:
   - The operation that created it has `sync_status = synced`
   - The tombstone is older than `tombstoneRetentionDays`
   - The coordinator confirms the tombstone has been distributed to all currently-active replicas

5. **User-facing warning:** When a replica reconnects after a long offline period approaching `maxOfflineDays`, VaultSync emits a warning via the observability system:

```json
{
  "level": "warn",
  "message": "Replica approaching stale threshold",
  "fields": {
    "offline_days": 85,
    "max_offline_days": 90,
    "action": "sync_immediately_to_avoid_bootstrap"
  }
}
```

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
    t.insert(&mut doc_map, "text", "build vaultsync");
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
CREATE TABLE vaultsync_oplog (
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

CREATE INDEX idx_oplog_sync_status ON vaultsync_oplog(sync_status, created_at);
CREATE INDEX idx_oplog_namespace   ON vaultsync_oplog(namespace, sequence);
CREATE INDEX idx_oplog_record      ON vaultsync_oplog(doc_id, record_id);
```

### Key Differences from the Old Spec

| Old Spec | New Spec | Why |
|---|---|---|
| 6 operation types (INSERT, UPDATE, DELETE, UPSERT, BATCH, TRUNCATE) | 4 types (CRDT_UPDATE, CRDT_INSERT, CRDT_DELETE, CRDT_BATCH) | CRDT operations are homogeneous; the diff IS the operation |
| Opaque JSON payload | Yrs binary diff | CRDT diff is smaller, self-contained, and merge-safe |
| `previous_payload` field for undo | Not stored (CRDT document snapshot handles undo) | Yrs documents can be rolled back by reloading a prior snapshot |
| Separate conflict_info column | Not needed | CRDTs have no conflicts to record |
| `pending_sync → synced → failed → conflict` statuses | `pending → synced → failed` | CRDTs never enter a "conflict" state |

### 9.5 Batch Operation Encoding

`db.batch([...])` allows multiple CRDT mutations to be submitted as a single logical unit. Understanding how batches are stored is important for sync correctness.

#### Encoding: One `CRDT_BATCH` Oplog Entry

A batch is stored as a **single oplog entry** with `mutation_type = CRDT_BATCH`. The `yrs_update` field contains a concatenation of all individual Yrs binary diffs, prefixed with a 4-byte count and per-diff length framing:

```
CRDT_BATCH binary layout (yrs_update field):
┌──────────────┬──────────────────────────────────────────────────────────┐
│ count: u32   │ N × (doc_id_len: u16, doc_id: bytes, record_id_len: u16, │
│ (4 bytes)    │       record_id: bytes, update_len: u32, yrs_update: bytes)│
└──────────────┴──────────────────────────────────────────────────────────┘
```

The `encrypted_blob` field contains a single AEAD ciphertext over the entire batch payload — one encrypt call, not N.

#### Atomicity

A batch is **committed atomically**:

```rust
// All mutations in one storage transaction — all succeed or all roll back
txn.begin();
  for op in batch_ops {
    let yrs_update = apply_crdt_mutation(op)?;
    collect_batch_yrs_updates(&mut batch, yrs_update);
  }
  let encrypted = encrypt(batch_payload, namespace_key);
  oplog.insert(OplogEntry {
    mutation_type: CrdtBatch,
    yrs_update: batch_payload,
    encrypted_blob: encrypted,
    ..
  });
txn.commit();
```

If any mutation in the batch fails (e.g., schema validation error), the entire transaction rolls back. No partial batch is written to storage.

#### Ordering Within a Batch

Mutations within a batch are applied in the order they appear in the batch array. On the receiving replica, the batch is unpacked and each Yrs update is merged in order:

```
db.batch([
  db.todos.update("todo:1", { priority: 5 }),   // applied first
  db.todos.update("todo:2", { completed: true }) // applied second
])
```

The ordering is deterministic and preserved through the coordinator (sequence assignment covers the batch as a unit, not individual operations within it).

#### Coordinator Behavior on Batch

The coordinator treats a `CRDT_BATCH` as an atomic unit:
- Either the entire batch is accepted and assigned one sequence number, or it is rejected entirely.
- There is no partial acceptance of individual operations within a batch.
- If the coordinator rejects a batch (schema version mismatch, auth failure), the entire batch enters `failed` state and the retry engine reschedules the whole batch.

#### Subscription Behavior

A batch fires **one subscription callback per affected document** after the entire batch commits — not one callback per mutation within the batch. If the batch touches 3 different documents, 3 subscription callbacks fire (one per document), each receiving the full post-batch state of that document.

---

## 10. Schema System & Migrations

### Schema Definition

Every table synchronized by VaultSync must have an explicit schema. The schema defines field types (CRDT types), indexes, and sync behavior.

```typescript
import { VaultSync } from "@vaultsync/core"

const vaultsync = new VaultSync({
  storage:    "sqlite",
  namespace:  "workspace:core"
})

vaultsync.schema.define("todos", {
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
vaultsync.migration("v1", async (db) => {
  // Initial schema — runs on fresh install
  await db.defineDocument("todos", {
    fields: {
      id:        { type: "string", crdtType: "lww", primaryKey: true },
      text:      { type: "string", crdtType: "lww" },
      completed: { type: "boolean", crdtType: "lww" }
    }
  })
})

vaultsync.migration("v2", async (db) => {
  // Add priority field as PN-Counter
  await db.addField("todos", "priority", { type: "number", crdtType: "counter" })
})

vaultsync.migration("v3", async (db) => {
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
- Migrations are stored in `vaultsync_migrations` table and are themselves synced as Yrs documents

### Schema Metadata Table

```sql
CREATE TABLE vaultsync_schema_meta (
  doc_id          TEXT    PRIMARY KEY,
  schema_version  INTEGER NOT NULL DEFAULT 1,
  schema_json     JSON    NOT NULL,
  applied_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE vaultsync_migrations (
  version     TEXT    PRIMARY KEY,
  applied_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  checksum    TEXT    NOT NULL,   -- SHA256 of migration code; detects tampering
  rollback    TEXT                -- rollback instructions (if applicable)
);
```

If a migration checksum does not match, VaultSync **refuses to apply the migration and logs the discrepancy to the observability system**. The operator must resolve the tampered migration before sync continues.

### 10.5 Schema Version Negotiation Between Replicas

When replicas with different schema versions interact, VaultSync must handle the version gap gracefully without data loss.

#### Backward Compatibility Rules

VaultSync schema changes follow additive-only constraints:
- **Adding a field** is always backward compatible. A v2 replica sends mutations with the new field; a v1 replica applies them and simply ignores fields it doesn't know about (CRDT documents are open maps — unknown fields are stored without breaking anything).
- **Removing a field** requires a migration that marks the field as deprecated. A v2 replica that drops a field still accepts incoming v1 mutations that set that field; it just doesn't surface it in queries.
- **Changing a CRDT type** (e.g., from `lww` to `counter`) is **not** backward compatible and requires a new field name. This is enforced by the schema validator on migration apply.

#### Version Negotiation Flow

```
Replica B (schema v2) receives mutation from Replica A (schema v1)
         │
         ▼
Replica B's schema engine checks:
  mutation.schema_version (1) vs local_schema_version (2)
         │
         ▼
If mutation.schema_version < local:
  → Check if mutation targets fields that exist in v1 schema
  → If yes: apply normally (additive fields in v2 are simply absent in mutation)
  → If no: reject with SchemaMismatch error, log to observability
         │
         ▼
If mutation.schema_version > local (Replica B is behind):
  → Apply the mutation (newer replicas only add fields)
  → Emit warning: "Received mutation from newer schema version; upgrade recommended"
  → Schedule schema pull from coordinator
```

#### Coordinator Enforcement

The coordinator checks schema version on `push()`:

| Replica Version | Coordinator Version | Result |
|---|---|---|
| Equal | Equal | ✅ Accept |
| Replica < Coordinator (minor gap, ≤ 2 versions) | Coordinator newer | ✅ Accept with warning |
| Replica < Coordinator (major gap, > 2 versions) | Coordinator much newer | ❌ Reject — `SCHEMA_TOO_OLD` |
| Replica > Coordinator | Replica is ahead | ✅ Accept (coordinator will be upgraded) |

On `SCHEMA_TOO_OLD`, the coordinator returns:

```json
{
  "error": "SCHEMA_TOO_OLD",
  "replica_version": 1,
  "coordinator_version": 5,
  "migration_instructions": [
    { "from": 1, "to": 2, "description": "Add priority field" },
    { "from": 2, "to": 3, "description": "Add tags OR-Set" }
  ]
}
```

The client surfaces this as a `SchemaMigrationRequired` error. The application must run pending migrations before sync resumes.

#### Local Schema Validation on Write

Before writing a mutation locally, the schema engine validates:
1. All required fields are present (fields without defaults)
2. Field values match declared types (string → string, number → number)
3. CRDT type is respected (cannot decrement a field declared as `counter` via LWW set)

Schema validation errors are surfaced immediately as synchronous errors on `db.todos.insert(...)` — they never enter the oplog.

---

## 11. End-to-End Encryption

### Why E2EE Must Be Default

Every other sync engine (LiveStoreJS, Zero, PowerSync, Replicache) stores plaintext data on the coordinator. This means:
- The coordinator operator can read all synced data
- A coordinator breach exposes all application data
- Compliance with HIPAA, GDPR, SOC2 requires additional layers

VaultSync's E2EE ensures **the coordinator is zero-trust**. It stores only encrypted blobs and routing metadata. It cannot read any application data.

### Key Generation

On namespace creation, VaultSync generates an X25519 keypair:

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

### 11.1 Key Distribution Protocol

This section specifies how replicas discover each other's public keys and how new devices obtain keys for historical data.

#### The Coordinator as a Key Registry

The coordinator's `vaultsync_coordinator_replicas` table is the authoritative key registry for a namespace. Every replica's public key is stored there on registration. The `Coordinator` trait is extended with a key-fetch method:

```rust
#[async_trait]
pub trait Coordinator: Send + Sync + Debug {
    // ... existing methods ...

    /// Fetch public keys for all active replicas in the namespace.
    /// Used by a replica before encrypting a mutation for broadcast.
    async fn get_peer_keys(
        &self,
        namespace: &str,
    ) -> Result<Vec<ReplicaPublicKey>, CoordinatorError>;

    /// Fetch the public key for a specific replica.
    async fn get_replica_key(
        &self,
        namespace: &str,
        replica_id: &str,
    ) -> Result<Option<ReplicaPublicKey>, CoordinatorError>;
}

pub struct ReplicaPublicKey {
    pub replica_id: String,
    pub public_key:  Vec<u8>,    // X25519 public key bytes
    pub key_version: u32,        // increments on rotation
    pub signed_at:   u64,        // Unix ms — when this key was registered
}
```

#### Key Caching on the Client

Replicas cache peer public keys locally in `vaultsync_keys` (type: `peer_pk`). The cache is refreshed:
1. On startup: fetch all current peer keys for the namespace
2. On `ReplicaKeyChanged` coordinator event (see Key Rotation Notification below)
3. On cache miss: if a mutation arrives from a replica whose key is not in cache, fetch immediately from coordinator

```typescript
// Cache miss flow
coordinator.subscribe("key:changed", async (event) => {
  const key = await coordinator.get_replica_key(namespace, event.replica_id);
  keyCache.set(event.replica_id, key);
});
```

#### Encryption Fan-Out

When a replica uploads a mutation, it does **not** encrypt once per recipient. Instead:

1. The mutation's `yrs_update` is encrypted with the **namespace shared key** — a symmetric key that all authorized replicas in the namespace share.
2. The namespace key itself is stored locally (device-encrypted) and is never sent to the coordinator.
3. When a new replica joins the namespace, it receives the namespace key via a secure out-of-band channel (see New Device Onboarding below).

> **Design note:** This symmetric-key model is simpler and more efficient than per-recipient X25519 DH for namespaces with many replicas. The original spec section described per-replica asymmetric encryption, which is corrected here to the symmetric namespace key model.

#### New Device Onboarding (Key Bootstrap)

A new device joining an existing namespace cannot decrypt historical mutations without the namespace symmetric key. The onboarding flow:

```
New Device generates X25519 keypair (device_sk, device_pk)
         │
         ▼
New Device authenticates to coordinator with JWT
         │
         ▼
New Device sends onboarding request to an existing trusted replica:
  { namespace, device_pk, auth_token }
         │
         ▼
Trusted Replica verifies auth_token
         │
         ▼
Trusted Replica encrypts namespace_sk with device_pk:
  wrapped_key = X25519_encrypt(namespace_sk, device_pk, replica_sk)
         │
         ▼
Trusted Replica sends wrapped_key to New Device
         │
         ▼
New Device decrypts: namespace_sk = X25519_decrypt(wrapped_key, device_sk, replica_pk)
         │
         ▼
New Device stores namespace_sk encrypted with device local key
         │
         ▼
New Device can now decrypt all historical and future mutations
```

If no trusted replica is online at onboarding time, the coordinator holds the wrapped key for async delivery (coordinator holds it encrypted with the coordinator's own key, not plaintext). The new device receives it on next trusted-replica connection.

#### Key Validation — TOFU Policy

VaultSync uses **Trust-On-First-Use (TOFU)** for public keys:

- The first time a replica sees another replica's public key, it is accepted and cached.
- Subsequent interactions validate that the public key has not changed (key pinning after first contact).
- If a key changes unexpectedly (not via the signed key rotation flow), the replica logs a `KEY_CHANGED_UNEXPECTEDLY` warning and pauses sync for operator review.

For high-security namespaces (healthcare, finance), operators can configure `keyValidation: "strict"` which requires manual operator approval for any new replica key before sync proceeds.

```typescript
const vaultsync = new VaultSync({
  namespace: "healthcare:patient-records",
  e2ee: {
    keyValidation: "strict",  // default: "tofu"
    onNewKeyDetected: async (replicaId, newKey) => {
      await notifySecurityTeam(replicaId, newKey);
      return "reject"; // "accept" | "reject"
    }
  }
})
```

#### Key Rotation Notification

When a replica rotates its key:

```
1. Replica generates new keypair (device_pk_new, device_sk_new)
2. Replica re-wraps namespace_sk with new device key
3. Replica uploads to coordinator:
   {
     replica_id,
     old_key_version: 1,
     new_key_version: 2,
     new_public_key:  device_pk_new,
     signature: sign(old_device_sk, concat(replica_id, new_public_key, new_key_version))
   }
4. Coordinator verifies signature with stored old public key
5. Coordinator updates vaultsync_coordinator_replicas.public_key and key_version
6. Coordinator broadcasts { event: "key:changed", replica_id, new_key_version } to all connected replicas
7. All replicas update their key cache
```

Mutations in-flight at rotation time are unaffected — they were encrypted with the namespace symmetric key, not per-device keys.

---

### 11.2 Local At-Rest Encryption

The `vaultsync_oplog.yrs_update` column stores plaintext CRDT diffs locally for fast merge. This section specifies how to configure optional local encryption at rest.

#### Default Behavior (No At-Rest Encryption)

By default, the local database is not encrypted. Protection comes from:
- OS-level sandboxing (OPFS in browser is origin-private)
- Process isolation on native (SQLite file owned by app user)
- Full-device encryption (FDE) from the OS (FileVault, BitLocker, Android/iOS FDE)

For most applications, OS-level sandboxing + FDE is sufficient.

#### Opt-In At-Rest Encryption

For applications with higher local security requirements (shared devices, kiosks, healthcare), VaultSync supports optional at-rest encryption:

```typescript
const vaultsync = new VaultSync({
  storage: "sqlite",
  namespace: "healthcare:records",
  storageEncryption: {
    enabled: true,
    mode: "sqlcipher",     // "sqlcipher" | "app-level"
    keyDerivation: "os-keychain"  // "os-keychain" | "passphrase" | "hardware-key"
  }
})
```

#### Mode 1: SQLCipher (Full Database Encryption)

Encrypts the entire SQLite database file using SQLCipher (AES-256 CBC, PBKDF2 key derivation):

- **Key source (os-keychain):** VaultSync generates a 256-bit random key and stores it in the OS keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service). The SQLite file is unreadable without the OS keychain entry.
- **Key source (passphrase):** User-supplied passphrase → PBKDF2 (100,000 iterations, SHA-256) → 256-bit key. Prompted on app start.
- **Performance:** ~5–10% overhead on read/write operations (AES hardware acceleration on modern CPUs).

#### Mode 2: App-Level (Field-Level Encryption)

Encrypts individual CRDT fields before writing to storage. Only `yrs_update`, `encrypted_blob`, and `key_blob` columns are encrypted. Schema metadata and oplog status remain readable (for query performance).

#### Key Loss Policy

| Scenario | Result | Recovery |
|---|---|---|
| OS keychain key deleted | Database inaccessible | Must re-bootstrap from coordinator snapshot |
| Passphrase forgotten | Database inaccessible | Must re-bootstrap from coordinator snapshot |
| Device stolen (FDE enabled) | Data safe (FDE protects) | No action needed |
| Device stolen (no FDE, no at-rest) | Data exposed if offline | Enable at-rest encryption for sensitive namespaces |

**Key loss is intentional by design** — there is no "forgot my encryption key" recovery path. The coordinator holds encrypted blobs; a new device can re-bootstrap from coordinator snapshot without needing the lost local key. Applications must communicate this trade-off to users.

#### Browser (OPFS) At-Rest Encryption

In browser environments, the OPFS directory is already sandboxed to the application origin. At-rest encryption adds:

- **WebCrypto `subtle.generateKey()`** generates an AES-GCM key stored in the browser's `IndexedDB` (the `vaultsync_keys` system store, not application data).
- The IndexedDB storage of the key is protected by the browser's same-origin policy.
- This provides defense-in-depth against browser extension attacks that bypass origin isolation.

---

## 12. Multi-Tab & Multi-Process Architecture

### The Problem

When multiple browser tabs, Electron windows, or OS processes share the same local storage (SQLite file or OPFS), concurrent writes cause:

- SQLite locking errors (SQLITE_BUSY)
- CRDT document corruption (concurrent Yrs mutations)
- Oplog ordering violations
- Subscription double-firing

VaultSync solves this with **leader election**: one process writes, all others read via shared memory.

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
| **Browser (tabs)** | `BroadcastChannel` + `navigator.locks.request` | First tab acquires `vaultsync-leader` lock. All tabs communicate via BroadcastChannel. `SharedArrayBuffer` passes state |
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
Tab B reads vaultsync_sync_state from storage to get last confirmed sequence
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
vaultsync.db.todos.insert({ id: "todo:123", text: "build vaultsync" })
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
      INSERT into vaultsync_oplog (yrs_update, encrypted_blob, status: pending)
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

In the old spec, operations could enter a `conflict` state. In CRDT-native VaultSync, **mutations never conflict**. Every Yrs update can be applied to any document state and produces a deterministic result. The `conflict` status is eliminated.

### 13.1 WebSocket Wire Protocol

This section defines the exact message format over the WebSocket transport between VaultSync replicas and the coordinator server.

#### Connection URL

```
wss://<coordinator-host>/namespace/:namespace/ws
```

All messages are sent as binary WebSocket frames (not text). The frame format is:

```
┌──────────────┬──────────────┬──────────────────────────────────────┐
│ type: u8     │ length: u32  │ payload: JSON bytes (length bytes)    │
│ (1 byte)     │ (4 bytes)    │                                       │
└──────────────┴──────────────┴──────────────────────────────────────┘
Total header: 5 bytes. Payload: length bytes.
```

#### Message Type Enum

| Value | Name | Direction | Description |
|---|---|---|---|
| `0x01` | `AUTH` | Client → Server | Authenticate replica with JWT |
| `0x02` | `AUTH_ACK` | Server → Client | Authentication result |
| `0x03` | `REGISTER` | Client → Server | Register replica in namespace |
| `0x04` | `REGISTER_ACK` | Server → Client | Registration result + bootstrap info |
| `0x05` | `PUSH` | Client → Server | Upload batch of encrypted mutations |
| `0x06` | `PUSH_ACK` | Server → Client | Sequence numbers assigned |
| `0x07` | `PULL` | Client → Server | Request mutations after a sequence |
| `0x08` | `PULL_RESPONSE` | Server → Client | Batch of mutations |
| `0x09` | `SUBSCRIBE` | Client → Server | Subscribe to real-time mutations |
| `0x0A` | `MUTATION_PUSH` | Server → Client | Real-time mutation delivery |
| `0x0B` | `HEARTBEAT` | Client → Server | Keep-alive ping |
| `0x0C` | `HEARTBEAT_ACK` | Server → Client | Keep-alive pong |
| `0x0D` | `KEY_FETCH` | Client → Server | Fetch peer public keys |
| `0x0E` | `KEY_RESPONSE` | Server → Client | Peer public keys |
| `0x0F` | `ERROR` | Server → Client | Error response to any client message |
| `0x10` | `SCHEMA_SYNC` | Client → Server | Report current schema version |
| `0x11` | `SCHEMA_MIGRATION` | Server → Client | Migration instructions from coordinator |
| `0x12` | `KEY_CHANGED` | Server → Client | Broadcast: a replica rotated its key |

#### Message Payloads (JSON)

**`AUTH` (0x01)**
```json
{
  "token": "<JWT or Bearer Token>",
  "protocol_version": 1,
  "replica_id": "replica-abc123",
  "namespace": "workspace:core"
}
```

**`AUTH_ACK` (0x02)**
```json
{
  "status": "ok",           // "ok" | "error"
  "error": null,            // or "INVALID_TOKEN" | "NAMESPACE_NOT_FOUND" | etc.
  "session_id": "sess-xyz"
}
```

**`REGISTER` (0x03)**
```json
{
  "replica_id":     "replica-abc123",
  "namespace":      "workspace:core",
  "public_key":     "<base64 X25519 public key>",
  "schema_version": 3,
  "last_sequence":  42
}
```

**`REGISTER_ACK` (0x04)**
```json
{
  "status":              "ok",
  "coordinator_sequence": 150,
  "snapshot_available":  true,
  "snapshot_sequence":   100,
  "snapshot_url":        "https://coordinator/snapshots/snap-xyz",
  "error":               null
}
```

**`PUSH` (0x05)**
```json
{
  "request_id": "req-001",
  "mutations": [
    {
      "id":             "mut-abc123",
      "encrypted_blob": "<base64 AEAD ciphertext>",
      "timestamp":      1740000000000,
      "schema_version": 3,
      "mutation_type":  "CRDT_UPDATE"
    }
  ]
}
```

**`PUSH_ACK` (0x06)**
```json
{
  "request_id": "req-001",
  "sequences":  [43, 44, 45],
  "error":      null
}
```

**`PULL` (0x07)**
```json
{
  "request_id":   "req-002",
  "namespace":    "workspace:core",
  "after":        42,
  "limit":        100
}
```

**`PULL_RESPONSE` (0x08)**
```json
{
  "request_id": "req-002",
  "mutations": [
    {
      "id":             "mut-remote-001",
      "replica_id":     "replica-xyz",
      "sequence":       43,
      "encrypted_blob": "<base64>",
      "timestamp":      1740000001000
    }
  ],
  "has_more": false
}
```

**`MUTATION_PUSH` (0x0A)** — real-time push from coordinator
```json
{
  "id":             "mut-remote-002",
  "replica_id":     "replica-xyz",
  "sequence":       44,
  "encrypted_blob": "<base64>",
  "timestamp":      1740000002000
}
```

**`ERROR` (0x0F)**
```json
{
  "request_id":  "req-001",
  "code":        "SCHEMA_TOO_OLD",
  "message":     "Replica schema v1 is too old; coordinator at v5",
  "retryable":   false,
  "data": {
    "replica_version":      1,
    "coordinator_version":  5
  }
}
```

#### Authentication Handshake Sequence

```
Client                          Coordinator Server
  │                                     │
  │──── WebSocket upgrade (WSS) ────────►│
  │                                     │
  │──── AUTH (type=0x01) ───────────────►│  JWT + namespace + replica_id
  │                                     │
  │◄─── AUTH_ACK (type=0x02) ───────────│  status=ok | error
  │                                     │
  │  (if AUTH_ACK status=error: server closes connection with code 4001)
  │                                     │
  │──── REGISTER (type=0x03) ───────────►│  public_key + schema_version + last_sequence
  │                                     │
  │◄─── REGISTER_ACK (type=0x04) ───────│  coordinator_sequence + snapshot info
  │                                     │
  │──── SUBSCRIBE (type=0x09) ──────────►│  start real-time mutation stream
  │                                     │
  │  (connection now active, real-time sync running)
  │                                     │
  │──── PUSH (type=0x05) ───────────────►│  upload pending mutations
  │◄─── PUSH_ACK (type=0x06) ───────────│  sequence numbers assigned
  │                                     │
  │◄─── MUTATION_PUSH (type=0x0A) ──────│  mutations from other replicas
  │                                     │
  │──── HEARTBEAT (type=0x0B) ──────────►│  every 30 seconds
  │◄─── HEARTBEAT_ACK (type=0x0C) ──────│
```

#### Version Negotiation

The `AUTH` message includes `protocol_version: 1`. The server responds with:
- `AUTH_ACK { status: "ok" }` if it supports that version
- `AUTH_ACK { status: "error", error: "PROTOCOL_VERSION_UNSUPPORTED", supported: [1, 2] }` if not

Clients should attempt the highest version they support. If rejected, retry with a lower version from the server's `supported` list.

#### Connection Loss and Recovery

If the WebSocket connection drops mid-stream:
1. Client detects close event (or heartbeat timeout — 3 missed heartbeats = dead)
2. Client marks connection as `disconnected`, pauses upload queue
3. Client starts reconnect loop (exponential backoff: 1s → 2s → 4s → ... → 30s)
4. On reconnect: full `AUTH` + `REGISTER` handshake with `last_sequence` from `vaultsync_sync_state`
5. Coordinator replays all mutations after `last_sequence` via `PULL_RESPONSE`
6. If `PUSH` was in-flight when disconnect happened: client re-sends on reconnect (idempotent by `mutation_id`)

Partially sent `PUSH` frames (TCP half-open): the 5-byte frame header includes the `length` field. If the connection closes before `length` bytes are received, the server discards the partial frame and the client re-sends the full message on reconnect.

---

## 14. Offline Support & Durability

### What Happens Offline

When network connectivity is lost:

```
Network disconnects
         │
         ▼
VaultSync detects disconnect (WebSocket close / fetch failure)
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
VaultSync detects connectivity (WebSocket reconnect / heartbeat success)
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

1. **Single-tab case**: Mutations are in the oplog on persistent storage. On restart, VaultSync reads all `pending` and `failed` mutations and re-uploads them. CRDT updates are idempotent — re-uploading does not cause duplicates.

2. **Multi-tab case**: If leader crashes, the shared memory ring buffer preserves the last N pending mutations. The new leader replays them before taking over. If shared memory is also lost (full power failure), the new leader reads the oplog from persistent storage — same as single-tab recovery.

3. **E2EE at rest**: All Yrs updates stored in the oplog are encrypted with the namespace key. Local storage theft does not expose application data.

### Pending Sync Table

```sql
CREATE TABLE vaultsync_sync_state (
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
new VaultSync({
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

VaultSync maintains a WebSocket heartbeat. If the heartbeat is missed:

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

Unlike the old spec where subscriptions watched opaque table rows, VaultSync subscriptions watch CRDT documents. When a Yrs merge changes a document, the subscription engine compares the document's state before and after the merge and fires callbacks for changed fields.

**Key behavioral difference from the old spec:**
- Subscriptions fire **after** the CRDT merge completes, not after individual row operations
- A single remote mutation that affects 10 fields fires one subscription callback, not 10
- The callback receives the full CRDT document state (materialized), not a "change event"

### Table Subscription

```typescript
// Subscribe to all changes on a table (document collection)
const unsub = vaultsync.db.todos.subscribe((todos) => {
  renderTodoList(todos)
})

// Unsubscribe when component unmounts
unsub()
```

### Filtered Subscription (CRDT Field Index)

```typescript
// Subscribe to a filtered query
const unsub = vaultsync.db.todos.subscribe(
  (todos) => renderTodos(todos),
  { where: { assignee: "alice", completed: false } }
)
```

Filtered subscriptions use CRDT field indexes: the engine maintains a materialized index of LWW-Register fields and only fires subscriptions when indexed fields change matching the filter predicate.

### Single Record Subscription

```typescript
// Subscribe to a specific CRDT document
const unsub = vaultsync.db.todos.subscribeOne(
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
vaultsync.subscribe("sync:status", (status) => {
  updateSyncIndicator(status)
  // status: { connected, pendingUploads, lastSyncAt, leaderStatus }
})
```

### React Hooks

```typescript
import { useVaultSync, useVaultSyncQuery, useVaultSyncOne, useVaultSyncSync } from "@vaultsync/react"

function TodoList() {
  const todos = useVaultSyncQuery(db => db.todos.findAll())
  return <ul>{todos.map(t => <TodoItem key={t.id} todo={t} />)}</ul>
}

function TodoDetail({ id }: { id: string }) {
  const todo = useVaultSyncOne(db => db.todos.findById(id))
  if (!todo) return <NotFound />
  return <div>{todo.text}</div>
}

function ProjectTodos({ projectId }: { projectId: string }) {
  const todos = useVaultSyncQuery(db =>
    db.todos.findAll({ where: { projectId, completed: false } })
  )
  return <TodoList todos={todos} />
}

function SyncIndicator() {
  const { connected, pendingUploads } = useVaultSyncSync()
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

The old spec locked VaultSync to Cloudflare Durable Objects. This violated the principle of infrastructure choice. The Coordinator trait makes the coordinator a **pluggable backend** that users can choose, replace, or self-host without changing application code.

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
| **PostgreSQL** | `@vaultsync/coordinator-postgres` | `vaultsync_coordinator_mutations` table | `LISTEN/NOTIFY` |
| **Redis** | `@vaultsync/coordinator-redis` | Redis Streams per namespace | `PUBLISH/SUBSCRIBE` |
| **SQLite** | `@vaultsync/coordinator-sqlite` | Single SQLite file (dev/test) | Polling |
| **Cloudflare DO** | `@vaultsync/coordinator-cloudflare` | Durable Object state | WebSocket per DO |
| **Supabase** | `@vaultsync/coordinator-supabase` | Supabase tables | Supabase Realtime |
| **In-Memory** | `@vaultsync/coordinator-memory` | HashMap (testing) | Channel |

### Switching Coordinators

```typescript
// PostgreSQL
const vaultsync = new VaultSync({
  storage: "sqlite",
  namespace: "workspace:core",
  coordinator: new PostgresCoordinator({
    connectionString: "postgres://user:pass@host/db"
  })
})

// Cloudflare
const vaultsync = new VaultSync({
  storage: "opfs",
  namespace: "workspace:core",
  coordinator: new CloudflareCoordinator({
    accountId: "...",
    durableObjectId: "..."
  })
})

// Custom
const vaultsync = new VaultSync({
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

### 17.1 Coordinator Server — Deployable Binary

The `Coordinator` trait defines what a coordinator backend does (push, pull, subscribe, register, heartbeat). This section defines the **coordinator server** — the deployable process that exposes those capabilities over WebSocket (Protocol §13.1) to VaultSync replicas.

#### Architecture

```
┌────────────────────────────────────────────────────────────┐
│                  vaultsync-coordinator-server                   │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │           Axum HTTP/WebSocket Server               │    │
│  │   POST /vaultsync/v1/snapshot  (snapshot upload)       │    │
│  │   GET  /vaultsync/v1/snapshot/:id (snapshot download)  │    │
│  │   WS   /vaultsync/v1/sync  (main sync WebSocket)       │    │
│  │   GET  /health                                     │    │
│  │   GET  /metrics  (Prometheus)                      │    │
│  └────────────────────┬───────────────────────────────┘    │
│                        │                                    │
│  ┌─────────────────────▼──────────────────────────────┐    │
│  │           Connection Manager                       │    │
│  │   - Track connected replicas per namespace         │    │
│  │   - Session state (replica_id, namespace, seq)     │    │
│  │   - Fan-out: send MUTATION_PUSH to all connections │    │
│  └─────────────────────┬──────────────────────────────┘    │
│                         │                                   │
│  ┌──────────────────────▼─────────────────────────────┐    │
│  │           Coordinator Backend (trait impl)          │    │
│  │   PostgresCoordinator | RedisCoordinator | etc.     │    │
│  └─────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

#### Running the Coordinator Server

```bash
# Docker — PostgreSQL backend
docker run -p 8080:8080 \
  -e VAULTSYNC_DB_URL=postgres://user:pass@db:5432/vaultsync \
  -e VAULTSYNC_AUTH_JWKS_URL=https://your-auth.example.com/.well-known/jwks.json \
  vaultsyncsync/coordinator:latest

# Binary
vaultsync-server \
  --backend postgres \
  --db-url postgres://user:pass@localhost/vaultsync \
  --port 8080 \
  --auth-jwks-url https://your-auth.example.com/.well-known/jwks.json
```

#### Configuration

```toml
# vaultsync-coordinator.toml

[server]
host = "0.0.0.0"
port = 8080
tls  = true
tls_cert = "/etc/vaultsync/cert.pem"
tls_key  = "/etc/vaultsync/key.pem"

[backend]
type = "postgres"   # "postgres" | "redis" | "sqlite"
url  = "postgres://user:pass@localhost/vaultsync"

[auth]
jwks_url  = "https://your-auth.example.com/.well-known/jwks.json"
audience  = "vaultsync-sync"
algorithm = "RS256"

[limits]
max_connections_per_namespace = 10000
max_mutation_size_bytes       = 1048576   # 1MB
max_batch_size                = 1000
heartbeat_timeout_seconds     = 90

[compaction]
snapshot_interval_mutations   = 10000
mutation_retention_days       = 90       # matches tombstone GC threshold (§8.5)
```

#### Session Lifecycle

```
WebSocket connected
  → Auth (JWT validated)
  → Register (replica saved/updated in DB)
  → Subscribe (added to namespace fan-out group)
  → Real-time loop:
      Receive PUSH → store → assign sequence → PUSH_ACK + fan-out to others
      Receive PULL → query DB → PULL_RESPONSE
      Receive HEARTBEAT → update last_heartbeat → HEARTBEAT_ACK
  → WebSocket closed → removed from fan-out group
```

### 17.2 Partial Sync

Partial sync allows a replica to synchronize only a subset of records from a namespace rather than all records. This is essential for multi-tenant apps (sync only records belonging to this user) and large datasets.

#### Subset Declaration

A replica declares its sync subset at initialization:

```typescript
const vaultsync = new VaultSync({
  namespace: "workspace:core",
  sync: {
    partial: {
      todos: { where: { assignee: userId } },  // only my todos
      projects: true,                           // all projects
      settings: { where: { scope: "global" } }
    }
  }
})
```

The subset is declared as a per-table filter predicate over **unencrypted, indexed fields** only (see E2EE Constraint below).

#### How Partial Sync Works

```
Replica registers with coordinator:
  { ..., partial_sync: { "todos": { assignee: "user-123" }, "projects": {} } }
         │
         ▼
Coordinator stores partial_sync filter in vaultsync_coordinator_replicas.sync_filter
         │
         ▼
When a mutation arrives from another replica:
  coordinator checks: does this mutation's (doc_id, record_id) match the filter?
         │
  ├─ YES: include in MUTATION_PUSH to this replica
  └─ NO:  skip (do not push)
         │
         ▼
On PULL request: coordinator filters response by sync_filter
```

#### Enforcement Location

Partial sync is **enforced by the coordinator**, not the client. The client declares the filter; the coordinator applies it before delivery. This ensures:
1. Replicas only receive mutations they declared interest in (bandwidth efficiency)
2. A replica cannot "declare away" records it should sync (filter is advisory, not a security boundary — see Authorization §23.1 for security)

#### E2EE Constraint

The coordinator cannot inspect encrypted field values. Partial sync filters can only operate on **routing metadata** that is stored unencrypted by the coordinator:

```
Available for partial sync filtering:
  ✅ doc_id        (table name)
  ✅ record_id     (record identifier, if structured: "user-123:todo-456")
  ✅ replica_id    (which device produced the mutation)
  ✅ timestamp     (when it was produced)

NOT available for partial sync filtering (E2EE):
  ❌ field values  (encrypted, coordinator cannot read)
  ❌ CRDT type     (encrypted, coordinator cannot read)
```

**Best practice:** Embed filter dimensions into the `record_id`:

```typescript
// Instead of: { id: "todo-456", assignee: "user-123" }
// Use structured record IDs:
db.todos.insert({ id: `user-${userId}:todo-${todoId}`, text: "..." })
// Partial sync filter: { where: { record_id: { startsWith: `user-${userId}:` } } }
```

#### When Records Enter or Exit the Subset

If a record moves out of a replica's partial sync set (e.g., a todo is re-assigned to another user):

```
Todo "user-123:todo-456" updated: assignee → "user-999"
         │
         ▼
Record ID changes to "user-999:todo-456" (new coordinator routing)
         │
         ▼
User 123's replica: no longer receives updates for this record
         │
         ▼
User 123's replica: CRDT document "user-123:todo-456" becomes stale
         │
         ▼
Coordinator sends "RECORD_EVICTED" notification:
  { doc_id: "todos", old_record_id: "user-123:todo-456" }
         │
         ▼
Replica soft-deletes the local CRDT document
```

### 17.3 Snapshot Bootstrap Protocol

This section details how coordinator-side snapshots are created, stored, and used to onboard new replicas without replaying the full mutation history.

#### Who Creates Snapshots

Snapshots are created by the **coordinator server**, not by individual replicas. This ensures all replicas in a namespace see a consistent snapshot point.

```
Coordinator server background job (runs every snapshot_interval_mutations):
  1. Lock namespace (pause PUSH processing for < 100ms)
  2. Fetch all coordinator mutations from last_snapshot_sequence to latest
  3. Compute the union Yrs state: apply all mutations in sequence order
     (coordinator cannot decrypt — stores the opaque encrypted diffs; snapshot is
      the encrypted diff history, not a plaintext Yrs document)
  4. Compress: zstd compress the encrypted mutation batch
  5. Store in vaultsync_coordinator_snapshots:
     { namespace, sequence: current_max, snapshot_bytes: compressed, created_at }
  6. Record snapshot_sequence as new baseline
  7. Unlock namespace
```

> **Important:** The coordinator snapshot is **not** a decrypted Yrs document state. It is a compressed batch of encrypted mutations from the last snapshot to the current point. This preserves E2EE — the coordinator never decrypts.

#### Snapshot Format

```
snapshot_bytes layout (after zstd decompression):
┌─────────────────────────────────────────────────────────┐
│ header: { version: u8, namespace: str, sequence: u64,   │
│           mutation_count: u32, created_at: u64 }         │
├─────────────────────────────────────────────────────────┤
│ mutations: [ { id, replica_id, encrypted_blob, ts }, … ] │
│ (ordered by sequence, from last_snapshot_sequence to    │
│  snapshot_sequence inclusive)                            │
└─────────────────────────────────────────────────────────┘
```

#### New Replica Bootstrap Flow

```
New replica sends REGISTER with last_sequence = 0
         │
         ▼
Coordinator: snapshot_available = true?
         │
    ┌────┴────────────────────────────────────────────────┐
    │ YES                                                  │ NO
    ▼                                                      ▼
REGISTER_ACK with snapshot_url              REGISTER_ACK with snapshot_available=false
         │                                                 │
         ▼                                                 ▼
Replica downloads snapshot               Replica sends PULL(after=0, limit=1000)
         │                               and pages through full history
         ▼
Replica decompresses snapshot
         │
         ▼
Replica decrypts each mutation with namespace_sk
         │
         ▼
Replica applies each mutation to local Yrs documents in sequence order
         │
         ▼
Replica sends PULL(after=snapshot_sequence) for mutations since snapshot
         │
         ▼
Normal sync begins from snapshot_sequence
```

#### Snapshot Integrity

The new replica validates snapshot integrity by:
1. Verifying each mutation's `replica_id` is registered in the coordinator (via KEY_FETCH)
2. Verifying the mutation sequence numbers are contiguous and match the snapshot header
3. Any gap in sequence numbers is reported as `SNAPSHOT_INTEGRITY_ERROR` and the replica falls back to full history replay

#### Snapshot Frequency

| Dataset Size | Recommended Interval |
|---|---|
| < 10K mutations | Every 10K mutations (or monthly) |
| 10K–100K mutations | Every 10K mutations |
| > 100K mutations | Every 10K mutations, older snapshots pruned after 2× tombstone retention |

### 17.4 Coordinator Fan-Out Mechanism

When a mutation is stored by the coordinator, it must be delivered in real-time to all other connected replicas in the same namespace. This section specifies how each coordinator backend handles fan-out.

#### Fan-Out Pattern

```
Replica A uploads mutation → Coordinator stores (sequence=44)
         │
         ▼
Coordinator broadcasts MUTATION_PUSH to:
  Replica B (connected via WebSocket session)
  Replica C (connected via WebSocket session)
  (not back to Replica A — filtered by replica_id)
```

#### Per-Backend Fan-Out

| Backend | Fan-Out Mechanism | Real-Time Latency |
|---|---|---|
| **PostgreSQL** | `LISTEN/NOTIFY` channel per namespace. Server process listens; when `pg_notify('namespace:workspace:core', seq)` fires, server pushes to connected WebSocket sessions. | 5–50 ms |
| **Redis** | `PUBLISH` to channel `vaultsync:{namespace}`. Server subscribes via `SUBSCRIBE vaultsync:*`; delivers to matching WebSocket sessions. | 1–10 ms |
| **SQLite** | Polling loop: server polls `SELECT ... WHERE sequence > last_known` every 250ms. Suitable only for dev/single-instance. | 250ms (polling interval) |
| **Cloudflare Durable Objects** | DO single-writer model: all WebSocket connections to the DO get push via DO's built-in WebSocket hibernation API. | 10–50 ms |
| **Supabase** | Supabase Realtime channel subscribed to `vaultsync_coordinator_mutations` insert events. | 10–100 ms |

#### Connection Manager (Server-Side State)

The coordinator server maintains an in-memory connection map per namespace:

```rust
// Server-side, per process
struct ConnectionManager {
    // namespace → set of (replica_id, tx: mpsc::Sender<WsMessage>)
    sessions: DashMap<String, Vec<ReplicaSession>>,
}

impl ConnectionManager {
    pub async fn fan_out(&self, namespace: &str, mutation: PendingMutation, from_replica: &str) {
        if let Some(sessions) = self.sessions.get(namespace) {
            for session in sessions.iter() {
                if session.replica_id != from_replica {  // don't echo back
                    let _ = session.tx.send(WsMessage::MutationPush(mutation.clone())).await;
                }
            }
        }
    }
}
```

For multi-instance coordinator deployments (HA), the in-memory fan-out is replaced with the backend pub/sub mechanism — Postgres LISTEN/NOTIFY or Redis PUBLISH ensures mutations are fanned out across all server instances.

### 17.5 Coordinator High Availability

For production deployments requiring uptime guarantees, the coordinator can run in high-availability configuration.

#### Architecture: Active-Passive with Postgres

The simplest HA model uses Postgres replication (primary + replica) and multiple coordinator server instances:

```
┌──────────────────┐    ┌──────────────────┐
│  Coordinator     │    │  Coordinator     │
│  Server A        │    │  Server B        │
│  (primary)       │    │  (standby)       │
└────────┬─────────┘    └────────┬─────────┘
         │                       │
         └───────────┬───────────┘
                     │ (load balanced via HAProxy / AWS ALB)
                     ▼
         ┌─────────────────────┐
         │  Postgres Primary   │◄──── Postgres Replica (read replica)
         └─────────────────────┘
```

Both coordinator server instances connect to the same Postgres primary. Fan-out uses `LISTEN/NOTIFY` which is shared across all connections to the same Postgres instance.

#### Sequence Number Safety on Failover

Sequence numbers are assigned by Postgres `BIGSERIAL` (atomic, durable). Even if Coordinator Server A crashes after assigning sequence=44 but before sending `PUSH_ACK`:
- The sequence is committed in Postgres
- The client re-sends the mutation on reconnect (idempotent by `mutation_id`)
- Coordinator deduplicates by `mutation_id` (UNIQUE constraint) — returns existing sequence=44

Sequence numbers are never reused or rolled back. Gaps in sequence numbers (from rejected mutations) are normal and clients handle them gracefully.

#### Client Failover

VaultSync clients are configured with multiple coordinator endpoints:

```typescript
const vaultsync = new VaultSync({
  coordinator: new PostgresCoordinator({
    endpoints: [
      "wss://coordinator-a.example.com/vaultsync/v1/sync",
      "wss://coordinator-b.example.com/vaultsync/v1/sync"
    ],
    strategy: "round-robin"  // "round-robin" | "failover"
  })
})
```

On connection failure, the client rotates to the next endpoint automatically. The new connection's `REGISTER` message includes `last_sequence`, so the coordinator catches the client up from where it left off.

### 17.6 Coordinator-Side Compaction

As months of mutations accumulate, the coordinator's `vaultsync_coordinator_mutations` table grows unboundedly. Compaction prunes old entries while maintaining correctness.

#### Safety Rule: Minimum Sequence Fence

The coordinator tracks the **minimum acknowledged sequence** across all registered replicas:

```sql
SELECT MIN(last_sequence) as min_seq
FROM vaultsync_coordinator_replicas
WHERE namespace = 'workspace:core'
  AND last_heartbeat > NOW() - INTERVAL '90 days';  -- only active replicas
```

Mutations with `sequence < min_seq` are safe to delete — every active replica has already received and applied them.

#### Compaction Process

```
Background job runs daily (or triggered by mutation count threshold):
  1. Compute min_seq = MIN(last_sequence) across active replicas
  2. Snapshot: create a compressed snapshot of mutations [last_snapshot_seq, min_seq]
  3. Store snapshot in vaultsync_coordinator_snapshots
  4. Delete from vaultsync_coordinator_mutations WHERE sequence < min_seq
  5. Update compaction watermark
```

#### New Replica After Compaction

If a new replica joins and requests `PULL(after=0)`, but the coordinator has compacted mutations before sequence 5000:
- Coordinator returns `PULL_RESPONSE` starting from the oldest available sequence
- `REGISTER_ACK` includes `compaction_floor: 5000` — sequences below this are unavailable
- Client bootstraps from the latest snapshot (§17.3) which covers the compacted range

#### Stale Replica Detection

If a replica's `last_sequence` is below the `compaction_floor` when it reconnects (offline longer than retention period):
1. Coordinator returns `REPLICA_TOO_STALE` error
2. Client performs full bootstrap from latest snapshot
3. All local pending mutations (with sequences < compaction_floor) are re-uploaded with new IDs

---

## 18. Full Database Schema

### Local (Client-Side) Tables

```sql
-- CRDT Document Store
-- Each document is a Yrs binary snapshot of current state
CREATE TABLE vaultsync_documents (
  doc_id      TEXT    NOT NULL,          -- table/document name
  record_id   TEXT    NOT NULL,          -- record identifier
  doc_bytes   BLOB   NOT NULL,          -- Yrs binary snapshot
  updated_at  INTEGER NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at  DATETIME,                 -- soft delete marker
  PRIMARY KEY (doc_id, record_id)
);

-- Operation Log (CRDT mutations)
CREATE TABLE vaultsync_oplog (
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

CREATE INDEX idx_oplog_sync_status ON vaultsync_oplog(sync_status, created_at);
CREATE INDEX idx_oplog_namespace   ON vaultsync_oplog(namespace, sequence);
CREATE INDEX idx_oplog_record      ON vaultsync_oplog(doc_id, record_id);

-- Sync state per namespace
CREATE TABLE vaultsync_sync_state (
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
CREATE TABLE vaultsync_schema_meta (
  doc_id          TEXT    PRIMARY KEY,
  schema_version  INTEGER NOT NULL DEFAULT 1,
  schema_json     JSON    NOT NULL,
  applied_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Migration history
CREATE TABLE vaultsync_migrations (
  version     TEXT    PRIMARY KEY,
  applied_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  checksum    TEXT    NOT NULL,
  rollback    TEXT
);

-- Replica identity
CREATE TABLE vaultsync_replica_meta (
  id            TEXT    PRIMARY KEY,
  device_name   TEXT,
  device_type   TEXT,
  namespace     TEXT    NOT NULL,
  public_key    BLOB   NOT NULL,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Encryption key storage
CREATE TABLE vaultsync_keys (
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
CREATE TABLE vaultsync_coordinator_mutations (
  sequence      BIGSERIAL  PRIMARY KEY,
  namespace     TEXT       NOT NULL,
  replica_id    TEXT       NOT NULL,
  mutation_id   TEXT       NOT NULL UNIQUE,
  encrypted     BYTEA      NOT NULL,     -- AEAD ciphertext
  timestamp     BIGINT     NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_coord_mutations_namespace_seq
  ON vaultsync_coordinator_mutations(namespace, sequence);
CREATE INDEX idx_coord_mutations_replica
  ON vaultsync_coordinator_mutations(replica_id);
CREATE INDEX idx_coord_mutations_mutation_id
  ON vaultsync_coordinator_mutations(mutation_id);

-- Replica registry
CREATE TABLE vaultsync_coordinator_replicas (
  id              TEXT    PRIMARY KEY,
  namespace       TEXT    NOT NULL,
  public_key      BYTEA   NOT NULL,
  device_info     JSONB,
  last_heartbeat  TIMESTAMPTZ,
  last_sequence   BIGINT  NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Snapshot registry
CREATE TABLE vaultsync_coordinator_snapshots (
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
| `vaultsync_oplog` — JSON payload | Now `yrs_update BLOB` + `encrypted_blob BLOB` | CRDT binary diffs replace opaque JSON; E2EE adds ciphertext storage |
| `vaultsync_oplog` — `conflict_info` | **Removed** | CRDTs have no conflicts |
| `vaultsync_oplog` — `previous_payload` | **Removed** | CRDT document snapshots handle rollback |
| `vaultsync_oplog` — `sync_status conflict` | **Removed** | CRDT mutations never conflict |
| `vaultsync_sync_state` — `replica_id` PK (old section 11) vs `namespace` PK (old section 16) | **Fixed**: `namespace` PK consistently | Resolved schema contradiction |
| `vaultsync_conflicts` | **Removed** | No conflict log needed |
| `vaultsync_keys` | **New** | E2EE key storage table |
| `vaultsync_documents` — `doc_bytes BLOB` | **New** | CRDT document snapshot storage |

---

## 19. Performance Model

### Honest Performance Targets

VaultSync does not claim "0ms latency." All latency numbers are measured with Criterion.rs benchmarks in CI, and performance regressions >10% fail the build. Targets are for the **engine overhead** — network latency is excluded and depends on the user's infrastructure.

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

Every VaultSync component emits OpenTelemetry traces, metrics, and structured logs. All VaultSync code is MIT-licensed — there are no proprietary components. Any failure can be traced to its exact cause.

### OpenTelemetry Traces

Every operation produces a trace:

```
Trace: "vaultsync.write" (trace_id: abc123)
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
| `vaultsync_mutations_total` | Counter (labels: status, namespace) | Total mutations processed |
| `vaultsync_mutations_pending` | Gauge (labels: namespace) | Current pending upload queue depth |
| `vaultsync_mutations_failed` | Counter (labels: namespace, reason) | Mutations in failed state |
| `vaultsync_sync_lag_ms` | Histogram (labels: namespace) | Time from local write to coordinator ACK |
| `vaultsync_download_lag_ms` | Histogram (labels: namespace) | Time from coordinator sequence to replica apply |
| `vaultsync_encryption_time_us` | Histogram (labels: operation) | E2EE encrypt/decrypt duration |
| `vaultsync_crdt_merge_time_us` | Histogram (labels: doc_id) | CRDT merge duration |
| `vaultsync_replica_count` | Gauge (labels: namespace) | Active replicas per namespace |
| `vaultsync_connection_status` | Gauge (labels: namespace) | 1=connected, 0=disconnected |
| `vaultsync_leader_status` | Gauge | 1=leader, 0=reader |
| `vaultsync_oplog_size` | Gauge (labels: namespace) | Total oplog entries per namespace |
| `vaultsync_doc_count` | Gauge (labels: doc_id) | Documents per CRDT document collection |

### Structured Logging

All logs are structured JSON via the `tracing` crate:

```json
{
  "timestamp": "2025-01-01T10:00:01.234Z",
  "level": "warn",
  "target": "vaultsync::sync::upload",
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

Every VaultSync process exposes a debug HTTP endpoint on localhost:9876 (disabled in production unless configured):

```
GET /debug/vaultsync/state           → Full internal state as JSON
GET /debug/vaultsync/state/oplog     → Last 1000 oplog entries
GET /debug/vaultsync/state/documents → All CRDT document snapshots (sizes, doc_ids)
GET /debug/vaultsync/state/keys      → Key metadata (not key material)
GET /debug/vaultsync/state/replicas  → Connected replica info
GET /debug/vaultsync/metrics         → Prometheus metrics
GET /debug/vaultsync/traces          → Recent trace spans
GET /debug/vaultsync/leader          → Current leader status
POST /debug/vaultsync/force-sync     → Trigger immediate sync
POST /debug/vaultsync/force-election → Trigger leader re-election
```

### CLI: `vaultsync inspect`

```bash
# Attach to a running VaultSync process and watch operations in real-time
$ vaultsync inspect --pid 1234

# Watch live operation stream
$ vaultsync inspect --pid 1234 --stream

# Dump current state
$ vaultsync inspect --pid 1234 --state

# Replay a trace file
$ vaultsync replay trace_abc123.json
```

### Trace Replay Tool

```bash
# Export a trace from a running process
$ vaultsync inspect --pid 1234 --export-trace > trace.json

# Replay the trace in a deterministic test environment
$ vaultsync replay trace.json
# Verifies: all replicas converge, no data loss, all ACKs match
```

---

## 21. SDK Reference

### Installation

```bash
# Browser / React
npm install @vaultsync/web @vaultsync/react

# Node.js
npm install @vaultsync/node

# React Native
npm install @vaultsync/native
```

### Initialization

```typescript
import { VaultSync } from "@vaultsync/web"
import { VaultSyncProvider } from "@vaultsync/react"

const vaultsync = new VaultSync({
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
vaultsync.schema.define("todos", {
  fields: {
    id:        { type: "string",  crdtType: "lww",    primaryKey: true },
    text:      { type: "string",  crdtType: "lww"                      },
    completed: { type: "boolean", crdtType: "lww"                      },
    tags:      { type: "array",   crdtType: "orset"                    },
    priority:  { type: "number",  crdtType: "counter"                  }
  }
})

// Register migrations
vaultsync.migration("v1", async (db) => { ... })
vaultsync.migration("v2", async (db) => { ... })

// Initialize (runs migrations, connects to coordinator, E2EE key setup)
await vaultsync.initialize()
```

### CRUD Operations

```typescript
const db = vaultsync.db

// INSERT — creates a new CRDT document
const todo = await db.todos.insert({
  id:        "todo:123",
  text:      "build vaultsync",
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
  text: "build vaultsync runtime"
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
vaultsync.subscribe("sync:status", status => updateUI(status))

// Encrypted mutations inspection (debug only)
vaultsync.subscribe("sync:mutations", mutations => {
  // mutations that were just synced
})
```

### E2EE Key Management

```typescript
// Keys are generated automatically on initialize()
// Manual management for advanced use cases:

// Rotate namespace key
await vaultsync.keys.rotate()

// Export public key (for sharing with other replicas)
const pubKey = await vaultsync.keys.publicKey()

// Check key status
const keyInfo = await vaultsync.keys.status()
// { version: 2, createdAt: ..., algorithm: "X25519+ChaCha20-Poly1305" }
```

### Multi-Tab

```typescript
// VaultSync handles multi-tab automatically
// Configuration options:

const vaultsync = new VaultSync({
  // ...
  multiTab: {
    enabled: true,
    electionTimeout: 3000,  // ms
    heartbeatInterval: 1000, // ms
    sharedMemorySize: 64 * 1024 * 1024 // 64MB shared memory region
  }
})

// Query current leader status
const status = await vaultsync.leaderStatus()
// { amILeader: true, leaderId: "tab-A", uptime: 120000 }

// Force leader re-election (debug only)
await vaultsync.forceElection()
```

### Coordinator Switching

```typescript
// Switch coordinator at runtime (graceful)
await vaultsync.switchCoordinator(new RedisCoordinator({
  url: "redis://localhost:6379"
}))
// Pending uploads drain to old coordinator, then switch
```

### Oplog & Sync Inspection

```typescript
// Pending mutations count
const pending = await vaultsync.pendingUploads()

// Full mutation history for a record
const history = await vaultsync.mutationLog("todos", "todo:123")

// Force sync (useful for debugging)
await vaultsync.forceSync()

// Sync status
const status = await vaultsync.syncStatus()
// { connected, pendingUploads, lastSyncAt, replicaId, schemaVersion, leaderStatus }
```

### Shutdown

```typescript
// Graceful shutdown — flush pending uploads, close WebSocket, release leader lock
await vaultsync.shutdown()
```

---

## 22. Deployment Modes

### Mode 1: Embedded Local-Only (No Sync)

Local CRDT document store only. No coordinator. No sync. Useful for offline-first apps that don't need multi-device sync.

```typescript
const vaultsync = new VaultSync({
  storage:   "sqlite",
  namespace: "local",
  sync:      false    // ← no coordinator connection
})
```

### Mode 2: Embedded + Managed Coordinator (Postgres, Redis, etc.)

Application embeds VaultSync; coordinator is a managed database. Simplest path to multi-device sync.

```typescript
const vaultsync = new VaultSync({
  storage:    "opfs",
  namespace:  `user:${userId}`,
  coordinator: new PostgresCoordinator({
    connectionString: process.env.DATABASE_URL
  })
})
```

### Mode 3: Embedded + Self-Hosted Coordinator

Application embeds VaultSync; coordinator is a self-hosted VaultSync coordination worker.

```typescript
const vaultsync = new VaultSync({
  storage:   "sqlite",
  namespace: "workspace:core",
  coordinator: new CustomCoordinator({
    url: "wss://your-coordinator.example.com/vaultsync"
  })
})
```

### Mode 4: Multi-Instance Server Sync

Multiple server instances sharing a coordinator. Used when server-side data must sync between instances.

```
App Server A (VaultSync embedded, namespace: workspace:core)
App Server B (VaultSync embedded, namespace: workspace:core)
App Server C (VaultSync embedded, namespace: workspace:core)
                        │
                Shared Coordinator (Postgres)
```

### Mode 5: Hybrid Client + Server + P2P

Both clients (browsers/mobile) and server instances participate in the same namespace. Optional P2P sync for low-latency collaboration between clients on the same LAN.

```typescript
const vaultsync = new VaultSync({
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
const vaultsync = new VaultSync({
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

The `vaultsync_keys` table stores private keys encrypted with the device's local key (OS keychain on native, WebCrypto `subtle.wrapKey` on browser). The `vaultsync_oplog.yrs_update` column stores plaintext CRDT diffs locally (for fast merge) but can be configured for local-only encryption as well.

### Operation Validation

Every incoming mutation is validated by the coordinator:

- Schema conformance (CRDT document structure)
- Record ID format validation
- Encrypted blob size limits
- Authorization via Aegis (optional plugin)
- Sequence ordering sanity checks

Mutations failing validation are rejected with a specific error code. The client logs the rejection and surfaces it as a sync error.

Each mutation includes a client-generated unique ID (`mut_abc123`). The coordinator deduplicates by mutation ID. A replayed mutation is silently dropped — idempotent by design. The E2EE layer also prevents meaningful replay since the coordinator cannot produce new valid ciphertexts without the private key.

### 23.1 Authorization Plugin Interface

By default, any replica with a valid JWT for a namespace can push and pull any mutation in that namespace. For finer-grained control, the coordinator supports an authorization plugin interface.

#### The Authz Interface

When a replica pushes a mutation or attempts a pull, the coordinator calls the configured authorization plugin:

```rust
#[async_trait]
pub trait AuthorizationPlugin: Send + Sync {
    /// Authorize a mutation push.
    async fn authorize_push(
        &self,
        replica_id: &str,
        namespace: &str,
        mutation_metadata: &MutationMetadata,
    ) -> Result<AuthzDecision, AuthzError>;

    /// Authorize a pull request.
    async fn authorize_pull(
        &self,
        replica_id: &str,
        namespace: &str,
    ) -> Result<AuthzDecision, AuthzError>;
}

pub enum AuthzDecision {
    Allow,
    Deny(String), // Reason for denial
}

pub struct MutationMetadata {
    pub doc_id: String,       // e.g., "todos"
    pub record_id: String,    // e.g., "user-123:todo-456"
    pub mutation_type: String,// "CRDT_UPDATE", "CRDT_INSERT", "CRDT_DELETE"
}
```

#### The E2EE Constraint

**CRITICAL:** Because VaultSync uses End-to-End Encryption, the coordinator cannot read the contents of the mutation. The `yrs_update` and the affected field names/values are inside the `encrypted_blob`.

Therefore, **authorization can only be performed on routing metadata**. You cannot write an authorization rule like:
❌ *"Deny if `priority > 5`"*
❌ *"Deny if updating the `role` field"*

You can only write rules like:
✅ *"Deny if replica does not own the namespace"*
✅ *"Deny if `doc_id` is 'system_settings'"*
✅ *"Deny if `record_id` does not start with `user-${replica_id}:`"*

If field-level authorization is required, those fields must be extracted from the CRDT document and placed in the routing metadata (like `record_id`) before encryption.

#### Coordinator Enforcement

1. **On Push Deny:** The mutation is rejected. The coordinator returns `{"error": "FORBIDDEN", "reason": "<reason>"}` in the `PUSH_ACK`. The client marks the mutation as `failed` (permanent failure, no retry).
2. **On Pull Deny:** The `PULL` request is rejected with `FORBIDDEN`.
3. **Redaction:** Because mutations are E2EE encrypted blobs, the coordinator cannot "redact" specific fields from a mutation before sending it to a reader. It must allow or deny the entire blob.

---

## 24. Integrations (Optional Plugins)

### Aegis (Authorization)

Every sync operation can be authorized by Aegis before the coordinator applies it. This is an **optional plugin** — not required for VaultSync to function.

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

Arc workflows handle complex sync-related background tasks — operations that are too involved for VaultSync's built-in retry logic.

```typescript
arc.workflow("vaultsync-conflict-notification", async (ctx) => {
  // CRDTs mean no conflicts, but you may want to notify on certain merge events
  const { docId, recordId, changedFields } = ctx.payload
  await ctx.step("notify", () => notifyUsers(docId, recordId, changedFields))
})
```

### FluxBus (Event Bus)

CRDT mutations can optionally emit FluxBus events for system-wide reactivity.

```typescript
vaultsync.schema.define("todos", {
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
- [ ] React hooks (`@vaultsync/react`)
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
- [ ] `vaultsync inspect` CLI tool
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
- [ ] VaultSync Cloud dashboard (optional managed UI)
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
VaultSync encrypts and uploads mut: INSERT todos/todo:123
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
  → VaultSync reconnects to coordinator
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

## 27. Multi-Namespace & Multi-Tenant Architecture

VaultSync isolates data by `namespace`. The spec generally describes one replica syncing one namespace. This section defines how applications handle multiple namespaces concurrently.

### Client-Side: Multiple Namespaces per Device

A user may belong to multiple workspaces or projects, each mapped to a distinct VaultSync namespace (e.g., `workspace:design` and `workspace:engineering`).

#### Storage Isolation

All namespaces on a device share the same underlying local storage (e.g., one SQLite file or OPFS directory) to minimize overhead. Data is isolated via the `namespace` column in the `vaultsync_oplog`, `vaultsync_sync_state`, and `vaultsync_documents` tables.

#### Client Instantiation

The application creates one `VaultSync` instance per namespace:

```typescript
const designSync = new VaultSync({ namespace: "workspace:design", storage: "sqlite" })
const engSync = new VaultSync({ namespace: "workspace:engineering", storage: "sqlite" })
```

#### Connection Multiplexing

To avoid opening N WebSocket connections for N namespaces, VaultSync multiplexes namespaces over a single physical WebSocket connection to the coordinator:

1. The underlying transport maintains one WSS connection per coordinator host.
2. The `AUTH` message authenticates the connection.
3. The client sends multiple `REGISTER` messages over the single connection, one per namespace.
4. All `PUSH`, `PULL`, and `MUTATION_PUSH` frames include the `namespace` field to route data appropriately.

#### Leader Election Sharing

In a multi-tab environment, the leader election lock is acquired **per storage backend**, not per namespace. If Tab A wins the lock for the SQLite file, it becomes the leader for *all* namespaces stored in that file.

### Server-Side: Headless Replicas

A backend service (e.g., a Node.js worker) may need to act as a replica for thousands of user namespaces simultaneously to bridge VaultSync data to a legacy system or perform AI processing.

#### The "Super-Replica" Pattern

A backend service initializes VaultSync with a super-token and dynamically registers namespaces:

```typescript
const serverVaultSync = new VaultSyncServer({
  storage: "postgres", // Server uses Postgres for its local replica storage
  coordinator: "wss://coordinator.internal"
})

// Dynamically attach to namespaces as users log in or events occur
await serverVaultSync.attachNamespace("user:123", serverKeypair)
await serverVaultSync.attachNamespace("user:456", serverKeypair)
```

#### Server Performance Considerations

- **Oplog batching:** The server-side storage implementation batches CRDT writes across namespaces to maximize throughput.
- **Memory limits:** Yrs documents (the CRDT cache) are kept in memory only for actively syncing namespaces. Inactive namespaces are evicted from memory and reloaded from storage when a new mutation arrives.
- **Key management:** The server maintains its own `server_sk` and registers its `server_pk` in every namespace it joins. When a user creates a namespace, they must encrypt the namespace symmetric key for the server's public key to allow the server to decrypt mutations.

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

*VaultSync — Conflict-free, encrypted, infrastructure-agnostic synchronization for the local-first era.*
*MIT License — Open source. Auditable. No black boxes.*
