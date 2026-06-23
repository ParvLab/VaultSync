# @vaultsync/web

> **VaultSync** — Local-first, encrypted, real-time sync database for the browser.

[![npm version](https://img.shields.io/npm/v/@vaultsync/web)](https://www.npmjs.com/package/@vaultsync/web)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.4+-blue)](https://www.typescriptlang.org/)

`@vaultsync/web` is the core browser SDK for **VaultSync**. It is powered by a high-performance WebAssembly engine (Rust + WASM), giving you a fully local-first database in the browser with optional end-to-end encrypted sync across tabs and devices — no backend required to get started.

---

## Features

- 🦀 **WASM-powered engine** — Rust compiled to WebAssembly for near-native performance
- 🔒 **End-to-end encryption** — All data encrypted client-side before sync
- 📡 **Real-time sync** — Live subscription to document changes across tabs and devices
- 🌐 **Offline-first** — Works completely offline; syncs when reconnected
- 🔀 **CRDT-based** — Automatic conflict-free merge — no conflict resolution code needed
- 💾 **Persistent storage** — Data stored via IndexedDB / OPFS, survives page reloads
- 🔑 **Key management** — Built-in encryption key rotation via `KeyManager`
- 👥 **Presence** — Real-time peer awareness across tabs via BroadcastChannel
- 📊 **Metrics & Observability** — Built-in counters for mutations, snapshots, optimistic writes, HLC wraps, active peers

---

## Installation

```bash
npm install @vaultsync/web
```

> **Note:** This package ships with a `.wasm` binary in the `wasm/` folder. Your bundler (Vite, Webpack, etc.) must be configured to handle `.wasm` files. Vite handles this automatically with no extra config.

---

## Quick Start

```typescript
import { VaultSyncClient } from '@vaultsync/web';

// Create a local-first client
const client = await VaultSyncClient.create({
  namespace: 'my-app',
  replicaId: crypto.randomUUID(),
  // Optional: connect to a sync coordinator
  // coordinatorUrl: 'https://your-coordinator.example.com',
  // authToken: 'your-token',
});

// Insert a record
await client.insert('todos', 'todo-1', {
  title: 'Buy groceries',
  done: false,
  createdAt: Date.now(),
});

// Query a document
const todos = await client.find('todos');
console.log(todos); // [{ title: 'Buy groceries', done: false, ... }]

// Get a single record
const todo = await client.get('todos', 'todo-1');

// Update a record
await client.update('todos', 'todo-1', { done: true });

// Delete a record
await client.delete('todos', 'todo-1');

// Subscribe to real-time changes
const unsubscribe = client.subscribe('todos', (recordId, fields) => {
  console.log('Changed:', recordId, fields);
});

// Check sync status
const status = await client.syncStatus();
// { connected: true, pendingMutations: 0, lastSyncedSequence: 42 }

// Check if this tab is the sync leader
const isLeader = client.isLeader();

// Graceful shutdown
await client.shutdown();
unsubscribe();
```

---

## API Reference

### `VaultSyncClient.create(config)`

Creates and initializes a new VaultSync client. Loads and initializes the WebAssembly module on first call.

```typescript
const client = await VaultSyncClient.create(config: VaultSyncConfig);
```

#### `VaultSyncConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `namespace` | `string` | ✅ | Logical database namespace. Separate apps/tenants should use different namespaces. |
| `replicaId` | `string` | ✅ | Unique ID for this client device/tab. Use `crypto.randomUUID()`. |
| `coordinatorUrl` | `string` | ❌ | URL of the VaultSync coordinator server for multi-device sync. |
| `authToken` | `string` | ❌ | Bearer token sent to the coordinator for authentication. |
| `storageBackend` | `'opfs' \| 'indexeddb'` | ❌ | Local storage backend. Defaults to `'indexeddb'`. |

---

### `client.insert(docId, recordId, fields)`

Insert a new record into a document (collection).

```typescript
await client.insert('users', 'user-123', { name: 'Alice', age: 30 });
```

---

### `client.update(docId, recordId, fields)`

Update fields on an existing record. This is a **merge** operation — only specified fields are changed.

```typescript
await client.update('users', 'user-123', { age: 31 });
```

---

### `client.delete(docId, recordId)`

Delete a record from a document.

```typescript
await client.delete('users', 'user-123');
```

---

### `client.get(docId, recordId)`

Fetch a single record by ID. Returns `null` if not found.

```typescript
const user = await client.get('users', 'user-123');
// { name: 'Alice', age: 31 } | null
```

---

### `client.find(docId)`

Fetch all records in a document.

```typescript
const users = await client.find('users');
// [{ name: 'Alice', age: 31 }, ...]
```

---

### `client.subscribe(docId, callback)`

Subscribe to real-time changes on a document. Returns an `unsubscribe` function.

```typescript
const unsubscribe = client.subscribe('users', (recordId, fields) => {
  console.log(`Record ${recordId} changed:`, fields);
});

// Later:
unsubscribe();
```

---

### `client.syncStatus()`

Returns the current sync state.

```typescript
const status = await client.syncStatus();
// {
//   connected: boolean,              // Is the coordinator connection active?
//   pendingMutations: number,        // How many local writes are unsynced?
//   lastSyncedSequence: number,      // Last confirmed server sequence
//   optimisticWrites: number,        // Optimistic writes pending confirmation
//   activePeers: number,             // Active peers across tabs
//   pushMutationsReceived: number,   // Push mutations received via coordinator
//   snapshotsApplied: number,        // Snapshots applied during catch-up
//   hlcLogicalWraps: number          // HLC logical counter wraps
// }
```

---

### `client.metrics()`

Returns a `MetricsSnapshot` JSON string with all recorded counter values.

```typescript
const metrics = JSON.parse(await client.metrics());
// {
//   mutations_downloaded: number,
//   mutations_uploaded: number,
//   optimistic_writes: number,
//   push_mutations_received: number,
//   snapshots_applied: number,
//   hlc_logical_wraps: number,
//   active_peers: number,
//   sync_errors: number,
//   mutations_failed: number
// }
```

---

### `client.presence()`

Returns an optional `PresenceManager` instance for multi-tab peer awareness.

```typescript
const presence = client.presence();

if (presence) {
  const count = presence.peer_count();
  console.log('Active peers:', count);

  const peersJson = presence.active_peers_json();
  // '{"replica-1": 1719000000000, "replica-2": 1719000000123}'
}
```

The `PresenceManager` is created automatically during client initialization and uses a dedicated BroadcastChannel (`vaultsync-presence-{namespace}`). It is available only in browser environments that support `BroadcastChannel`.

#### `PresenceManager` methods

| Method | Returns | Description |
|---|---|---|
| `peer_count()` | `number` | Number of currently active peers |
| `active_peers_json()` | `string` | JSON map of `replica_id` → `last_seen_epoch_ms` |

---

### `client.isLeader()`

Returns `true` if this tab/replica is currently the sync leader. Only the leader tab communicates with the coordinator.

```typescript
const isLeader = client.isLeader();
```

---

### `client.keys` — `KeyManager`

Access the encryption key manager for manual E2EE key operations.

```typescript
// Generate a new encryption key
await client.keys.generateKey('my-key-id');

// Rotate to a new key
await client.keys.rotateKey();

// List all key versions
const keys = await client.keys.listKeys();
```

---

### `client.db` — Collection Proxy

A type-safe proxy for collection access using dot notation:

```typescript
// Equivalent to client.find('users')
const users = await client.db.users.find();

// Equivalent to client.insert('users', 'u1', { name: 'Bob' })
await client.db.users.insert('u1', { name: 'Bob' });
```

---

## Schema Validation (Optional)

VaultSync includes optional schema definition utilities for runtime field validation:

```typescript
import { defineSchema } from '@vaultsync/web';

const schema = defineSchema({
  users: {
    name: { type: 'string', required: true },
    age: { type: 'number' },
    active: { type: 'boolean', default: true },
  }
});
```

---

## Coordinators

For multi-device or multi-user sync, connect a coordinator:

```typescript
import { PostgresCoordinator } from '@vaultsync/web';

// Server-side: Initialize coordinator
const coordinator = new PostgresCoordinator({ connectionString: process.env.DATABASE_URL });

// Client-side: Point the client at it
const client = await VaultSyncClient.create({
  namespace: 'my-app',
  replicaId: crypto.randomUUID(),
  coordinatorUrl: 'https://api.example.com/sync',
  authToken: sessionToken,
});
```

Built-in coordinators:
- **`PostgresCoordinator`** — PostgreSQL-backed server coordinator
- **`RedisCoordinator`** — Redis-backed coordinator for high-throughput
- **`CustomCoordinator`** — Implement your own coordinator interface

---

## Presence

Multi-tab presence is achieved via a dedicated **BroadcastChannel** (`vaultsync-presence-{namespace}`) separate from the mutation transport channel. Each tab sends periodic heartbeat messages (Join / Heartbeat / Leave) to announce its presence to other tabs.

- **Join** — Sent on client initialization to announce a new replica
- **Heartbeat** — Sent every 30s to maintain active status
- **Leave** — Sent on `beforeunload` to immediately signal departure

This channel is distinct from the mutation BroadcastChannel to avoid message type confusion. The `PresenceManager` is only available in environments that support `BroadcastChannel`; in other environments `client.presence()` returns `undefined`.

---

## Metrics & Observability

VaultSync automatically records internal counters during sync operations:

- **Mutations downloaded / uploaded** — Total mutations synced
- **Optimistic writes** — Locally applied writes awaiting server confirmation
- **Push mutations received** — Remote mutations pushed via the coordinator
- **Snapshots applied** — Full-state snapshots applied during catch-up
- **HLC logical wraps** — Hybrid Logical Clock wraps (tracks clock drift)
- **Active peers** — Concurrent tabs/peers in the same namespace
- **Sync errors / mutations failed** — Error counters for diagnostics

Call `client.metrics()` to snapshot all counters as a JSON string. For React applications, `useSyncStatus()` includes metric fields (`optimisticWrites`, `activePeers`, `pushMutationsReceived`, `snapshotsApplied`, `hlcLogicalWraps`) directly in the status object.

---

## Bundler Setup

### Vite (Recommended)

No extra configuration needed. Vite handles `.wasm` files natively.

### Webpack 5

```js
// webpack.config.js
module.exports = {
  experiments: {
    asyncWebAssembly: true,
  },
};
```

---

## License

Apache 2.0 © [Parv Labs](https://github.com/parv68)
