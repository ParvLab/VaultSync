# @vaultsync/node

> **VaultSync** — High-performance Node.js SDK for local-first database replication, powered by native Rust/NAPI bindings.

[![npm version](https://img.shields.io/npm/v/@vaultsync/node)](https://www.npmjs.com/package/@vaultsync/node)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

`@vaultsync/node` is the Server and Node.js SDK for **VaultSync**. It utilizes high-performance native Rust bindings (via NAPI) for ultra-fast, multi-process-safe operations, SQLite storage, and secure synchronization in backend environments.

---

## Features

- ⚡ **Native Performance** — Multi-threaded Rust core bound directly via NAPI-RS.
- 💾 **SQLite Storage** — Durable local-first persistent storage out of the box.
- 🔒 **End-to-End Encryption** — Cryptographically secure local data-at-rest and sync payload encryption.
- 📡 **Real-time Sync** — Seamless background sync with central coordinators.
- 👥 **Multi-Process Safe** — Automatic file-locking coordinates access across multiple Node.js workers or processes.

---

## Installation

```bash
npm install @vaultsync/node
```

> **Note:** This package requires the native compiled binary (`vaultsync_napi.node`). It will automatically scan standard locations (including build outputs and release target folders) to find and load it.

---

## Quick Start

```typescript
import { VaultSyncClient, isNativeAvailable } from '@vaultsync/node';

// Check if native bindings loaded successfully
if (!isNativeAvailable()) {
  console.warn("Native addon not loaded. Please verify build target.");
}

// Create a node client
const client = await VaultSyncClient.create({
  namespace: 'workspace:production',
  replicaId: 'server-replica-1',
  storagePath: './data/vaultsync.db', // Path to the local database file
  // Optional sync settings:
  // coordinatorUrl: 'https://sync.example.com',
  // authToken: 'your-secure-jwt',
});

// Insert a record
await client.insert('items', 'item-001', {
  name: 'Backend Config',
  active: true,
  updatedAt: Date.now()
});

// Read the record
const configItem = await client.get('items', 'item-001');
console.log(configItem); // { name: 'Backend Config', active: true, ... }

// Query all records in a collection
const items = await client.find('items');

// Subscribe to real-time updates
const unsubscribe = client.subscribe('items', (recordId, fields) => {
  console.log(`Document changed: ${recordId}`, fields);
});

// Graceful shutdown
await client.shutdown();
unsubscribe();
```

---

## API Reference

### `VaultSyncClient.create(config)`

Creates and initializes a new Node.js client.

```typescript
const client = await VaultSyncClient.create(config: VaultSyncConfig);
```

#### `VaultSyncConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `namespace` | `string` | ✅ | Logical database namespace. |
| `replicaId` | `string` | ✅ | Unique identifier for this replica. |
| `storagePath` | `string` | ❌ | Path to local SQLite file. Defaults to memory-only if not specified. |
| `coordinatorUrl` | `string` | ❌ | URL of the central sync coordinator. |
| `authToken` | `string` | ❌ | Bearer token for authenticating sync requests. |

---

### `client.insert(docId, recordId, fields)`
Insert a record into a collection.
```typescript
await client.insert('configs', 'cfg-1', { debugMode: true });
```

### `client.update(docId, recordId, fields)`
Updates fields of a record (performs merge).
```typescript
await client.update('configs', 'cfg-1', { debugMode: false });
```

### `client.delete(docId, recordId)`
Deletes a record.
```typescript
await client.delete('configs', 'cfg-1');
```

### `client.get(docId, recordId)`
Fetch a single record by ID.
```typescript
const doc = await client.get('configs', 'cfg-1');
```

### `client.find(docId)`
Fetch all records from a document collection.
```typescript
const allDocs = await client.find('configs');
```

### `client.subscribe(docId, callback)`
Subscribe to real-time changes of a document collection.
```typescript
const unsub = client.subscribe('configs', (id, data) => {
  console.log("Updated config:", id, data);
});
```

---

## License

Apache 2.0 © [VaultSync Authors](https://github.com/parv68)
