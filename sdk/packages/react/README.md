# @vaultsync/react

> **VaultSync** — React integration for VaultSync. Build real-time, local-first reactive UIs with hooks.

[![npm version](https://img.shields.io/npm/v/@vaultsync/react)](https://www.npmjs.com/package/@vaultsync/react)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

`@vaultsync/react` provides hooks, context, and provider wrappers to connect your React application to the high-performance local-first sync capabilities of **VaultSync**.

---

## Features

- ⚛️ **Reactive State** — Components re-render automatically when local database updates occur.
- 🪝 **Convenient Hooks** — Clear hooks for querying collections (`useQuery`), single documents (`useVaultSyncOne`), mutation handlers (`useVaultSyncMutations`), and tracking status (`useSyncStatus`).
- 📡 **Offline State Awareness** — Visual component hooks to easily display online/offline sync indicators.
- 🔒 **Secure-By-Default** — Automatically inherits E2EE client encryption under the hood.
- 👥 **Presence awareness** — Built-in peer tracking; use `activePeers` from `useSyncStatus()`.

---

## Installation

```bash
npm install @vaultsync/web @vaultsync/react
```

---

## Quick Start

### 1. Setup the Provider

Wrap your application in `VaultSyncProvider` and pass the client configuration:

```tsx
import React from 'react';
import ReactDOM from 'react-dom/client';
import { VaultSyncProvider } from '@vaultsync/react';
import App from './App';

const config = {
  namespace: 'todo-app',
  replicaId: crypto.randomUUID(),
  coordinatorUrl: 'wss://sync.example.com',
  authToken: 'user-jwt-token'
};

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <VaultSyncProvider config={config}>
      <App />
    </VaultSyncProvider>
  </React.StrictMode>
);
```

### 2. Query and Mutate Data in Components

```tsx
import { useQuery, useVaultSyncMutations, useSyncStatus, SyncIndicator } from '@vaultsync/react';

export default function TodoList() {
  const { data: todos, loading, error } = useQuery('todos');
  const { insert, update, delete: remove } = useVaultSyncMutations('todos');
  const sync = useSyncStatus();

  if (loading) return <div>Loading database...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      <header style={{ display: 'flex', justifyContent: 'space-between' }}>
        <h1>My Tasks</h1>
        <SyncIndicator showPeers />
      </header>

      <button onClick={() => insert(crypto.randomUUID(), { text: 'New Task', done: false })}>
        Add Task
      </button>

      <ul>
        {todos.map((todo: any) => (
          <li key={todo.id}>
            <input
              type="checkbox"
              checked={todo.done}
              onChange={() => update(todo.id, { done: !todo.done })}
            />
            <span style={{ textDecoration: todo.done ? 'line-through' : 'none' }}>
              {todo.text}
            </span>
            <button onClick={() => remove(todo.id)}>Delete</button>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

---

## API Reference

### `<VaultSyncProvider config={config} />`

React context provider that instantiates and manages the lifecycle of the `VaultSyncClient`. Shows a loading fallback while initialization (WASM compilation & IndexedDB loading) takes place.

### `useVaultSyncClient()`

Access the raw, underlying `VaultSyncClient` instance directly. Useful for lower-level operations, keys, and manual transactions.

```typescript
const client = useVaultSyncClient();
```

### `useQuery(docId, options?)`

Reactive hook that queries a collection and subscribes to updates. Re-runs queries only when documents in the collection are mutated.

```typescript
const { data, loading, error } = useQuery('todos', {
  filter: (todo) => !todo.done // optional frontend filter function
});
```

### `useVaultSyncOne(docId, recordId)`

Reactive hook to get and subscribe to a single document record.

```typescript
const { data: profile, loading } = useVaultSyncOne('users', 'user-123');
```

### `useVaultSyncMutations(docId)`

Returns helpers to perform CRUD mutations on a collection.

```typescript
const { insert, update, delete: remove } = useVaultSyncMutations('todos');

// Signature:
// insert(recordId: string, fields: RecordFields) => Promise<void>
// update(recordId: string, fields: RecordFields) => Promise<void>
// delete(recordId: string) => Promise<void>
```

### `useSyncStatus()`

Access the sync progress state.

```typescript
const { connected, pendingMutations, lastSyncedSequence, activePeers } = useSyncStatus();
```

| Field | Description |
|-------|-------------|
| `connected` | `boolean` — `true` when connected to the sync coordinator |
| `pendingMutations` | `number` — Count of local writes not yet confirmed by the server. Includes **optimistic writes** (local mutations that have been applied but not yet uploaded). |
| `lastSyncedSequence` | `number` — The last sequence number confirmed by the server |
| `activePeers` | `number` — Number of other peers currently connected to the same namespace (requires presence enabled on the coordinator) |

### `<SyncIndicator showPeers />`

Renders a compact sync status badge. Shows connection state, pending mutation count, E2EE key version, and — when `showPeers` is `true` — the number of active peers.

```tsx
<SyncIndicator />           // connection + pending + E2EE key version
<SyncIndicator showPeers />  // also shows active peer count
```

---

## License

Apache 2.0 © [VaultSync Authors](https://github.com/parv68)
