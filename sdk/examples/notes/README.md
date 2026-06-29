# VaultSync Notes — Local-First Workspace

A professional, offline-first notes application demonstrating the **VaultSync** synchronization engine.

This application is built with **React**, **Vite**, and **TypeScript**, showcasing all core features of VaultSync running simultaneously in a real-world user workspace.

---

## ⚡ What it Demonstrates

* **Local-First Architecture:** Open, edit, and search notes instantly. All reads and writes are performed against the local CRDT database, meaning zero spinner states and instant feedback.
* **Seamless Offline-First Sync:** Disconnect from the network, make edits, and reconnect. Mutations are queued in the local oplog and sync automatically when connection is restored.
* **Multi-Tab Leader Election:** Open the app in multiple browser tabs on the same namespace. One tab is elected as the "Leader" to handle coordinator communication, while other tabs ("Followers") instantly sync via shared state.
* **End-to-End Encryption (E2EE) with Key Rotation:** All data is encrypted client-side using WebCrypto APIs before syncing. The coordinator only sees encrypted blobs. Rotate keys instantly with a single click from the UI.
* **Real-Time Presence & Status:** Live display of sync status (connected, offline, syncing), active peer count (from BroadcastChannel heartbeat), pending mutation counters, and E2EE key versions.
* **Built-in Metrics:** Monitor push mutations received, snapshots applied, optimistic writes, and active peers — accessible via `vaultsync.metrics()`.

---

## 🏗️ Architecture

```
                       BROWSER WORKSPACE (Local Device)
 ┌───────────────────────────────────────────────────────────────────────────┐
 │                                                                           │
 │   Multiple Browser Tabs (Followers)     Main Browser Tab (Leader)         │
 │  ┌──────────────────────────────┐      ┌────────────────────────────────┐ │
 │  │        React UI App          │      │        React UI App            │ │
 │  │  useQuery() useSyncStatus()  │      │  useQuery() useSyncStatus()   │ │
 │  └──────────────┬───────────────┘      └──────────────────┬─────────────┘ │
 │                 │ BroadcastChannel                        │               │
 │                 └─────────────────────────────────────────┼───────┐       │
 │                                                           │       │       │
 │                                                           ▼       ▼       │
 │                                          ┌────────────────────────────┐   │
 │                                          │      VaultSyncClient       │   │
 │                                          │        (WASM Core)         │   │
 │                                          │  ┌──────────────────────┐  │   │
 │                                          │  │  HybridLogicalClock  │  │   │
 │                                          │  │  (HLC — CAS-based)   │  │   │
 │                                          │  └──────────────────────┘  │   │
 │                                          │  ┌──────────────────────┐  │   │
 │                                          │  │   TransportRouter    │  │   │
 │                                          │  │  BC + WS merge       │  │   │
 │                                          │  └──────────────────────┘  │   │
 │                                          │  ┌──────────────────────┐  │   │
 │                                          │  │   MutationStore      │  │   │
 │                                          │  │  (OPFS-backed queue) │  │   │
 │                                          │  └──────────────────────┘  │   │
 │                                          │  ┌──────────────────────┐  │   │
 │                                          │  │   PresenceManager   │  │   │
 │                                          │  │  (BC heartbeat)     │  │   │
 │                                          │  └──────────────────────┘  │   │
 │                                          └──────────┬─────────────────┘   │
 │                                                     │                     │
 │                                                     ▼                     │
 │                                            Local IndexedDB / OPFS         │
 │                                            (Encrypted SQLite)             │
 └──────────────────────────────────────────────────────┬────────────────────┘
                                                        │
                                                        │ Secure WebSockets (TLS)
                                                        ▼
                                             ┌───────────────────────┐
                                             │  VaultSync Coordinator│
                                             │   (Memory/Postgres)   │
                                             └───────────────────────┘
```

---

## 🚀 Quick Start

### 1. Build the SDK
Ensure you have built the WebAssembly core and JS packages from the SDK root directory first:
```bash
# From the sdk root directory:
npm run build
```

### 2. Start the Coordinator Server
Start the local Rust-based coordinator server (compiled from crates):
```bash
# Run from repository root:
cargo run -p vaultsync-coordinator-server -- --backend memory --port 9876
```

### 3. Run the Notes App
```bash
# From sdk/examples/notes:
npm run dev
```
Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## 🧪 Testing Multi-Tab & E2E Sync

To test multi-tab synchronization and real-time updates:
1. Open [http://localhost:5173/?ns=test-workspace&replica=tab-a](http://localhost:5173/?ns=test-workspace&replica=tab-a)
2. Open a second window next to it at [http://localhost:5173/?ns=test-workspace&replica=tab-b](http://localhost:5173/?ns=test-workspace&replica=tab-b)
3. Write a note in the first window and watch it sync to the second window instantly!
4. Observe the SyncBar in each window — it shows the active peer count, confirming real-time presence detection via BroadcastChannel heartbeat.

---

## 🛠️ Code Walkthrough

### 1. Wrapping your App with `VaultSyncProvider`
Configure and supply the client context to your component tree:
```tsx
import { VaultSyncProvider } from '@vaultsync/react';

const config = {
  namespace: 'user-workspace-id',
  replicaId: 'device-unique-id',
  coordinatorUrl: 'http://localhost:9876',
};

export default function App() {
  return (
    <VaultSyncProvider config={config}>
      <MainLayout />
    </VaultSyncProvider>
  );
}
```

> **Note:** Presence is enabled automatically via an internal BroadcastChannel. No extra configuration is needed — the `PresenceManager` starts on client initialization and exposes peer state through `vaultsync.presence().activePeers()`.

### 2. Querying Collections
Bind data reactively to your UI:
```tsx
import { useQuery } from '@vaultsync/react';

export function NoteList() {
  const { data: notes, loading, error } = useQuery('notes');

  if (loading) return <div>Loading...</div>;

  return (
    <ul>
      {notes.map(note => (
        <li key={note.record_id}>{note.title}</li>
      ))}
    </ul>
  );
}
```

### 3. Performing Mutations
Insert, update, and delete records seamlessly:
```tsx
import { useVaultSyncMutations } from '@vaultsync/react';

export function NoteEditor({ noteId }) {
  const { update } = useVaultSyncMutations('notes');

  const handleTitleChange = (newTitle: string) => {
    update(noteId, {
      title: newTitle,
      updatedAt: Date.now(),
    });
  };
}

### 4. Accessing Presence & Metrics

The VaultSync client exposes APIs for monitoring real-time collaboration and engine health:

```tsx
import { useVaultSync } from '@vaultsync/react';

function SyncBar() {
  const vaultsync = useVaultSync();
  const peers = vaultsync.presence().activePeers();
  const metrics = vaultsync.metrics().snapshot();

  return (
    <div className="sync-bar">
      <span>Active peers: {peers.length}</span>
      <span>Push mutations: {metrics.pushMutationsReceived}</span>
      <span>Snapshots applied: {metrics.snapshotsApplied}</span>
      <span>Optimistic writes: {metrics.optimisticWrites}</span>
    </div>
  );
}
```
```
