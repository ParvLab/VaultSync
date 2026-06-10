# @vaultsync/next

> **VaultSync** — Next.js integration for VaultSync. Build seamless server-rendered and hydrated local-first applications.

[![npm version](https://img.shields.io/npm/v/@vaultsync/next)](https://www.npmjs.com/package/@vaultsync/next)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

`@vaultsync/next` provides Next.js integration helpers, supporting both server-side rendering (SSR) data hydration on the client and hosting sync Route Handlers on the server.

---

## Features

- 🌊 **Hydration Support** — Hydrate client-side state seamlessly using initial data queried on the server.
- ⚡ **Server-Side Queries** — Query local SQLite databases directly in React Server Components (RSC) or Server Actions via `@vaultsync/node`.
- 📡 **API Route Handler** — Quickly turn any Next.js Route Handler into a VaultSync Sync Coordinator.

---

## Installation

```bash
npm install @vaultsync/web @vaultsync/react @vaultsync/next
```

---

## Quick Start

### 1. Server-Side Data Fetching (App Router)

Fetch data on the server and pass it to the hydration provider so client-side components render immediately without loading indicators.

```tsx
// app/todos/page.tsx (React Server Component)
import { queryServerDatabase } from '@vaultsync/next/server';
import ClientTodosPage from './ClientTodosPage';

export default async function Page() {
  // Query server-side database directly
  const initialTodos = await queryServerDatabase(
    './data/vaultsync.db',
    'workspace:production',
    'todos'
  );

  const initialData = {
    todos: initialTodos,
  };

  const config = {
    namespace: 'workspace:production',
    replicaId: 'client-device-id', // Retrieve from cookie or session in real app
    coordinatorUrl: 'https://api.example.com/sync',
  };

  return (
    <ClientTodosPage config={config} initialData={initialData} />
  );
}
```

### 2. Client Hydration & Live Sync

Use the `VaultSyncHydrationProvider` and `useNextQuery` hook to display pre-fetched server data instantly, and then seamlessly switch to live local-first subscriptions once the client-side database loads.

```tsx
// app/todos/ClientTodosPage.tsx (Client Component)
'use client';

import { VaultSyncHydrationProvider, useNextQuery } from '@vaultsync/next';

export default function ClientTodosPage({ config, initialData }) {
  return (
    <VaultSyncHydrationProvider config={config} initialData={initialData}>
      <TodoList />
    </VaultSyncHydrationProvider>
  );
}

function TodoList() {
  // Renders initialData.todos immediately, then subscribes to updates on client database load
  const { data: todos, loading } = useNextQuery('todos');

  return (
    <div>
      {loading && <div>Synchronizing database...</div>}
      <ul>
        {todos.map((todo: any) => (
          <li key={todo.id}>{todo.text}</li>
        ))}
      </ul>
    </div>
  );
}
```

### 3. Creating a Sync Coordinator Route Handler

Host a central synchronization coordinator API directly inside your Next.js application:

```typescript
// app/api/sync/route.ts
import { NextRequest } from 'next/server';
import { createCoordinatorHandler } from '@vaultsync/next/server';
import { InMemoryCoordinator } from '@vaultsync/node';

// Replace with a real persistent coordinator (e.g. PostgresCoordinator, RedisCoordinator)
const coordinator = new InMemoryCoordinator();

const handler = createCoordinatorHandler(coordinator, {
  validateToken: async (namespace, token) => {
    // Perform authentication/token validation here
    return token === 'secret-session-token';
  }
});

export const GET = handler.GET;
export const POST = handler.POST;
```

---

## API Reference

### Client-side API

#### `<VaultSyncHydrationProvider config={config} initialData={initialData} />`
Wraps the application. Populates the hydration context with pre-fetched server data, enabling client-side hooks to render it instantly while the client `VaultSyncClient` initializes in the background.

#### `useHydratedData(docId)`
Retrieve the raw hydrated data passed from the server for a specific document ID.

#### `useNextQuery(docId)`
Returns `{ data, loading }`. Uses the hydrated server data on initial load, then registers a real-time reactive subscriber once the database finishes initializing.

---

### Server-side API (`@vaultsync/next/server`)

#### `queryServerDatabase(dbPath, namespace, docId)`
Queries a local SQLite database directly on the server. Shuts down the client connection safely when done. Ideal for Server Components and Server Actions.

#### `createCoordinatorHandler(coordinator, options?)`
Creates a standard Next.js route handler for both `GET` and `POST` methods that handles HTTP polling sync requests (`push`, `pull`, `register`, `heartbeat`).

---

## License

Apache 2.0 © [VaultSync Authors](https://github.com/parv68)
