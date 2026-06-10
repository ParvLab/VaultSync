import React, { useState } from 'react';
import { VaultSyncProvider, useQuery, useVaultSyncClient, useSyncStatus } from '@vaultsync/react';
import type { RecordFields } from '@vaultsync/web';

function TodoApp({ replicaName }: { replicaName: string }) {
  const client = useVaultSyncClient();
  const { data: todos, loading } = useQuery('todos');
  const status = useSyncStatus();
  const [text, setText] = useState('');

  const addTodo = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!text.trim()) return;

    const todoId = `todo-${Date.now()}`;
    await client.insert('todos', todoId, {
      text: text.trim(),
      completed: false,
    });
    setText('');
  };

  const deleteTodo = async (todoId: string) => {
    await client.delete('todos', todoId);
  };

  return (
    <div className="panel">
      <h2>Replica {replicaName}</h2>
      
      <div className="status-bar">
        <div className={`status-dot ${status.connected ? 'connected' : 'disconnected'}`} />
        <span>{status.connected ? 'Connected' : 'Offline'}</span>
        <span style={{ marginLeft: 'auto', fontSize: '0.9rem', opacity: 0.7 }}>
          Pending: {status.pendingMutations}
        </span>
      </div>

      <form onSubmit={addTodo} className="input-group">
        <input
          type="text"
          placeholder="What needs to be done?"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
        <button type="submit">Add</button>
      </form>

      {loading ? (
        <div>Loading todos...</div>
      ) : (
        <ul className="todo-list">
          {todos.map((todo: RecordFields) => (
            <li key={todo.record_id as string || String(Math.random())} className="todo-item">
              <span className="todo-text">{todo.text as string}</span>
              <button
                className="todo-delete"
                onClick={() => deleteTodo(todo.record_id as string)}
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default function App() {
  const configA = {
    namespace: 'collab-demo',
    replicaId: 'replica-A',
    coordinatorUrl: 'http://127.0.0.1:8080',
  };

  const configB = {
    namespace: 'collab-demo',
    replicaId: 'replica-B',
    coordinatorUrl: 'http://127.0.0.1:8080',
  };

  return (
    <div className="container">
      <h1>VaultSync Collaborative Sync Demo</h1>
      <p style={{ opacity: 0.8, marginBottom: '2rem' }}>
        This demo spawns two independent sync engine instances on the same page.
        Ensure the VaultSync Coordinator Server is running on port 8080 (`cargo run -p vaultsync-coordinator-server -- -p 8080`).
      </p>

      <div className="grid">
        <VaultSyncProvider config={configA}>
          <TodoApp replicaName="A" />
        </VaultSyncProvider>

        <VaultSyncProvider config={configB}>
          <TodoApp replicaName="B" />
        </VaultSyncProvider>
      </div>
    </div>
  );
}
