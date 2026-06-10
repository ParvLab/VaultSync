import React, { useState, useEffect } from 'react';
import { useQuery, useDriftMutations, SyncIndicator } from '@drift/react';
import { Drift } from '@drift/web';

interface Todo {
  id: string;
  text: string;
  completed: boolean;
}

export function App({ replicaId, drift }: { replicaId: string; drift: Drift }) {
  const [text, setText] = useState('');
  const [isLeader, setIsLeader] = useState(drift.leader);

  // Poll for leadership changes so tabs update dynamically when the leader changes
  useEffect(() => {
    const timer = setInterval(() => {
      setIsLeader(drift.leader);
    }, 500);
    return () => clearInterval(timer);
  }, [drift]);

  // Query todos collection
  const { data: todos, loading } = useQuery('todos');
  const mutations = useDriftMutations('todos');

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!text.trim()) return;
    const todoId = Math.random().toString(36).substring(2, 9);
    await mutations.insert(todoId, {
      text: text.trim(),
      completed: false,
    });
    setText('');
  };

  const handleToggle = async (todo: any) => {
    await mutations.update(todo.id, {
      text: todo.text,
      completed: !todo.completed,
    });
  };

  const handleDelete = async (id: string) => {
    await mutations.delete(id);
  };

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <div style={styles.headerTitle}>
          <h1>Drift Multi-Tab Todos</h1>
          <p style={styles.subtitle}>Symmetric offline-first synchronization across tabs</p>
        </div>
        <SyncIndicator />
      </header>

      <div style={styles.infoCard}>
        <div style={styles.badgeContainer}>
          <span style={styles.badge}>Replica ID: {replicaId}</span>
          <span style={{
            ...styles.badge,
            backgroundColor: isLeader ? '#059669' : '#4b5563',
          }}>
            {isLeader ? '⚡ Leader Tab' : '👤 Reader Tab'}
          </span>
        </div>
        <p style={styles.infoText}>
          {isLeader 
            ? 'This tab is elected as the Lead replica. It is directly running sync loops with storage and coordinator services.' 
            : 'This tab is running in replica mode. It listens for updates from the Leader tab in real-time.'}
        </p>
      </div>

      <form onSubmit={handleAdd} style={styles.form}>
        <input
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="What needs to be done?"
          style={styles.input}
        />
        <button type="submit" style={styles.button}>Add Task</button>
      </form>

      {loading ? (
        <div style={styles.loading}>Loading todos...</div>
      ) : (
        <ul style={styles.list}>
          {todos.length === 0 ? (
            <li style={styles.empty}>No todos yet. Open another tab to test synchronization!</li>
          ) : (
            todos.map((todo: any) => (
              <li key={todo.id} style={styles.listItem}>
                <label style={styles.label}>
                  <input
                    type="checkbox"
                    checked={Boolean(todo.completed)}
                    onChange={() => handleToggle(todo)}
                    style={styles.checkbox}
                  />
                  <span style={{
                    ...styles.todoText,
                    textDecoration: todo.completed ? 'line-through' : 'none',
                    color: todo.completed ? '#9ca3af' : '#f3f4f6',
                  }}>
                    {todo.text}
                  </span>
                </label>
                <button onClick={() => handleDelete(todo.id)} style={styles.deleteButton}>
                  Delete
                </button>
              </li>
            ))
          )}
        </ul>
      )}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    maxWidth: '600px',
    margin: '40px auto',
    padding: '24px',
    borderRadius: '16px',
    backgroundColor: '#1f2937',
    color: '#f3f4f6',
    fontFamily: 'Inter, system-ui, sans-serif',
    boxShadow: '0 10px 25px -5px rgba(0, 0, 0, 0.3), 0 8px 10px -6px rgba(0, 0, 0, 0.3)',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderBottom: '1px solid #374151',
    paddingBottom: '16px',
    marginBottom: '20px',
  },
  headerTitle: {
    display: 'flex',
    flexDirection: 'column',
  },
  subtitle: {
    fontSize: '14px',
    color: '#9ca3af',
    margin: '4px 0 0 0',
  },
  infoCard: {
    backgroundColor: '#111827',
    padding: '16px',
    borderRadius: '12px',
    marginBottom: '24px',
  },
  badgeContainer: {
    display: 'flex',
    gap: '8px',
    marginBottom: '8px',
  },
  badge: {
    fontSize: '12px',
    fontWeight: 'bold',
    padding: '4px 8px',
    borderRadius: '6px',
    backgroundColor: '#374151',
    color: '#f9fafb',
  },
  infoText: {
    fontSize: '13px',
    color: '#9ca3af',
    margin: 0,
    lineHeight: '1.5',
  },
  form: {
    display: 'flex',
    gap: '12px',
    marginBottom: '20px',
  },
  input: {
    flex: 1,
    padding: '12px 16px',
    borderRadius: '8px',
    border: '1px solid #4b5563',
    backgroundColor: '#374151',
    color: '#f9fafb',
    fontSize: '16px',
    outline: 'none',
  },
  button: {
    padding: '12px 20px',
    backgroundColor: '#3b82f6',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontWeight: '600',
    cursor: 'pointer',
  },
  loading: {
    textAlign: 'center',
    color: '#9ca3af',
    padding: '20px',
  },
  list: {
    listStyle: 'none',
    padding: 0,
    margin: 0,
  },
  empty: {
    textAlign: 'center',
    color: '#9ca3af',
    padding: '40px 20px',
    backgroundColor: '#111827',
    borderRadius: '8px',
  },
  listItem: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '12px 16px',
    backgroundColor: '#111827',
    borderRadius: '8px',
    marginBottom: '8px',
  },
  label: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    cursor: 'pointer',
  },
  checkbox: {
    width: '18px',
    height: '18px',
    cursor: 'pointer',
  },
  todoText: {
    fontSize: '16px',
  },
  deleteButton: {
    padding: '6px 12px',
    backgroundColor: '#ef4444',
    border: 'none',
    borderRadius: '6px',
    color: '#fff',
    fontSize: '13px',
    cursor: 'pointer',
  },
};
