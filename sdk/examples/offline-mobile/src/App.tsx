import React, { useState, useEffect } from 'react';
import { useQuery, useVaultSyncMutations, useSyncStatus } from '@vaultsync/react';
import { VaultSync } from '@vaultsync/web';

export function App({ replicaId, vaultsync }: { replicaId: string; vaultsync: VaultSync }) {
  const [title, setTitle] = useState('');
  const [content, setContent] = useState('');
  
  // Simulate connection state
  const [isNetworkOnline, setIsNetworkOnline] = useState(true);
  
  // Query notes collection
  const { data: notes, loading } = useQuery('notes');
  const mutations = useVaultSyncMutations('notes');
  
  // Use real-time sync status from VaultSync
  const syncStatus = useSyncStatus();

  // Handle Note insertion
  const handleAddNote = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    const noteId = Math.random().toString(36).substring(2, 9);
    await mutations.insert(noteId, {
      title: title.trim(),
      content: content.trim(),
      timestamp: Date.now(),
    });
    setTitle('');
    setContent('');
  };

  const handleDeleteNote = async (id: string) => {
    await mutations.delete(id);
  };

  return (
    <div style={styles.phoneFrame}>
      <div style={styles.statusBar}>
        <span>VaultSync Mobile</span>
        <div style={styles.statusBarStatus}>
          <span style={{
            ...styles.dot,
            backgroundColor: isNetworkOnline ? '#10b981' : '#f59e0b',
          }} />
          <span>{isNetworkOnline ? 'LTE' : 'No Service'}</span>
        </div>
      </div>

      <div style={styles.container}>
        <div style={styles.networkSwitch}>
          <span style={styles.switchLabel}>Simulate Network:</span>
          <button 
            onClick={() => setIsNetworkOnline(!isNetworkOnline)} 
            style={{
              ...styles.switchButton,
              backgroundColor: isNetworkOnline ? '#10b981' : '#f59e0b',
            }}
          >
            {isNetworkOnline ? 'ONLINE' : 'OFFLINE'}
          </button>
        </div>

        <div style={styles.syncCard}>
          <h3>Queue Status</h3>
          <div style={styles.syncRow}>
            <span>Pending Sync:</span>
            <span style={styles.syncVal}>{syncStatus.pendingMutations} mutations</span>
          </div>
          <div style={styles.syncRow}>
            <span>Sync Connection:</span>
            <span style={styles.syncVal}>{syncStatus.connected ? 'Connected' : 'Disconnected'}</span>
          </div>
          <p style={styles.helperText}>
            When Offline, writes are saved locally instantly. They will sync automatically when you toggle the connection back to ONLINE.
          </p>
        </div>

        <form onSubmit={handleAddNote} style={styles.form}>
          <input
            type="text"
            placeholder="Note Title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            style={styles.input}
          />
          <textarea
            placeholder="Type note details..."
            value={content}
            onChange={(e) => setContent(e.target.value)}
            style={{ ...styles.input, height: '60px', resize: 'none' }}
          />
          <button type="submit" style={styles.submitButton}>Save Note</button>
        </form>

        <div style={styles.notesList}>
          <h3>Notes List</h3>
          {loading ? (
            <div style={styles.loading}>Loading notes...</div>
          ) : notes.length === 0 ? (
            <div style={styles.empty}>No notes yet. Create one above!</div>
          ) : (
            notes.map((note: any) => (
              <div key={note.id} style={styles.noteItem}>
                <div style={styles.noteContent}>
                  <h4 style={styles.noteTitle}>{note.title}</h4>
                  <p style={styles.noteBody}>{note.content}</p>
                </div>
                <button onClick={() => handleDeleteNote(note.id)} style={styles.deleteButton}>
                  ×
                </button>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  phoneFrame: {
    width: '360px',
    height: '640px',
    margin: '40px auto',
    borderRadius: '32px',
    border: '12px solid #1f2937',
    backgroundColor: '#0f172a',
    color: '#f8fafc',
    fontFamily: 'Inter, system-ui, sans-serif',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)',
  },
  statusBar: {
    height: '24px',
    padding: '0 20px',
    backgroundColor: '#1e293b',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    fontSize: '11px',
    fontWeight: '600',
    color: '#94a3b8',
  },
  statusBarStatus: {
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
  },
  dot: {
    width: '8px',
    height: '8px',
    borderRadius: '50%',
  },
  container: {
    flex: 1,
    padding: '16px',
    overflowY: 'auto',
    display: 'flex',
    flexDirection: 'column',
    gap: '16px',
  },
  networkSwitch: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    backgroundColor: '#1e293b',
    padding: '10px 14px',
    borderRadius: '12px',
  },
  switchLabel: {
    fontSize: '13px',
    fontWeight: '500',
  },
  switchButton: {
    padding: '6px 14px',
    border: 'none',
    borderRadius: '20px',
    color: '#fff',
    fontWeight: 'bold',
    cursor: 'pointer',
    fontSize: '11px',
  },
  syncCard: {
    backgroundColor: '#1e293b',
    padding: '14px',
    borderRadius: '12px',
  },
  syncRow: {
    display: 'flex',
    justifyContent: 'space-between',
    fontSize: '13px',
    marginBottom: '6px',
  },
  syncVal: {
    fontWeight: '600',
    color: '#38bdf8',
  },
  helperText: {
    fontSize: '11px',
    color: '#64748b',
    margin: '8px 0 0 0',
    lineHeight: '1.4',
  },
  form: {
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    backgroundColor: '#1e293b',
    padding: '14px',
    borderRadius: '12px',
  },
  input: {
    padding: '10px 12px',
    borderRadius: '8px',
    border: '1px solid #334155',
    backgroundColor: '#0f172a',
    color: '#f8fafc',
    fontSize: '14px',
    outline: 'none',
  },
  submitButton: {
    padding: '10px',
    backgroundColor: '#0284c7',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontWeight: '600',
    cursor: 'pointer',
    fontSize: '14px',
  },
  notesList: {
    flex: 1,
  },
  loading: {
    textAlign: 'center',
    padding: '20px',
    color: '#94a3b8',
  },
  empty: {
    textAlign: 'center',
    padding: '30px 10px',
    color: '#64748b',
    backgroundColor: '#1e293b',
    borderRadius: '12px',
    fontSize: '13px',
  },
  noteItem: {
    backgroundColor: '#1e293b',
    padding: '12px 14px',
    borderRadius: '12px',
    marginBottom: '8px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  noteContent: {
    flex: 1,
  },
  noteTitle: {
    margin: '0 0 4px 0',
    fontSize: '14px',
    fontWeight: '600',
  },
  noteBody: {
    margin: 0,
    fontSize: '12px',
    color: '#94a3b8',
    lineHeight: '1.4',
  },
  deleteButton: {
    background: 'none',
    border: 'none',
    color: '#ef4444',
    fontSize: '18px',
    cursor: 'pointer',
    padding: '0 4px',
  },
};
