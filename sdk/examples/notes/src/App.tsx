import { useState, useMemo, useEffect } from 'react';
import { VaultSyncProvider, BootstrapProvider, useVaultSyncMutations, useQuery, useVaultSyncClient } from '@vaultsync/react';
import { SyncBar } from './components/SyncBar';
import { NoteList } from './components/NoteList';
import { NoteEditor } from './components/NoteEditor';
import { EncryptionBadge } from './components/EncryptionBadge';
import './index.css';

function MainLayout() {
  const client = useVaultSyncClient();
  const [activeNoteId, setActiveNoteId] = useState<string | null>(null);
  const mutations = useVaultSyncMutations('notes');
  const { data: notes } = useQuery('notes');

  // Expose client for E2E testing
  useEffect(() => {
    (window as any).__VAULTSYNC__ = client;
  }, [client]);

  // If there are notes and no active note is selected, select the first one automatically
  useEffect(() => {
    if (notes.length > 0 && !activeNoteId) {
      // Find the most recently updated note
      const sorted = [...notes].sort((a, b) => {
        const timeA = Number(a.updatedAt || 0);
        const timeB = Number(b.updatedAt || 0);
        return timeB - timeA;
      });
      const latestId = (sorted[0].record_id || sorted[0].id) as string;
      setActiveNoteId(latestId);
    }
  }, [notes, activeNoteId]);

  const handleCreateNote = async () => {
    const id = `note-${Math.random().toString(36).substring(7)}`;
    try {
      await mutations.insert(id, {
        title: '',
        body: '',
      });
      setActiveNoteId(id);
    } catch (err) {
      console.error('Failed to create new note:', err);
    }
  };

  return (
    <>
      <div className="status-bar">
        <SyncBar />
        <div className="status-indicators">
          <EncryptionBadge />
        </div>
      </div>
      <div className="app-container">
        <NoteList
          activeNoteId={activeNoteId}
          onSelectNote={setActiveNoteId}
          onCreateNote={handleCreateNote}
        />
        {activeNoteId ? (
          <NoteEditor noteId={activeNoteId} />
        ) : (
          <div className="main-content">
            <div className="empty-state">
              <div className="empty-state-icon">📝</div>
              <h2>Welcome to VaultSync Notes</h2>
              <p>Create a note on the sidebar to get started.</p>
              <button 
                className="new-note-btn" 
                onClick={handleCreateNote}
                style={{ width: 'auto', marginTop: '1rem', padding: '0.6rem 1.5rem' }}
                data-testid="empty-new-note-btn"
              >
                ➕ Create First Note
              </button>
            </div>
          </div>
        )}
      </div>
    </>
  );
}

export default function App() {
  // Read params from URL to facilitate multi-tab / testing
  const config = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const namespace = params.get('ns') || 'notes-demo';
    const replicaId = params.get('replica') || `replica-${Math.random().toString(36).substring(7)}`;
    const coordinatorUrl = params.get('coordinator') || 'http://localhost:9876';
    
    return {
      namespace,
      replicaId,
      coordinatorUrl,
    };
  }, []);

  return (
    <VaultSyncProvider config={config}>
      <BootstrapProvider docIds={['notes']}>
        <MainLayout />
      </BootstrapProvider>
    </VaultSyncProvider>
  );
}
