import { useState } from 'react';
import type { MouseEvent } from 'react';
import { useQuery, useVaultSyncMutations } from '@vaultsync/react';

interface NoteListProps {
  activeNoteId: string | null;
  onSelectNote: (id: string) => void;
  onCreateNote: () => void;
}

export function NoteList({ activeNoteId, onSelectNote, onCreateNote }: NoteListProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const { data: notes, loading } = useQuery('notes');
  const mutations = useVaultSyncMutations('notes');

  const handleDelete = async (e: MouseEvent, id: string) => {
    e.stopPropagation();
    if (!id) {
      console.warn('[NoteList] delete with empty id — skipping', {
        id,
        type: typeof id,
        keys: Object.keys(id || {}),
      });
      return;
    }
    try {
      await mutations.delete(id);
    } catch (err) {
      console.error('Failed to delete note:', err);
    }
  };

  // Instrument: log rendered notes
  console.debug('[NOTE_LIST] rendering notes=%d ids=[%s]',
    notes.length,
    notes.map((n: any) => {
      const id = (n.record_id ?? n.id ?? n.doc_id ?? '') as string;
      const del = n._deleted === true || n.__deleted__ === true;
      return `${id}(del=${del})`;
    }).join(','));

  // Filter and sort notes
  const filteredNotes = notes
    .filter((note) => {
      const title = (note.title as string || '').toLowerCase();
      const body = (note.body as string || '').toLowerCase();
      const query = searchQuery.toLowerCase();
      return title.includes(query) || body.includes(query);
    })
    .sort((a, b) => {
      const timeA = Number(a.updatedAt || 0);
      const timeB = Number(b.updatedAt || 0);
      return timeB - timeA;
    });

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <button 
          className="new-note-btn" 
          onClick={onCreateNote}
          data-testid="new-note-btn"
        >
          <span>➕</span> New Note
        </button>
        
        <div className="search-wrapper">
          <span className="search-icon">🔍</span>
          <input
            type="text"
            className="search-input"
            placeholder="Search notes..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            data-testid="search-input"
          />
        </div>
      </div>

      <div className="note-list-container" data-testid="note-list">
        {loading && notes.length === 0 ? (
          <div className="empty-list-text">Loading notes...</div>
        ) : filteredNotes.length === 0 ? (
          <div className="empty-list-text">
            {searchQuery ? 'No matching notes found' : 'No notes yet. Create one!'}
          </div>
        ) : (
          filteredNotes.map((note) => {
            const id = (note.record_id ?? note.id ?? note.doc_id ?? '') as string;
            if (!id) {
              console.warn('[NoteList] malformed note without identifier', {
                keys: Object.keys(note),
                type: typeof note,
                constructor: (note as any)?.constructor?.name,
                json: JSON.stringify(note).slice(0, 200),
              });
            }
            const title = (note.title as string) || 'Untitled Note';
            const body = (note.body as string) || '';
            const preview = body.length > 60 ? `${body.substring(0, 60)}...` : body || 'Empty note';
            
            // Format updated timestamp
            const updatedAt = Number(note.updatedAt || Date.now());
            const dateStr = new Date(updatedAt).toLocaleTimeString([], {
              hour: '2-digit',
              minute: '2-digit',
            });

            return (
              <div
                key={id}
                className={`note-item ${activeNoteId === id ? 'active' : ''}`}
                onClick={() => onSelectNote(id)}
                data-testid="note-item"
                data-id={id}
              >
                <div className="note-item-title">{title}</div>
                <div className="note-item-preview">{preview}</div>
                <div className="note-item-footer">
                  <span className="note-item-time">{dateStr}</span>
                  <button
                    className="note-delete-btn"
                    onClick={(e) => handleDelete(e, id)}
                    title="Delete Note"
                    data-testid="delete-note-btn"
                  >
                    🗑️
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
