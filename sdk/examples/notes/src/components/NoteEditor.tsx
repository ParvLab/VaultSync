import { useState, useEffect, useRef } from 'react';
import { useVaultSyncOne, useVaultSyncMutations } from '@vaultsync/react';

interface NoteEditorProps {
  noteId: string;
}

export function NoteEditor({ noteId }: NoteEditorProps) {
  const { data: note, loading } = useVaultSyncOne('notes', noteId);
  const mutations = useVaultSyncMutations('notes');
  
  const [title, setTitle] = useState('');
  const [body, setBody] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [lastSaved, setLastSaved] = useState<string | null>(null);

  // Keep track of the active noteId to reset state when switched
  const prevNoteIdRef = useRef<string | null>(null);
  // Ref to store latest inputs for the debounced save
  const titleRef = useRef(title);
  const bodyRef = useRef(body);
  // Suppress auto-save during initial hydration or external merge
  const isHydrating = useRef(false);
  // Track the last saved timestamp to avoid redundant setLastSaved
  const lastSavedRef = useRef(0);
  // Track last user edit timestamp per field (to distinguish stale focus from active editing)
  const lastEditRef = useRef<{ title: number; body: number }>({ title: 0, body: 0 });
  // Cooldown: if field has focus but no edit within this window, treat focus as stale
  const FOCUS_COOLDOWN_MS = 2000;

  useEffect(() => {
    titleRef.current = title;
  }, [title]);

  useEffect(() => {
    bodyRef.current = body;
  }, [body]);

  // Handle incoming updates from the sync engine
  useEffect(() => {
    if (note) {
      // If we switched notes, reset local state to matching values
      if (prevNoteIdRef.current !== noteId) {
        isHydrating.current = true;
        setTitle((note.title as string) || '');
        setBody((note.body as string) || '');
        prevNoteIdRef.current = noteId;
        
        const timestamp = Number(note.updatedAt || Date.now());
        if (timestamp !== lastSavedRef.current) {
          lastSavedRef.current = timestamp;
          setLastSaved(new Date(timestamp).toLocaleTimeString());
        }

        requestAnimationFrame(() => { isHydrating.current = false; });
      } else {
        // If it's the same note but updated externally (e.g. from another tab),
        // merge the updates unless the user is actively editing that field.
        // A field is "actively editing" if it has focus AND the user typed within
        // the last FOCUS_COOLDOWN_MS. Stale focus (focused but no recent keystroke)
        // does NOT block the merge.
        let merged = false;

        const titleActive = document.activeElement?.id === 'note-title-input'
          && Date.now() - lastEditRef.current.title < FOCUS_COOLDOWN_MS;
        if (!titleActive && titleRef.current !== note.title) {
          if (document.activeElement?.id === 'note-title-input') {
            console.warn('[FocusGuard] title field has stale focus (no recent edit), allowing merge');
          }
          setTitle((note.title as string) || '');
          merged = true;
        }

        const bodyActive = document.activeElement?.id === 'note-body-textarea'
          && Date.now() - lastEditRef.current.body < FOCUS_COOLDOWN_MS;
        if (!bodyActive && bodyRef.current !== note.body) {
          if (document.activeElement?.id === 'note-body-textarea') {
            console.warn('[FocusGuard] body field has stale focus (no recent edit), allowing merge');
          }
          setBody((note.body as string) || '');
          merged = true;
        }

        if (merged) {
          isHydrating.current = true;
          requestAnimationFrame(() => { isHydrating.current = false; });

          const timestamp = Number(note.updatedAt || Date.now());
          if (timestamp !== lastSavedRef.current) {
            lastSavedRef.current = timestamp;
            setLastSaved(new Date(timestamp).toLocaleTimeString());
          }
        }
      }
    }
  }, [note, noteId]);

  // Debounced auto-save effect
  useEffect(() => {
    // Skip auto-save on initial mount or load, and during hydration/merge
    if (loading || !note || isHydrating.current) return;

    // Check if anything actually changed
    const hasChanges = title !== (note.title as string || '') || body !== (note.body as string || '');
    if (!hasChanges) return;

    setIsSaving(true);
    const timer = setTimeout(async () => {
      try {
        await mutations.update(noteId, {
          title: title.trim(),
          body,
        });
        setLastSaved(new Date().toLocaleTimeString());
      } catch (err) {
        console.error('Auto-save failed:', err);
      } finally {
        setIsSaving(false);
      }
    }, 600); // 600ms debounce

    return () => clearTimeout(timer);
  }, [title, body, noteId, loading, mutations]);

  if (loading && !note) {
    return (
      <div className="main-content">
        <div className="empty-state">
          <div className="empty-state-icon">📝</div>
          <div>Loading note...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="main-content" data-testid="note-editor">
      <div className="editor-container">
        <div className="editor-header">
          <input
            id="note-title-input"
            type="text"
            className="title-input"
            placeholder="Untitled Note"
            value={title}
            onChange={(e) => { setTitle(e.target.value); lastEditRef.current.title = Date.now(); }}
            data-testid="note-title"
          />
          <div className="editor-meta-info">
            <span className="save-indicator" data-testid="save-status">
              {isSaving ? '⏳ Saving...' : lastSaved ? `✓ Saved at ${lastSaved}` : '✓ Synced'}
            </span>
          </div>
        </div>
        
        <textarea
          id="note-body-textarea"
          className="body-textarea"
          placeholder="Start writing..."
          value={body}
          onChange={(e) => { setBody(e.target.value); lastEditRef.current.body = Date.now(); }}
          data-testid="note-body"
        />
      </div>
    </div>
  );
}
