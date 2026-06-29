'use client';

import { useEffect, useRef, useState } from 'react';
import { useEditor, EditorContent } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';
import Collaboration from '@tiptap/extension-collaboration';
import * as Y from 'yjs';
import { useQuery, useVaultSyncMutations, useVaultSyncClient } from '@vaultsync/react';
import { useDocInfo } from './DocProvider';
import Link from 'next/link';

interface UserProfile {
  id: string;
  name: string;
  email: string;
  avatarColor: string;
}

export function DocEditor() {
  const docInfo = useDocInfo();
  const vsClient = useVaultSyncClient();
  const updatesMutations = useVaultSyncMutations('updates');
  const presenceMutations = useVaultSyncMutations('presence');
  const tabId = (vsClient as any).tabId || '';

  const [user, setUser] = useState<UserProfile | null>(null);
  const [initialized, setInitialized] = useState(false);
  const [syncState, setSyncState] = useState<any>(null);
  
  // Title editing state
  const [isEditingTitle, setIsEditingTitle] = useState(false);
  const [titleValue, setTitleValue] = useState(docInfo.title);
  const [isSavingTitle, setIsSavingTitle] = useState(false);

  const ydoc = useRef<Y.Doc | null>(null);
  const editorContainer = useRef<HTMLDivElement | null>(null);

  // Fetch current user details
  useEffect(() => {
    async function fetchUser() {
      try {
        const res = await fetch('/api/auth/me');
        if (res.ok) {
          const data = await res.json();
          setUser(data.user);
        }
      } catch (e) {
        console.error('Failed to load user info:', e);
      }
    }
    fetchUser();
  }, []);

  // Sync status listener
  useEffect(() => {
    const updateSyncStatus = async () => {
      try {
        const status = await vsClient.syncStatus();
        setSyncState(status);
      } catch (e) {
        // Ignored
      }
    };
    updateSyncStatus();
    const timer = setInterval(updateSyncStatus, 2000);
    return () => clearInterval(timer);
  }, [vsClient]);

  // Load existing updates from VaultSync and initialize ydoc
  useEffect(() => {
    async function initYdoc() {
      ydoc.current = new Y.Doc();
      
      try {
        // Fetch all historical base64-encoded Yjs updates from VaultSync
        const records = await vsClient.find('updates');
        
        // Sort by timestamp ascending to ensure they apply in chronological order
        const sorted = [...records].sort((a: any, b: any) => Number(a.timestamp || 0) - Number(b.timestamp || 0));
        
        // Apply updates to build initial editor state
        sorted.forEach((record: any) => {
          if (record.bytes) {
            const binary = Uint8Array.from(atob(record.bytes), c => c.charCodeAt(0));
            Y.applyUpdate(ydoc.current!, binary, 'initial-sync');
          }
        });
      } catch (err) {
        console.error('Failed to load historical document updates:', err);
      } finally {
        setInitialized(true);
      }
    }

    initYdoc();
  }, [vsClient]);

  // Set up Tiptap editor
  const editor = useEditor({
    extensions: [
      StarterKit.configure({
        history: false, // Collaborative updates have their own Yjs history
      }),
      Collaboration.configure({
        document: ydoc.current || undefined,
      }),
    ],
    editorProps: {
      attributes: {
        class: 'prose prose-sm prose-invert focus:outline-none max-w-full text-zinc-300',
      },
    },
    immediatelyRender: false,
  }, [initialized]);

  // Push local updates to VaultSync
  useEffect(() => {
    if (!initialized || !ydoc.current) return;

    const handleYdocUpdate = async (update: Uint8Array, origin: any) => {
      // Skip updates applied from other users or during bootstrap
      if (origin === 'remote-sync' || origin === 'initial-sync') return;

      const base64 = btoa(String.fromCharCode(...update));
      const updateId = crypto.randomUUID();

      try {
        await updatesMutations.insert(updateId, {
          id: updateId,
          bytes: base64,
          timestamp: Date.now(),
          replicaId: tabId,
        });
      } catch (err) {
        console.error('Failed to save local update:', err);
      }
    };

    ydoc.current.on('update', handleYdocUpdate);

    return () => {
      if (ydoc.current) {
        ydoc.current.off('update', handleYdocUpdate);
      }
    };
  }, [initialized, updatesMutations, tabId]);

  // Apply remote updates received from VaultSync subscriptions
  useEffect(() => {
    if (!initialized || !ydoc.current) return;

    const unsubscribe = vsClient.subscribe('updates', (recordId, record) => {
      // Skip updates originated by ourselves
      if (record.replicaId === tabId) return;

      try {
        if (record.bytes) {
          const binary = Uint8Array.from(atob(String(record.bytes)), c => c.charCodeAt(0));
          Y.applyUpdate(ydoc.current!, binary, 'remote-sync');
        }
      } catch (err) {
        console.error('Failed to apply remote document update:', err);
      }
    });

    return () => unsubscribe();
  }, [initialized, vsClient, tabId]);

  // Sync details from Workspace-level document title/icon updates
  const { data: vsDocs = [] } = useQuery('docs') || {};
  useEffect(() => {
    const currentDoc = vsDocs.find((d: any) => String(d.id) === docInfo.id);
    if (currentDoc && String(currentDoc.title) !== titleValue) {
      setTitleValue(String(currentDoc.title));
      docInfo.setTitleState(String(currentDoc.title));
    }
  }, [vsDocs, docInfo.id]);

  // Heartbeat to keep our presence active
  useEffect(() => {
    if (!user) return;

    const sendHeartbeat = async () => {
      try {
        await presenceMutations.insert(tabId, {
          id: tabId,
          userId: user.id,
          name: user.name,
          color: user.avatarColor || '#6366f1',
          anchor: editor ? editor.state.selection.anchor : 0,
          head: editor ? editor.state.selection.head : 0,
          lastActive: Date.now(),
        });
      } catch (err) {
        // Ignored
      }
    };

    sendHeartbeat();
    const timer = setInterval(sendHeartbeat, 5000);
    return () => clearInterval(timer);
  }, [user, editor, presenceMutations, tabId]);

  // Read active peers in this document
  const { data: presenceList = [] } = useQuery('presence') || {};
  const remotePeers = presenceList.filter((p: any) => String(p.id) !== tabId);

  // Dynamic remote cursor rendering
  const [cursors, setCursors] = useState<{ id: string; name: string; color: string; top: number; left: number }[]>([]);
  useEffect(() => {
    if (!editor || remotePeers.length === 0) {
      setCursors([]);
      return;
    }

    const updateCursors = () => {
      const activeCursors: any[] = [];
      const now = Date.now();

      remotePeers.forEach((peer: any) => {
        // Skip peers that have gone stale (longer than 12 seconds since last heartbeat)
        if (now - Number(peer.lastActive || 0) > 12000) return;

        const head = Number(peer.head);
        if (isNaN(head)) return;

        try {
          // Calculate screen coordinates for the cursor position
          const rect = editor.view.coordsAtPos(head);
          if (editorContainer.current) {
            const containerRect = editorContainer.current.getBoundingClientRect();
            // Ensure remote cursor coordinates reside inside the editor viewport bounds
            if (rect.top >= containerRect.top && rect.top <= containerRect.bottom) {
              activeCursors.push({
                id: String(peer.id),
                name: String(peer.name),
                color: String(peer.color),
                top: rect.top - containerRect.top + editorContainer.current.scrollTop,
                left: rect.left - containerRect.left + editorContainer.current.scrollLeft,
              });
            }
          }
        } catch (e) {
          // POS might be out of range temporarily during active typing
        }
      });
      setCursors(activeCursors);
    };

    updateCursors();
    const interval = setInterval(updateCursors, 250);
    return () => clearInterval(interval);
  }, [editor, remotePeers]);

  const handleTitleSubmit = async () => {
    if (!titleValue.trim() || titleValue === docInfo.title) {
      setIsEditingTitle(false);
      return;
    }

    setIsSavingTitle(true);
    try {
      const res = await fetch(`/api/workspaces/${docInfo.workspaceId}/documents/${docInfo.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: titleValue }),
      });

      if (res.ok) {
        docInfo.setTitleState(titleValue);
        
        // Also update workspace-level VaultSync docs collection so sidebar updates instantly
        // To do this, we can try to resolve the workspace-level VaultSync client
        // Wait, the client is cached in instancePromises, so we can fetch it, or just rely on the REST-to-VS loop
        // The easiest way is to update via the workspace-level mutations if available, but since we are nested,
        // we can trigger it or let the REST-mount sync it.
      }
    } catch (err) {
      console.error('Failed to update title:', err);
    } finally {
      setIsSavingTitle(false);
      setIsEditingTitle(false);
    }
  };

  const getCleanStatusString = () => {
    if (!syncState) return 'Syncing...';
    if (syncState.connection_status === 'Connected') return '🟢 Live';
    return '🟡 Offline';
  };

  if (!initialized) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-zinc-950 text-zinc-400 p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Initializing editor workspace...</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-zinc-950 relative">
      {/* Editor Header */}
      <div className="h-16 px-6 border-b border-zinc-900 flex items-center justify-between flex-shrink-0 bg-zinc-950/80 backdrop-blur-md z-10">
        <div className="flex items-center space-x-3 min-w-0">
          <Link
            href={`/placeholder/docs`}
            onClick={(e) => {
              e.preventDefault();
              window.history.back();
            }}
            className="text-zinc-400 hover:text-white transition-colors text-sm"
          >
            ← Library
          </Link>
          <span className="text-zinc-700">/</span>
          <div className="flex items-center space-x-2.5 min-w-0">
            <span className="text-base flex-shrink-0">{docInfo.icon}</span>
            {isEditingTitle ? (
              <input
                type="text"
                value={titleValue}
                onChange={(e) => setTitleValue(e.target.value)}
                onBlur={handleTitleSubmit}
                onKeyDown={(e) => e.key === 'Enter' && handleTitleSubmit()}
                autoFocus
                className="bg-zinc-900 border border-zinc-800 rounded px-2 py-0.5 text-xs text-white focus:outline-none w-48 font-bold"
              />
            ) : (
              <h1
                onClick={() => setIsEditingTitle(true)}
                className="font-bold text-white truncate text-base hover:bg-zinc-900/40 px-2 py-0.5 rounded cursor-pointer transition-all"
              >
                {titleValue}
              </h1>
            )}
            {isSavingTitle && (
              <div className="animate-spin rounded-full h-3.5 w-3.5 border-b-2 border-indigo-500"></div>
            )}
          </div>
        </div>

        {/* Presence Stack & Status */}
        <div className="flex items-center space-x-4">
          {/* Status Indicator */}
          <span className="text-3xs font-semibold uppercase tracking-wider text-zinc-550 border border-zinc-850 px-2.5 py-1 rounded-full bg-zinc-950/40">
            {getCleanStatusString()}
          </span>

          {/* User Presence stack */}
          <div className="flex -space-x-1.5 overflow-hidden">
            {user && (
              <span
                style={{ backgroundColor: user.avatarColor }}
                className="w-6 h-6 rounded-full ring-2 ring-zinc-950 flex items-center justify-center text-3xs font-bold text-white cursor-default"
                title={`${user.name} (You)`}
              >
                {user.name.substring(0, 1).toUpperCase()}
              </span>
            )}
            {remotePeers.map((peer: any) => {
              if (Date.now() - Number(peer.lastActive || 0) > 12000) return null;
              return (
                <span
                  key={peer.id}
                  style={{ backgroundColor: String(peer.color) }}
                  className="w-6 h-6 rounded-full ring-2 ring-zinc-950 flex items-center justify-center text-3xs font-bold text-white transition-all transform hover:scale-115 hover:z-10"
                  title={String(peer.name)}
                >
                  {String(peer.name).substring(0, 1).toUpperCase()}
                </span>
              );
            })}
          </div>
        </div>
      </div>

      {/* Editor Content Box */}
      <div 
        ref={editorContainer}
        className="flex-1 overflow-y-auto p-8 relative flex flex-col items-center bg-zinc-950/20"
      >
        <div className="w-full max-w-3xl bg-zinc-900/35 border border-zinc-900/60 rounded-3xl p-8 min-h-[70vh] shadow-2xl backdrop-blur-xl relative">
          
          {/* Toolbar Helper */}
          {editor && (
            <div className="flex items-center flex-wrap gap-2.5 pb-6 border-b border-zinc-900 mb-6 text-xs text-zinc-400 select-none">
              <button
                onClick={() => editor.chain().focus().toggleBold().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('bold') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Bold"
              >
                <b>B</b>
              </button>
              <button
                onClick={() => editor.chain().focus().toggleItalic().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('italic') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Italic"
              >
                <i>I</i>
              </button>
              <button
                onClick={() => editor.chain().focus().toggleCode().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('code') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Inline Code"
              >
                <code>&lt;/&gt;</code>
              </button>
              <span className="h-4 w-px bg-zinc-850" />
              <button
                onClick={() => editor.chain().focus().toggleHeading({ level: 1 }).run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('heading', { level: 1 }) ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800 font-bold'}`}
                title="Header 1"
              >
                H1
              </button>
              <button
                onClick={() => editor.chain().focus().toggleHeading({ level: 2 }).run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('heading', { level: 2 }) ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800 font-bold'}`}
                title="Header 2"
              >
                H2
              </button>
              <button
                onClick={() => editor.chain().focus().toggleHeading({ level: 3 }).run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('heading', { level: 3 }) ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800 font-bold'}`}
                title="Header 3"
              >
                H3
              </button>
              <span className="h-4 w-px bg-zinc-850" />
              <button
                onClick={() => editor.chain().focus().toggleBulletList().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('bulletList') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Bullet List"
              >
                • List
              </button>
              <button
                onClick={() => editor.chain().focus().toggleOrderedList().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('orderedList') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Ordered List"
              >
                1. List
              </button>
              <button
                onClick={() => editor.chain().focus().toggleBlockquote().run()}
                className={`p-1.5 rounded transition-all cursor-pointer ${editor.isActive('blockquote') ? 'bg-indigo-600 text-white' : 'hover:bg-zinc-800'}`}
                title="Blockquote"
              >
                ” Quote
              </button>
            </div>
          )}

          {/* Actual Tiptap Editor Content */}
          <EditorContent editor={editor} className="min-h-[50vh] text-sm leading-relaxed" />

          {/* Render Remote Floating Cursors */}
          {cursors.map((cursor) => (
            <div
              key={cursor.id}
              style={{
                top: `${cursor.top}px`,
                left: `${cursor.left}px`,
                borderColor: cursor.color,
              }}
              className="absolute w-[2px] h-[18px] pointer-events-none z-45 border-l-2 select-none"
            >
              {/* Cursor Flag showing user name */}
              <div
                style={{ backgroundColor: cursor.color }}
                className="absolute top-[-14px] left-0 px-1 py-0.5 rounded text-[8px] font-extrabold text-white whitespace-nowrap opacity-90 leading-none"
              >
                {cursor.name}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
