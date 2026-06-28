'use client';

import { useState, useEffect } from 'react';
import { useQuery, useVaultSyncMutations } from '@vaultsync/react';
import { useWorkspaceInfo } from '../workspace/WorkspaceProvider';
import { useRouter, useParams } from 'next/navigation';
import Link from 'next/link';

interface DocItem {
  id: string;
  title: string;
  icon: string;
  projectId: string | null;
  createdAt: number;
}

const ICON_PRESETS = ['📄', '📝', '📚', '📓', '💡', '🛠️', '🚀', '🎯', '📊'];

export function DocSidebar({ workspaceSlug }: { workspaceSlug: string }) {
  const workspace = useWorkspaceInfo();
  const router = useRouter();
  const params = useParams();
  const activeDocId = params.docId as string;

  // Workspace-level VaultSync queries for real-time document listing
  const { data: vsDocs = [], loading: vsLoading } = useQuery('docs') || {};
  const vsDocsMutations = useVaultSyncMutations('docs');

  // REST API load on mount as bootstrap
  const [restDocs, setRestDocs] = useState<DocItem[]>([]);
  const [restLoading, setRestLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [newTitle, setNewTitle] = useState('');
  const [selectedIcon, setSelectedIcon] = useState(ICON_PRESETS[0]);
  const [associatedProjectId, setAssociatedProjectId] = useState('');
  const [createLoading, setCreateLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Projects list for dropdown selector
  const [projects, setProjects] = useState<any[]>([]);

  useEffect(() => {
    async function loadInitialDocsAndProjects() {
      try {
        // Fetch docs
        const docRes = await fetch(`/api/workspaces/${workspace.id}/documents`);
        if (docRes.ok) {
          const docData = await docRes.json();
          setRestDocs(docData.documents || []);

          // Synced back to VaultSync to keep local cache alive
          for (const doc of docData.documents || []) {
            await vsDocsMutations.insert(doc.id, {
              id: doc.id,
              title: doc.title,
              icon: doc.icon,
              projectId: doc.project_id || null,
              createdAt: doc.created_at,
            });
          }
        }

        // Fetch projects
        const projRes = await fetch(`/api/workspaces/${workspace.id}/projects`);
        if (projRes.ok) {
          const projData = await projRes.json();
          setProjects(projData.projects || []);
        }
      } catch (err) {
        console.error('Failed to bootstrap docs list:', err);
      } finally {
        setRestLoading(false);
      }
    }

    loadInitialDocsAndProjects();
  }, [workspace.id, vsDocsMutations]);

  // Combine REST loaded docs and VaultSync synced state to be bulletproof
  const docMap = new Map<string, DocItem>();
  restDocs.forEach(d => docMap.set(d.id, d));
  vsDocs.forEach((d: any) => {
    docMap.set(String(d.id), {
      id: String(d.id),
      title: String(d.title),
      icon: String(d.icon),
      projectId: d.projectId ? String(d.projectId) : null,
      createdAt: Number(d.createdAt || Date.now()),
    });
  });

  const allDocuments = Array.from(docMap.values()).sort((a, b) => b.createdAt - a.createdAt);

  const handleCreateDocument = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTitle.trim()) return;

    setCreateLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/workspaces/${workspace.id}/documents`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: newTitle,
          icon: selectedIcon,
          projectId: associatedProjectId || null,
        }),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to create document');

      // Update local VaultSync list instantly
      await vsDocsMutations.insert(data.id, {
        id: data.id,
        title: data.title,
        icon: data.icon,
        projectId: data.projectId,
        createdAt: data.createdAt,
      });

      setModalOpen(false);
      setNewTitle('');
      setSelectedIcon(ICON_PRESETS[0]);
      setAssociatedProjectId('');

      router.push(`/${workspaceSlug}/docs/${data.id}`);
    } catch (err: any) {
      setError(err.message || 'Error creating document');
    } finally {
      setCreateLoading(false);
    }
  };

  const getProjectName = (projId: string | null) => {
    if (!projId) return null;
    const p = projects.find(item => item.id === projId);
    return p ? p.name : null;
  };

  return (
    <div className="w-64 border-r border-zinc-900 bg-zinc-950/70 backdrop-blur-md flex flex-col justify-between flex-shrink-0 h-full">
      <div className="flex flex-col flex-grow min-h-0">
        {/* Header */}
        <div className="p-4 border-b border-zinc-900 flex items-center justify-between">
          <span className="text-xs font-bold uppercase tracking-wider text-zinc-400">Library Docs</span>
          <button
            onClick={() => setModalOpen(true)}
            className="p-1 rounded bg-zinc-900 hover:bg-zinc-800 text-zinc-300 hover:text-white transition-all text-xs cursor-pointer font-bold"
            title="Create Document"
          >
            ＋ New
          </button>
        </div>

        {/* Tree list */}
        <div className="flex-1 overflow-y-auto px-2 py-3 space-y-4">
          <div className="space-y-1">
            {allDocuments.map((doc) => {
              const isActive = doc.id === activeDocId;
              const projectName = getProjectName(doc.projectId);

              return (
                <Link
                  key={doc.id}
                  href={`/${workspaceSlug}/docs/${doc.id}`}
                  className={`flex flex-col p-2.5 rounded-lg text-left transition-all ${
                    isActive
                      ? 'bg-indigo-600/10 border border-indigo-500/25 text-white'
                      : 'hover:bg-zinc-900/60 border border-transparent text-zinc-400 hover:text-zinc-200'
                  }`}
                >
                  <div className="flex items-center space-x-2.5 min-w-0">
                    <span className="text-sm flex-shrink-0">{doc.icon}</span>
                    <span className="text-xs font-semibold truncate flex-grow leading-tight">{doc.title}</span>
                  </div>
                  {projectName && (
                    <span className="text-3xs font-medium text-indigo-400/80 mt-1 pl-6 uppercase tracking-wider truncate">
                      📁 {projectName}
                    </span>
                  )}
                </Link>
              );
            })}

            {!restLoading && allDocuments.length === 0 && (
              <p className="text-3xs text-zinc-650 italic text-center py-6">No documents yet. Click New to create one!</p>
            )}

            {(restLoading || vsLoading) && allDocuments.length === 0 && (
              <div className="flex justify-center py-6">
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-indigo-500"></div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Create Doc Modal */}
      {modalOpen && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4 z-50">
          <form
            onSubmit={handleCreateDocument}
            className="w-full max-w-md bg-zinc-900 border border-zinc-800 rounded-2xl shadow-2xl p-6 space-y-4"
          >
            <div>
              <h3 className="text-sm font-bold text-white uppercase tracking-wider mb-1">Create Document</h3>
              <p className="text-3xs text-zinc-500">Add a new collaborative rich-text notes or spec sheet.</p>
            </div>

            {error && <div className="text-xs text-red-400 bg-red-950/20 border border-red-900/40 p-2 rounded-lg">{error}</div>}

            <div className="space-y-1">
              <label className="block text-3xs font-semibold text-zinc-400 uppercase tracking-wider">Icon</label>
              <div className="flex flex-wrap gap-2 pt-1">
                {ICON_PRESETS.map((ico) => (
                  <button
                    key={ico}
                    type="button"
                    onClick={() => setSelectedIcon(ico)}
                    className={`w-8 h-8 rounded-lg flex items-center justify-center text-sm border transition-all cursor-pointer ${
                      selectedIcon === ico
                        ? 'bg-indigo-600 border-indigo-500 text-white scale-110'
                        : 'bg-zinc-950 border-zinc-850 text-zinc-400 hover:border-zinc-700'
                    }`}
                  >
                    {ico}
                  </button>
                ))}
              </div>
            </div>

            <div className="space-y-1">
              <label className="block text-3xs font-semibold text-zinc-400 uppercase tracking-wider">Title</label>
              <input
                type="text"
                value={newTitle}
                onChange={(e) => setNewTitle(e.target.value)}
                placeholder="Product Specification, Meeting Notes..."
                required
                className="w-full px-3 py-2 bg-zinc-950 border border-zinc-850 rounded-lg text-xs text-zinc-300 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
            </div>

            <div className="space-y-1">
              <label className="block text-3xs font-semibold text-zinc-400 uppercase tracking-wider">Link to Project (Optional)</label>
              <select
                value={associatedProjectId}
                onChange={(e) => setAssociatedProjectId(e.target.value)}
                className="w-full px-3 py-2 bg-zinc-950 border border-zinc-850 rounded-lg text-xs text-zinc-400 focus:outline-none"
              >
                <option value="">None (Workspace Level)</option>
                {projects.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-center justify-end space-x-3 pt-2">
              <button
                type="button"
                onClick={() => setModalOpen(false)}
                className="px-4 py-2 border border-zinc-800 hover:bg-zinc-850 text-xs font-semibold text-zinc-400 rounded-lg transition-colors cursor-pointer"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={createLoading || !newTitle.trim()}
                className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-xs font-semibold text-white rounded-lg transition-colors cursor-pointer"
              >
                {createLoading ? 'Creating...' : 'Create'}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
