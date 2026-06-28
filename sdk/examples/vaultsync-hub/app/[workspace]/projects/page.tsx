'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useWorkspaceInfo } from '@/components/workspace/WorkspaceProvider';
import Link from 'next/link';

interface Project {
  id: string;
  name: string;
  color: string;
  created_at: number;
}

const COLOR_PALETTE = [
  '#6366f1', // Indigo
  '#3b82f6', // Blue
  '#0ea5e9', // Sky
  '#10b981', // Emerald
  '#eab308', // Yellow
  '#f97316', // Orange
  '#ef4444', // Red
  '#ec4899', // Pink
  '#a855f7', // Purple
];

export default function ProjectsPage() {
  const workspace = useWorkspaceInfo();
  const router = useRouter();
  
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [newProjectName, setNewProjectName] = useState('');
  const [selectedColor, setSelectedColor] = useState(COLOR_PALETTE[0]);
  const [createLoading, setCreateLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function loadProjects() {
      try {
        const res = await fetch(`/api/workspaces/${workspace.id}/projects`);
        if (res.ok) {
          const data = await res.json();
          setProjects(data.projects || []);
        }
      } catch (err) {
        console.error('Failed to load projects:', err);
      } finally {
        setLoading(false);
      }
    }
    loadProjects();
  }, [workspace.id]);

  const handleCreateProject = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newProjectName.trim()) return;

    setCreateLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/workspaces/${workspace.id}/projects`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newProjectName, color: selectedColor }),
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || 'Failed to create project');
      }

      setProjects(prev => [data, ...prev]);
      setModalOpen(false);
      setNewProjectName('');
      setSelectedColor(COLOR_PALETTE[0]);
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setCreateLoading(false);
    }
  };

  const isAdminOrOwner = ['owner', 'admin'].includes(workspace.role);

  if (loading) {
    return (
      <div className="p-8 max-w-5xl mx-auto flex flex-col items-center justify-center min-h-[50vh] text-zinc-400">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Loading projects...</p>
      </div>
    );
  }

  return (
    <div className="p-8 max-w-6xl mx-auto space-y-8">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-extrabold text-white">Kanban Projects</h1>
          <p className="text-zinc-400 text-sm mt-1">
            Create and manage boards, columns, and task dependencies.
          </p>
        </div>
        
        {isAdminOrOwner && (
          <button
            onClick={() => setModalOpen(true)}
            className="px-5 py-2.5 rounded-lg text-white text-sm font-semibold gradient-btn shadow-lg cursor-pointer"
          >
            Create New Project
          </button>
        )}
      </div>

      {projects.length === 0 ? (
        <div className="p-12 text-center rounded-2xl bg-zinc-900 border border-zinc-800/80 max-w-xl mx-auto space-y-4">
          <div className="text-5xl">📋</div>
          <h3 className="text-lg font-bold text-white">No Projects Found</h3>
          <p className="text-zinc-400 text-sm">
            Create a project board to start tracking tasks and subtasks collaboratively with your team.
          </p>
          {isAdminOrOwner && (
            <button
              onClick={() => setModalOpen(true)}
              className="px-5 py-2.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-sm font-semibold cursor-pointer"
            >
              Get Started
            </button>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-6">
          {projects.map((project) => (
            <Link
              key={project.id}
              href={`/${workspace.slug}/projects/${project.id}`}
              className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 hover:border-indigo-500/30 transition-all flex flex-col justify-between h-40 group shadow-lg"
            >
              <div>
                <div className="flex items-center space-x-2.5 mb-2">
                  <span
                    style={{ backgroundColor: project.color }}
                    className="w-3 h-3 rounded-full flex-shrink-0"
                  />
                  <h3 className="text-lg font-bold text-white group-hover:text-indigo-400 transition-colors truncate">
                    {project.name}
                  </h3>
                </div>
                <p className="text-xs text-zinc-500">
                  Created {new Date(project.created_at).toLocaleDateString()}
                </p>
              </div>
              <span className="text-xs font-semibold text-indigo-400 group-hover:text-indigo-300 flex items-center space-x-1">
                <span>Open Board</span>
                <span>→</span>
              </span>
            </Link>
          ))}
        </div>
      )}

      {/* Create Project Modal */}
      {modalOpen && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
          <div className="w-full max-w-md bg-zinc-900 border border-zinc-800 rounded-2xl shadow-2xl p-6 relative overflow-hidden">
            <button
              onClick={() => setModalOpen(false)}
              className="absolute top-4 right-4 text-zinc-400 hover:text-white text-lg transition-colors cursor-pointer"
            >
              ✕
            </button>

            <h3 className="text-xl font-bold text-white mb-6">Create New Board</h3>
            {error && (
              <div className="mb-4 p-3 bg-red-950/40 border border-red-500/20 text-red-200 text-xs rounded-lg">
                {error}
              </div>
            )}

            <form onSubmit={handleCreateProject} className="space-y-6">
              <div>
                <label className="block text-xs font-semibold uppercase tracking-wider text-zinc-500 mb-2">
                  Board Name
                </label>
                <input
                  type="text"
                  value={newProjectName}
                  onChange={(e) => setNewProjectName(e.target.value)}
                  placeholder="e.g. Q3 Launch, Core Roadmap"
                  required
                  autoFocus
                  className="w-full px-4 py-2.5 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>

              <div>
                <label className="block text-xs font-semibold uppercase tracking-wider text-zinc-500 mb-3">
                  Select Accent Color
                </label>
                <div className="flex flex-wrap gap-3">
                  {COLOR_PALETTE.map((color) => (
                    <button
                      key={color}
                      type="button"
                      onClick={() => setSelectedColor(color)}
                      style={{ backgroundColor: color }}
                      className={`w-7 h-7 rounded-full border-2 transition-all cursor-pointer ${
                        selectedColor === color ? 'border-white scale-110' : 'border-transparent hover:scale-105'
                      }`}
                    />
                  ))}
                </div>
              </div>

              <div className="flex justify-end space-x-3 pt-2">
                <button
                  type="button"
                  onClick={() => setModalOpen(false)}
                  className="px-4 py-2.5 bg-zinc-850 hover:bg-zinc-800 text-sm font-semibold text-zinc-400 hover:text-white rounded-lg transition-colors cursor-pointer"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={createLoading}
                  className="px-5 py-2.5 rounded-lg text-white text-sm font-semibold gradient-btn shadow-lg disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
                >
                  {createLoading ? 'Creating...' : 'Create Board'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
