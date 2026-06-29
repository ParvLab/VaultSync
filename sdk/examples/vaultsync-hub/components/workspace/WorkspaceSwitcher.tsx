'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useWorkspaceInfo } from './WorkspaceProvider';

interface WorkspaceItem {
  id: string;
  name: string;
  slug: string;
  role: string;
}

export function WorkspaceSwitcher() {
  const router = useRouter();
  const currentWorkspace = useWorkspaceInfo();
  const [workspaces, setWorkspaces] = useState<WorkspaceItem[]>([]);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    async function loadWorkspaces() {
      try {
        const res = await fetch('/api/workspaces');
        if (res.ok) {
          const data = await res.json();
          setWorkspaces(data.workspaces || []);
        }
      } catch (err) {
        console.error('Failed to load workspaces:', err);
      }
    }
    loadWorkspaces();
  }, []);

  const handleSelect = (slug: string) => {
    setOpen(false);
    router.push(`/${slug}`);
  };

  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-4 py-3 bg-zinc-900 border border-zinc-800 hover:bg-zinc-800/80 rounded-xl text-white font-semibold transition-all cursor-pointer text-left"
      >
        <div className="flex items-center space-x-3 overflow-hidden">
          <div className="flex-shrink-0 w-8 h-8 rounded-lg gradient-btn flex items-center justify-center font-bold text-sm text-white">
            {currentWorkspace.name.substring(0, 2).toUpperCase()}
          </div>
          <span className="truncate">{currentWorkspace.name}</span>
        </div>
        <svg
          className={`w-4 h-4 text-zinc-400 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
          <div className="absolute left-0 right-0 mt-2 p-1 bg-zinc-900 border border-zinc-800 rounded-xl shadow-2xl z-20 overflow-hidden max-h-60 overflow-y-auto">
            <div className="px-3 py-2 text-xs font-semibold uppercase tracking-wider text-zinc-500 border-b border-zinc-800/50 mb-1">
              Switch Workspace
            </div>
            {workspaces
              .filter(w => w.slug !== currentWorkspace.slug)
              .map(w => (
                <button
                  key={w.id}
                  onClick={() => handleSelect(w.slug)}
                  className="w-full flex items-center space-x-3 px-3 py-2.5 hover:bg-zinc-800 text-sm text-zinc-300 hover:text-white rounded-lg text-left transition-colors cursor-pointer"
                >
                  <div className="w-6 h-6 rounded bg-zinc-800 flex items-center justify-center font-semibold text-xs text-zinc-400 uppercase">
                    {w.name.substring(0, 2)}
                  </div>
                  <span className="truncate flex-1">{w.name}</span>
                </button>
              ))}

            {workspaces.filter(w => w.slug !== currentWorkspace.slug).length === 0 && (
              <div className="px-3 py-2.5 text-xs text-zinc-600 italic">
                No other workspaces
              </div>
            )}

            <div className="border-t border-zinc-850/50 mt-1 pt-1">
              <button
                onClick={() => {
                  setOpen(false);
                  router.push('/');
                }}
                className="w-full flex items-center space-x-3 px-3 py-2.5 hover:bg-zinc-800 text-sm text-indigo-400 hover:text-indigo-300 rounded-lg text-left transition-colors cursor-pointer"
              >
                <span className="text-lg font-semibold leading-none">+</span>
                <span>Create Workspace</span>
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
