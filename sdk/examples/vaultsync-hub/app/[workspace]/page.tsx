'use client';

import { useWorkspaceInfo } from '@/components/workspace/WorkspaceProvider';
import Link from 'next/link';

export default function WorkspaceDashboard() {
  const workspace = useWorkspaceInfo();

  return (
    <div className="p-8 max-w-5xl mx-auto space-y-8">
      {/* Welcome Banner */}
      <div className="p-8 rounded-2xl bg-zinc-900 border border-zinc-800 flex flex-col md:flex-row md:items-center md:justify-between gap-6 shadow-xl">
        <div className="space-y-2">
          <h1 className="text-3xl font-extrabold text-white">
            {workspace.name} Dashboard
          </h1>
          <p className="text-zinc-400 text-sm max-w-xl">
            Welcome to your local-first team hub. Sync is online, and your data is stored securely in your browser's private storage (OPFS).
          </p>
        </div>
        <div className="flex-shrink-0">
          <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950/40 text-emerald-400 border border-emerald-500/20">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse mr-2"></span>
            Sync Connected
          </span>
        </div>
      </div>

      {/* Grid of Quick Actions & Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Kanban Board Link Card */}
        <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 hover:border-indigo-500/40 transition-all flex flex-col justify-between h-48 group shadow-lg">
          <div>
            <div className="text-2xl mb-3">📋</div>
            <h3 className="text-lg font-bold text-white mb-1 group-hover:text-indigo-400 transition-colors">
              Kanban Projects
            </h3>
            <p className="text-xs text-zinc-400">
              Manage tasks, assignees, priorities, subtasks, and dependency link graphs.
            </p>
          </div>
          <Link
            href={`/${workspace.slug}/projects`}
            className="text-xs font-bold text-indigo-400 hover:text-indigo-300 flex items-center space-x-1"
          >
            <span>Open boards</span>
            <span>→</span>
          </Link>
        </div>

        {/* Documents Link Card */}
        <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 hover:border-indigo-500/40 transition-all flex flex-col justify-between h-48 group shadow-lg">
          <div>
            <div className="text-2xl mb-3">📄</div>
            <h3 className="text-lg font-bold text-white mb-1 group-hover:text-indigo-400 transition-colors">
              Collaborative Docs
            </h3>
            <p className="text-xs text-zinc-400">
              Co-author notes and specifications with real-time cursor tracking and Yjs.
            </p>
          </div>
          <Link
            href={`/${workspace.slug}/docs`}
            className="text-xs font-bold text-indigo-400 hover:text-indigo-300 flex items-center space-x-1"
          >
            <span>Open documents</span>
            <span>→</span>
          </Link>
        </div>

        {/* Member Settings Link Card */}
        <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 hover:border-indigo-500/40 transition-all flex flex-col justify-between h-48 group shadow-lg">
          <div>
            <div className="text-2xl mb-3">⚙️</div>
            <h3 className="text-lg font-bold text-white mb-1 group-hover:text-indigo-400 transition-colors">
              Manage Settings
            </h3>
            <p className="text-xs text-zinc-400">
              Generate invite links and manage access for members of this workspace.
            </p>
          </div>
          <Link
            href={`/${workspace.slug}/settings`}
            className="text-xs font-bold text-indigo-400 hover:text-indigo-300 flex items-center space-x-1"
          >
            <span>Open settings</span>
            <span>→</span>
          </Link>
        </div>
      </div>

      {/* Basic Metrics List */}
      <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 shadow-xl">
        <h3 className="text-lg font-bold text-white mb-4">Workspace Details</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div className="flex justify-between py-2 border-b border-zinc-850">
            <span className="text-zinc-500">Slug Identifier:</span>
            <span className="font-semibold text-zinc-300">{workspace.slug}</span>
          </div>
          <div className="flex justify-between py-2 border-b border-zinc-850">
            <span className="text-zinc-500">Your Membership Role:</span>
            <span className="font-semibold text-indigo-400 uppercase text-xs tracking-wider">{workspace.role}</span>
          </div>
          <div className="flex justify-between py-2 border-b border-zinc-850">
            <span className="text-zinc-500">Active Collaborators:</span>
            <span className="font-semibold text-zinc-300">{workspace.members.length} users</span>
          </div>
          <div className="flex justify-between py-2 border-b border-zinc-850">
            <span className="text-zinc-500">OPFS Database File:</span>
            <span className="font-semibold text-zinc-400">ws_{workspace.id}_db</span>
          </div>
        </div>
      </div>
    </div>
  );
}
