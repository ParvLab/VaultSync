'use client';

import { useState } from 'react';
import { useWorkspaceInfo } from '@/components/workspace/WorkspaceProvider';
import { InviteLink } from '@/components/workspace/InviteLink';
import { MemberList } from '@/components/workspace/MemberList';

export default function WorkspaceSettingsPage() {
  const workspace = useWorkspaceInfo();
  const [name, setName] = useState(workspace.name);
  const [slug, setSlug] = useState(workspace.slug);
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleUpdateSettings = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setSuccess(false);
    setError(null);

    try {
      const res = await fetch(`/api/workspaces/${workspace.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, slug }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || 'Failed to update settings');
      }

      setSuccess(true);
      setTimeout(() => setSuccess(false), 3000);
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  const isOwnerOrAdmin = ['owner', 'admin'].includes(workspace.role);

  return (
    <div className="p-8 max-w-5xl mx-auto space-y-8">
      <div>
        <h1 className="text-3xl font-extrabold text-white">Workspace Settings</h1>
        <p className="text-zinc-400 text-sm mt-1">
          Manage collaboration links, members lists, and workspace settings details.
        </p>
      </div>

      {isOwnerOrAdmin && (
        <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800 shadow-lg">
          <h3 className="text-lg font-bold text-white mb-4">Workspace Details</h3>
          {error && (
            <div className="mb-4 p-3 bg-red-950/30 border border-red-500/20 text-red-200 text-xs rounded-lg">
              {error}
            </div>
          )}
          {success && (
            <div className="mb-4 p-3 bg-emerald-950/30 border border-emerald-500/20 text-emerald-200 text-xs rounded-lg">
              Settings updated successfully! Please reload to apply slug changes.
            </div>
          )}
          <form onSubmit={handleUpdateSettings} className="space-y-4 max-w-md">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-zinc-500 mb-2">
                Workspace Name
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                className="w-full px-4 py-2.5 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-zinc-500 mb-2">
                Slug Identifier
              </label>
              <input
                type="text"
                value={slug}
                onChange={(e) => setSlug(e.target.value)}
                required
                className="w-full px-4 py-2.5 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="px-5 py-2.5 rounded-lg text-white text-sm font-medium gradient-btn shadow-lg disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
            >
              {loading ? 'Saving...' : 'Save Settings'}
            </button>
          </form>
        </div>
      )}

      {/* Invite Generation Card */}
      <InviteLink />

      {/* Active Members Table */}
      <MemberList />
    </div>
  );
}
