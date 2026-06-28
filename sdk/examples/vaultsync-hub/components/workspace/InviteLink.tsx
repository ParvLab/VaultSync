'use client';

import { useState } from 'react';
import { useWorkspaceInfo } from './WorkspaceProvider';

export function InviteLink() {
  const workspace = useWorkspaceInfo();
  const [inviteUrl, setInviteUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const generateLink = async () => {
    setLoading(true);
    setError(null);
    setCopied(false);

    try {
      const res = await fetch(`/api/workspaces/${workspace.id}/invites`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: 'member', uses_left: 10 }),
      });

      if (!res.ok) {
        throw new Error('Failed to generate invite link');
      }

      const data = await res.json();
      const origin = window.location.origin;
      setInviteUrl(`${origin}/invite/${data.token}`);
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  const copyToClipboard = () => {
    if (inviteUrl) {
      navigator.clipboard.writeText(inviteUrl);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const isAdminOrOwner = ['owner', 'admin'].includes(workspace.role);

  if (!isAdminOrOwner) {
    return (
      <div className="p-6 rounded-2xl bg-zinc-900/50 border border-zinc-800/40 text-center text-zinc-500 text-sm">
        Only administrators or owners can generate invite links for this workspace.
      </div>
    );
  }

  return (
    <div className="p-6 rounded-2xl bg-zinc-900 border border-zinc-800">
      <h3 className="text-lg font-bold text-white mb-2">Invite Members</h3>
      <p className="text-sm text-zinc-400 mb-6">
        Generate a reusable invitation link. Anyone with this link can join this workspace as a member.
      </p>

      {error && (
        <div className="mb-4 p-3 bg-red-950/30 border border-red-500/20 text-red-200 text-xs rounded-lg">
          {error}
        </div>
      )}

      {inviteUrl ? (
        <div className="space-y-4">
          <div className="flex space-x-2">
            <input
              type="text"
              readOnly
              value={inviteUrl}
              onClick={(e) => (e.target as HTMLInputElement).select()}
              className="flex-grow px-4 py-2.5 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300 select-all focus:outline-none"
            />
            <button
              onClick={copyToClipboard}
              className="px-4 py-2.5 bg-indigo-600 hover:bg-indigo-500 active:scale-95 text-sm font-semibold text-white rounded-lg transition-all cursor-pointer"
            >
              {copied ? 'Copied!' : 'Copy'}
            </button>
          </div>
          <p className="text-xs text-zinc-500 italic">
            * This link is active for 7 days and can be used up to 10 times.
          </p>
        </div>
      ) : (
        <button
          onClick={generateLink}
          disabled={loading}
          className="px-5 py-2.5 rounded-lg text-white text-sm font-medium gradient-btn shadow-lg disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
        >
          {loading ? 'Generating...' : 'Create Invite Link'}
        </button>
      )}
    </div>
  );
}
