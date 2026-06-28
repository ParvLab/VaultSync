'use client';

import { useState } from 'react';
import { useWorkspaceInfo } from './WorkspaceProvider';

export function MemberList() {
  const workspace = useWorkspaceInfo();
  const [membersList, setMembersList] = useState<any[]>(workspace.members);
  const [loadingId, setLoadingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleRemove = async (userId: string) => {
    if (!confirm('Are you sure you want to remove this member from the workspace?')) {
      return;
    }

    setLoadingId(userId);
    setError(null);

    try {
      const res = await fetch(`/api/workspaces/${workspace.id}/members/${userId}`, {
        method: 'DELETE',
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || 'Failed to remove member');
      }

      setMembersList(prev => prev.filter(m => m.id !== userId));
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setLoadingId(null);
    }
  };

  const isOwnerOrAdmin = ['owner', 'admin'].includes(workspace.role);

  return (
    <div className="rounded-2xl bg-zinc-900 border border-zinc-800 overflow-hidden">
      <div className="px-6 py-4 border-b border-zinc-800/80">
        <h3 className="text-lg font-bold text-white">Active Members</h3>
        <p className="text-xs text-zinc-400 mt-1">
          Collaborating in real-time in this workspace
        </p>
      </div>

      {error && (
        <div className="p-4 bg-red-950/30 border-b border-red-500/20 text-red-200 text-xs">
          {error}
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm text-zinc-300">
          <thead className="bg-zinc-950 text-xs font-semibold uppercase text-zinc-500 border-b border-zinc-850">
            <tr>
              <th className="px-6 py-3.5">Member</th>
              <th className="px-6 py-3.5">Email</th>
              <th className="px-6 py-3.5">Role</th>
              {isOwnerOrAdmin && <th className="px-6 py-3.5 text-right">Actions</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-850/50">
            {membersList.map((member) => (
              <tr key={member.id} className="hover:bg-zinc-850/20">
                <td className="px-6 py-4 flex items-center space-x-3">
                  <div
                    style={{ backgroundColor: member.avatar_color }}
                    className="w-8 h-8 rounded-full flex items-center justify-center font-bold text-xs text-white"
                  >
                    {member.name.substring(0, 1).toUpperCase()}
                  </div>
                  <span className="font-medium text-white">{member.name}</span>
                </td>
                <td className="px-6 py-4 text-zinc-400">{member.email}</td>
                <td className="px-6 py-4">
                  <span
                    className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      member.role === 'owner'
                        ? 'bg-amber-950/40 text-amber-300 border border-amber-500/20'
                        : member.role === 'admin'
                        ? 'bg-purple-950/40 text-purple-300 border border-purple-500/20'
                        : 'bg-zinc-800 text-zinc-300'
                    }`}
                  >
                    {member.role}
                  </span>
                </td>
                {isOwnerOrAdmin && (
                  <td className="px-6 py-4 text-right">
                    {member.role !== 'owner' && (
                      <button
                        onClick={() => handleRemove(member.id)}
                        disabled={loadingId === member.id}
                        className="text-xs text-red-400 hover:text-red-300 font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
                      >
                        {loadingId === member.id ? 'Removing...' : 'Remove'}
                      </button>
                    )}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
