'use client';

import { useState } from 'react';
import { useQuery, useVaultSyncMutations } from '@vaultsync/react';
import { useWorkspaceInfo } from '../workspace/WorkspaceProvider';

export function CardComments({ cardId }: { cardId: string }) {
  const workspace = useWorkspaceInfo();
  const docId = `comments:card_${cardId}`;
  
  const { data: comments = [] } = useQuery(docId) || {};
  const mutations = useVaultSyncMutations(docId);

  const [newComment, setNewComment] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newComment.trim()) return;

    setLoading(true);
    try {
      const commentId = crypto.randomUUID();
      await mutations.insert(commentId, {
        id: commentId,
        text: newComment,
        authorId: String(workspace.members[0]?.id || ''), // use workspace member ID
        createdAt: Date.now(),
      });
      setNewComment('');
    } catch (err) {
      console.error('Failed to add comment:', err);
    } finally {
      setLoading(false);
    }
  };

  const sortedComments = [...comments].sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0));

  const getAuthorDetails = (authorId: string) => {
    const member = workspace.members.find(m => String(m.id) === authorId || String(m.userId) === authorId);
    return member || { name: 'Unknown User', avatar_color: '#a1a1aa' };
  };

  return (
    <div className="space-y-4">
      <h4 className="text-xs font-bold uppercase tracking-wider text-zinc-500 mb-2">Discussion</h4>

      {/* Comments List */}
      <div className="space-y-3 max-h-60 overflow-y-auto pr-1">
        {sortedComments.map((comment) => {
          const author = getAuthorDetails(String(comment.authorId));
          return (
            <div key={String(comment.record_id || comment.id)} className="flex items-start space-x-3 text-sm">
              <div
                style={{ backgroundColor: author.avatar_color }}
                className="w-7 h-7 rounded-full flex items-center justify-center font-bold text-xs text-white flex-shrink-0"
              >
                {author.name.substring(0, 1).toUpperCase()}
              </div>
              <div className="flex-1 bg-zinc-950 border border-zinc-850 p-3 rounded-xl space-y-1">
                <div className="flex items-center justify-between">
                  <span className="font-semibold text-white text-xs">{author.name}</span>
                  <span className="text-2xs text-zinc-650">
                    {new Date(Number(comment.createdAt)).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
                <p className="text-zinc-300 leading-relaxed text-xs">{String(comment.text)}</p>
              </div>
            </div>
          );
        })}

        {sortedComments.length === 0 && (
          <p className="text-xs text-zinc-600 italic">No comments yet. Start the conversation!</p>
        )}
      </div>

      {/* Comment Form */}
      <form onSubmit={handleSubmit} className="flex items-start space-x-3 pt-2">
        <input
          type="text"
          value={newComment}
          onChange={(e) => setNewComment(e.target.value)}
          placeholder="Write a comment..."
          disabled={loading}
          required
          className="flex-grow px-3 py-2 bg-zinc-950 border border-zinc-800 rounded-lg text-xs text-zinc-300 focus:outline-none focus:ring-1 focus:ring-indigo-500"
        />
        <button
          type="submit"
          disabled={loading || !newComment.trim()}
          className="px-3.5 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-xs font-semibold text-white rounded-lg transition-colors cursor-pointer"
        >
          Send
        </button>
      </form>
    </div>
  );
}
