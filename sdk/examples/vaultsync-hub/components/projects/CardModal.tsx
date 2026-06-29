'use client';

import { useState, useEffect } from 'react';
import { useQuery, useVaultSyncMutations, useVaultSyncOne } from '@vaultsync/react';
import { useWorkspaceInfo } from '../workspace/WorkspaceProvider';
import { CardComments } from './CardComments';

interface CardModalProps {
  cardId: string;
  onClose: () => void;
}

export function CardModal({ cardId, onClose }: CardModalProps) {
  const workspace = useWorkspaceInfo();
  const card = useVaultSyncOne('cards', cardId);
  const { data: cards = [] } = useQuery('cards') || {};
  const { data: columns = [] } = useQuery('columns') || {};
  const { data: links = [] } = useQuery('links') || {};
  
  const cardMutations = useVaultSyncMutations('cards');
  const linkMutations = useVaultSyncMutations('links');

  // Input states
  const [title, setTitle] = useState('');
  const [desc, setDesc] = useState('');
  const [columnId, setColumnId] = useState('');
  const [assigneeId, setAssigneeId] = useState('');
  const [priority, setPriority] = useState('');
  const [dueDate, setDueDate] = useState('');

  // Subtask inputs
  const [newSubtaskTitle, setNewSubtaskTitle] = useState('');
  const [subtaskError, setSubtaskError] = useState<string | null>(null);

  // Link inputs
  const [linkTargetId, setLinkTargetId] = useState('');
  const [linkType, setLinkType] = useState('relates_to');
  const [linkError, setLinkError] = useState<string | null>(null);

  // Load data on mount/change
  useEffect(() => {
    if (card.data) {
      setTitle(String(card.data.title || ''));
      setDesc(String(card.data.desc || ''));
      setColumnId(String(card.data.columnId || ''));
      setAssigneeId(String(card.data.assigneeId || ''));
      setPriority(String(card.data.priority || 'medium'));
      setDueDate(String(card.data.dueDate || ''));
    }
  }, [card.data]);

  if (card.loading) {
    return (
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
        <div className="w-full max-w-2xl bg-zinc-900 border border-zinc-800 rounded-2xl p-8 text-center text-zinc-400">
          <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500 mx-auto mb-3"></div>
          <p className="text-sm">Loading task details...</p>
        </div>
      </div>
    );
  }

  if (!card.data) {
    return (
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
        <div className="w-full max-w-md bg-zinc-900 border border-zinc-800 rounded-2xl p-6 text-center text-zinc-400">
          <h3 className="text-lg font-bold text-white mb-2">Task Not Found</h3>
          <p className="text-sm text-zinc-500 mb-4">The task card may have been deleted by another collaborator.</p>
          <button onClick={onClose} className="px-4 py-2 bg-zinc-800 text-white rounded-lg text-sm">Close</button>
        </div>
      </div>
    );
  }

  // Handle direct updates
  const handleUpdateField = async (field: string, value: any) => {
    if (!card.data) return;
    await cardMutations.update(cardId, {
      ...card.data,
      [field]: value,
    });
  };

  const handleDeleteCard = async () => {
    if (!confirm('Are you sure you want to delete this task? All subtasks and dependencies will be removed.')) {
      return;
    }

    // 1. Delete all subtasks
    const subtaskCards = cards.filter((c: any) => String(c.parentId) === cardId);
    for (const sub of subtaskCards) {
      await cardMutations.delete(String(sub.record_id || sub.id));
    }

    // 2. Delete all links associated with this card
    const cardLinks = links.filter((l: any) => String(l.sourceId) === cardId || String(l.targetId) === cardId);
    for (const lnk of cardLinks) {
      await linkMutations.delete(String(lnk.record_id || lnk.id));
    }

    // 3. Delete card itself
    await cardMutations.delete(cardId);
    onClose();
  };

  // Subtasks logic
  const subtasks = cards.filter((c: any) => String(c.parentId) === cardId);

  const handleAddSubtask = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newSubtaskTitle.trim() || !card.data) return;

    setSubtaskError(null);
    try {
      const subtaskId = crypto.randomUUID();
      await cardMutations.insert(subtaskId, {
        id: subtaskId,
        title: newSubtaskTitle,
        parentId: cardId,
        columnId: String(card.data.columnId),
        completed: false,
        priority: 'low',
        desc: '',
        createdAt: Date.now(),
      });
      setNewSubtaskTitle('');
    } catch (err: any) {
      setSubtaskError(err.message || 'Failed to create subtask');
    }
  };

  const handleToggleSubtask = async (sub: any) => {
    const subId = String(sub.record_id || sub.id);
    await cardMutations.update(subId, {
      ...sub,
      completed: !sub.completed,
    });
  };

  const handleDeleteSubtask = async (subId: string) => {
    await cardMutations.delete(subId);
  };

  // Links logic
  const cardLinks = links.filter((l: any) => String(l.sourceId) === cardId || String(l.targetId) === cardId);

  const handleAddLink = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!linkTargetId) return;

    setLinkError(null);
    
    // Check if link already exists
    const duplicate = links.some(
      (l: any) =>
        (String(l.sourceId) === cardId && String(l.targetId) === linkTargetId && String(l.type) === linkType) ||
        (String(l.sourceId) === linkTargetId && String(l.targetId) === cardId && String(l.type) === linkType)
    );

    if (duplicate) {
      setLinkError('A relationship link between these tasks already exists.');
      return;
    }

    try {
      const linkId = crypto.randomUUID();
      await linkMutations.insert(linkId, {
        id: linkId,
        sourceId: cardId,
        targetId: linkTargetId,
        type: linkType,
      });
      setLinkTargetId('');
    } catch (err: any) {
      setLinkError(err.message || 'Failed to link tasks');
    }
  };

  const handleDeleteLink = async (linkId: string) => {
    await linkMutations.delete(linkId);
  };

  const getCardTitle = (id: string) => {
    const c = cards.find((item: any) => String(item.id) === id || String(item.record_id) === id);
    return c ? String(c.title) : 'Deleted Task';
  };

  return (
    <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4 z-50 overflow-y-auto">
      <div className="w-full max-w-4xl bg-zinc-900 border border-zinc-800 rounded-2xl shadow-2xl flex flex-col md:flex-row overflow-hidden max-h-[90vh]">
        {/* Left main info column */}
        <div className="flex-1 p-6 space-y-6 overflow-y-auto border-r border-zinc-850 max-h-[90vh]">
          {/* Close trigger for mobile */}
          <div className="flex justify-between items-center md:hidden">
            <span className="text-xs text-zinc-500 font-bold uppercase">Task Details</span>
            <button onClick={onClose} className="text-zinc-400 hover:text-white">✕</button>
          </div>

          {/* Title */}
          <div>
            <input
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              onBlur={() => handleUpdateField('title', title)}
              placeholder="Task Title"
              className="w-full bg-transparent text-2xl font-extrabold text-white border-b border-transparent hover:border-zinc-800 focus:border-indigo-500 focus:outline-none py-1 transition-colors"
            />
          </div>

          {/* Description */}
          <div className="space-y-2">
            <h4 className="text-xs font-bold uppercase tracking-wider text-zinc-500">Description</h4>
            <textarea
              value={desc}
              onChange={(e) => setDesc(e.target.value)}
              onBlur={() => handleUpdateField('desc', desc)}
              placeholder="Add a detailed description for this task..."
              rows={4}
              className="w-full px-4 py-3 bg-zinc-950 border border-zinc-800 rounded-xl text-sm text-zinc-300 placeholder-zinc-550 focus:outline-none focus:ring-1 focus:ring-indigo-500 transition-all"
            />
          </div>

          {/* Subtasks Section */}
          <div className="space-y-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-zinc-500">Subtasks</h4>
            
            {subtaskError && (
              <div className="p-2.5 bg-red-950/25 border border-red-500/20 text-red-200 text-2xs rounded-lg">
                {subtaskError}
              </div>
            )}

            {/* List */}
            <div className="space-y-2 max-h-40 overflow-y-auto">
              {subtasks.map((sub: any) => {
                const subId = String(sub.record_id || sub.id);
                return (
                  <div key={subId} className="flex items-center justify-between group px-2 py-1.5 hover:bg-zinc-850/20 rounded-lg text-sm">
                    <label className="flex items-center space-x-3 cursor-pointer text-zinc-300">
                      <input
                        type="checkbox"
                        checked={!!sub.completed}
                        onChange={() => handleToggleSubtask(sub)}
                        className="rounded border-zinc-800 bg-zinc-950 text-indigo-600 focus:ring-0"
                      />
                      <span className={sub.completed ? 'line-through text-zinc-550' : 'text-zinc-300'}>
                        {String(sub.title)}
                      </span>
                    </label>
                    <button
                      onClick={() => handleDeleteSubtask(subId)}
                      className="text-zinc-550 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-all cursor-pointer"
                    >
                      🗑️
                    </button>
                  </div>
                );
              })}

              {subtasks.length === 0 && (
                <p className="text-xs text-zinc-650 italic">No subtasks added yet.</p>
              )}
            </div>

            {/* Add Input */}
            <form onSubmit={handleAddSubtask} className="flex space-x-2">
              <input
                type="text"
                value={newSubtaskTitle}
                onChange={(e) => setNewSubtaskTitle(e.target.value)}
                placeholder="Add a subtask..."
                className="flex-grow px-3 py-1.5 bg-zinc-950 border border-zinc-800 rounded-lg text-xs text-zinc-300 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
              <button
                type="submit"
                disabled={!newSubtaskTitle.trim()}
                className="px-3.5 py-1.5 bg-zinc-800 hover:bg-zinc-750 text-xs font-semibold text-white rounded-lg transition-colors cursor-pointer"
              >
                Add
              </button>
            </form>
          </div>

          {/* Task Dependency Links Section */}
          <div className="space-y-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-zinc-500">Dependencies & Relations</h4>

            {linkError && (
              <div className="p-2.5 bg-red-950/25 border border-red-500/20 text-red-200 text-2xs rounded-lg">
                {linkError}
              </div>
            )}

            {/* Links List */}
            <div className="space-y-2">
              {cardLinks.map((lnk: any) => {
                const linkId = String(lnk.record_id || lnk.id);
                const isSource = String(lnk.sourceId) === cardId;
                const relCardId = String(isSource ? lnk.targetId : lnk.sourceId);
                const relCardTitle = getCardTitle(relCardId);

                let label = '';
                if (String(lnk.type) === 'blocks') {
                  label = isSource ? 'Blocks' : 'Blocked by';
                } else if (String(lnk.type) === 'blocked_by') {
                  label = isSource ? 'Blocked by' : 'Blocks';
                } else {
                  label = 'Relates to';
                }

                return (
                  <div key={linkId} className="flex items-center justify-between px-3 py-2 bg-zinc-950/40 border border-zinc-850 rounded-lg text-xs">
                    <div className="flex items-center space-x-2 truncate">
                      <span className="font-semibold text-indigo-400 uppercase tracking-wider text-3xs border border-indigo-500/20 px-1.5 py-0.5 rounded-full bg-indigo-950/20">
                        {label}
                      </span>
                      <span className="text-zinc-300 truncate font-medium">{relCardTitle}</span>
                    </div>
                    <button
                      onClick={() => handleDeleteLink(linkId)}
                      className="text-zinc-550 hover:text-red-400 ml-2 cursor-pointer"
                    >
                      ✕
                    </button>
                  </div>
                );
              })}

              {cardLinks.length === 0 && (
                <p className="text-xs text-zinc-650 italic">No linked task dependencies defined.</p>
              )}
            </div>

            {/* Link Form */}
            <form onSubmit={handleAddLink} className="flex flex-col sm:flex-row gap-2">
              <select
                value={linkType}
                onChange={(e) => setLinkType(e.target.value)}
                className="px-2.5 py-1.5 bg-zinc-950 border border-zinc-800 rounded-lg text-xs text-zinc-400 focus:outline-none"
              >
                <option value="relates_to">Relates to</option>
                <option value="blocks">Blocks</option>
                <option value="blocked_by">Blocked by</option>
              </select>

              <select
                value={linkTargetId}
                onChange={(e) => setLinkTargetId(e.target.value)}
                required
                className="flex-grow px-2.5 py-1.5 bg-zinc-950 border border-zinc-800 rounded-lg text-xs text-zinc-400 focus:outline-none"
              >
                <option value="">Select task card...</option>
                {cards
                  .filter((c: any) => c.id !== cardId && c.record_id !== cardId && !c.parentId)
                  .map((c: any) => (
                    <option key={String(c.id)} value={String(c.id || c.record_id)}>
                      {String(c.title)}
                    </option>
                  ))}
              </select>

              <button
                type="submit"
                disabled={!linkTargetId}
                className="px-3.5 py-1.5 bg-zinc-800 hover:bg-zinc-750 text-xs font-semibold text-white rounded-lg transition-colors cursor-pointer"
              >
                Link
              </button>
            </form>
          </div>

          <hr className="border-zinc-850" />

          {/* Comments Discussion Section */}
          <CardComments cardId={cardId} />
        </div>

        {/* Right meta sidebar column */}
        <div className="w-full md:w-64 p-6 bg-zinc-950 flex flex-col justify-between max-h-[90vh]">
          <div className="space-y-6">
            <div className="hidden md:flex justify-between items-center">
              <span className="text-xs font-bold uppercase tracking-wider text-zinc-500">Metadata Details</span>
              <button
                onClick={onClose}
                className="text-zinc-450 hover:text-white transition-colors cursor-pointer text-lg font-bold"
              >
                ✕
              </button>
            </div>

            {/* Column Selector */}
            <div className="space-y-2">
              <label className="block text-3xs font-semibold uppercase tracking-wider text-zinc-550">Column / Status</label>
              <select
                value={columnId}
                onChange={(e) => {
                  setColumnId(e.target.value);
                  handleUpdateField('columnId', e.target.value);
                }}
                className="w-full px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-xs text-zinc-300 focus:outline-none"
              >
                {columns.map((col: any) => (
                  <option key={String(col.id)} value={String(col.id || col.record_id)}>
                    {String(col.name)}
                  </option>
                ))}
              </select>
            </div>

            {/* Assignee Selector */}
            <div className="space-y-2">
              <label className="block text-3xs font-semibold uppercase tracking-wider text-zinc-550">Assignee</label>
              <select
                value={assigneeId}
                onChange={(e) => {
                  setAssigneeId(e.target.value);
                  handleUpdateField('assigneeId', e.target.value);
                }}
                className="w-full px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-xs text-zinc-300 focus:outline-none"
              >
                <option value="">Unassigned</option>
                {workspace.members.map((member) => (
                  <option key={member.id} value={member.id || member.userId}>
                    {member.name}
                  </option>
                ))}
              </select>
            </div>

            {/* Priority Selector */}
            <div className="space-y-2">
              <label className="block text-3xs font-semibold uppercase tracking-wider text-zinc-550">Priority</label>
              <select
                value={priority}
                onChange={(e) => {
                  setPriority(e.target.value);
                  handleUpdateField('priority', e.target.value);
                }}
                className="w-full px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-xs text-zinc-300 focus:outline-none"
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="urgent">Urgent</option>
              </select>
            </div>

            {/* Due Date Input */}
            <div className="space-y-2">
              <label className="block text-3xs font-semibold uppercase tracking-wider text-zinc-550">Due Date</label>
              <input
                type="date"
                value={dueDate}
                onChange={(e) => {
                  setDueDate(e.target.value);
                  handleUpdateField('dueDate', e.target.value);
                }}
                className="w-full px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-xs text-zinc-350 focus:outline-none"
              />
            </div>
          </div>

          {/* Delete Action Footer */}
          <div className="pt-6 border-t border-zinc-900/60 mt-6 md:mt-0">
            <button
              onClick={handleDeleteCard}
              className="w-full py-2.5 bg-red-950/20 hover:bg-red-950/40 border border-red-900/35 hover:border-red-500/20 text-red-300 text-xs font-semibold rounded-lg transition-colors cursor-pointer"
            >
              Delete Task Card
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
