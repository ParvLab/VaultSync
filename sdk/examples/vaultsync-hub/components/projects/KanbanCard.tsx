'use client';

import { Draggable } from '@hello-pangea/dnd';
import { useQuery } from '@vaultsync/react';

interface KanbanCardProps {
  cardId: string;
  index: number;
  title: string;
  priority: string;
  onClick: () => void;
}

export function KanbanCard({
  cardId,
  index,
  title,
  priority,
  onClick,
}: KanbanCardProps) {
  // Query all cards to calculate subtask statistics for this card
  const { data: allCards = [] } = useQuery('cards') || {};
  const subtasks = allCards.filter((c: any) => String(c.parentId) === cardId);
  const completedSubtasks = subtasks.filter((c: any) => c.completed === true);

  const priorityColors: Record<string, string> = {
    low: 'bg-zinc-800 text-zinc-400 border-zinc-700/30',
    medium: 'bg-blue-950/40 text-blue-300 border-blue-500/20',
    high: 'bg-orange-950/40 text-orange-300 border-orange-500/20',
    urgent: 'bg-red-950/40 text-red-300 border-red-500/20',
  };

  return (
    <Draggable draggableId={cardId} index={index}>
      {(provided, snapshot) => (
        <div
          ref={provided.innerRef}
          {...provided.draggableProps}
          {...provided.dragHandleProps}
          onClick={onClick}
          className={`p-4 rounded-xl bg-zinc-950 border hover:border-zinc-750 transition-all select-none cursor-pointer shadow-md ${
            snapshot.isDragging ? 'border-indigo-500/50 shadow-2xl scale-103' : 'border-zinc-850/60'
          }`}
        >
          {/* Card Header & Priority */}
          <div className="flex items-center justify-between gap-2 mb-2">
            <span
              className={`inline-flex items-center px-2 py-0.5 rounded-full text-2xs font-semibold uppercase tracking-wider border ${
                priorityColors[priority] || priorityColors.medium
              }`}
            >
              {priority}
            </span>
          </div>

          {/* Title */}
          <h4 className="text-sm font-semibold text-white mb-3 line-clamp-2 leading-relaxed">
            {title}
          </h4>

          {/* Card Footer / Metrics */}
          {subtasks.length > 0 && (
            <div className="flex items-center justify-between pt-2.5 border-t border-zinc-900/60 text-2xs font-medium text-zinc-500">
              <span className="flex items-center space-x-1">
                <span>☑️</span>
                <span>
                  {completedSubtasks.length}/{subtasks.length} Subtasks
                </span>
              </span>

              {/* Progress bar */}
              <div className="w-16 h-1.5 rounded-full bg-zinc-900 overflow-hidden">
                <div
                  style={{ width: `${(completedSubtasks.length / subtasks.length) * 100}%` }}
                  className="h-full bg-indigo-500 rounded-full transition-all duration-300"
                />
              </div>
            </div>
          )}
        </div>
      )}
    </Draggable>
  );
}
