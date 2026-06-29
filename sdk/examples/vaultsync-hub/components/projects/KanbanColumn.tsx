'use client';

import { useState } from 'react';
import { Draggable, Droppable } from '@hello-pangea/dnd';
import { KanbanCard } from './KanbanCard';

interface KanbanColumnProps {
  columnId: string;
  index: number;
  name: string;
  color: string;
  cards: any[];
  onAddCard: (columnId: string, title: string) => Promise<void>;
  onCardClick: (cardId: string) => void;
}

export function KanbanColumn({
  columnId,
  index,
  name,
  color,
  cards,
  onAddCard,
  onCardClick,
}: KanbanColumnProps) {
  const [showAddForm, setShowAddForm] = useState(false);
  const [newCardTitle, setNewCardTitle] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newCardTitle.trim()) return;

    await onAddCard(columnId, newCardTitle);
    setNewCardTitle('');
    setShowAddForm(false);
  };

  return (
    <Draggable draggableId={columnId} index={index}>
      {(provided, snapshot) => (
        <div
          ref={provided.innerRef}
          {...provided.draggableProps}
          className={`flex flex-col w-72 max-h-full rounded-2xl bg-zinc-900 border transition-all ${
            snapshot.isDragging ? 'border-indigo-500/30 shadow-2xl scale-102' : 'border-zinc-850/60'
          }`}
        >
          {/* Header */}
          <div
            {...provided.dragHandleProps}
            className="p-4 border-b border-zinc-850/60 flex items-center justify-between flex-shrink-0 cursor-grab active:cursor-grabbing"
          >
            <div className="flex items-center space-x-2.5 min-w-0">
              <span
                style={{ backgroundColor: color }}
                className="w-2.5 h-2.5 rounded-full flex-shrink-0"
              />
              <h3 className="font-bold text-white text-sm truncate">{name}</h3>
              <span className="px-2 py-0.5 rounded-full bg-zinc-950 text-zinc-500 text-xs font-semibold">
                {cards.length}
              </span>
            </div>
            
            <button
              onClick={() => setShowAddForm(!showAddForm)}
              className="w-6 h-6 rounded-md hover:bg-zinc-800 flex items-center justify-center text-zinc-400 hover:text-white transition-colors cursor-pointer text-sm font-semibold"
            >
              +
            </button>
          </div>

          {/* Cards List Area */}
          <Droppable droppableId={columnId} type="card">
            {(provided, snapshot) => (
              <div
                ref={provided.innerRef}
                {...provided.droppableProps}
                className={`flex-grow overflow-y-auto p-3 space-y-3 min-h-[100px] transition-colors ${
                  snapshot.isDraggingOver ? 'bg-zinc-850/10' : ''
                }`}
              >
                {cards.map((card, idx) => {
                  const cardId = card.record_id || card.id;
                  return (
                    <KanbanCard
                      key={cardId}
                      cardId={cardId}
                      index={idx}
                      title={card.title}
                      priority={card.priority}
                      onClick={() => onCardClick(cardId)}
                    />
                  );
                })}
                {provided.placeholder}
              </div>
            )}
          </Droppable>

          {/* Add Task Form Footer */}
          {showAddForm ? (
            <form onSubmit={handleSubmit} className="p-3 border-t border-zinc-850/40 space-y-2 flex-shrink-0">
              <input
                type="text"
                value={newCardTitle}
                onChange={(e) => setNewCardTitle(e.target.value)}
                placeholder="Enter task title..."
                required
                autoFocus
                className="w-full px-3 py-2 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300 placeholder-zinc-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
              <div className="flex justify-end space-x-2">
                <button
                  type="button"
                  onClick={() => setShowAddForm(false)}
                  className="px-2.5 py-1.5 bg-zinc-850 hover:bg-zinc-800 text-xs font-semibold text-zinc-400 rounded-md transition-colors cursor-pointer"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-xs font-semibold text-white rounded-md transition-colors cursor-pointer"
                >
                  Add
                </button>
              </div>
            </form>
          ) : (
            <button
              onClick={() => setShowAddForm(true)}
              className="p-3 text-left text-xs font-semibold text-zinc-500 hover:text-zinc-300 hover:bg-zinc-850/20 border-t border-zinc-850/20 flex-shrink-0 transition-colors cursor-pointer"
            >
              + Add Task Card
            </button>
          )}
        </div>
      )}
    </Draggable>
  );
}
