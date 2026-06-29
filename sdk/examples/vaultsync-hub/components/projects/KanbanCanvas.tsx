'use client';

import { useEffect, useState } from 'react';
import { useQuery, useVaultSyncMutations, useVaultSyncClient } from '@vaultsync/react';
import { useProjectInfo } from './ProjectProvider';
import { DragDropContext, Droppable, DropResult } from '@hello-pangea/dnd';
import { KanbanColumn } from './KanbanColumn';
import { CardModal } from './CardModal';
import Link from 'next/link';

// Helper to generate fractional indices for sorting
export function getFractionalIndex(prev: string | null, next: string | null): string {
  if (!prev && !next) return 'h';
  if (!prev) {
    const nextChar = next!.charCodeAt(0);
    return String.fromCharCode(Math.max(97, nextChar - 1)) + next!.slice(1);
  }
  if (!next) {
    const prevChar = prev!.charCodeAt(0);
    return String.fromCharCode(Math.min(122, prevChar + 1)) + prev!.slice(1);
  }
  
  // Midpoint calculation
  let i = 0;
  while (true) {
    const pCode = prev.charCodeAt(i) || 96; // `a` - 1
    const nCode = next.charCodeAt(i) || 123; // `z` + 1
    
    if (nCode - pCode > 1) {
      const mid = Math.round((pCode + nCode) / 2);
      return prev.slice(0, i) + String.fromCharCode(mid);
    }
    i++;
  }
}

export function KanbanCanvas() {
  const project = useProjectInfo();
  const vsClient = useVaultSyncClient();
  const { data: columns = [] } = useQuery('columns') || {};
  const { data: cards = [] } = useQuery('cards') || {};
  
  const colMutations = useVaultSyncMutations('columns');
  const cardMutations = useVaultSyncMutations('cards');

  const [activeCardId, setActiveCardId] = useState<string | null>(null);
  const [bootstrapping, setBootstrapping] = useState(false);

  // 1. Bootstrap default columns if none exist
  useEffect(() => {
    async function bootstrap() {
      if (columns.length === 0 && !bootstrapping) {
        setBootstrapping(true);
        try {
          const defaults = [
            { id: 'todo', name: 'To Do', color: '#a1a1aa', order: 'h' },
            { id: 'in_progress', name: 'In Progress', color: '#6366f1', order: 'p' },
            { id: 'done', name: 'Done', color: '#10b981', order: 'x' },
          ];
          for (const col of defaults) {
            await colMutations.insert(col.id, col);
          }
        } catch (err) {
          console.error('Failed to bootstrap columns:', err);
        } finally {
          setBootstrapping(false);
        }
      }
    }
    bootstrap();
  }, [columns.length, colMutations, bootstrapping]);

  // Sort columns and cards
  const sortedColumns = [...columns].sort((a: any, b: any) => String(a.order || '').localeCompare(String(b.order || '')));
  const rootCards = cards.filter((c: any) => !c.parentId);

  const handleDragEnd = async (result: DropResult) => {
    const { destination, source, draggableId, type } = result;
    if (!destination) return;

    // Check if item was dropped in the same position
    if (
      destination.droppableId === source.droppableId &&
      destination.index === source.index
    ) {
      return;
    }

    if (type === 'column') {
      // Reordering columns
      const colId = draggableId;
      const targetIndex = destination.index;
      
      const colToMove = sortedColumns.find((c: any) => String(c.record_id || c.id) === colId);
      if (!colToMove) return;

      let prevOrder: string | null = null;
      let nextOrder: string | null = null;

      if (targetIndex > source.index) {
        // Moving right
        prevOrder = String(sortedColumns[targetIndex].order);
        nextOrder = sortedColumns[targetIndex + 1] ? String(sortedColumns[targetIndex + 1].order) : null;
      } else {
        // Moving left
        prevOrder = sortedColumns[targetIndex - 1] ? String(sortedColumns[targetIndex - 1].order) : null;
        nextOrder = String(sortedColumns[targetIndex].order);
      }

      const newOrder = getFractionalIndex(prevOrder, nextOrder);
      await colMutations.update(colId, { ...colToMove, order: newOrder });
      return;
    }

    // Reordering cards
    const cardId = draggableId;
    const destColId = destination.droppableId;
    const sourceColId = source.droppableId;

    const cardsInDest = rootCards
      .filter((c: any) => String(c.columnId) === destColId)
      .sort((a: any, b: any) => String(a.order || '').localeCompare(String(b.order || '')));

    const cardToMove = rootCards.find((c: any) => String(c.record_id || c.id) === cardId);
    if (!cardToMove) return;

    let prevOrder: string | null = null;
    let nextOrder: string | null = null;

    if (destColId === sourceColId) {
      // Moving in same column
      const targetIndex = destination.index;
      if (targetIndex > source.index) {
        prevOrder = String(cardsInDest[targetIndex].order);
        nextOrder = cardsInDest[targetIndex + 1] ? String(cardsInDest[targetIndex + 1].order) : null;
      } else {
        prevOrder = cardsInDest[targetIndex - 1] ? String(cardsInDest[targetIndex - 1].order) : null;
        nextOrder = String(cardsInDest[targetIndex].order);
      }
    } else {
      // Moving to different column
      const targetIndex = destination.index;
      prevOrder = cardsInDest[targetIndex - 1] ? String(cardsInDest[targetIndex - 1].order) : null;
      nextOrder = cardsInDest[targetIndex] ? String(cardsInDest[targetIndex].order) : null;
    }

    const newOrder = getFractionalIndex(prevOrder, nextOrder);
    await cardMutations.update(cardId, {
      ...cardToMove,
      columnId: destColId,
      order: newOrder,
    });
  };

  const handleAddCard = async (columnId: string, title: string) => {
    const cardId = crypto.randomUUID();
    const cardsInCol = rootCards
      .filter((c: any) => String(c.columnId) === columnId)
      .sort((a: any, b: any) => String(a.order || '').localeCompare(String(b.order || '')));
    
    const lastOrder = cardsInCol[cardsInCol.length - 1] ? String(cardsInCol[cardsInCol.length - 1].order) : null;
    const newOrder = getFractionalIndex(lastOrder, null);

    await cardMutations.insert(cardId, {
      id: cardId,
      title,
      columnId,
      order: newOrder,
      desc: '',
      assigneeId: '',
      priority: 'medium',
      labels: '', // stored as simple serialized text/blank primitive
      parentId: null,
      createdAt: Date.now(),
    });
  };

  return (
    <div className="flex flex-col h-screen overflow-hidden">
      {/* Top Header */}
      <div className="h-16 px-6 border-b border-zinc-900 flex items-center justify-between flex-shrink-0 bg-zinc-950/80 backdrop-blur-md z-10">
        <div className="flex items-center space-x-3 min-w-0">
          <Link
            href={`/${project.workspaceId}/projects`}
            className="text-zinc-400 hover:text-white transition-colors"
          >
            ← Boards
          </Link>
          <span className="text-zinc-600">/</span>
          <div className="flex items-center space-x-2">
            <span
              style={{ backgroundColor: project.color }}
              className="w-2.5 h-2.5 rounded-full flex-shrink-0"
            />
            <h1 className="font-bold text-white truncate text-base">{project.name}</h1>
          </div>
        </div>
      </div>

      {/* Board Scrollable View */}
      <div className="flex-1 overflow-x-auto overflow-y-hidden bg-zinc-950/30 p-6">
        <DragDropContext onDragEnd={handleDragEnd}>
          <Droppable droppableId="board" type="column" direction="horizontal">
            {(provided) => (
              <div
                ref={provided.innerRef}
                {...provided.droppableProps}
                className="flex space-x-6 h-full items-start"
              >
                {sortedColumns.map((col: any, index) => {
                  const colId = String(col.record_id || col.id);
                  const colCards = rootCards
                    .filter((c: any) => String(c.columnId) === colId)
                    .sort((a: any, b: any) => String(a.order || '').localeCompare(String(b.order || '')));

                  return (
                    <KanbanColumn
                      key={colId}
                      columnId={colId}
                      index={index}
                      name={String(col.name)}
                      color={String(col.color)}
                      cards={colCards}
                      onAddCard={handleAddCard}
                      onCardClick={setActiveCardId}
                    />
                  );
                })}
                {provided.placeholder}
              </div>
            )}
          </Droppable>
        </DragDropContext>
      </div>

      {/* Task details Modal */}
      {activeCardId && (
        <CardModal
          cardId={activeCardId}
          onClose={() => setActiveCardId(null)}
        />
      )}
    </div>
  );
}
