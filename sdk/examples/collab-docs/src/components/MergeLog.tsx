import React from 'react';

export interface MergeEvent {
  id: string;
  timestamp: number;
  replicaId: string;
  docId: string;
  recordId: string;
  title: string;
  body: string;
  direction: 'local-update' | 'remote-sync';
}

interface MergeLogProps {
  events: MergeEvent[];
  onClear: () => void;
}

export function MergeLog({ events, onClear }: MergeLogProps) {
  return (
    <div className="merge-log-container">
      <div className="merge-log-header">
        <h3>⚡ Real-Time Merge & CRDT Sync Log</h3>
        <button className="clear-log-btn" onClick={onClear}>Clear Log</button>
      </div>
      <div className="merge-log-list">
        {events.length === 0 ? (
          <div className="log-empty-state">No sync events recorded yet. Type in the editors or toggle sync to see CRDT updates merge.</div>
        ) : (
          events.map((evt) => (
            <div key={evt.id} className={`log-item ${evt.direction}`}>
              <div className="log-meta">
                <span className="log-time">
                  {new Date(evt.timestamp).toLocaleTimeString()}
                </span>
                <span className={`log-badge ${evt.direction}`}>
                  {evt.direction === 'local-update' ? 'Local Edit' : 'CRDT Sync'}
                </span>
                <span className="log-replica">Replica: {evt.replicaId}</span>
              </div>
              <div className="log-content">
                <strong>{evt.title ? `"${evt.title}"` : '(Empty Title)'}</strong> —{' '}
                <span>{evt.body || '(Empty Body)'}</span>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
