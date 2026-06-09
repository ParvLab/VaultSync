import React from 'react';
import { useSyncStatus } from './useSyncStatus.js';

interface SyncIndicatorProps {
  className?: string;
  showText?: boolean;
}

export function SyncIndicator({ className = '', showText = true }: SyncIndicatorProps) {
  const status = useSyncStatus();

  let statusColor = '#ef4444'; // Red (offline)
  let statusText = 'Offline';

  if (status.connected) {
    if (status.pendingMutations > 0) {
      statusColor = '#f97316'; // Orange (pending)
      statusText = `Syncing (${status.pendingMutations} pending)`;
    } else {
      statusColor = '#22c55e'; // Green (connected)
      statusText = 'Synced';
    }
  }

  return (
    <div
      id="drift-sync-indicator"
      className={`drift-sync-indicator ${className}`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '8px',
        fontFamily: 'system-ui, -apple-system, sans-serif',
        fontSize: '14px',
        color: '#374151',
      }}
    >
      <span
        style={{
          width: '8px',
          height: '8px',
          borderRadius: '50%',
          backgroundColor: statusColor,
          display: 'inline-block',
          boxShadow: `0 0 8px ${statusColor}`,
          transition: 'background-color 0.3s, box-shadow 0.3s',
        }}
      />
      {showText && <span>{statusText}</span>}
    </div>
  );
}
