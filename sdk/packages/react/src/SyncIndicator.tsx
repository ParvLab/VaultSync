import React, { useState, useEffect } from 'react';
import { useSyncStatus } from './useSyncStatus.js';
import { useVaultSyncClient } from './useVaultSyncClient.js';

interface SyncIndicatorProps {
  className?: string;
  showText?: boolean;
  showPeers?: boolean;
}

export function SyncIndicator({ className = '', showText = true, showPeers = false }: SyncIndicatorProps) {
  const status = useSyncStatus();
  const client = useVaultSyncClient();
  const [runtimeMode, setRuntimeMode] = useState<string>('Connecting');

  useEffect(() => {
    try {
      const mode = client.runtimeStatus?.mode || 'Connecting';
      setRuntimeMode(mode);
    } catch (e) {
      // ignore
    }
  }, [client]);

  // A follower is connected via BC even when WS is closed
  const isConnected = status.connected || runtimeMode === 'Mirror' || runtimeMode === 'Promoting';

  let statusColor = '#ef4444'; // Red (offline)
  let statusText = 'Offline';

  if (runtimeMode === 'Mirror') {
    statusColor = '#3b82f6'; // Blue (mirror/BC)
    statusText = status.pendingMutations > 0 ? `Follower (${status.pendingMutations} pending)` : 'Follower (BC)';
  } else if (isConnected) {
    if (status.pendingMutations > 0) {
      statusColor = '#f97316'; // Orange (pending)
      statusText = `Syncing (${status.pendingMutations} pending)`;
    } else {
      statusColor = '#22c55e'; // Green (connected)
      statusText = 'Synced';
    }
  }

  if (showPeers && status.activePeers > 0 && runtimeMode === 'Leader') {
    statusText += ` · ${status.activePeers} tab${status.activePeers !== 1 ? 's' : ''}`;
  }

  return (
    <div
      id="vaultsync-sync-indicator"
      className={`vaultsync-sync-indicator ${className}`}
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
