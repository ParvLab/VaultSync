import { useState, useEffect } from 'react';
import { useSyncStatus, useVaultSyncClient } from '@vaultsync/react';

export function SyncBar() {
  const status = useSyncStatus();
  const client = useVaultSyncClient();
  const [isLeader, setIsLeader] = useState(false);
  const [showSyncing, setShowSyncing] = useState(false);

  // Log badge changes for pending-count pipeline debugging
  useEffect(() => {
    if (status.pendingMutations > 0) {
      console.debug(`[pending] badge pending=${status.pendingMutations} connected=${status.connected}`);
    }
  }, [status.pendingMutations, status.connected]);

  // Debounce: keep "Syncing..." visible for at least 1.5s after pending drops to 0
  useEffect(() => {
    if (status.pendingMutations > 0) {
      setShowSyncing(true);
    } else if (showSyncing) {
      const timer = setTimeout(() => setShowSyncing(false), 1500);
      return () => clearTimeout(timer);
    }
  }, [status.pendingMutations, showSyncing]);

  const [runtimeMode, setRuntimeMode] = useState<string>('Connecting');

  useEffect(() => {
    const checkLeader = () => {
      try {
        setIsLeader(client.isLeader());
        const mode = client.runtimeStatus?.mode || 'Connecting';
        setRuntimeMode(mode);
      } catch (e) {
        // ignore
      }
    };
    checkLeader();
    const interval = setInterval(checkLeader, 1000);
    return () => clearInterval(interval);
  }, [client]);

  // A follower is connected via BC even when WS is closed
  const isConnected = status.connected || runtimeMode === 'Mirror' || runtimeMode === 'Promoting';
  const isSyncing = isConnected && (status.pendingMutations > 0 || showSyncing);

  const connectionLabel = (() => {
    if (isSyncing) return 'Syncing...';
    if (runtimeMode === 'Mirror') return 'Mirror (BC)';
    if (runtimeMode === 'Promoting') return 'Promoting...';
    if (isConnected) return 'Connected';
    return 'Offline';
  })();

  return (
    <div className="status-bar">
      <div className="brand-section">
        <span className="brand-logo">VaultSync</span>
        <span className="badge-wrapper">
          <span 
            className={`badge ${isLeader ? 'leader' : 'follower'}`}
            data-testid="tab-role"
            data-role={isLeader ? 'leader' : 'follower'}
          >
            {isLeader ? 'Leader' : 'Follower'}
          </span>
        </span>
      </div>

      <div className="status-indicators">
        <div className="indicator-item">
          <div 
            className={`status-dot ${isSyncing ? 'syncing' : isConnected ? 'connected' : 'disconnected'}`}
            data-testid="sync-status"
            data-status={isConnected ? 'connected' : 'disconnected'}
          />
          <span>{connectionLabel}</span>
        </div>

        {status.pendingMutations > 0 && (
          <div className="indicator-item" style={{ borderColor: 'rgba(245, 158, 11, 0.3)' }}>
            <span style={{ color: 'var(--color-warning)', fontWeight: 600 }} data-testid="pending-count">
              {status.pendingMutations} pending
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
