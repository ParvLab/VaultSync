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

  useEffect(() => {
    const checkLeader = () => {
      try {
        setIsLeader(client.isLeader());
      } catch (e) {
        // ignore
      }
    };
    checkLeader();
    const interval = setInterval(checkLeader, 1000);
    return () => clearInterval(interval);
  }, [client]);

  const isSyncing = status.connected && (status.pendingMutations > 0 || showSyncing);

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
            className={`status-dot ${isSyncing ? 'syncing' : status.connected ? 'connected' : 'disconnected'}`}
            data-testid="sync-status"
            data-status={status.connected ? 'connected' : 'disconnected'}
          />
          <span>{isSyncing ? 'Syncing...' : status.connected ? 'Connected' : 'Offline'}</span>
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
