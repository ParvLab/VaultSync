import { useState, useEffect } from 'react';
import { useSyncStatus, useVaultSyncClient } from '@vaultsync/react';

export function SyncBar() {
  const status = useSyncStatus();
  const client = useVaultSyncClient();
  const [isLeader, setIsLeader] = useState(false);

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
            className={`status-dot ${status.connected ? (status.pendingMutations > 0 ? 'syncing' : 'connected') : 'disconnected'}`}
            data-testid="sync-status"
            data-status={status.connected ? 'connected' : 'disconnected'}
          />
          <span>{status.connected ? (status.pendingMutations > 0 ? 'Syncing...' : 'Connected') : 'Offline'}</span>
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
