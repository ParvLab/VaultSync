import { useEffect, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import type { SyncStatus } from '@vaultsync/web';

export function useSyncStatus() {
  const client = useVaultSyncClient();
  const [status, setStatus] = useState<SyncStatus>({
    connected: false,
    pendingMutations: 0,
    lastSyncedSequence: 0,
  });

  useEffect(() => {
    let active = true;

    async function updateStatus() {
      try {
        const s = await client.syncStatus();
        if (active) {
          setStatus(s);
        }
      } catch (err) {
        console.error('Failed to get sync status:', err);
      }
    }

    updateStatus();
    const interval = setInterval(updateStatus, 1000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [client]);

  return status;
}
