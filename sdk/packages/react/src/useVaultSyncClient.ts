import { useContext } from 'react';
import { VaultSyncClient } from '@vaultsync/web';
import { VaultSyncContext } from './context.js';

export function useVaultSyncClient(): VaultSyncClient {
  const client = useContext(VaultSyncContext);
  if (!client) {
    throw new Error('useVaultSyncClient must be used within a VaultSyncProvider');
  }
  return client;
}
