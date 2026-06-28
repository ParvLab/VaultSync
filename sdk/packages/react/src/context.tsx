import React, { createContext, useContext, useEffect, useState } from 'react';
import { VaultSyncClient } from '@vaultsync/web';
import type { VaultSyncConfig } from '@vaultsync/web';

export const VaultSyncContext = createContext<VaultSyncClient | null>(null);

export interface VaultSyncProviderProps {
  config: VaultSyncConfig;
  children: React.ReactNode;
}

export function VaultSyncProvider({ config, children }: VaultSyncProviderProps) {
  const [client, setClient] = useState<VaultSyncClient | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function init() {
      try {
        const c = await VaultSyncClient.create(config);
        if (!cancelled) {
          setClient(c);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err as Error);
        }
      }
    }

    init();

    // No shutdown in cleanup: the singleton cache in VaultSyncClient.create()
    // manages the client lifecycle. Stopping StrictMode from creating a
    // cross-instance WS echo loop (two clients with different replica_ids).
    return () => {
      cancelled = true;
    };
  }, [config.namespace, config.replicaId, config.mode, config.coordinatorUrl, config.authToken, config.dbName, config.storageBackend]);

  if (error) {
    return (
      <div style={{ color: 'red', padding: '1rem', border: '1px solid red' }}>
        <h4>Failed to initialize VaultSync Client</h4>
        <pre>{error.message}</pre>
      </div>
    );
  }

  if (!client) {
    return <div>Initializing sync client...</div>;
  }

  return (
    <VaultSyncContext.Provider value={client}>
      {children}
    </VaultSyncContext.Provider>
  );
}
