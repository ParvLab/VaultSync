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
    let active = true;
    let currentClient: VaultSyncClient | null = null;

    async function init() {
      try {
        const c = await VaultSyncClient.create(config);
        if (active) {
          currentClient = c;
          setClient(c);
        } else {
          c.shutdown().catch(() => {});
        }
      } catch (err) {
        if (active) {
          setError(err as Error);
        }
      }
    }

    init();

    return () => {
      active = false;
      if (currentClient) {
        currentClient.shutdown().catch(() => {});
      }
    };
  }, [config.namespace, config.replicaId, config.coordinatorUrl, config.authToken, config.dbName, config.storageBackend]);

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
