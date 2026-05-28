import React, { createContext, useContext, useEffect, useState } from 'react';
import { DriftClient } from '@drift/web';
import type { DriftConfig } from '@drift/web';

const DriftContext = createContext<DriftClient | null>(null);

export interface DriftProviderProps {
  config: DriftConfig;
  children: React.ReactNode;
}

export function DriftProvider({ config, children }: DriftProviderProps) {
  const [client, setClient] = useState<DriftClient | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;
    let currentClient: DriftClient | null = null;

    async function init() {
      try {
        const c = await DriftClient.create(config);
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
  }, [config.namespace, config.replicaId, config.coordinatorUrl, config.authToken]);

  if (error) {
    return (
      <div style={{ color: 'red', padding: '1rem', border: '1px solid red' }}>
        <h4>Failed to initialize Drift Client</h4>
        <pre>{error.message}</pre>
      </div>
    );
  }

  if (!client) {
    return <div>Initializing sync client...</div>;
  }

  return (
    <DriftContext.Provider value={client}>
      {children}
    </DriftContext.Provider>
  );
}

export function useDriftClient(): DriftClient {
  const client = useContext(DriftContext);
  if (!client) {
    throw new Error('useDriftClient must be used within a DriftProvider');
  }
  return client;
}
