import React, { createContext, useContext, useEffect, useState, useRef } from 'react';
import { VaultSyncClient } from '@vaultsync/web';
import type { RuntimeStore } from '@vaultsync/web';
import { VaultSyncContext } from './context.js';

export type RuntimeLifecycleStatus = 
  | 'initializing'
  | 'bootstrapping'
  | 'replaying'
  | 'ready'
  | 'offline'
  | 'recovering';

export interface RuntimeLifecycleState {
  status: RuntimeLifecycleStatus;
  store: RuntimeStore | null;
  client: VaultSyncClient | null;
}

const defaultLifecycle: RuntimeLifecycleState = {
  status: 'initializing',
  store: null,
  client: null,
};

export const RuntimeLifecycleContext = createContext<RuntimeLifecycleState>(defaultLifecycle);

export interface BootstrapProviderProps {
  docIds?: string[];
  children: React.ReactNode;
}

export function BootstrapProvider({ docIds, children }: BootstrapProviderProps) {
  const client = useContext(VaultSyncContext);
  const [lifecycle, setLifecycle] = useState<RuntimeLifecycleState>(defaultLifecycle);
  const bootstrapped = useRef(false);
  const docIdsRef = useRef(docIds);
  docIdsRef.current = docIds;

  useEffect(() => {
    if (!client) {
      setLifecycle({ status: 'initializing', store: null, client: null });
      return;
    }

    if (bootstrapped.current) return;
    bootstrapped.current = true;

    const store = client.runtimeStore;
    setLifecycle({ status: 'bootstrapping', store, client });

    client.bootstrap(docIdsRef.current).then(() => {
      setLifecycle({ status: 'ready', store, client });
    }).catch((err) => {
      console.warn('[BOOTSTRAP] bootstrap failed:', err);
      setLifecycle({ status: 'ready', store, client });
    });
  }, [client]);

  return (
    <RuntimeLifecycleContext.Provider value={lifecycle}>
      {children}
    </RuntimeLifecycleContext.Provider>
  );
}
