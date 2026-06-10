import React, { createContext, useContext, useState, useEffect } from 'react';
import { useVaultSyncClient, VaultSyncProvider } from '@vaultsync/react';
import type { RecordFields, VaultSyncConfig } from '@vaultsync/web';

export const VaultSyncHydrationContext = createContext<Record<string, RecordFields[]> | null>(null);

export interface VaultSyncHydrationProviderProps {
  config: VaultSyncConfig;
  initialData: Record<string, RecordFields[]>;
  children: React.ReactNode;
}

export function VaultSyncHydrationProvider({ config, initialData, children }: VaultSyncHydrationProviderProps) {
  return (
    <VaultSyncHydrationContext.Provider value={initialData}>
      <VaultSyncProvider config={config}>
        {children}
      </VaultSyncProvider>
    </VaultSyncHydrationContext.Provider>
  );
}

export function useHydratedData(docId: string): RecordFields[] | null {
  const context = useContext(VaultSyncHydrationContext);
  return context ? context[docId] || null : null;
}

export function useNextQuery(docId: string) {
  const hydrated = useHydratedData(docId);
  
  let client: any = null;
  try {
    client = useVaultSyncClient();
  } catch (e) {
    // In SSR or loading state
  }

  const [data, setData] = useState<RecordFields[]>(() => hydrated || []);
  const [loading, setLoading] = useState(() => !hydrated && !client);

  useEffect(() => {
    if (!client) return;

    let active = true;
    async function fetchLatest() {
      try {
        const records = await client.find(docId);
        if (active) {
          setData(records);
          setLoading(false);
        }
      } catch (e) {
        console.error("Failed to query records:", e);
      }
    }

    fetchLatest();

    const unsubscribe = client.subscribe(docId, () => {
      fetchLatest();
    });

    return () => {
      active = false;
      unsubscribe();
    };
  }, [client, docId]);

  return {
    data,
    loading,
  };
}
