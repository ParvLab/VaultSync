import React, { createContext, useContext, useState, useEffect } from 'react';
import { useDriftClient, DriftProvider } from '@drift/react';
import type { RecordFields, DriftConfig } from '@drift/web';

export const DriftHydrationContext = createContext<Record<string, RecordFields[]> | null>(null);

export interface DriftHydrationProviderProps {
  config: DriftConfig;
  initialData: Record<string, RecordFields[]>;
  children: React.ReactNode;
}

export function DriftHydrationProvider({ config, initialData, children }: DriftHydrationProviderProps) {
  return (
    <DriftHydrationContext.Provider value={initialData}>
      <DriftProvider config={config}>
        {children}
      </DriftProvider>
    </DriftHydrationContext.Provider>
  );
}

export function useHydratedData(docId: string): RecordFields[] | null {
  const context = useContext(DriftHydrationContext);
  return context ? context[docId] || null : null;
}

export function useNextQuery(docId: string) {
  const hydrated = useHydratedData(docId);
  
  let client: any = null;
  try {
    client = useDriftClient();
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
