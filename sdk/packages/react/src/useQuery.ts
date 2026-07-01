import { useEffect, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import type { RecordFields } from '@vaultsync/web';

export interface UseQueryOptions {
  filter?: (fields: RecordFields) => boolean;
}

export function useQuery(docId: string, options?: UseQueryOptions) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;

    async function fetchInitial() {
      try {
        const records = await client.find(docId);
        console.debug(`[React useQuery] fetchInitial for ${docId}: found ${records.length} records`);
        if (active) {
          setData(records);
          setLoading(false);
        }
      } catch (err) {
        console.error(`[React useQuery] fetchInitial error for ${docId}:`, err);
        if (active) {
          setError(err as Error);
          setLoading(false);
        }
      }
    }

    // Register subscription FIRST so no subscription fires are missed during fetchInitial's async window
    let notifySeq = 0;
    const unsubscribe = client.subscribe(docId, async (recordId: string | undefined) => {
      const seq = ++notifySeq;
      console.debug(`[notify] seq=${seq} doc=${docId} phase=react_callback t=${Date.now()}`);
      if (!active) return;
      try {
        const records = await client.find(docId);
        console.debug(`[notify] seq=${seq} doc=${docId} phase=react_setData records=${records.length} t=${Date.now()}`);
        if (active) {
          setData(records);
        }
      } catch (err) {
        console.error('Failed to refresh query data on mutation notification:', err);
      }
    });

    fetchInitial();

    return () => {
      active = false;
      unsubscribe();
    };
  }, [client, docId]);

  const filteredData = options?.filter ? data.filter(options.filter) : data;

  return {
    data: filteredData,
    loading,
    error,
  };
}
