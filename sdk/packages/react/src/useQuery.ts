import { useEffect, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import { RuntimeStore } from '@vaultsync/web';
import type { RecordFields } from '@vaultsync/web';

export interface UseQueryOptions {
  filter?: (fields: RecordFields) => boolean;
}

/** Phase 6: useQuery hook that uses RuntimeStore for instant cache hits.
 *  No WASM calls during rendering. Subscriptions fire via BC mutation streaming. */
export function useQuery(docId: string, options?: UseQueryOptions) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;

    // Get or create RuntimeStore for this client
    const store = (client as any).__runtimeStore as RuntimeStore | undefined;

    async function fetchInitial() {
      try {
        if (store) {
          // Prefer cache
          const cached = store.query(docId);
          if (cached.length > 0) {
            if (active) {
              setData(cached as RecordFields[]);
              setLoading(false);
            }
          }
        }
        // Fallback: still fetch from WASM for completeness
        const records = await client.find(docId);
        console.debug(`[useQuery] fetchInitial for ${docId}: found ${records.length} records`);
        if (active) {
          setData(records);
          setLoading(false);
        }
      } catch (err) {
        console.error(`[useQuery] fetchInitial error for ${docId}:`, err);
        if (active) {
          setError(err as Error);
          setLoading(false);
        }
      }
    }

    // Register subscription — on mutation, read from cache if available
    let notifySeq = 0;
    const unsubscribe = client.subscribe(docId, async (recordId: string | undefined) => {
      const seq = ++notifySeq;
      if (!active) return;
      try {
        if (store && store.size > 0) {
          const cached = store.query(docId);
          if (active) {
            setData(cached as RecordFields[]);
            return;
          }
        }
        const records = await client.find(docId);
        if (active) {
          setData(records);
        }
      } catch (err) {
        console.error('Failed to refresh query data:', err);
      }
    });

    fetchInitial();

    return () => {
      active = false;
      unsubscribe();
    };
  }, [client, docId]);

  const filteredData = options?.filter ? data.filter(options.filter) : data;

  return { data: filteredData, loading, error };
}
