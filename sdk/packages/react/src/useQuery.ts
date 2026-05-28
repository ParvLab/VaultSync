import { useEffect, useState } from 'react';
import { useDriftClient } from './context.js';
import type { RecordFields } from '@drift/web';

export interface UseQueryOptions {
  filter?: (fields: RecordFields) => boolean;
}

export function useQuery(docId: string, options?: UseQueryOptions) {
  const client = useDriftClient();
  const [data, setData] = useState<RecordFields[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;

    async function fetchInitial() {
      try {
        const records = await client.find(docId);
        if (active) {
          setData(records);
          setLoading(false);
        }
      } catch (err) {
        if (active) {
          setError(err as Error);
          setLoading(false);
        }
      }
    }

    fetchInitial();

    // Subscribe to real-time changes on docId
    const unsubscribe = client.subscribe(docId, async () => {
      if (!active) return;
      try {
        const records = await client.find(docId);
        if (active) {
          setData(records);
        }
      } catch (err) {
        console.error('Failed to refresh query data on mutation notification:', err);
      }
    });

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
