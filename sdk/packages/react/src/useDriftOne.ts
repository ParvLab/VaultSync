import { useEffect, useState } from 'react';
import { useDriftClient } from './context.js';
import type { RecordFields } from '@drift/web';

export function useDriftOne(docId: string, recordId: string) {
  const client = useDriftClient();
  const [data, setData] = useState<RecordFields | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;

    async function fetchInitial() {
      try {
        const record = await client.get(docId, recordId);
        if (active) {
          setData(record);
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

    const unsubscribe = client.subscribe(docId, async (changedRecordId) => {
      if (!active) return;
      if (changedRecordId === recordId) {
        try {
          const record = await client.get(docId, recordId);
          if (active) {
            setData(record);
          }
        } catch (err) {
          console.error('Failed to refresh single record:', err);
        }
      }
    });

    return () => {
      active = false;
      unsubscribe();
    };
  }, [client, docId, recordId]);

  return {
    data,
    loading,
    error,
  };
}
