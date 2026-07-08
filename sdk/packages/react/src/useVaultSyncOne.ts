import { useEffect, useRef, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import { RuntimeStore } from '@vaultsync/web';
import type { RecordFields } from '@vaultsync/web';

/** Phase 6: useVaultSyncOne with RuntimeStore cache. */
export function useVaultSyncOne(docId: string, recordId: string) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const revisionRef = useRef<string>('');

  useEffect(() => {
    let active = true;

    const store = (client as any).__runtimeStore as RuntimeStore | undefined;

    async function fetchInitial() {
      try {
        if (store) {
          const cached = store.get(docId, recordId);
          if (cached) {
            revisionRef.current = JSON.stringify(cached);
            if (active) {
              setData(cached as RecordFields);
              setLoading(false);
              return;
            }
          }
        }
        const record = await client.get(docId, recordId);
        if (active) {
          revisionRef.current = JSON.stringify(record);
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

    let notifySeq = 0;
    const unsubscribe = client.subscribe(docId, async (changedRecordId: string) => {
      const seq = ++notifySeq;
      if (!active) return;
      if (changedRecordId === recordId) {
        try {
          if (store) {
            const cached = store.get(docId, recordId);
            if (cached) {
              const newRev = JSON.stringify(cached);
              if (newRev !== revisionRef.current) {
                revisionRef.current = newRev;
                if (active) setData(cached as RecordFields);
              }
              return;
            }
          }
          const record = await client.get(docId, recordId);
          const newRevision = JSON.stringify(record);
          if (newRevision === revisionRef.current) return;
          revisionRef.current = newRevision;
          if (active) {
            setData(record);
          }
        } catch (err) {
          console.error('Failed to refresh single record:', err);
        }
      }
    });

    fetchInitial();

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
