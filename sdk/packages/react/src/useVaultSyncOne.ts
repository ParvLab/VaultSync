import { useEffect, useRef, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import type { RecordFields } from '@vaultsync/web';

export function useVaultSyncOne(docId: string, recordId: string) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const revisionRef = useRef<string>('');

  useEffect(() => {
    let active = true;

    async function fetchInitial() {
      try {
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
      console.debug(`[notify] seq=${seq} doc=${docId} phase=react_callback_one t=${Date.now()}`);
      if (!active) return;
      if (changedRecordId === recordId) {
        try {
          const record = await client.get(docId, recordId);
          const newRevision = JSON.stringify(record);
          if (newRevision === revisionRef.current) return;
          revisionRef.current = newRevision;
          console.debug(`[notify] seq=${seq} doc=${docId} record=${recordId} phase=react_setData_one t=${Date.now()}`);
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
