import { useEffect, useRef, useState } from 'react';
import { useRuntimeLifecycle } from './useRuntimeLifecycle.js';
import type { RecordFields } from '@vaultsync/web';

export function useVaultSyncOne(docId: string, recordId: string) {
  const { store, status } = useRuntimeLifecycle();
  const [data, setData] = useState<RecordFields | null>(() => store?.get(docId, recordId) as RecordFields | null);
  const prevRef = useRef<RecordFields | null>(null);

  useEffect(() => {
    if (!store) return;

    const current = store.get(docId, recordId) as RecordFields | null;
    if (current !== prevRef.current) {
      prevRef.current = current;
      setData(current);
    }

    return store.subscribe(docId, recordId, () => {
      const record = store.get(docId, recordId) as RecordFields | null;
      if (record !== prevRef.current) {
        prevRef.current = record;
        setData(record);
      }
    });
  }, [store, docId, recordId]);

  return { data, loading: status !== 'ready', error: null };
}
