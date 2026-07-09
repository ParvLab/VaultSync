import { useEffect, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import { RuntimeStore } from '@vaultsync/web';
import type { RecordFields } from '@vaultsync/web';

/** Phase 9: useVaultSyncOne reads exclusively from RuntimeStore. No WASM calls.
 *  The store is hydrated via RuntimeSnapshot (Phase 4) and kept current
 *  via BC mutation streaming. One mutation → one store update → one render. */
export function useVaultSyncOne(docId: string, recordId: string) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields | null>(() => {
    const store = (client as any).__runtimeStore as RuntimeStore | undefined;
    return (store?.get(docId, recordId) ?? null) as RecordFields | null;
  });

  useEffect(() => {
    const store = (client as any).__runtimeStore as RuntimeStore | undefined;
    if (!store) return;

    // Subscribe directly to RuntimeStore with record-level granularity
    const unsub = store.subscribe(docId, recordId, () => {
      const record = store.get(docId, recordId) as RecordFields | null;
      setData(record);
    });

    return unsub;
  }, [client, docId, recordId]);

  return { data, loading: false, error: null };
}
