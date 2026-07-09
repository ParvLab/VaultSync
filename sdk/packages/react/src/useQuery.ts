import { useEffect, useState } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import { RuntimeStore } from '@vaultsync/web';
import type { RecordFields } from '@vaultsync/web';

export interface UseQueryOptions {
  filter?: (fields: RecordFields) => boolean;
}

/** Phase 9: useQuery reads exclusively from RuntimeStore. No WASM calls.
 *  The store is hydrated via RuntimeSnapshot (Phase 4) and kept current
 *  via BC mutation streaming. One mutation → one store update → one render. */
export function useQuery(docId: string, options?: UseQueryOptions) {
  const client = useVaultSyncClient();
  const [data, setData] = useState<RecordFields[]>(() => {
    const store = (client as any).__runtimeStore as RuntimeStore | undefined;
    return (store?.query(docId) ?? []) as RecordFields[];
  });

  useEffect(() => {
    const store = (client as any).__runtimeStore as RuntimeStore | undefined;
    if (!store) return;

    // Subscribe directly to RuntimeStore — no WASM calls needed
    const unsub = store.subscribe(docId, null, () => {
      const records = store.query(docId) as RecordFields[];
      setData(records);
    });

    return unsub;
  }, [client, docId]);

  const filteredData = options?.filter ? data.filter(options.filter) : data;
  return { data: filteredData, loading: false, error: null };
}
