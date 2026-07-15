import { useEffect, useState } from 'react';
import { useRuntimeLifecycle } from './useRuntimeLifecycle.js';
import type { RecordFields } from '@vaultsync/web';

export interface UseQueryOptions {
  filter?: (fields: RecordFields) => boolean;
}

export function useQuery(docId: string, options?: UseQueryOptions) {
  const { store, status } = useRuntimeLifecycle();
  const [data, setData] = useState<RecordFields[]>(() => store?.query(docId) ?? []);

  useEffect(() => {
    if (!store) return;

    const current = store.query(docId) as RecordFields[];
    if (current.length > 0 || data.length > 0) {
      setData(current);
    }

    return store.subscribe(docId, null, () => {
      setData(store.query(docId) as RecordFields[]);
    });
  }, [store, docId]);

  const filteredData = options?.filter ? data.filter(options.filter) : data;
  return { data: filteredData, loading: status !== 'ready', error: null };
}
