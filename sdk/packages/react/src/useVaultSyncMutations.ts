import { useState, useCallback } from 'react';
import { useVaultSyncClient } from './useVaultSyncClient.js';
import type { RecordFields } from '@vaultsync/web';

export function useVaultSyncMutations(docId: string) {
  const client = useVaultSyncClient();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const insert = useCallback(async (recordId: string, fields: RecordFields) => {
    setLoading(true);
    setError(null);
    try {
      await client.insert(docId, recordId, fields);
    } catch (err) {
      setError(err as Error);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [client, docId]);

  const update = useCallback(async (recordId: string, fields: RecordFields) => {
    setLoading(true);
    setError(null);
    try {
      await client.update(docId, recordId, fields);
    } catch (err) {
      setError(err as Error);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [client, docId]);

  const deleteRecord = useCallback(async (recordId: string) => {
    setLoading(true);
    setError(null);
    try {
      await client.delete(docId, recordId);
    } catch (err) {
      setError(err as Error);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [client, docId]);

  return {
    insert,
    update,
    delete: deleteRecord,
    loading,
    error,
  };
}
