import { useContext } from 'react';
import { SyncStatusContext } from './context.js';

export function useSyncStatus() {
  return useContext(SyncStatusContext);
}
