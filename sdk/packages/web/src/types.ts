export interface VaultSyncConfig {
  namespace: string;
  replicaId: string;
  coordinatorUrl?: string;
  authToken?: string;
  storageBackend?: 'opfs' | 'indexeddb';
}

export type RecordFields = Record<string, string | number | boolean | null>;

export type SyncStatus = {
  connected: boolean;
  pendingMutations: number;
  lastSyncedSequence: number;
};

export type SubscriptionCallback = (recordId: string, fields: RecordFields) => void;
export type UnsubscribeFn = () => void;

export interface KeyInfo {
  version: number;
  createdAt: number;
  isActive: boolean;
}
