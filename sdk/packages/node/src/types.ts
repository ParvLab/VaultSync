export interface DriftConfig {
  namespace: string;
  replicaId: string;
  coordinatorUrl?: string;
  authToken?: string;
  storagePath?: string; // SQLite file path. If not provided, InMemory storage will be used.
}

export type RecordFields = Record<string, string | number | boolean | null>;

export type SyncStatus = {
  connected: boolean;
  pendingMutations: number;
  lastSyncedSequence: number;
};

export type SubscriptionCallback = (recordId: string, fields: RecordFields) => void;
export type UnsubscribeFn = () => void;
