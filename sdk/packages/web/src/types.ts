export interface VaultSyncConfig {
  namespace: string;
  replicaId: string;
  coordinatorUrl?: string;
  authToken?: string;
  storageBackend?: 'opfs' | 'indexeddb';
  dbName?: string;
}

export type RecordFields = Record<string, string | number | boolean | null>;

export type SyncStatus = {
  connected: boolean;
  pendingMutations: number;
  lastSyncedSequence: number;
  optimisticWrites: number;
  pushMutationsReceived: number;
  snapshotsApplied: number;
  activePeers: number;
};

export type MetricsSnapshot = {
  mutationsUploaded: number;
  mutationsDownloaded: number;
  syncErrors: number;
  lastSyncLagMs: number;
  pendingMutations: number;
  replicaCount: number;
  docCount: number;
  pushMutationsReceived: number;
  snapshotsApplied: number;
  optimisticWrites: number;
  hlcLogicalWraps: number;
  activePeers: number;
};

export type SubscriptionCallback = (recordId: string, fields: RecordFields) => void;
export type UnsubscribeFn = () => void;

export interface KeyInfo {
  version: number;
  createdAt: number;
  isActive: boolean;
}
