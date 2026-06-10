import type { VaultSyncClient } from './index.js';
import type { SyncStatus } from './types.js';

export type SyncStatusListener = (status: SyncStatus) => void;

export class SyncStatusObservable {
  private client: VaultSyncClient;
  private listeners: Set<SyncStatusListener> = new Set();
  private intervalId: any = null;
  private lastStatus: SyncStatus | null = null;

  constructor(client: VaultSyncClient, pollIntervalMs: number = 1000) {
    this.client = client;
    this.startPolling(pollIntervalMs);
  }

  private startPolling(intervalMs: number): void {
    this.intervalId = setInterval(async () => {
      try {
        const status = await this.client.syncStatus();
        if (
          !this.lastStatus ||
          this.lastStatus.connected !== status.connected ||
          this.lastStatus.pendingMutations !== status.pendingMutations ||
          this.lastStatus.lastSyncedSequence !== status.lastSyncedSequence
        ) {
          this.lastStatus = status;
          this.notify(status);
        }
      } catch (e) {
        console.error('Error polling sync status:', e);
      }
    }, intervalMs);
  }

  subscribe(listener: SyncStatusListener): () => void {
    this.listeners.add(listener);
    if (this.lastStatus) {
      listener(this.lastStatus);
    }
    return () => {
      this.listeners.delete(listener);
    };
  }

  private notify(status: SyncStatus): void {
    for (const listener of this.listeners) {
      try {
        listener(status);
      } catch (e) {
        console.error('Error in SyncStatusObservable listener:', e);
      }
    }
  }

  destroy(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.listeners.clear();
  }
}
