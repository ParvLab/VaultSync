import { VaultSyncClient } from './index.js';
import { SyncStatusObservable } from './sync.js';
import { KeyManager } from './keys.js';
import type { VaultSyncConfig } from './types.js';
import type { MetricsSnapshot } from './types.js';

export class VaultSync {
  private client: VaultSyncClient;
  private syncObservable: SyncStatusObservable;

  private constructor(client: VaultSyncClient) {
    this.client = client;
    this.syncObservable = new SyncStatusObservable(client);
  }

  static async create(config: VaultSyncConfig): Promise<VaultSync> {
    const client = await VaultSyncClient.create(config);
    return new VaultSync(client);
  }

  static detectStorage(): 'opfs' | 'indexeddb' {
    if (typeof navigator !== 'undefined' && navigator.storage && typeof (navigator.storage as any).getDirectory === 'function') {
      return 'opfs';
    }
    return 'indexeddb';
  }

  get db() {
    return this.client.db;
  }

  get sync(): SyncStatusObservable {
    return this.syncObservable;
  }

  get keys(): KeyManager {
    return this.client.keys;
  }

  get leader(): boolean {
    return this.client.isLeader();
  }

  /** Returns a metrics snapshot. */
  async metrics(): Promise<MetricsSnapshot> {
    return this.client.metrics();
  }

  /** Returns the PresenceManager for cross-tab awareness, or null. */
  presence() {
    return this.client.presence();
  }

  async shutdown(): Promise<void> {
    this.syncObservable.destroy();
    await this.client.shutdown();
  }
}
