import { DriftClient } from './index.js';
import { SyncStatusObservable } from './sync.js';
import { KeyManager } from './keys.js';
import type { DriftConfig } from './types.js';

export class Drift {
  private client: DriftClient;
  private syncObservable: SyncStatusObservable;

  private constructor(client: DriftClient) {
    this.client = client;
    this.syncObservable = new SyncStatusObservable(client);
  }

  static async create(config: DriftConfig): Promise<Drift> {
    const client = await DriftClient.create(config);
    return new Drift(client);
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

  async shutdown(): Promise<void> {
    this.syncObservable.destroy();
    await this.client.shutdown();
  }
}
