import { VaultSyncClient } from './index.js';
import { KeyManager } from './keys.js';
import type { VaultSyncConfig } from './types.js';

export class VaultSync {
  private client: VaultSyncClient;
  private _keys?: KeyManager;

  private constructor(client: VaultSyncClient) {
    this.client = client;
  }

  static async create(config: VaultSyncConfig): Promise<VaultSync> {
    const client = await VaultSyncClient.create(config);
    return new VaultSync(client);
  }

  static detectStorage(): 'sqlite' | 'memory' {
    return 'sqlite';
  }

  get db() {
    return this.client.db;
  }

  get keys(): KeyManager {
    if (!this._keys) {
      // Access private inner using bracket notation
      this._keys = new KeyManager((this.client as any).inner);
    }
    return this._keys;
  }

  get leader(): boolean {
    return this.client.isLeader();
  }

  async shutdown(): Promise<void> {
    await this.client.shutdown();
  }
}
