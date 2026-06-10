import { DriftClient } from './index.js';
import { KeyManager } from './keys.js';
import type { DriftConfig } from './types.js';

export class Drift {
  private client: DriftClient;
  private _keys?: KeyManager;

  private constructor(client: DriftClient) {
    this.client = client;
  }

  static async create(config: DriftConfig): Promise<Drift> {
    const client = await DriftClient.create(config);
    return new Drift(client);
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
