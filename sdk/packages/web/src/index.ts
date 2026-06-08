import init, { WasmDriftClient } from '../wasm/drift_wasm.js';
import type { DriftConfig, RecordFields, SyncStatus, SubscriptionCallback, UnsubscribeFn } from './types.js';
import { KeyManager } from './keys.js';

export * from './types.js';
export { KeyManager };

export class DriftClient {
  private inner: any;
  private _keys?: KeyManager;

  private constructor(inner: any) {
    this.inner = inner;
  }

  get keys(): KeyManager {
    if (!this._keys) {
      this._keys = new KeyManager(this.inner);
    }
    return this._keys;
  }

  static async create(config: DriftConfig): Promise<DriftClient> {
    // Initialize the WebAssembly module
    await init();

    let inner: any;
    if (config.coordinatorUrl) {
      inner = await WasmDriftClient.new_with_coordinator(
        config.namespace,
        config.replicaId,
        config.coordinatorUrl,
        config.authToken || null
      );
    } else {
      inner = await WasmDriftClient.new(config.namespace, config.replicaId);
    }

    return new DriftClient(inner);
  }

  async insert(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    await this.inner.insert(docId, recordId, JSON.stringify(fields));
  }

  async update(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    await this.inner.update(docId, recordId, JSON.stringify(fields));
  }

  async delete(docId: string, recordId: string): Promise<void> {
    await this.inner.delete(docId, recordId);
  }

  async get(docId: string, recordId: string): Promise<RecordFields | null> {
    const jsonStr = await this.inner.get(docId, recordId);
    if (!jsonStr) return null;
    return JSON.parse(jsonStr);
  }

  async find(docId: string): Promise<RecordFields[]> {
    const arr = await this.inner.find(docId);
    const result: RecordFields[] = [];
    for (let i = 0; i < arr.length; i++) {
      result.push(JSON.parse(arr[i]));
    }
    return result;
  }

  async syncStatus(): Promise<SyncStatus> {
    const statusStr = await this.inner.sync_status();
    return JSON.parse(statusStr);
  }

  subscribe(docId: string, callback: SubscriptionCallback): UnsubscribeFn {
    const wasmCallback = (recordId: string, jsonStr: string) => {
      callback(recordId, JSON.parse(jsonStr));
    };
    const handle = this.inner.subscribe(docId, wasmCallback);
    return () => {
      this.inner.unsubscribe(handle);
    };
  }

  async shutdown(): Promise<void> {
    await this.inner.shutdown();
  }
}
