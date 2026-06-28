import init, { WasmVaultSyncClient } from '../wasm/vaultsync_wasm.js';
import type { VaultSyncConfig, RecordFields, SyncStatus, MetricsSnapshot, SubscriptionCallback, UnsubscribeFn } from './types.js';
import type { PresenceManager } from '../wasm/vaultsync_wasm.js';
import { KeyManager } from './keys.js';
import { createDbProxy, DbProxy, Collection } from './db.js';
import { defineSchema, FieldDef, SchemaDefinition } from './schema.js';

export * from './types.js';
export { KeyManager, DbProxy, Collection, defineSchema, FieldDef, SchemaDefinition, createDbProxy };
export { VaultSync } from './vaultsync.js';
export { Subscription } from './subscription.js';
export { SyncStatusObservable } from './sync.js';
export { PostgresCoordinator } from './coordinator/postgres.js';
export { RedisCoordinator } from './coordinator/redis.js';
export { CustomCoordinator } from './coordinator/custom.js';
export type { MetricsSnapshot } from './types.js';

const instancePromises = new Map<string, Promise<VaultSyncClient>>();

export class VaultSyncClient {
  private inner: any;
  private _keys?: KeyManager;
  private _db?: any;
  private channel?: BroadcastChannel;
  private namespace: string;
  private tabId: string;

  private constructor(inner: any, namespace: string, replicaId: string) {
    this.inner = inner;
    this.namespace = namespace;
    this.tabId = replicaId;
  }

  get db(): DbProxy & Record<string, Collection<any>> {
    if (!this._db) {
      this._db = createDbProxy(this);
    }
    return this._db;
  }

  get keys(): KeyManager {
    if (!this._keys) {
      this._keys = new KeyManager(this.inner);
    }
    return this._keys;
  }

  private setupBroadcastChannel(namespace: string) {
    const channelName = `vaultsync-ipc-${namespace}`;
    console.log(`[JS BC] Setting up BroadcastChannel: ${channelName}`);
    this.channel = new BroadcastChannel(channelName);
    this.channel.onmessage = (e) => {
      console.log(`[JS BC] Received message on channel ${channelName}:`, e.data);
      if (typeof e.data === 'string') {
        try {
          const val = JSON.parse(e.data);
          if (val && typeof val.doc_id === 'string' && typeof val.record_id === 'string') {
            if (val.tab_id && val.tab_id === this.tabId) {
              return; // skip own notification
            }
            setTimeout(async () => {
              try {
                console.log(`[JS BC] Firing subscription for doc_id=${val.doc_id}, record_id=${val.record_id}`);
                await this.inner.fire_subscription(val.doc_id, val.record_id);
              } catch (err) {
                console.error("[JS BC] Failed to fire subscription in WASM: ", err);
              }
            }, 0);
          }
        } catch (err) {
          console.error("[JS BC] Failed to parse message:", err);
        }
      }
    };
  }

  static create(config: VaultSyncConfig): Promise<VaultSyncClient> {
    const key = config.namespace;
    const existing = instancePromises.get(key);
    if (existing) {
      console.log(`[VaultSync] Reusing existing client for namespace=${key}`);
      return existing;
    }

    const promise = (async () => {
      await init();

      let inner: any;
      if (config.coordinatorUrl) {
        inner = await WasmVaultSyncClient.new_with_coordinator(
          config.namespace,
          config.replicaId,
          config.coordinatorUrl,
          config.authToken || null,
          config.dbName || null,
          config.storageBackend || null
        );
      } else {
        inner = await WasmVaultSyncClient.new(
          config.namespace,
          config.replicaId,
          config.dbName || null,
          config.storageBackend || null
        );
      }

      const client = new VaultSyncClient(inner, config.namespace, config.replicaId);
      client.setupBroadcastChannel(config.namespace);
      return client;
    })();

    instancePromises.set(key, promise);
    return promise;
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

  /** Returns a snapshot of all metrics counters. */
  async metrics(): Promise<MetricsSnapshot> {
    const jsonStr = await this.inner.metricsSnapshot();
    return JSON.parse(jsonStr);
  }

  /** Returns the PresenceManager for observing cross-tab peers, or null. */
  presence(): PresenceManager | null {
    return this.inner.presence();
  }

  subscribe(docId: string, callback: SubscriptionCallback): UnsubscribeFn {
    const wasmCallback = (recordId: string, jsonStr: string, notifySeq?: number) => {
      const t0 = Date.now();
      console.log('[notify] seq=' + (notifySeq ?? '?') + ' record=' + recordId + ' phase=js_callback t=' + t0);
      queueMicrotask(() => {
        console.log('[notify] seq=' + (notifySeq ?? '?') + ' record=' + recordId + ' phase=microtask_exec t=' + Date.now() + ' (elapsed=' + (Date.now() - t0) + 'ms)');
        callback(recordId, JSON.parse(jsonStr));
      });
    };
    const handle = this.inner.subscribe(docId, wasmCallback);
    return () => {
      this.inner.unsubscribe(handle);
    };
  }

  isLeader(): boolean {
    return this.inner.is_leader();
  }

  async shutdown(): Promise<void> {
    if (this.channel) {
      this.channel.close();
      this.channel = undefined;
    }
    instancePromises.delete(this.namespace);
    await this.inner.shutdown();
  }
}
