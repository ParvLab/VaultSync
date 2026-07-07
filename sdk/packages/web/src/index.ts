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

export const ClientEvent = {
    StatusDirty: 0,
} as const;

export class VaultSyncClient {
  private inner: any;
  private _keys?: KeyManager;
  private _db?: any;
  private channel?: BroadcastChannel;
  private namespace: string;
  private tabId: string;
  private commandQueue: Promise<void> = Promise.resolve();
  private subscriptionCache = new Map<string, { unsub: () => void; callbacks: Set<SubscriptionCallback> }>();
  private eventListeners = new Set<(event: number) => void>();

  private constructor(inner: any, namespace: string, replicaId: string) {
    this.inner = inner;
    this.namespace = namespace;
    this.tabId = replicaId;
  }

  /** Serializes mutations (local + remote) so they run one-at-a-time on the leader. */
  private enqueueMutation<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.commandQueue.then(fn, fn);
    this.commandQueue = result.then(() => {}, () => {});
    return result;
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
    console.debug(`[JS BC] Setting up BroadcastChannel: ${channelName}`);
    this.channel = new BroadcastChannel(channelName);
    this.channel.onmessage = (e) => {
      const raw = typeof e.data === 'string' ? e.data : JSON.stringify(e.data);
      if (typeof e.data === 'string') {
        try {
          const val = JSON.parse(e.data);
          if (!val) {
            return; // no message payload
          }
          if (!val.type) {
            val.type = 'invalidate'; // backward compat: old WASM sent no type
          }
          const sender = val.tab_id || '(missing)';
          const receiver = this.tabId || '(missing)';
          const ignored = !!(val.tab_id && val.tab_id === this.tabId);

          switch (val.type) {
            case 'invalidate': {
              if (val.doc_id && val.record_id) {
                console.debug(
                  `[JS BC] recv sender=${sender} receiver=${receiver} namespace=${namespace} ignored=${ignored} type=invalidate doc=${val.doc_id} record=${val.record_id}`
                );
                if (ignored) break;
                setTimeout(async () => {
                  try {
                    console.debug(`[JS BC] fire sender=${sender} receiver=${receiver} doc=${val.doc_id} record=${val.record_id}`);
                    await this.inner.fire_subscription(val.doc_id, val.record_id);
                  } catch (err) {
                    console.error("[JS BC] Failed to fire subscription: ", err);
                  }
                }, 0);
              }
              break;
            }
            case 'insert':
            case 'update':
            case 'delete': {
              if (val.doc_id && val.record_id && !ignored && this.inner.is_leader()) {
                console.debug(
                  `[JS BC] command sender=${sender} receiver=${receiver} type=${val.type} doc=${val.doc_id} record=${val.record_id}`
                );
                this.enqueueMutation(async () => {
                  try {
                    await this.inner.handle_command(val.type, val.doc_id, val.record_id, val.payload || '');
                  } catch (err) {
                    console.error(`[JS BC] Command ${val.type} failed: `, err);
                  }
                });
              }
              break;
            }
            default:
              console.warn(`[JS BC] recv unknown type=${val.type} on ${channelName}:`, raw);
          }
        } catch (err) {
          console.error("[JS BC] Failed to parse message:", err);
        }
      } else {
        console.warn(`[JS BC] recv non-string data on ${channelName}:`, raw);
      }
    };
  }

  static create(config: VaultSyncConfig): Promise<VaultSyncClient> {
    const key = config.namespace;
    const existing = instancePromises.get(key);
    if (existing) {
      console.debug(`[VaultSync] Reusing existing client for namespace=${key}`);
      return existing;
    }

    const promise = (async () => {
      await init();

      const mode = config.mode || (config.coordinatorUrl ? 'online' : 'offline');
      let inner: any;
      if (mode === 'online' && config.coordinatorUrl) {
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

      // Wire event bridge: Rust → JS
      inner.on_event((eventType: number) => {
        for (const listener of client.eventListeners) {
          listener(eventType);
        }
      });

      return client;
    })();

    instancePromises.set(key, promise);
    return promise;
  }

  async insert(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(() => this.inner.insert(docId, recordId, JSON.stringify(fields)));
    }
    await this.inner.insert(docId, recordId, JSON.stringify(fields));
  }

  async update(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(() => this.inner.update(docId, recordId, JSON.stringify(fields)));
    }
    await this.inner.update(docId, recordId, JSON.stringify(fields));
  }

  async delete(docId: string, recordId: string): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(() => this.inner.delete(docId, recordId));
    }
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
    let entry = this.subscriptionCache.get(docId);
    if (!entry) {
      const jsCallbacks: Set<SubscriptionCallback> = new Set();
      const wasmCallback = (recordId: string, jsonStr: string, notifySeq?: number) => {
        const t0 = Date.now();
        console.debug('[notify] seq=' + (notifySeq ?? '?') + ' record=' + recordId + ' phase=js_callback t=' + t0);
        const parsed = JSON.parse(jsonStr);
        queueMicrotask(() => {
          const elapsed = Date.now() - t0;
          console.debug('[notify] seq=' + (notifySeq ?? '?') + ' record=' + recordId + ' phase=microtask_exec t=' + Date.now() + ' (elapsed=' + elapsed + 'ms)');
          for (const cb of jsCallbacks) {
            cb(recordId, parsed);
          }
        });
      };
      const handle = this.inner.subscribe(docId, wasmCallback);
      entry = {
        unsub: () => this.inner.unsubscribe(handle),
        callbacks: jsCallbacks,
      };
      this.subscriptionCache.set(docId, entry);
    }
    entry.callbacks.add(callback);
    return () => {
      entry!.callbacks.delete(callback);
      if (entry!.callbacks.size === 0) {
        entry!.unsub();
        this.subscriptionCache.delete(docId);
      }
    };
  }

  onEvent(listener: (event: number) => void): () => void {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
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
