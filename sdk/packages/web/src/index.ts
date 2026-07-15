import init, { VaultSyncRuntime } from '../wasm/vaultsync_wasm.js';
import type { VaultSyncConfig, RecordFields, SyncStatus, MetricsSnapshot, SubscriptionCallback, UnsubscribeFn } from './types.js';
import type { PresenceManager } from '../wasm/vaultsync_wasm.js';
import { KeyManager } from './keys.js';
import { RuntimeStore } from './store.js';
import { createDbProxy, DbProxy, Collection } from './db.js';
import { defineSchema, FieldDef, SchemaDefinition } from './schema.js';

export * from './types.js';
export { KeyManager, DbProxy, Collection, defineSchema, FieldDef, SchemaDefinition, createDbProxy };
export { VaultSync } from './vaultsync.js';
export { Subscription } from './subscription.js';
export { SyncStatusObservable } from './sync.js';
export { RuntimeStore } from './store.js';
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
  private _runtimeStore?: RuntimeStore;
  private channel?: BroadcastChannel;
  private namespace: string;
  private tabId: string;
  private commandQueue: Promise<void> = Promise.resolve();
  private subscriptionCache = new Map<string, { unsub: () => void; callbacks: Set<SubscriptionCallback> }>();
  private eventListeners = new Set<(event: number) => void>();
  private _promotionConfig: { coordinatorUrl: string; authToken: string | null; dbName: string | null; storageBackend: string | null } | null = null;
  private _promotionResolve: (() => void) | null = null;
  private _leaderAlive: boolean = false;
  /** Replay batching: buffer mutations between SYNC_BEGIN and SYNC_DONE.
   *  isReplaying is a computed getter from replayDepth > 0. */
  private replayDepth = 0;
  private replayBuffer: Array<{docId: string, recordId: string, fields: any}> = [];
  private get isReplaying(): boolean { return this.replayDepth > 0; }
  /** Observable handler instance ID — proves no handler replacement during lifecycle. */
  private handlerId = '';
  /** Monotonic BC handler invocation counter (synthetic seq — use protocol seq when available). */
  private _bcInvokeCount = 0;
  /** performance.now() at start of each message handler invocation. */
  private _handlerStartTime = 0;

  private constructor(inner: any, namespace: string, replicaId: string) {
    this.inner = inner;
    this.namespace = namespace;
    this.tabId = replicaId;
  }

  /** RuntimeStore for cache-first reads. Created by default. */
  get runtimeStore(): RuntimeStore {
    if (!this._runtimeStore) {
      this._runtimeStore = new RuntimeStore();
    }
    return this._runtimeStore;
  }

  /** Current runtime status object with mode, leader_alive, cursor, etc.
   *  Legacy string comparison (=== "Leader") still works via toString(). */
  get runtimeStatus(): { mode: string; leader_alive?: boolean; cursor?: number; pending_count?: number; promotion_ready?: boolean; leader_left?: boolean; } {
    const raw: string = this.inner.runtimeStatus();
    try {
      return JSON.parse(raw);
    } catch {
      return { mode: raw };
    }
  }

  /** Enable RuntimeStore-based cache reads. Call once after construction. */
  enableRuntimeStore(): void {
    const store = this.runtimeStore;
    // Set on both client (used by React hooks) and inner (backward compat)
    (this as any).__runtimeStore = store;
    (this.inner as any).__runtimeStore = store;
    console.debug('[VaultSync] RuntimeStore enabled');
  }

  /** Wait for mirror→leader promotion via event-driven channel (no polling).
   *  When the current leader tab closes, the Web Lock is released and
   *  this tab (in mirror mode) can acquire it. waitForPromotion() resolves
   *  when LeaderEvent::Acquired fires in Rust, then calls promote(). */
  private async setupPromotionListener(config: VaultSyncConfig): Promise<void> {
    try {
      await this.inner.waitForPromotion();
      // Skip promotion if we recently received LEADER_READY from a live leader
      if (this._leaderAlive) {
        this._leaderAlive = false;
        return;
      }
      await this.inner.promote(
        this.namespace,
        config.coordinatorUrl || '',
        config.authToken || null,
        config.dbName || null,
        config.storageBackend || null,
      );
      console.debug('[Promotion] complete — tab is now Leader');
      // Broadcast LEADER_ELECTED so other tabs demote
      this.channel?.postMessage(`LEADER_ELECTED|${this.tabId}`);
      console.debug('[Promotion] broadcast LEADER_ELECTED');
    } catch (err) {
      console.warn('[Promotion] wait/promote failed:', err);
    }
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
    this.handlerId = crypto.randomUUID();
    console.debug('[BC] handler installed', JSON.stringify({ handlerId: this.handlerId, channel: channelName, timestamp: Date.now() }));
    this.channel = new BroadcastChannel(channelName);
    this.channel.addEventListener('message', (e) => {
      this._handlerStartTime = performance.now();
      const seq = ++this._bcInvokeCount;
      const raw = typeof e.data === 'string' ? e.data : JSON.stringify(e.data);
      const entryPrefix = typeof e.data === 'string' ? e.data.split('|', 1)[0] : '(non-string)';
      const logMeta = () => JSON.stringify({
        seq, handler: this.handlerId.slice(0, 8),
        replay: this.isReplaying, rDepth: this.replayDepth,
        elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
      });
      console.debug('[RX]', JSON.stringify({
        seq, prefix: entryPrefix,
        replay: this.isReplaying, rDepth: this.replayDepth,
        len: typeof e.data === 'string' ? e.data.length : 0,
        from: this.tabId, handler: this.handlerId.slice(0, 8),
      }));

      if (typeof e.data === 'string') {
        // ── MUTATION: direct format — update RuntimeStore ──
        // Format: MUTATION|bc_seq|tab_id|doc_id|record_id|fields_json
        if (entryPrefix === 'MUTATION') {
          const p0 = 9;
          const p1 = e.data.indexOf('|', p0);
          if (p1 < 0) { console.warn('[RX] MUTATION malformed', logMeta()); return; }
          const p2 = e.data.indexOf('|', p1 + 1);
          if (p2 < 0) { console.warn('[RX] MUTATION malformed', logMeta()); return; }
          const p3 = e.data.indexOf('|', p2 + 1);
          if (p3 < 0) { console.warn('[RX] MUTATION malformed', logMeta()); return; }
          const p4 = e.data.indexOf('|', p3 + 1);
          if (p4 < 0) { console.warn('[RX] MUTATION malformed', logMeta()); return; }
          const docId = e.data.slice(p2 + 1, p3);
          const recordId = e.data.slice(p3 + 1, p4);
          const fieldsJson = e.data.slice(p4 + 1);
          try {
            const fields = JSON.parse(fieldsJson);
            if (this.isReplaying) {
              this.replayBuffer.push({ docId, recordId, fields });
              if (this.replayBuffer.length % 100 === 0) {
                console.debug('[REPLAY] buffered', JSON.stringify({
                  seq, bufSize: this.replayBuffer.length,
                  elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
                }));
              }
              return;
            }
            console.debug('[STORE] applySetFieldBatch', JSON.stringify({
              seq, doc: docId, record: recordId,
              fields: Object.keys(fields),
              elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
            }));
            this.runtimeStore.applySetFieldBatch(docId, recordId, fields);
          } catch (err) {
            console.error('[RX] MUTATION parse error', logMeta(), err);
          }
          return;
        }

        // ── LEADER_ELECTED: another tab claimed leadership ──
        if (entryPrefix === 'LEADER_ELECTED') {
          const claimedTabId = e.data.slice(15);
          if (claimedTabId && claimedTabId !== this.tabId && this.inner.is_leader()) {
            console.log('[LEADER] demoting self from', claimedTabId, logMeta());
            this.inner.demote();
          }
          return;
        }

        // ── SYNC_BEGIN: replay session started ──
        if (entryPrefix === 'SYNC_BEGIN') {
          // Extract optional replay=N from trailing field
          const replayMatch = e.data.match(/replay=(\d+)/);
          const replayId = replayMatch ? parseInt(replayMatch[1], 10) : 0;
          const prevDepth = this.replayDepth;
          this.replayDepth++;
          this.replayBuffer = [];
          console.log('[TIMELINE] T2: SYNC_BEGIN', JSON.stringify({
            depth: `${prevDepth}→${this.replayDepth}`,
            replay: replayId,
            seq, elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
          }));
          return;
        }

        // ── SYNC_DONE: replay session complete ──
        if (entryPrefix === 'SYNC_DONE') {
          // Extract optional replay=N from trailing field
          const replayMatch = e.data.match(/replay=(\d+)/);
          const replayId = replayMatch ? parseInt(replayMatch[1], 10) : 0;
          const prevDepth = this.replayDepth;
          this.replayDepth--;
          const bufLen = this.replayBuffer.length;
          const buffer = this.replayBuffer;
          this.replayBuffer = [];
          console.debug('[REPLAY]', JSON.stringify({
            depth: `${prevDepth}→${this.replayDepth}`,
            flushed: bufLen, replay: replayId, seq,
            elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
          }));
          this.runtimeStore.beginBatch();
          try {
            for (let i = 0; i < buffer.length; i++) {
              const m = buffer[i];
              if (i === 0) {
                console.log('[TIMELINE] T3: First record inserted', JSON.stringify({
                  doc: m.docId, record: m.recordId, seq,
                  elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
                }));
              }
              this.runtimeStore.applySetFieldBatch(m.docId, m.recordId, m.fields);
            }
          } finally {
            this.runtimeStore.endBatch();
          }
          return;
        }

        // ── LEADER_READY: promotion announcement ──
        if (entryPrefix === 'LEADER_READY') {
          // A leader is alive — re-sync with the new leader immediately
          // Format: LEADER_READY|{tab_id}|{cursor}
          this._leaderAlive = true;
          const parts = e.data.split('|');
          const leaderTabId = parts[1];
          const cursor = parseInt(parts[2] || '0', 10);
          // Always send SYNC if the message is from a different tab (regardless of mode).
          // Fix: mode check (`this.inner.runtimeStatus().mode === 'Mirror'`) was incorrect
          // because LEADER_READY can arrive before LEADER_ELECTED, meaning the demoted tab
          // is still `Leader` mode and would miss its opportunity to request a sync.
          if (leaderTabId && leaderTabId !== this.tabId) {
            this.channel?.postMessage(`SYNC|1|0|${this.namespace}|${cursor}|0`);
          }
          return;
        }

        // ── Known WASM-handled protocol prefixes ──
        if (e.data.includes('|')) {
          switch (entryPrefix) {
            case 'PROTO':
            case 'SYNC':
            case 'HEARTBEAT':
            case 'FOLLOWER_ATTACH':
            case 'FOLLOWER_DETACH':
            case 'FOLLOWER_ACK':
            case 'LEFT':
            case 'PING':
              return;
            default:
              if (entryPrefix !== 'MUTATION' && entryPrefix !== 'SYNC_BEGIN'
                  && entryPrefix !== 'SYNC_DONE' && entryPrefix !== 'LEADER_ELECTED'
                  && entryPrefix !== 'LEADER_READY') {
                console.warn('[PROTO] Unknown prefix', entryPrefix, logMeta());
              }
          }
        }

        // ── Legacy JSON messages ──
        try {
          const val = JSON.parse(e.data);
          if (!val) { return; }
          if (!val.type) { val.type = 'invalidate'; }
          const sender = val.tab_id || '(missing)';
          const receiver = this.tabId || '(missing)';
          const ignored = !!(val.tab_id && val.tab_id === this.tabId);

          switch (val.type) {
            case 'invalidate': {
              if (val.doc_id && val.record_id) {
                console.debug('[PROTO] invalidate', JSON.stringify({ sender, receiver, ignored, doc: val.doc_id, record: val.record_id, seq }));
                if (ignored) break;
                setTimeout(async () => {
                  try { await this.inner.fire_subscription(val.doc_id, val.record_id); }
                  catch (err) { console.error('[PROTO] fire_subscription failed', err); }
                }, 0);
              }
              break;
            }
            case 'insert':
            case 'update':
            case 'delete': {
              if (val.doc_id && val.record_id && !ignored && this.inner.is_leader()) {
                console.debug('[PROTO] command', JSON.stringify({ type: val.type, sender, receiver, doc: val.doc_id, record: val.record_id, seq }));
                this.enqueueMutation(async () => {
                  try { await this.inner.handle_command(val.type, val.doc_id, val.record_id, val.payload || ''); }
                  catch (err) { console.error('[PROTO] handle_command failed', err); }
                });
              }
              break;
            }
            default:
              console.warn('[PROTO] unknown type=' + val.type, logMeta());
          }
        } catch (err) {
          console.error('[PROTO] Failed to parse message (expected JSON):', err);
        }
      } else {
        console.warn('[RX] non-string data', logMeta());
      }
    });
  }

  static create(config: VaultSyncConfig): Promise<VaultSyncClient> {
    const key = config.namespace;
    const existing = instancePromises.get(key);
    if (existing) {
      return existing;
    }

    const promise = (async () => {
      await init();

      const mode = config.mode || (config.coordinatorUrl ? 'online' : 'offline');
      let inner: any;
      if (mode === 'online' && config.coordinatorUrl) {
        inner = await VaultSyncRuntime.new_with_coordinator(
          config.namespace,
          config.replicaId,
          config.coordinatorUrl,
          config.authToken || null,
          config.dbName || null,
          config.storageBackend || null
        );
      } else {
        inner = await VaultSyncRuntime.new(
          config.namespace,
          config.replicaId,
          config.dbName || null,
          config.storageBackend || null
        );
      }

      const client = new VaultSyncClient(inner, config.namespace, config.replicaId);
      client.setupBroadcastChannel(config.namespace);

      // Global error hooks for BC handler chain
      window.addEventListener('unhandledrejection', (e) => {
        console.error('[PROTO] Unhandled rejection:', e.reason?.toString?.()?.slice(0, 200));
      });
      window.addEventListener('error', (e) => {
        console.error('[PROTO] Uncaught error:', e.error?.toString?.()?.slice(0, 200));
      });

      // Wire event bridge: Rust → JS
      inner.on_event((eventType: number) => {
        for (const listener of client.eventListeners) {
          listener(eventType);
        }
      });

      // Enable RuntimeStore for cache-first reads
      client.enableRuntimeStore();

      // Timeline: T0 — runtime created (capture mode and timing)
      const t0Mode = client.runtimeStatus?.mode;
      console.log('[TIMELINE] T0: runtime created', JSON.stringify({
        mode: t0Mode, elapsed: '0ms',
      }));

      // Fire-and-forget promotion listener (event-driven, no polling)
      client.setupPromotionListener(config);

      return client;
    })();

    instancePromises.set(key, promise);
    return promise;
  }

  async insert(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(async () => {
        // Apply to RuntimeStore first for instant cache hit.
        // Use applySetFieldBatch (not per-field applySetField) to avoid
        // orphan records when fields include `id` or `record_id`.
        this.runtimeStore.applySetFieldBatch(docId, recordId, fields);
        await this.inner.insert(docId, recordId, JSON.stringify(fields));
      });
    }
    await this.inner.insert(docId, recordId, JSON.stringify(fields));
  }

  async update(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(async () => {
        this.runtimeStore.applySetFieldBatch(docId, recordId, fields);
        await this.inner.update(docId, recordId, JSON.stringify(fields));
      });
    }
    await this.inner.update(docId, recordId, JSON.stringify(fields));
  }

  async delete(docId: string, recordId: string): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(async () => {
        this.runtimeStore.applyDeleteDocument(docId, recordId);
        await this.inner.delete(docId, recordId);
      });
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

  /** Engine V2: expose Runtime + StorageEngine stats (WAL, page cache, health). */
  async runtimeStorageStats(): Promise<any> {
    const jsonStr = await this.inner.runtime_storage_stats();
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
      const wasmCallback = (recordId: string, jsonStr: string, _notifySeq?: number) => {
        const parsed = JSON.parse(jsonStr);
        for (const cb of jsCallbacks) {
          cb(recordId, parsed);
        }
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

  /** Bootstrap the RuntimeStore with data from OPFS (leader) or replay (follower).
   *  On the leader, loads all specified doc IDs from storage into the RuntimeStore.
   *  On the follower, the replay pipeline populates the RuntimeStore automatically.
   *  Call this once during app initialization, before React mounts. */
  async bootstrap(docIds?: string[]): Promise<void> {
    const mode = this.runtimeStatus.mode;
    if (mode === 'Leader') {
      if (docIds && docIds.length > 0) {
        for (const docId of docIds) {
          try {
            const arr = await this.inner.find(docId);
            for (const json of arr) {
              const fields = JSON.parse(json);
              const recordId = (fields.record_id ?? fields.id) as string;
              this.runtimeStore.applySetFieldBatch(docId, recordId, fields);
            }
          } catch (err) {
            console.warn(`[BOOTSTRAP] Failed to load doc "${docId}":`, err);
          }
        }
      }
    }
    // Mirror/Follower: nothing needed — replay pipeline populates RuntimeStore
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
