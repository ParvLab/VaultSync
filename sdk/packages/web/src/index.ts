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
  /** Hydration promise: resolves when the first SYNC_DONE applies data to RuntimeStore.
   *  Used by mirror bootstrap to wait for replay data before declaring "empty". */
  private _hydrationResolve: (() => void) | null = null;
  private _hydrationPromise: Promise<void> | null = null;
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
  private _bcCounts = { received: 0, begin: 0, mutation: 0, done: 0 };

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
  get runtimeStatus(): { mode: string; state: string; leader_alive?: boolean; cursor?: number; pending_count?: number; promotion_ready?: boolean; leader_left?: boolean; } {
    const raw: string = this.inner.runtimeStatus();
    try {
      const parsed = JSON.parse(raw);
      if (!parsed.state) {
        parsed.state = this.inner.runtimeState();
      }
      return parsed;
    } catch {
      return { mode: raw, state: this.inner.runtimeState() };
    }
  }

  /** Check if mutation fields represent a delete. */
  private isDeleteMutation(fields: Record<string, unknown>): boolean {
    return fields._deleted === true || fields.__deleted__ === true;
  }

  /** Apply a mutation (set or delete) to the RuntimeStore. Returns "delete" if the record was removed. */
  private applyMutation(mutation: { docId: string; recordId: string; fields: Record<string, unknown> }, caller?: string): "set" | "delete" {
    if (this.isDeleteMutation(mutation.fields)) {
      this.runtimeStore.applyDeleteDocument(mutation.docId, mutation.recordId, caller);
      return "delete";
    }
    this.runtimeStore.applySetFieldBatch(mutation.docId, mutation.recordId, mutation.fields as Record<string, any>, caller);
    return "set";
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
    if (this.inner.is_leader()) return;
    const promotionId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    try {
      await this.inner.waitForPromotion();
      // Skip promotion if we recently received LEADER_READY from a live leader
      if (this._leaderAlive) {
        this._leaderAlive = false;
        return;
      }

      // Phase 3: Pause mirror mutation processing for deterministic snapshot.
      // After pause(), no BC MUTATION frames will be applied to the in-memory store.
      const beforeState = this.inner.mirrorState();
      const drainSeq = this.inner.pauseMirror();
      // Yield to event loop so any in-flight BC callback completes.
      await new Promise(r => setTimeout(r, 0));
      // Verify drain seq has not advanced (no mutations in flight).
      const afterState = this.inner.mirrorState();
      if (drainSeq !== afterState?.drain_seq && afterState?.drain_seq > 0) {
        console.warn('[PROMO] pid=%s drain_seq advanced %d->%d — mutation in flight during pause',
          promotionId, drainSeq, afterState?.drain_seq);
      }

      // Phase 3: Before promotion snapshot — RuntimeStore + Rust
      const rs_b_notes = this.runtimeStore.query('notes');
      const rust_b_raw = await this.inner.find('notes');
      const rust_b_ids = rust_b_raw.map((j: string) => { const f = JSON.parse(j); return f.record_id ?? f.id; });
      console.log('[PROMO_SNAPSHOT] phase=before doc=notes RuntimeStore=%d ids=[%s]',
        rs_b_notes.length,
        rs_b_notes.map((r: any) => `${r.record_id ?? r.id}(del=${r._deleted ?? r.__deleted__})`).join(','));
      console.log('[PROMO_COMPARE] phase=before doc=notes Rust=%d ids=[%s]',
        rust_b_ids.length, rust_b_ids.join(','));

      await this.inner.promote(
        this.namespace,
        config.coordinatorUrl || '',
        config.authToken || null,
        config.dbName || null,
        config.storageBackend || null,
      );

      // Phase 3: After promotion snapshot — RuntimeStore + Rust + invariant
      const rs_a_notes = this.runtimeStore.query('notes');
      const rust_a_raw = await this.inner.find('notes');
      const rust_a_ids = rust_a_raw.map((j: string) => { const f = JSON.parse(j); return f.record_id ?? f.id; });
      console.log('[PROMO_SNAPSHOT] phase=after doc=notes RuntimeStore=%d ids=[%s]',
        rs_a_notes.length,
        rs_a_notes.map((r: any) => `${r.record_id ?? r.id}(del=${r._deleted ?? r.__deleted__})`).join(','));
      console.log('[PROMO_COMPARE] phase=after doc=notes Rust=%d ids=[%s]',
        rust_a_ids.length, rust_a_ids.join(','));

      // Phase 3: Set-based comparison with rich diagnostics
      const rs_set = new Set(rs_a_notes.map((r: any) => r.record_id ?? r.id));
      const rust_set = new Set(rust_a_ids);
      const rs_only = [...rs_set].filter(id => !rust_set.has(id));
      const rust_only = [...rust_set].filter(id => !rs_set.has(id));
      if (rs_only.length > 0 || rust_only.length > 0) {
        console.warn(
          '[PROMO_INVARIANT] MISMATCH pid=%s rs_only=[%s] rust_only=[%s] mirror=%s',
          promotionId, rs_only.join(','), rust_only.join(','),
          JSON.stringify(beforeState),
        );
      } else {
        console.debug('[PROMO_INVARIANT] OK pid=%s count=%d mirror=%s',
          promotionId, rs_set.size, JSON.stringify(beforeState));
      }

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
    console.log('[BC_REGISTER]', JSON.stringify({ channel: channelName, handler: this.handlerId.slice(0, 8) }));
    this.channel = new BroadcastChannel(channelName);
    this.channel.addEventListener('message', (e) => {
      this._handlerStartTime = performance.now();
      const seq = ++this._bcInvokeCount;
      const raw = typeof e.data === 'string' ? e.data : JSON.stringify(e.data);
      const entryPrefix = typeof e.data === 'string' ? e.data.split('|', 1)[0] : '(non-string)';
      if (typeof e.data === 'string') {
        const allParts = e.data.split('|');
        const senderId = entryPrefix === 'MUTATION' ? (allParts[2] || '?') :
                         entryPrefix === 'LEADER_READY' ? (allParts[1] || '?') : '?';
        this._bcCounts.received++;
        console.log('[BC_RAW]', JSON.stringify({
          from: senderId, to: this.tabId, prefix: entryPrefix, len: e.data.length,
          replay: this.isReplaying, depth: this.replayDepth,
        }));
      }
      const logMeta = () => JSON.stringify({
        seq, handler: this.handlerId.slice(0, 8),
        replay: this.isReplaying, rDepth: this.replayDepth,
        elapsed: (performance.now() - this._handlerStartTime).toFixed(1) + 'ms',
      });
      if (typeof e.data === 'string') {
        // ── MUTATION: direct format — update RuntimeStore ──
        // Format: MUTATION|bc_seq|tab_id|doc_id|record_id|fields_json
        if (entryPrefix === 'MUTATION') {
          this._bcCounts.mutation++;
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
              console.log('[REPLAY_BUFFER]', JSON.stringify({
                before: this.replayBuffer.length - 1, after: this.replayBuffer.length,
                record: recordId,
              }));
              return;
            }
            if (this.isDeleteMutation(fields)) {
              console.debug('[DELETE_MUTATION] source=live docId=%s recordId=%s fields=%s replayDepth=%d seq=%d',
                docId, recordId, JSON.stringify(fields), this.replayDepth, seq);
            }
            this.applyMutation({ docId, recordId, fields }, 'BC_live');
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
          // Guard: ignore nested SYNC_BEGIN if already replaying.
          // The existing replay already captures the latest mutation stream.
          // Without this guard, a cascade of SYNC_BEGIN→buffer-reset→replay
          // can leave the tab stuck in "Promoting" with replayDepth > 0.
          if (this.isReplaying) {
            const replayMatch = e.data.match(/replay=(\d+)/);
            const replayId = replayMatch ? parseInt(replayMatch[1], 10) : 0;
            console.debug('[SYNC] ignoring nested SYNC_BEGIN depth=%d replay=%s',
              this.replayDepth, replayId ? `replay=${replayId}` : '(no id)');
            return;
          }
          // Extract optional replay=N from trailing field
          const replayMatch = e.data.match(/replay=(\d+)/);
          const replayId = replayMatch ? parseInt(replayMatch[1], 10) : 0;
          const prevDepth = this.replayDepth;
          this.replayDepth++;
          this.replayBuffer = [];
          this._bcCounts.begin++;
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
          if (this.replayDepth <= 0) {
            console.warn('[SYNC] SYNC_DONE without matching SYNC_BEGIN depth=%d — ignoring', this.replayDepth);
            return;
          }
          this.replayDepth--;
          this._bcCounts.done++;
          const bufLen = this.replayBuffer.length;
          const buffer = this.replayBuffer;
          this.replayBuffer = [];
          this.runtimeStore.beginBatch();
          console.log('[BATCH_BEGIN]');
          let deletedCount = 0;
          const touchedDocs = new Set<string>();
          try {
            for (let i = 0; i < buffer.length; i++) {
              const recsBefore = this.runtimeStore.getRecordCount(buffer[i].docId);
              touchedDocs.add(buffer[i].docId);
              if (this.applyMutation(buffer[i], 'replay_flush') === "delete") {
                deletedCount++;
              }
              const recsAfter = this.runtimeStore.getRecordCount(buffer[i].docId);
              console.log('[REPLAY_APPLY]', JSON.stringify({
                i, total: buffer.length, doc: buffer[i].docId, rec: buffer[i].recordId,
                recsBefore, recsAfter,
              }));
            }
          } finally {
            console.log('[BATCH_END]');
            this.runtimeStore.endBatch();
          }
          // Notify any bootstrap() caller waiting for initial hydration
          if (this._hydrationResolve) {
            this._hydrationResolve();
            this._hydrationResolve = null;
          }
          // If we were in Hydrating state (e.g., after demotion), transition to ready.
          // Always hydrate from mirror for non-Leader tabs to catch any gaps.
          const syncState = this.inner.runtimeState();
          if (syncState !== 'Leading') {
            const safeDocs = ['notes'];
            let totalHydrated = 0;
            for (const safeDoc of safeDocs) {
              try {
                const arr = this.inner.mirrorDocuments(safeDoc);
                if (arr.length > 0) {
                  this.runtimeStore.beginBatch();
                  for (let i = 0; i < arr.length; i++) {
                    const fields = JSON.parse(arr[i]);
                    const recordId = (fields.record_id ?? fields.id) as string;
                    this.runtimeStore.applySetFieldBatch(safeDoc, recordId, fields, 'demotion_sync');
                    totalHydrated++;
                  }
                  this.runtimeStore.endBatch();
                }
              } catch (err) {
                console.warn('[HYDRATE] mirror snapshot failed for doc', safeDoc, err);
              }
            }
            this.inner.markReady();
            console.log('[HYDRATION]', JSON.stringify({
              source: 'SYNC_DONE', state: syncState, mirrorRecords: totalHydrated,
              runtimeStore: this.runtimeStore.query('notes').length,
            }));
          }
          console.log('[BC_SUMMARY]', JSON.stringify({
            ...this._bcCounts, buffered: bufLen, deleted: deletedCount,
            runtime: this.runtimeStore.query('notes').length,
          }));
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
          console.debug('[LEADER_READY]', {
            sender: leaderTabId,
            thisTab: this.tabId,
            mode: this.runtimeStatus.mode,
            replayDepth: this.replayDepth,
            isReplaying: this.isReplaying,
          });
          // Guard: skip SYNC if already replaying — the existing replay already
          // contains the latest mutation stream. Sending another SYNC during replay
          // causes the leader to spawn a nested replay, creating a cascade that
          // can leave this tab permanently stuck with replayDepth > 0.
          if (this.isReplaying) {
            console.debug('[SYNC_REQUEST] skipped: replaying', {
              reason: 'leader_ready',
              from: this.tabId,
              leader: leaderTabId,
              cursor,
              depth: this.replayDepth,
            });
            return;
          }
          // Always send SYNC if the message is from a different tab (regardless of mode).
          // The leaderTabId === this.tabId guard prevents self-SYNC after promotion.
          // A mode-based guard (`mode !== 'Mirror'`) was previously removed because
          // LEADER_READY can arrive before LEADER_ELECTED — the demoted tab is still
          // in 'Leader' mode and would miss its sync opportunity.
          if (leaderTabId && leaderTabId !== this.tabId) {
            console.debug('[SYNC_REQUEST]', {
              reason: 'leader_ready',
              from: this.tabId,
              leader: leaderTabId,
              same: false,
              cursor,
            });
            this.channel?.postMessage(`SYNC|1|0|${this.namespace}|${cursor}|0`);
          } else {
            console.debug('[SYNC_REQUEST] skipped: same tab', {
              leader: leaderTabId,
              thisTab: this.tabId,
              cursor,
            });
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
              this.runtimeStore.applySetFieldBatch(docId, recordId, fields, 'bootstrap');
        await this.inner.insert(docId, recordId, JSON.stringify(fields));
      });
    }
    await this.inner.insert(docId, recordId, JSON.stringify(fields));
  }

  async update(docId: string, recordId: string, fields: RecordFields): Promise<void> {
    if (this.inner.is_leader()) {
      return this.enqueueMutation(async () => {
        this.runtimeStore.applySetFieldBatch(docId, recordId, fields, 'local_update');
        await this.inner.update(docId, recordId, JSON.stringify(fields));
      });
    }
    await this.inner.update(docId, recordId, JSON.stringify(fields));
  }

  async delete(docId: string, recordId: string): Promise<void> {
    if (!docId || !recordId) {
      console.warn('[VaultSync] delete skipped: missing docId/recordId', { docId, recordId });
      return;
    }
    if (this.inner.is_leader()) {
      return this.enqueueMutation(async () => {
        this.runtimeStore.applyDeleteDocument(docId, recordId, 'local_delete');
        await this.inner.delete(docId, recordId);
      });
    }
    await this.inner.delete(docId, recordId);
  }

  async get(docId: string, recordId: string): Promise<RecordFields | null> {
    await this._waitForReady();
    const jsonStr = await this.inner.get(docId, recordId);
    if (!jsonStr) return null;
    return JSON.parse(jsonStr);
  }

  /** Wait until runtime is ready (not Hydrating/Starting). Polls every 50ms. */
  private async _waitForReady(): Promise<void> {
    while (true) {
      const s = this.inner.runtimeState();
      if (s !== 'Hydrating' && s !== 'Starting') break;
      await new Promise(r => setTimeout(r, 50));
    }
  }

  async find(docId: string): Promise<RecordFields[]> {
    await this._waitForReady();
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
      // Wait for initial sync to write data to OPFS before populating DocumentStore.
      // Uses event-driven `waitForInitialSync()` which awaits `InitialSyncComplete`
      // from the download worker's `process_batch() -> Ok(0)` cycle.
      // Replaces previous cursor polling (3s timer) with deterministic signal.
      const synced = await this.inner.waitForInitialSync();
      if (synced) {
        console.debug('[BOOTSTRAP] initial sync complete');
      } else {
        console.warn('[BOOTSTRAP] waitForInitialSync timeout — proceeding without signal');
      }
      if (docIds && docIds.length > 0) {
        for (const docId of docIds) {
          try {
            const before = this.runtimeStore.query(docId).length;
            // Re-populate the Rust DocumentStore from OPFS now that initial sync
            // has completed. Patch 1 hydrated during construction before any data
            // arrived — this second pass catches data that sync'd in the meantime.
            const populated = await this.inner.populateDocumentStore(docId);
            // Direct hydration from Rust DocumentStore — bypasses find() and _waitForReady().
            const arr = this.inner.hydrateRuntimeStore();
            console.log('[BS_RAW]', JSON.stringify({
                docId, arrLength: arr.length, populated,
                records: (Array.from(arr) as string[]).map((json: string, i: number) => {
                    const f = JSON.parse(json);
                    return { i, recordId: f.record_id ?? f.id, fields: Object.keys(f) };
                }),
            }));
            const records: Array<{ recordId: string; deleted: boolean }> = [];
            for (const json of arr) {
              const fields = JSON.parse(json);
              const recordId = (fields.record_id ?? fields.id) as string;
              const isDel = fields._deleted === true || fields.__deleted__ === true;
              records.push({ recordId, deleted: isDel });
              this.runtimeStore.applySetFieldBatch(docId, recordId, fields, 'local_insert');
            }
            const after = this.runtimeStore.query(docId).length;
            const deletedRecords = records.filter(r => r.deleted).length;
            console.debug('[BOOTSTRAP] doc=%s path=hydrate mode=%s before=%d records_found=%d deleted=%d after=%d ids=[%s]',
              docId, mode, before, records.length, deletedRecords, after,
              records.map(r => `${r.recordId}(del=${r.deleted})`).join(','));
          } catch (err) {
            console.warn(`[BOOTSTRAP] Failed to load doc "${docId}":`, err);
          }
        }
      }
      // Phase 2: Mark runtime as ready — unblocks find()/get() queries
      this.inner.markReady();
      const rsCount = this.runtimeStore.query('notes').length;
      const rustArr = this.inner.hydrateRuntimeStore();
      console.log('[HYDRATION_INVARIANT]', JSON.stringify({
        role: 'Leader', runtimeStore: rsCount, rustStore: rustArr.length,
      }));
      const docStoreDebug = this.inner.debugDocumentStore();
      console.log('[DOCSTORE] leader after markReady:', docStoreDebug);
      console.log('[LIFECYCLE] leader hydrated — state=Ready');
    } else {
      // Mirror/follower: set Hydrating state and wait for SYNC_DONE.
      // The SYNC_DONE handler populates RuntimeStore from mirror and calls markReady().
      this.inner.beginHydration();
      if (docIds && docIds.length > 0) {
        // Create a hydration promise if one isn't already pending
        if (!this._hydrationPromise) {
          this._hydrationPromise = new Promise<void>(resolve => {
            this._hydrationResolve = resolve;
          });
        }
        // Wait for first SYNC_DONE or timeout (3s)
        const hydrated = await Promise.race([
          this._hydrationPromise.then(() => true),
          new Promise<boolean>(r => setTimeout(() => r(false), 3000)),
        ]);
        if (!hydrated) {
          // Fall back to mirror Documents for any docs that weren't hydrated
          console.debug('[BOOTSTRAP] sync timeout — falling back to mirror snapshot');
        }
        for (const docId of docIds) {
          if (this.runtimeStore.query(docId).length > 0) {
            console.debug('[BOOTSTRAP] doc=%s path=sync mode=%s already_hydrated=%d',
              docId, mode, this.runtimeStore.query(docId).length);
            continue;
          }
          try {
            // Direct read from MirrorRuntime's DocumentStore (bypasses find())
            const arr = this.inner.mirrorDocuments(docId);
            const records: Array<{ recordId: string; deleted: boolean }> = [];
            for (const json of arr) {
              const fields = JSON.parse(json);
              const recordId = (fields.record_id ?? fields.id) as string;
              const isDel = fields._deleted === true || fields.__deleted__ === true;
              records.push({ recordId, deleted: isDel });
              this.runtimeStore.applySetFieldBatch(docId, recordId, fields, 'mirror_snapshot');
            }
            const after = this.runtimeStore.query(docId).length;
            const deletedRecords = records.filter(r => r.deleted).length;
            console.debug('[BOOTSTRAP] doc=%s path=mirror_snapshot mode=%s records_found=%d deleted=%d after=%d ids=[%s]',
              docId, mode, records.length, deletedRecords, after,
              records.map(r => `${r.recordId}(del=${r.deleted})`).join(','));
          } catch (err) {
            console.warn(`[BOOTSTRAP] Failed to load doc "${docId}" from mirror:`, err);
          }
        }
      }
      // Don't markReady here — delegate to SYNC_DONE handler which calls markReady()
      // after applying replay buffer + mirror snapshot.
      const rsCount = this.runtimeStore.query('notes').length;
      console.log('[HYDRATION_INVARIANT]', JSON.stringify({
        role: 'Follower', runtimeStore: rsCount,
      }));
      // Reset hydration state for next bootstrap call
      this._hydrationResolve = null;
      this._hydrationPromise = null;
    }
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
