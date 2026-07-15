/** Phase 6: RuntimeStore — framework-agnostic JS cache with dependency tracking.
 *  Eliminates WASM calls during rendering. All mutations arrive via BC streaming. */
type DocKey = string;
type FieldMap = Record<string, any>;

type SubscriberId = number;
type NotifyCallback = () => void;

export class RuntimeStore {
  private cache: Map<string, Map<string, FieldMap>> = new Map();
  private docDeps: Map<string, Set<SubscriberId>> = new Map();
  private recDeps: Map<string, Set<SubscriberId>> = new Map();
  private subs: Map<SubscriberId, NotifyCallback> = new Map();
  private nextId: SubscriberId = 1;

  private _revision = 0;
  private maxEntries: number;
  private accessOrder: string[] = [];
  /** Transaction batching: beginBatch() increments, endBatch() decrements.
   *  Notifications are deferred until depth reaches 0. */
  private _batchDepth = 0;
  private _pendingDocs = new Set<string>();

  constructor(maxEntries = 10000) {
    this.maxEntries = maxEntries;
  }

  /** Returns a monotonically increasing revision number that increments on every
   *  observable state change. Use this to detect whether the store has been
   *  mutated since a previous point in time (e.g., to guard stale async results). */
  getRevision(): number {
    return this._revision;
  }

  /** Begin a batch transaction. Multiple beginBatch() calls nest correctly.
   *  Must be paired with endBatch() in try/finally. */
  beginBatch(): void {
    this._batchDepth++;
  }

  /** End a batch transaction. Fires one notification per unique doc that changed
   *  during the batch when depth reaches 0. */
  endBatch(): void {
    if (this._batchDepth <= 0) {
      console.warn('[STORE] endBatch without beginBatch');
      return;
    }
    this._batchDepth--;
    if (this._batchDepth === 0 && this._pendingDocs.size > 0) {
      const docs = Array.from(this._pendingDocs);
      this._pendingDocs.clear();
      for (const docId of docs) {
        this.notify(docId);
      }
    }
  }

  /** Get a specific record. */
  get(docId: string, recordId: string): FieldMap | null {
    const doc = this.cache.get(docId);
    if (!doc) return null;
    this.touchKey(`${docId}::${recordId}`);
    return doc.get(recordId) ?? null;
  }

  /** Query all records for a doc ID. */
  query(docId: string): FieldMap[] {
    const doc = this.cache.get(docId);
    if (!doc) return [];
    this.touchKey(docId);
    return Array.from(doc.values());
  }

  /** Apply a set-field mutation from WASM/BC. */
  applySetField(docId: string, recordId: string, field: string, value: any): void {
    let doc = this.cache.get(docId);
    if (!doc) {
      doc = new Map();
      this.cache.set(docId, doc);
    }
    if (field === 'id' && typeof value === 'string') {
      if (value === recordId) {
        const existing = doc.get(recordId);
        doc.set(recordId, { ...(existing || {}), [field]: value });
        this._revision++;
        this.notify(docId, value);
        return;
      }
      const existing = doc.get(recordId);
      doc.set(value, { ...(existing || {}), [field]: value });
      doc.delete(recordId);
      this._revision++;
      this.notify(docId, value);
      return;
    }
    const existing = doc.get(recordId);
    doc.set(recordId, { ...(existing || {}), [field]: value });
    this._revision++;
    this.notify(docId, recordId);
  }

  /** Apply multiple fields atomically with a single notification.
   *  Use this for BC MUTATION batches to avoid per-field re-render cascade. */
  applySetFieldBatch(docId: string, recordId: string, fields: Record<string, any>): void {
    const t0 = performance.now();
    let doc = this.cache.get(docId);
    if (!doc) {
      doc = new Map();
      this.cache.set(docId, doc);
    }

    // Bug 5: Normalize: if fields.id differs from recordId param, check if
    // there's an existing entry keyed by fields.id that should be merged.
    const effectiveRecordId = (fields.record_id ?? fields.id ?? recordId) as string;

    const merged = effectiveRecordId !== recordId;
    if (merged) {
      const existingRec = doc.get(effectiveRecordId);
      if (existingRec && doc.has(recordId)) {
        doc.set(effectiveRecordId, { ...existingRec, ...fields });
        doc.delete(recordId);
        this._revision++;
        this.notify(docId, effectiveRecordId);
        return;
      }
      doc.set(effectiveRecordId, { ...(doc.get(effectiveRecordId) || {}), ...fields });
      if (recordId !== effectiveRecordId && doc.has(recordId)) {
        doc.delete(recordId);
      }
      this._revision++;
      this.notify(docId, effectiveRecordId);
      return;
    }

    doc.set(recordId, { ...(doc.get(recordId) || {}), ...fields });
    this._revision++;
    this.notify(docId, recordId);
    console.debug('[STORE] applySetFieldBatch done', JSON.stringify({
      doc: docId, record: recordId,
      merged: merged ? effectiveRecordId : null,
      fields: Object.keys(fields).length,
      elapsed: (performance.now() - t0).toFixed(2) + 'ms',
    }));
  }

  /** Apply a delete-field mutation. */
  applyDeleteField(docId: string, recordId: string, field: string): void {
    const doc = this.cache.get(docId);
    if (!doc) return;
    const existing = doc.get(recordId);
    if (!existing) return;
    const { [field]: _, ...rest } = existing;
    doc.set(recordId, rest);
    this._revision++;
    this.notify(docId, recordId);
  }

  /** Apply a delete-document mutation. */
  applyDeleteDocument(docId: string, recordId: string): void {
    const doc = this.cache.get(docId);
    if (!doc) return;
    doc.delete(recordId);
    if (doc.size === 0) {
      this.cache.delete(docId);
    }
    this._revision++;
    this.notify(docId, recordId);
  }

  /** Track subscriber identities per (docId, recordId, component) for duplicate detection. */
  private _subIdentities = new Map<string, { docSubs: Set<number>; recSubs: Map<string, Set<number>> }>();

  /** Subscribe to changes. Returns unsubscribe function. */
  subscribe(
    docId: string,
    recordId: string | null,
    callback: NotifyCallback,
    componentStack?: string,
  ): () => void {
    const id = this.nextId++;
    this.subs.set(id, callback);

    // Track subscriber identity for diagnostics
    let identity = this._subIdentities.get(docId);
    if (!identity) {
      identity = { docSubs: new Set(), recSubs: new Map() };
      this._subIdentities.set(docId, identity);
    }
    if (recordId) {
      let recSet = identity.recSubs.get(recordId);
      if (!recSet) { recSet = new Set(); identity.recSubs.set(recordId, recSet); }
      recSet.add(id);
    } else {
      identity.docSubs.add(id);
    }

    // Warn when multiple distinct subscribers exist for the same doc
    const docCount = identity.docSubs.size;
    const recCountAll = Array.from(identity.recSubs.values()).reduce((sum, s) => sum + s.size, 0);
    if (docCount + recCountAll > 1) {
      console.warn('[STORE] subscriber joined', JSON.stringify({
        doc: docId, record: recordId,
        docSubscribers: docCount,
        recordSubscribers: recCountAll,
        subscriptionId: id,
        stack: componentStack?.slice(0, 120),
      }));
    }

    if (recordId) {
      const key = `${docId}::${recordId}`;
      let deps = this.recDeps.get(key);
      if (!deps) {
        deps = new Set();
        this.recDeps.set(key, deps);
      }
      deps.add(id);
    } else {
      let deps = this.docDeps.get(docId);
      if (!deps) {
        deps = new Set();
        this.docDeps.set(docId, deps);
      }
      deps.add(id);
    }

    return () => {
      this.subs.delete(id);
      if (recordId) {
        identity.recSubs.get(recordId)?.delete(id);
        const key = `${docId}::${recordId}`;
        this.recDeps.get(key)?.delete(id);
      } else {
        identity.docSubs.delete(id);
        this.docDeps.get(docId)?.delete(id);
      }
    };
  }

  /** Notify subscribers of a change. During a batch, defers notification to endBatch(). */
  private notify(docId: string, recordId?: string): void {
    if (this._batchDepth > 0) {
      this._pendingDocs.add(docId);
      return;
    }
    const t0 = performance.now();
    let totalFired = 0;
    if (recordId) {
      const deps = this.recDeps.get(`${docId}::${recordId}`);
      if (deps) {
        for (const id of deps) {
          this.subs.get(id)?.();
          totalFired++;
        }
      }
    }
    const docDeps = this.docDeps.get(docId);
    if (docDeps) {
      for (const id of docDeps) {
        this.subs.get(id)?.();
        totalFired++;
      }
    }
    if (totalFired > 0) {
      console.debug('[STORE] notify', JSON.stringify({
        doc: docId, record: recordId, fired: totalFired,
        elapsed: (performance.now() - t0).toFixed(2) + 'ms',
      }));
    }
  }

  /** LRU promotion. */
  private touchKey(key: string): void {
    const idx = this.accessOrder.indexOf(key);
    if (idx >= 0) {
      this.accessOrder.splice(idx, 1);
    }
    this.accessOrder.push(key);
    if (this.accessOrder.length > this.maxEntries) {
      this.evict();
    }
  }

  /** Evict oldest entries. */
  private evict(): void {
    while (this.accessOrder.length > this.maxEntries * 0.9) {
      const oldest = this.accessOrder.shift();
      if (!oldest) break;
      const [docId, recordId] = oldest.split('::');
      if (recordId) {
        this.cache.get(docId)?.delete(recordId);
      }
    }
  }

  /** Clear all cached data. */
  clear(): void {
    this.cache.clear();
    this.accessOrder = [];
  }

  get size(): number {
    let count = 0;
    for (const doc of this.cache.values()) {
      count += doc.size;
    }
    return count;
  }
}
