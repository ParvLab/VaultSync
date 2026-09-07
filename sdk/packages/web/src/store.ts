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

  /** Diagnostic: count records for a doc (direct map access, no transformation). */
  getRecordCount(docId: string): number {
    return this.cache.get(docId)?.size ?? 0;
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
    const result = Array.from(doc.entries()).map(([recordId, fields]) => {
      const { id: _id, record_id: _rid, ...rest } = fields as Record<string, unknown>;
      return { id: recordId, record_id: recordId, ...rest } as FieldMap;
    });
    const ids = result.map((v: FieldMap) => (v.record_id ?? v.id ?? '?') as string);
    const deleted = result.filter((v: FieldMap) => v._deleted === true || v.__deleted__ === true).length;
    console.debug('[RUNTIME_STORE] query doc=%s total=%d deleted=%d visible=%d ids=[%s]',
      docId, result.length, deleted, result.length - deleted, ids.join(','));
    this.touchKey(docId);
    return result;
  }

  /** Apply a set-field mutation from WASM/BC. */
  applySetField(docId: string, recordId: string, field: string, value: any): void {
    console.debug('[RUNTIME_STORE] applySetField doc=%s rec=%s field=%s val=%s',
      docId, recordId, field, typeof value === 'object' ? JSON.stringify(value).slice(0, 80) : String(value));
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
  applySetFieldBatch(docId: string, recordId: string, fields: Record<string, any>, caller?: string): void {
    const isDel = fields._deleted === true || fields.__deleted__ === true;
    if (isDel) {
      this.applyDeleteDocument(docId, recordId, caller);
      return;
    }
    let doc = this.cache.get(docId);
    if (!doc) {
      doc = new Map();
      this.cache.set(docId, doc);
    }

    const exists_before = doc.has(recordId);
    const size_before = doc.size;
    console.log('[RT_SET]', JSON.stringify({
      doc: docId, rec: recordId, caller: caller ?? '?',
      fieldCount: Object.keys(fields).length, exists: exists_before,
    }));

    const effectiveRecordId = (fields.record_id ?? fields.id ?? recordId) as string;

    const merged = effectiveRecordId !== recordId;
    if (merged) {
      const existingRec = doc.get(effectiveRecordId);
      if (existingRec && doc.has(recordId)) {
        doc.set(effectiveRecordId, { ...existingRec, ...fields });
        doc.delete(recordId);
        this._revision++;
        this.notify(docId, effectiveRecordId);
        if (!exists_before) {
          console.debug('[RT_MUTATION] source=%s record=%s exists_before=%s action=CREATE doc_size=%d→%d',
            caller ?? '?', recordId, exists_before, size_before, doc.size);
        }
        return;
      }
      doc.set(effectiveRecordId, { ...(doc.get(effectiveRecordId) || {}), ...fields });
      if (recordId !== effectiveRecordId && doc.has(recordId)) {
        doc.delete(recordId);
      }
      this._revision++;
      this.notify(docId, effectiveRecordId);
      if (!exists_before) {
        console.debug('[RT_MUTATION] source=%s record=%s exists_before=%s action=CREATE doc_size=%d→%d',
          caller ?? '?', recordId, exists_before, size_before, doc.size);
      }
      return;
    }

    doc.set(recordId, { ...(doc.get(recordId) || {}), ...fields });
    this._revision++;
    this.notify(docId, recordId);
    if (!exists_before) {
      console.debug('[RT_MUTATION] source=%s record=%s exists_before=%s action=CREATE doc_size=%d→%d',
        caller ?? '?', recordId, exists_before, size_before, doc.size);
    }
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
  applyDeleteDocument(docId: string, recordId: string, caller?: string): void {
    const doc = this.cache.get(docId);
    if (!doc) {
      console.debug('[RUNTIME_STORE] applyDeleteDocument doc=%s rec=%s — no such doc (already deleted?)', docId, recordId);
      return;
    }
    const had = doc.has(recordId);
    doc.delete(recordId);
    console.debug('[RUNTIME_STORE] applyDeleteDocument doc=%s rec=%s existed=%s remaining=%d size=%d',
      docId, recordId, String(had), doc.size, this.cache.size);
    if (doc.size === 0) {
      this.cache.delete(docId);
    }
    this._revision++;
    this.notify(docId, recordId);
  }

  /** Subscribe to changes. Returns unsubscribe function. */
  subscribe(docId: string, recordId: string | null, callback: NotifyCallback): () => void {
    const id = this.nextId++;
    this.subs.set(id, callback);

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
        const key = `${docId}::${recordId}`;
        this.recDeps.get(key)?.delete(id);
      } else {
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
    if (recordId) {
      const deps = this.recDeps.get(`${docId}::${recordId}`);
      if (deps) {
        for (const id of deps) {
          this.subs.get(id)?.();
        }
      }
    }
    const docDeps = this.docDeps.get(docId);
    if (docDeps) {
      for (const id of docDeps) {
        this.subs.get(id)?.();
      }
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
