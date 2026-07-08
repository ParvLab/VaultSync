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

  private maxEntries: number;
  private accessOrder: string[] = [];

  constructor(maxEntries = 10000) {
    this.maxEntries = maxEntries;
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
    let rec = doc.get(recordId);
    if (!rec) {
      rec = {};
      doc.set(recordId, rec);
    }
    rec[field] = value;
    this.notify(docId, recordId);
  }

  /** Apply a delete-field mutation. */
  applyDeleteField(docId: string, recordId: string, field: string): void {
    const doc = this.cache.get(docId);
    if (!doc) return;
    const rec = doc.get(recordId);
    if (!rec) return;
    delete rec[field];
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
    this.notify(docId, recordId);
  }

  /** Subscribe to changes. Returns unsubscribe function. */
  subscribe(
    docId: string,
    recordId: string | null,
    callback: NotifyCallback
  ): () => void {
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

  /** Notify subscribers of a change. */
  private notify(docId: string, recordId?: string): void {
    // Notify record-level subscribers
    if (recordId) {
      const key = `${docId}::${recordId}`;
      const deps = this.recDeps.get(key);
      if (deps) {
        for (const id of deps) {
          this.subs.get(id)?.();
        }
      }
    }
    // Notify doc-level subscribers
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
