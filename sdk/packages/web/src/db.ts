import type { VaultSyncClient } from './index.js';
import type { RecordFields } from './types.js';

export class Collection<T extends { id: string }> {
  constructor(
    private name: string,
    private client: VaultSyncClient
  ) {}

  async insert(record: T): Promise<void> {
    const { id, ...fields } = record;
    await this.client.insert(this.name, id, fields as unknown as RecordFields);
  }

  async update(id: string, fields: Partial<Omit<T, 'id'>>): Promise<void> {
    await this.client.update(this.name, id, fields as unknown as RecordFields);
  }

  async delete(id: string): Promise<void> {
    await this.client.delete(this.name, id);
  }

  async findById(id: string): Promise<T | null> {
    const fields = await this.client.get(this.name, id);
    if (!fields) return null;
    const { record_id, doc_id, schema_version, ...rest } = fields;
    return { id: (record_id as string) || id, ...rest } as unknown as T;
  }

  async findAll(filter?: (item: T) => boolean): Promise<T[]> {
    const records = await this.client.find(this.name);
    const results: T[] = [];
    for (const fields of records) {
      const { record_id, doc_id, schema_version, ...rest } = fields;
      const id = (record_id as string);
      if (!id) continue;
      const item = { id, ...rest } as unknown as T;
      if (!filter || filter(item)) {
        results.push(item);
      }
    }
    return results;
  }

  subscribe(callback: (results: T[]) => void, filter?: (item: T) => boolean): () => void {
    // When a document in this collection changes, we fetch the updated state and call the callback
    const handle = this.client.subscribe(this.name, async (recordId, fields) => {
      const all = await this.findAll(filter);
      callback(all);
    });
    return handle;
  }

  subscribeOne(id: string, callback: (record: T | null) => void): () => void {
    const handle = this.client.subscribe(this.name, async (recordId, fields) => {
      if (recordId === id) {
        const item = await this.findById(id);
        callback(item);
      }
    });
    return handle;
  }
}

export class DbProxy {
  private collections: Record<string, Collection<any>> = {};

  constructor(private client: VaultSyncClient) {}

  getCollection<T extends { id: string }>(name: string): Collection<T> {
    if (!this.collections[name]) {
      this.collections[name] = new Collection<T>(name, this.client);
    }
    return this.collections[name];
  }
}

export function createDbProxy(client: VaultSyncClient): DbProxy & Record<string, Collection<any>> {
  const db = new DbProxy(client);
  return new Proxy(db, {
    get(target, prop) {
      if (typeof prop === 'string' && !(prop in target)) {
        return target.getCollection(prop);
      }
      return Reflect.get(target, prop);
    }
  }) as any;
}
