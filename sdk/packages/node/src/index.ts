import { createRequire } from 'module';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';
import type { VaultSyncConfig, RecordFields, SyncStatus, SubscriptionCallback, UnsubscribeFn } from './types.js';
import { createDbProxy, DbProxy, Collection } from './db.js';

export * from './types.js';
export { DbProxy, Collection, createDbProxy };
export { VaultSync } from './vaultsync.js';
export { NodeStorage } from './storage.js';
export * from './coordinator/index.js';

const nodeRequire = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Paths to scan for the native addon
const paths = [
  path.join(__dirname, '../vaultsync_napi.node'),
  path.join(__dirname, '../../../../target/release/vaultsync_napi.node'),
  path.join(__dirname, '../../../../target/release/vaultsync_napi.dll'),
  path.join(__dirname, '../../../../target/release/libvaultsync_napi.so'),
  path.join(__dirname, '../../../../target/release/libvaultsync_napi.dylib'),
  path.join(__dirname, '../../../../target/debug/vaultsync_napi.node'),
  path.join(__dirname, '../../../../target/debug/vaultsync_napi.dll'),
  path.join(__dirname, '../../../../target/debug/libvaultsync_napi.so'),
  path.join(__dirname, '../../../../target/debug/libvaultsync_napi.dylib'),
];

let nativeModule: any = null;
for (const p of paths) {
  if (fs.existsSync(p)) {
    let targetPath = p;
    // Node requires the extension to be '.node' to load via dlopen.
    if (!p.endsWith('.node')) {
      const dir = path.dirname(p);
      const ext = path.extname(p);
      const base = path.basename(p, ext);
      const dest = path.join(dir, `${base}.node`);
      if (!fs.existsSync(dest) || fs.statSync(p).mtimeMs > fs.statSync(dest).mtimeMs) {
        try {
          fs.copyFileSync(p, dest);
        } catch (e) {
          // If we fail to copy (e.g. read-only filesystem), continue
        }
      }
      targetPath = dest;
    }
    try {
      nativeModule = nodeRequire(targetPath);
      break;
    } catch (e) {
      // Continue searching
    }
  }
}

export function isNativeAvailable(): boolean {
  return nativeModule !== null;
}

export function createNativeClient(config: VaultSyncConfig): Promise<VaultSyncClient> {
  return VaultSyncClient.create(config);
}

export class VaultSyncClient {
  // Marked as public so VaultSync facade can access it
  public inner: any;
  private _db?: any;

  private constructor(inner: any) {
    this.inner = inner;
  }

  get db(): DbProxy & Record<string, Collection<any>> {
    if (!this._db) {
      this._db = createDbProxy(this);
    }
    return this._db;
  }

  static async create(config: VaultSyncConfig): Promise<VaultSyncClient> {
    if (!nativeModule) {
      throw new Error(
        "Could not find or load vaultsync-napi native addon. " +
        "Make sure the native addon is built and available in your path."
      );
    }
    const inner = await nativeModule.createClient(
      config.namespace,
      config.replicaId,
      config.storagePath || null,
      config.coordinatorUrl || null,
      config.authToken || null
    );
    return new VaultSyncClient(inner);
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
    const statusStr = await this.inner.syncStatus();
    return JSON.parse(statusStr);
  }

  isLeader(): boolean {
    return this.inner.is_leader();
  }

  subscribe(docId: string, callback: SubscriptionCallback): UnsubscribeFn {
    const wasmCallback = (err: any, recordId: string, jsonStr: string) => {
      if (err) {
        console.error("Subscription error:", err);
        return;
      }
      callback(recordId, JSON.parse(jsonStr));
    };
    const subId = this.inner.subscribe(docId, wasmCallback);
    return () => {
      this.inner.unsubscribe(subId);
    };
  }

  async shutdown(): Promise<void> {
    await this.inner.shutdown();
  }
}
