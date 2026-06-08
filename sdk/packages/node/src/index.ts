import { createRequire } from 'module';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';
import type { DriftConfig, RecordFields, SyncStatus, SubscriptionCallback, UnsubscribeFn } from './types.js';

export * from './types.js';

const nodeRequire = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Paths to scan for the native addon
const paths = [
  path.join(__dirname, '../drift_napi.node'),
  path.join(__dirname, '../../../../target/release/drift_napi.node'),
  path.join(__dirname, '../../../../target/release/drift_napi.dll'),
  path.join(__dirname, '../../../../target/release/libdrift_napi.so'),
  path.join(__dirname, '../../../../target/release/libdrift_napi.dylib'),
  path.join(__dirname, '../../../../target/debug/drift_napi.node'),
  path.join(__dirname, '../../../../target/debug/drift_napi.dll'),
  path.join(__dirname, '../../../../target/debug/libdrift_napi.so'),
  path.join(__dirname, '../../../../target/debug/libdrift_napi.dylib'),
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

if (!nativeModule) {
  throw new Error("Could not find or load drift-napi native addon");
}

export class DriftClient {
  private inner: any;

  private constructor(inner: any) {
    this.inner = inner;
  }

  static async create(config: DriftConfig): Promise<DriftClient> {
    const inner = await nativeModule.createClient(
      config.namespace,
      config.replicaId,
      config.storagePath || null,
      config.coordinatorUrl || null,
      config.authToken || null
    );
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
    const statusStr = await this.inner.syncStatus();
    return JSON.parse(statusStr);
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

export class InMemoryCoordinator {
  private nextSeq = 1n;
  private ops: Map<string, Array<{
    id: string;
    namespace: string;
    sequence: string;
    encryptedBlob: any;
    timestamp: number;
    schemaVersion: number;
    keyVersion: number;
  }>> = new Map();
  private replicas: Map<string, any[]> = new Map();

  async push(namespace: string, mutations: any[]): Promise<string[]> {
    if (!this.ops.has(namespace)) {
      this.ops.set(namespace, []);
    }
    const list = this.ops.get(namespace)!;
    const seqs: string[] = [];
    for (const m of mutations) {
      const existing = list.find(x => x.id === m.id);
      if (existing) {
        seqs.push(existing.sequence);
        continue;
      }
      const seq = this.nextSeq++;
      const seqStr = seq.toString();
      list.push({
        id: m.id,
        namespace,
        sequence: seqStr,
        encryptedBlob: m.encryptedBlob || m.encrypted_blob,
        timestamp: m.timestamp,
        schemaVersion: m.schemaVersion || m.schema_version || 0,
        keyVersion: m.keyVersion || m.key_version || 0,
      });
      seqs.push(seqStr);
    }
    return seqs;
  }

  async pull(namespace: string, after: string | bigint, limit: number): Promise<any[]> {
    const list = this.ops.get(namespace) || [];
    const afterBig = BigInt(after);
    const filtered = list.filter(x => BigInt(x.sequence) > afterBig);
    const sliced = filtered.slice(0, limit);
    return sliced.map(x => ({
      id: x.id,
      namespace: x.namespace,
      sequence: BigInt(x.sequence),
      encrypted_blob: x.encryptedBlob,
      timestamp: x.timestamp,
      schema_version: x.schemaVersion,
      key_version: x.keyVersion,
    }));
  }

  async register(namespace: string, info: any): Promise<void> {
    if (!this.replicas.has(namespace)) {
      this.replicas.set(namespace, []);
    }
    this.replicas.get(namespace)!.push(info);
  }

  async heartbeat(namespace: string, replicaId: string): Promise<void> {
    // No-op
  }

  async schema_version(namespace: string): Promise<bigint> {
    const list = this.replicas.get(namespace) || [];
    let max = 0n;
    for (const r of list) {
      const v = BigInt(r.schemaVersion || r.schema_version || 0);
      if (v > max) max = v;
    }
    return max;
  }
}
