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

export interface PostgresCoordinatorConfig {
  url: string;
  authToken?: string;
}

export class PostgresCoordinator {
  readonly url: string;
  readonly authToken?: string;

  constructor(config: PostgresCoordinatorConfig) {
    this.url = config.url;
    this.authToken = config.authToken;
  }
}

export interface RedisCoordinatorConfig {
  url: string;
  authToken?: string;
}

export class RedisCoordinator {
  readonly url: string;
  readonly authToken?: string;

  constructor(config: RedisCoordinatorConfig) {
    this.url = config.url;
    this.authToken = config.authToken;
  }
}
