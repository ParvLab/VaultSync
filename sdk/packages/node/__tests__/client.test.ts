import { jest } from '@jest/globals';

// Define the mock NAPI module
const mockNapi = {
  createClient: jest.fn().mockImplementation(async (namespace, replicaId, storagePath, coordinatorUrl, authToken) => {
    const db = new Map<string, string>();
    const subscriptions = new Map<string, Function>();
    let subIdCounter = 0;

    return {
      insert: jest.fn().mockImplementation(async (docId: string, recordId: string, fieldsJson: string) => {
        db.set(`${docId}:${recordId}`, fieldsJson);
        const sub = subscriptions.get(docId);
        if (sub) {
          sub(null, recordId, fieldsJson);
        }
      }),
      update: jest.fn().mockImplementation(async (docId: string, recordId: string, fieldsJson: string) => {
        const existing = db.get(`${docId}:${recordId}`);
        const parsedExisting = existing ? JSON.parse(existing) : {};
        const parsedNew = JSON.parse(fieldsJson);
        const merged = { ...parsedExisting, ...parsedNew };
        const mergedJson = JSON.stringify(merged);
        db.set(`${docId}:${recordId}`, mergedJson);
        const sub = subscriptions.get(docId);
        if (sub) {
          sub(null, recordId, mergedJson);
        }
      }),
      delete: jest.fn().mockImplementation(async (docId: string, recordId: string) => {
        db.delete(`${docId}:${recordId}`);
      }),
      get: jest.fn().mockImplementation(async (docId: string, recordId: string) => {
        return db.get(`${docId}:${recordId}`) || null;
      }),
      find: jest.fn().mockImplementation(async (docId: string) => {
        const results: string[] = [];
        for (const [key, value] of db.entries()) {
          if (key.startsWith(`${docId}:`)) {
            results.push(value);
          }
        }
        return results;
      }),
      syncStatus: jest.fn().mockImplementation(async () => {
        return JSON.stringify({
          connected: true,
          pendingMutations: 0,
          lastSyncedSequence: 42,
        });
      }),
      subscribe: jest.fn().mockImplementation((docId: string, callback: Function) => {
        subscriptions.set(docId, callback);
        return ++subIdCounter;
      }),
      unsubscribe: jest.fn().mockImplementation((subId: number) => {
        // No-op for mock
      }),
      shutdown: jest.fn().mockImplementation(async () => {}),
    };
  }),
};

// Mock the modules using Jest
jest.mock('fs', () => {
  const actual = jest.requireActual('fs') as any;
  return {
    ...actual,
    existsSync: (p: string) => {
      if (p.includes('vaultsync_napi') || p.includes('libvaultsync_napi')) return true;
      return actual.existsSync(p);
    },
  };
});

jest.mock('module', () => {
  return {
    createRequire: () => (p: string) => {
      if (p.includes('vaultsync_napi') || p.includes('libvaultsync_napi')) {
        return mockNapi;
      }
      throw new Error(`Cannot find module '${p}'`);
    },
  };
});

// Import VaultSyncClient & InMemoryCoordinator AFTER mocking
const { VaultSyncClient, InMemoryCoordinator } = await import('../src/index.js');

describe('VaultSyncClient (Node.js)', () => {
  let client: VaultSyncClient;

  beforeEach(async () => {
    client = await VaultSyncClient.create({
      namespace: 'test_namespace',
      replicaId: 'node_replica_1',
      coordinatorUrl: 'memory://test',
    });
  });

  afterEach(async () => {
    await client.shutdown();
  });

  test('insert and get', async () => {
    const docId = 'doc1';
    const recordId = 'rec1';
    const fields = { name: 'VaultSync Node', version: 1, active: true };

    await client.insert(docId, recordId, fields);

    const doc = await client.get(docId, recordId);
    expect(doc).toMatchObject(fields);
  });

  test('update and find', async () => {
    const docId = 'doc2';
    const recordId = 'rec2';
    
    await client.insert(docId, recordId, { val: 'initial' });
    await client.update(docId, recordId, { val: 'updated', flag: true });

    const doc = await client.get(docId, recordId);
    expect(doc).toMatchObject({ val: 'updated', flag: true });

    const results = await client.find(docId);
    expect(results.length).toBe(1);
    expect(results[0]).toMatchObject({ val: 'updated', flag: true });
  });

  test('delete', async () => {
    const docId = 'doc_delete';
    const recordId = 'rec_delete';
    
    await client.insert(docId, recordId, { val: 'todelete' });
    let doc = await client.get(docId, recordId);
    expect(doc).not.toBeNull();

    await client.delete(docId, recordId);
    doc = await client.get(docId, recordId);
    expect(doc).toBeNull();
  });

  test('subscribe to changes', async () => {
    const docId = 'doc_sub';
    const recordId = 'rec_sub';
    const updates: any[] = [];

    const unsubscribe = client.subscribe(docId, (rid, fields) => {
      updates.push({ rid, fields });
    });

    await client.insert(docId, recordId, { status: 'inserted' });
    
    // Give it a tiny bit of time for callback to fire
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(updates.length).toBe(1);
    expect(updates[0]).toMatchObject({ rid: recordId, fields: { status: 'inserted' } });

    unsubscribe();
  });

  test('sync status API', async () => {
    const status = await client.syncStatus();
    expect(typeof status.connected).toBe('boolean');
    expect(typeof status.pendingMutations).toBe('number');
    expect(typeof status.lastSyncedSequence).toBe('number');
  });
});

describe('InMemoryCoordinator', () => {
  test('register and get schema version', async () => {
    const coord = new InMemoryCoordinator();
    const namespace = 'ns_coord';
    
    await coord.register(namespace, { replica_id: 'rep1', schema_version: 3 });
    await coord.register(namespace, { replica_id: 'rep2', schema_version: 5 });

    const maxVer = await coord.schema_version(namespace);
    expect(maxVer).toBe(5n);
  });

  test('push and pull mutations', async () => {
    const coord = new InMemoryCoordinator();
    const namespace = 'ns_coord_ops';

    const mutations = [
      { id: 'm1', timestamp: 100, encryptedBlob: 'blob1' },
      { id: 'm2', timestamp: 200, encryptedBlob: 'blob2' },
    ];

    const seqs = await coord.push(namespace, mutations);
    expect(seqs.length).toBe(2);
    expect(seqs[0]).toBe('1');
    expect(seqs[1]).toBe('2');

    // Pull after seq 0
    const pulled0 = await coord.pull(namespace, 0n, 10);
    expect(pulled0.length).toBe(2);
    expect(pulled0[0].id).toBe('m1');
    expect(pulled0[1].id).toBe('m2');

    // Pull after seq 1
    const pulled1 = await coord.pull(namespace, 1n, 10);
    expect(pulled1.length).toBe(1);
    expect(pulled1[0].id).toBe('m2');
  });
});
