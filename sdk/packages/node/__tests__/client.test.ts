import { DriftClient } from '../src/index.js';

describe('DriftClient (Node.js)', () => {
  let client: DriftClient;

  beforeAll(async () => {
    client = await DriftClient.create({
      namespace: 'test_namespace',
      replicaId: 'node_replica_1',
      coordinatorUrl: 'memory://test',
    });
  });

  afterAll(async () => {
    await client.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 200));
  });

  test('insert and get', async () => {
    const docId = 'doc1';
    const recordId = 'rec1';
    const fields = { name: 'Drift Node', version: 1, active: true };

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

  test('subscribe to changes', async () => {
    const docId = 'doc_sub';
    const recordId = 'rec_sub';
    const updates: any[] = [];

    const unsubscribe = client.subscribe(docId, (rid, fields) => {
      updates.push({ rid, fields });
    });

    await client.insert(docId, recordId, { status: 'inserted' });
    
    // Give it a tiny bit of time for callback to fire
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(updates.length).toBe(1);
    expect(updates[0]).toMatchObject({ rid: recordId, fields: { status: 'inserted' } });

    unsubscribe();
  });
});
