import { jest } from '@jest/globals';

const mockWasmClientInstance: any = {
  insert: (jest.fn() as any).mockResolvedValue(undefined),
  update: (jest.fn() as any).mockResolvedValue(undefined),
  delete: (jest.fn() as any).mockResolvedValue(undefined),
  get: (jest.fn() as any).mockResolvedValue('{"record_id":"rec1","foo":"bar"}'),
  find: (jest.fn() as any).mockResolvedValue(['{"record_id":"rec1","foo":"bar"}']),
  subscribe: (jest.fn() as any).mockImplementation((docId: string, cb: any) => {
    mockWasmClientInstance._cb = cb;
    return 'sub-123';
  }),
  unsubscribe: (jest.fn() as any).mockResolvedValue(undefined),
  sync_status: (jest.fn() as any).mockResolvedValue('{"connected":true,"pendingMutations":0,"lastSyncedSequence":1}'),
  shutdown: (jest.fn() as any).mockResolvedValue(undefined),
  define_schema: (jest.fn() as any).mockResolvedValue(undefined),
  rotate_keys: (jest.fn() as any).mockResolvedValue(2),
  list_key_versions: (jest.fn() as any).mockResolvedValue('[{"version":1,"createdAt":1000,"isActive":false},{"version":2,"createdAt":2000,"isActive":true}]'),
  active_key_version: (jest.fn() as any).mockReturnValue(2),
  prune_key_versions: (jest.fn() as any).mockReturnValue(undefined),
};

const mockWasmClient: any = {
  new: (jest.fn() as any).mockImplementation(() => mockWasmClientInstance),
  new_with_coordinator: (jest.fn() as any).mockImplementation(() => mockWasmClientInstance),
};

jest.unstable_mockModule('../wasm/drift_wasm.js', () => {
  return {
    default: (jest.fn() as any).mockResolvedValue(true),
    WasmDriftClient: mockWasmClient,
  };
});

describe('DriftClient (Web)', () => {
  let DriftClient: any;
  let defineSchema: any;

  beforeAll(async () => {
    const mod = await import('../src/index.js');
    DriftClient = mod.DriftClient;
    defineSchema = mod.defineSchema;
  });

  test('create client and run operations', async () => {
    const client = await DriftClient.create({
      namespace: 'test-web',
      replicaId: 'replica-web-1',
    });

    expect(client).toBeDefined();

    // Test insert
    await client.insert('doc1', 'rec1', { name: 'web test' });
    expect(mockWasmClientInstance.insert).toHaveBeenCalledWith('doc1', 'rec1', '{"name":"web test"}');

    // Test get
    const record = await client.get('doc1', 'rec1');
    expect(record).toEqual({ record_id: 'rec1', foo: 'bar' });
    expect(mockWasmClientInstance.get).toHaveBeenCalledWith('doc1', 'rec1');

    // Test find
    const list = await client.find('doc1');
    expect(list).toEqual([{ record_id: 'rec1', foo: 'bar' }]);
    expect(mockWasmClientInstance.find).toHaveBeenCalledWith('doc1');

    // Test syncStatus
    const status = await client.syncStatus();
    expect(status).toEqual({ connected: true, pendingMutations: 0, lastSyncedSequence: 1 });

    // Test subscribe
    const cb = jest.fn();
    const unsubscribe = client.subscribe('doc1', cb);
    expect(mockWasmClientInstance.subscribe).toHaveBeenCalled();
    
    unsubscribe();
    expect(mockWasmClientInstance.unsubscribe).toHaveBeenCalledWith('sub-123');

    // Test shutdown
    await client.shutdown();
    expect(mockWasmClientInstance.shutdown).toHaveBeenCalled();
  });

  test('Collection API', async () => {
    const client = await DriftClient.create({
      namespace: 'test-web',
      replicaId: 'replica-web-1',
    });

    const collection = client.db.todos;
    expect(collection).toBeDefined();

    // Test insert
    await collection.insert({ id: 'todo-1', text: 'Test' });
    expect(mockWasmClientInstance.insert).toHaveBeenCalledWith('todos', 'todo-1', '{"text":"Test"}');

    // Test update
    await collection.update('todo-1', { text: 'Updated' });
    expect(mockWasmClientInstance.update).toHaveBeenCalledWith('todos', 'todo-1', '{"text":"Updated"}');

    // Test delete
    await collection.delete('todo-1');
    expect(mockWasmClientInstance.delete).toHaveBeenCalledWith('todos', 'todo-1');

    // Test findById
    mockWasmClientInstance.get.mockResolvedValueOnce('{"record_id":"todo-1","text":"Test"}');
    const todo = await collection.findById('todo-1');
    expect(todo).toEqual({ id: 'todo-1', text: 'Test' });

    // Test findById returning null
    mockWasmClientInstance.get.mockResolvedValueOnce(null);
    const todoNull = await collection.findById('todo-2');
    expect(todoNull).toBeNull();

    // Test findAll
    mockWasmClientInstance.find.mockResolvedValueOnce([
      '{"record_id":"todo-1","text":"Test 1"}',
      '{"record_id":"todo-2","text":"Test 2"}'
    ]);
    const all = await collection.findAll();
    expect(all).toEqual([
      { id: 'todo-1', text: 'Test 1' },
      { id: 'todo-2', text: 'Test 2' }
    ]);

    // Test findAll with filter
    mockWasmClientInstance.find.mockResolvedValueOnce([
      '{"record_id":"todo-1","text":"Test 1"}',
      '{"record_id":"todo-2","text":"Test 2"}'
    ]);
    const filtered = await collection.findAll((item: any) => item.text === 'Test 2');
    expect(filtered).toEqual([{ id: 'todo-2', text: 'Test 2' }]);

    // Test subscribe
    const subCb = jest.fn();
    mockWasmClientInstance.find.mockResolvedValue([
      '{"record_id":"todo-1","text":"Test 1"}'
    ]);
    const unsub = collection.subscribe(subCb);
    expect(mockWasmClientInstance.subscribe).toHaveBeenCalled();

    // Manually trigger mock callback
    await mockWasmClientInstance._cb('todo-1', '{"text":"Test 1"}');
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(subCb).toHaveBeenCalledWith([{ id: 'todo-1', text: 'Test 1' }]);

    unsub();

    // Test subscribeOne
    const subOneCb = jest.fn();
    mockWasmClientInstance.get.mockResolvedValue('{"record_id":"todo-1","text":"Updated Test 1"}');
    const unsubOne = collection.subscribeOne('todo-1', subOneCb);

    await mockWasmClientInstance._cb('todo-1', '{"text":"Updated Test 1"}');
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(subOneCb).toHaveBeenCalledWith({ id: 'todo-1', text: 'Updated Test 1' });

    unsubOne();
  });

  test('defineSchema validation and mapping', async () => {
    const client = await DriftClient.create({
      namespace: 'test-web',
      replicaId: 'replica-web-1',
    });

    await defineSchema(client, 'todos', {
      version: 1,
      fields: {
        id: { type: 'string', crdtType: 'lww', primaryKey: true },
        title: { type: 'string', crdtType: 'text' },
        completed: { type: 'boolean', crdtType: 'lww' },
        count: { type: 'number', crdtType: 'counter' },
      }
    });

    expect(mockWasmClientInstance.define_schema).toHaveBeenCalled();
    const [docId, schemaJson] = mockWasmClientInstance.define_schema.mock.calls[0];
    expect(docId).toBe('todos');
    
    const parsed = JSON.parse(schemaJson);
    expect(parsed.version).toBe(1);
    expect(parsed.doc_id).toBe('todos');
    expect(parsed.fields).toEqual([
      { name: 'id', value_type: 'String', crdt_type: 'LwwRegister', primary_key: true, indexed: false, sync: true },
      { name: 'title', value_type: 'String', crdt_type: 'Text', primary_key: false, indexed: false, sync: true },
      { name: 'completed', value_type: 'Boolean', crdt_type: 'LwwRegister', primary_key: false, indexed: false, sync: true },
      { name: 'count', value_type: 'Number', crdt_type: 'PnCounter', primary_key: false, indexed: false, sync: true },
    ]);
  });

  test('KeyManager API', async () => {
    const client = await DriftClient.create({
      namespace: 'test-web',
      replicaId: 'replica-web-1',
    });

    const keyManager = client.keys;
    expect(keyManager).toBeDefined();

    // Test activeVersion
    const activeVer = keyManager.activeVersion();
    expect(activeVer).toBe(2);
    expect(mockWasmClientInstance.active_key_version).toHaveBeenCalled();

    // Test listVersions
    const versions = await keyManager.listVersions();
    expect(versions).toEqual([
      { version: 1, createdAt: 1000, isActive: false },
      { version: 2, createdAt: 2000, isActive: true }
    ]);
    expect(mockWasmClientInstance.list_key_versions).toHaveBeenCalled();

    // Test rotate
    const onRotated = jest.fn();
    const keyManagerWithHook = new (await import('../src/keys.js')).KeyManager(mockWasmClientInstance, { onRotated });
    const newVer = await keyManagerWithHook.rotate();
    expect(newVer).toBe(2);
    expect(mockWasmClientInstance.rotate_keys).toHaveBeenCalled();
    expect(onRotated).toHaveBeenCalledWith(2, 1);

    // Test pruneOldVersions
    keyManager.pruneOldVersions(3);
    expect(mockWasmClientInstance.prune_key_versions).toHaveBeenCalledWith(3);
  });
});
