import { jest } from '@jest/globals';

const mockWasmClientInstance: any = {
  insert: (jest.fn() as any).mockResolvedValue(undefined),
  update: (jest.fn() as any).mockResolvedValue(undefined),
  delete: (jest.fn() as any).mockResolvedValue(undefined),
  get: (jest.fn() as any).mockResolvedValue('{"foo":"bar"}'),
  find: (jest.fn() as any).mockResolvedValue(['{"foo":"bar"}']),
  subscribe: (jest.fn() as any).mockReturnValue('sub-123'),
  unsubscribe: (jest.fn() as any).mockResolvedValue(undefined),
  sync_status: (jest.fn() as any).mockResolvedValue('{"connected":true,"pendingMutations":0,"lastSyncedSequence":1}'),
  shutdown: (jest.fn() as any).mockResolvedValue(undefined),
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
  test('create client and run operations', async () => {
    const { DriftClient } = await import('../src/index.js');
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
    expect(record).toEqual({ foo: 'bar' });
    expect(mockWasmClientInstance.get).toHaveBeenCalledWith('doc1', 'rec1');

    // Test find
    const list = await client.find('doc1');
    expect(list).toEqual([{ foo: 'bar' }]);
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
});
