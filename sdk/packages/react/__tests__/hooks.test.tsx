import React from 'react';
import { render, screen, act } from '@testing-library/react';
import { jest } from '@jest/globals';

const mockUnsubscribe: any = jest.fn();
const mockClientInstance: any = {
  find: (jest.fn() as any).mockImplementation(() => Promise.resolve([{ id: '1', title: 'Task 1' }])),
  get: (jest.fn() as any).mockImplementation(() => Promise.resolve({ id: '1', title: 'Task 1' })),
  insert: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
  update: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
  delete: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
  subscribe: (jest.fn() as any).mockImplementation((docId: string, cb: any) => {
    mockClientInstance._cb = cb;
    return mockUnsubscribe;
  }),
  syncStatus: (jest.fn() as any).mockImplementation(() => Promise.resolve({ connected: true, pendingMutations: 0, lastSyncedSequence: 10 })),
  shutdown: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
};

const mockVaultSyncClientClass: any = {
  create: (jest.fn() as any).mockImplementation(() => {
    return Promise.resolve(mockClientInstance);
  }),
};

jest.unstable_mockModule('@vaultsync/web', () => {
  return {
    VaultSyncClient: mockVaultSyncClientClass,
  };
});

let VaultSyncProvider: any;
let useVaultSyncClient: any;
let useQuery: any;
let useSyncStatus: any;
let useVaultSyncOne: any;
let useVaultSyncMutations: any;
let SyncIndicator: any;

describe('@vaultsync/react hooks & provider', () => {
  const config = {
    namespace: 'test-ns',
    replicaId: 'replica-1',
  };

  beforeAll(async () => {
    const context = await import('../src/context.js');
    VaultSyncProvider = context.VaultSyncProvider;
    useVaultSyncClient = context.useVaultSyncClient;

    const query = await import('../src/useQuery.js');
    useQuery = query.useQuery;

    const status = await import('../src/useSyncStatus.js');
    useSyncStatus = status.useSyncStatus;

    const one = await import('../src/useVaultSyncOne.js');
    useVaultSyncOne = one.useVaultSyncOne;

    const mutations = await import('../src/useVaultSyncMutations.js');
    useVaultSyncMutations = mutations.useVaultSyncMutations;

    const indicator = await import('../src/SyncIndicator.js');
    SyncIndicator = indicator.SyncIndicator;
  });

  test('VaultSyncProvider and useVaultSyncClient', async () => {
    let clientRef: any = null;
    const Child = () => {
      const client = useVaultSyncClient();
      clientRef = client;
      return <div>Client Loaded</div>;
    };

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <Child />
        </VaultSyncProvider>
      );
    });

    expect(mockVaultSyncClientClass.create).toHaveBeenCalledWith(config);
    expect(screen.queryByText('Client Loaded')).not.toBeNull();
    expect(clientRef).toBe(mockClientInstance);
  });

  test('useQuery fetches and subscribes', async () => {
    const QueryChild = () => {
      const { data, loading } = useQuery('tasks');
      if (loading) return <div>Loading Query...</div>;
      return <div>Data Length: {data.length}</div>;
    };

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <QueryChild />
        </VaultSyncProvider>
      );
    });

    // Wait for the find promise to resolve and render data length
    const el = await screen.findByText('Data Length: 1');
    expect(el).not.toBeNull();
    expect(mockClientInstance.find).toHaveBeenCalledWith('tasks');
    expect(mockClientInstance.subscribe).toHaveBeenCalledWith('tasks', expect.any(Function));
  });

  test('useSyncStatus returns status', async () => {
    const StatusChild = () => {
      const status = useSyncStatus();
      if (!status) return <div>No Status</div>;
      return <div>Synced: {status.lastSyncedSequence}</div>;
    };

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <StatusChild />
        </VaultSyncProvider>
      );
    });

    const el = await screen.findByText('Synced: 10');
    expect(el).not.toBeNull();
    expect(mockClientInstance.syncStatus).toHaveBeenCalled();
  });

  test('useVaultSyncOne fetches and subscribes', async () => {
    const VaultSyncOneChild = () => {
      const { data, loading } = useVaultSyncOne('tasks', 'task-1');
      if (loading) return <div>Loading Hook...</div>;
      return <div>Task: {data ? data.title : 'None'}</div>;
    };

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <VaultSyncOneChild />
        </VaultSyncProvider>
      );
    });

    const el = await screen.findByText('Task: Task 1');
    expect(el).not.toBeNull();
    expect(mockClientInstance.get).toHaveBeenCalledWith('tasks', 'task-1');
    expect(mockClientInstance.subscribe).toHaveBeenCalledWith('tasks', expect.any(Function));
  });

  test('useVaultSyncMutations executes insert, update, delete', async () => {
    let mut: any = null;
    const MutationsChild = () => {
      const mutations = useVaultSyncMutations('tasks');
      mut = mutations;
      return <div>Mutations Ready</div>;
    };

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <MutationsChild />
        </VaultSyncProvider>
      );
    });

    await act(async () => {
      await mut.insert('task-1', { title: 'New task' });
    });
    expect(mockClientInstance.insert).toHaveBeenCalledWith('tasks', 'task-1', { title: 'New task' });

    await act(async () => {
      await mut.update('task-1', { title: 'Updated task' });
    });
    expect(mockClientInstance.update).toHaveBeenCalledWith('tasks', 'task-1', { title: 'Updated task' });

    await act(async () => {
      await mut.delete('task-1');
    });
    expect(mockClientInstance.delete).toHaveBeenCalledWith('tasks', 'task-1');
  });

  test('SyncIndicator renders status based on useSyncStatus', async () => {
    // Red (Offline)
    mockClientInstance.syncStatus.mockImplementationOnce(() => Promise.resolve({ connected: false, pendingMutations: 0, lastSyncedSequence: 0 }));

    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <SyncIndicator />
        </VaultSyncProvider>
      );
    });

    let el = await screen.findByText('Offline');
    expect(el).toBeDefined();

    // Orange (Syncing)
    mockClientInstance.syncStatus.mockImplementationOnce(() => Promise.resolve({ connected: true, pendingMutations: 5, lastSyncedSequence: 10 }));
    
    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <SyncIndicator />
        </VaultSyncProvider>
      );
    });

    let el2 = await screen.findByText('Syncing (5 pending)');
    expect(el2).toBeDefined();

    // Green (Synced)
    mockClientInstance.syncStatus.mockImplementationOnce(() => Promise.resolve({ connected: true, pendingMutations: 0, lastSyncedSequence: 10 }));
    
    await act(async () => {
      render(
        <VaultSyncProvider config={config}>
          <SyncIndicator />
        </VaultSyncProvider>
      );
    });

    let el3 = await screen.findByText('Synced');
    expect(el3).toBeDefined();
  });
});
