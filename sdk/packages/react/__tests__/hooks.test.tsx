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

const mockDriftClientClass: any = {
  create: (jest.fn() as any).mockImplementation(() => {
    return Promise.resolve(mockClientInstance);
  }),
};

jest.unstable_mockModule('@drift/web', () => {
  return {
    DriftClient: mockDriftClientClass,
  };
});

let DriftProvider: any;
let useDriftClient: any;
let useQuery: any;
let useSyncStatus: any;
let useDriftOne: any;
let useDriftMutations: any;
let SyncIndicator: any;

describe('@drift/react hooks & provider', () => {
  const config = {
    namespace: 'test-ns',
    replicaId: 'replica-1',
  };

  beforeAll(async () => {
    const context = await import('../src/context.js');
    DriftProvider = context.DriftProvider;
    useDriftClient = context.useDriftClient;

    const query = await import('../src/useQuery.js');
    useQuery = query.useQuery;

    const status = await import('../src/useSyncStatus.js');
    useSyncStatus = status.useSyncStatus;

    const one = await import('../src/useDriftOne.js');
    useDriftOne = one.useDriftOne;

    const mutations = await import('../src/useDriftMutations.js');
    useDriftMutations = mutations.useDriftMutations;

    const indicator = await import('../src/SyncIndicator.js');
    SyncIndicator = indicator.SyncIndicator;
  });

  test('DriftProvider and useDriftClient', async () => {
    let clientRef: any = null;
    const Child = () => {
      const client = useDriftClient();
      clientRef = client;
      return <div>Client Loaded</div>;
    };

    await act(async () => {
      render(
        <DriftProvider config={config}>
          <Child />
        </DriftProvider>
      );
    });

    expect(mockDriftClientClass.create).toHaveBeenCalledWith(config);
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
        <DriftProvider config={config}>
          <QueryChild />
        </DriftProvider>
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
        <DriftProvider config={config}>
          <StatusChild />
        </DriftProvider>
      );
    });

    const el = await screen.findByText('Synced: 10');
    expect(el).not.toBeNull();
    expect(mockClientInstance.syncStatus).toHaveBeenCalled();
  });

  test('useDriftOne fetches and subscribes', async () => {
    const DriftOneChild = () => {
      const { data, loading } = useDriftOne('tasks', 'task-1');
      if (loading) return <div>Loading Hook...</div>;
      return <div>Task: {data ? data.title : 'None'}</div>;
    };

    await act(async () => {
      render(
        <DriftProvider config={config}>
          <DriftOneChild />
        </DriftProvider>
      );
    });

    const el = await screen.findByText('Task: Task 1');
    expect(el).not.toBeNull();
    expect(mockClientInstance.get).toHaveBeenCalledWith('tasks', 'task-1');
    expect(mockClientInstance.subscribe).toHaveBeenCalledWith('tasks', expect.any(Function));
  });

  test('useDriftMutations executes insert, update, delete', async () => {
    let mut: any = null;
    const MutationsChild = () => {
      const mutations = useDriftMutations('tasks');
      mut = mutations;
      return <div>Mutations Ready</div>;
    };

    await act(async () => {
      render(
        <DriftProvider config={config}>
          <MutationsChild />
        </DriftProvider>
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
        <DriftProvider config={config}>
          <SyncIndicator />
        </DriftProvider>
      );
    });

    let el = await screen.findByText('Offline');
    expect(el).toBeDefined();

    // Orange (Syncing)
    mockClientInstance.syncStatus.mockImplementationOnce(() => Promise.resolve({ connected: true, pendingMutations: 5, lastSyncedSequence: 10 }));
    
    await act(async () => {
      render(
        <DriftProvider config={config}>
          <SyncIndicator />
        </DriftProvider>
      );
    });

    let el2 = await screen.findByText('Syncing (5 pending)');
    expect(el2).toBeDefined();

    // Green (Synced)
    mockClientInstance.syncStatus.mockImplementationOnce(() => Promise.resolve({ connected: true, pendingMutations: 0, lastSyncedSequence: 10 }));
    
    await act(async () => {
      render(
        <DriftProvider config={config}>
          <SyncIndicator />
        </DriftProvider>
      );
    });

    let el3 = await screen.findByText('Synced');
    expect(el3).toBeDefined();
  });
});
