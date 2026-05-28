import React from 'react';
import { render, screen, act } from '@testing-library/react';
import { jest } from '@jest/globals';

const mockUnsubscribe: any = jest.fn();
const mockClientInstance: any = {
  find: (jest.fn() as any).mockImplementation(() => Promise.resolve([{ id: '1', title: 'Task 1' }])),
  subscribe: (jest.fn() as any).mockReturnValue(mockUnsubscribe),
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
});
