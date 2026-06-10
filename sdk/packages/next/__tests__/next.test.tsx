
import React from 'react';
import { render, screen, act } from '@testing-library/react';
import { jest } from '@jest/globals';

jest.unstable_mockModule('next/server', () => {
  class MockNextResponse {
    body: any;
    status: number;
    headers: any;
    constructor(body: any, init?: any) {
      this.body = body;
      this.status = init?.status ?? 200;
      this.headers = init?.headers ?? {};
    }
    static json(body: any, init?: any) {
      return new MockNextResponse(JSON.stringify(body), init);
    }
    async text() {
      return typeof this.body === 'string' ? this.body : JSON.stringify(this.body);
    }
    async json() {
      return typeof this.body === 'string' ? JSON.parse(this.body) : this.body;
    }
  }

  return {
    NextRequest: class {},
    NextResponse: MockNextResponse,
  };
});

let resolveFind: any = null;
const mockUnsubscribe = jest.fn();
const mockClientInstance: any = {
  find: (jest.fn() as any).mockImplementation(() => new Promise((resolve) => {
    resolveFind = resolve;
  })),
  subscribe: (jest.fn() as any).mockReturnValue(mockUnsubscribe),
};

const mockDriftClientClass: any = {
  create: (jest.fn() as any).mockImplementation(() => Promise.resolve(mockClientInstance)),
};

jest.unstable_mockModule('@drift/web', () => {
  return {
    DriftClient: mockDriftClientClass,
  };
});

jest.unstable_mockModule('@drift/react', () => {
  return {
    DriftProvider: ({ children }: any) => <div>{children}</div>,
    useDriftClient: () => mockClientInstance,
  };
});

const { DriftHydrationProvider, useNextQuery } = await import('../src/index.js');
const { createCoordinatorHandler } = await import('../src/server.js');

describe('@drift/next', () => {
  test('DriftHydrationProvider and useNextQuery hydration', async () => {
    const config = { namespace: 'test', replicaId: '1' };
    const initialData = {
      tasks: [{ id: '1', title: 'Task 1' }],
    };

    const TestComponent = () => {
      const { data, loading } = useNextQuery('tasks');
      return (
        <div>
          {loading ? 'Loading...' : `Data Length: ${data.length}, First Title: ${data[0]?.title}`}
        </div>
      );
    };

    act(() => {
      render(
        <DriftHydrationProvider config={config} initialData={initialData}>
          <TestComponent />
        </DriftHydrationProvider>
      );
    });

    // Hydrated server data should render immediately without loading state
    expect(screen.queryByText('Data Length: 1, First Title: Task 1')).not.toBeNull();

    // Now resolve the find promise to trigger client update
    await act(async () => {
      resolveFind([{ id: '2', title: 'Task 2' }]);
    });

    // After mount, the query fetches live client data and triggers state change
    const el = await screen.findByText('Data Length: 1, First Title: Task 2');
    expect(el).not.toBeNull();
    expect(mockClientInstance.find).toHaveBeenCalledWith('tasks');
  });

  test('createCoordinatorHandler Route Handler', async () => {
    const mockCoordinator: any = {
      push: (jest.fn() as any).mockImplementation(() => Promise.resolve([10n, 11n])),
      pull: (jest.fn() as any).mockImplementation(() => Promise.resolve([{ id: '1', sequence: 5n }])),
      register: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
      heartbeat: (jest.fn() as any).mockImplementation(() => Promise.resolve(undefined)),
      schema_version: (jest.fn() as any).mockImplementation(() => Promise.resolve(42n)),
    };

    const handler = createCoordinatorHandler(mockCoordinator);

    // Mock NextRequest and URL parsing
    const req = {
      url: 'http://localhost:3000/api/drift/namespace/test-ns/pull?after=0&limit=10',
      method: 'GET',
      headers: {
        get: () => '',
      },
    } as any;

    const pullResp: any = await handler.GET(req);
    expect(pullResp.status).toBe(200);
    const bodyStr = await pullResp.text();
    expect(bodyStr).toContain('"sequence":"5"');
    expect(mockCoordinator.pull).toHaveBeenCalledWith('test-ns', 0n, 10);
  });
});
