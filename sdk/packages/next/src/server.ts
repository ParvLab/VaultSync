import { NextRequest, NextResponse } from 'next/server';

export interface NextCoordinatorOptions {
  validateToken?: (namespace: string, token: string) => Promise<boolean>;
}

export function createCoordinatorHandler(coordinator: any, options?: NextCoordinatorOptions) {
  function getParams(req: NextRequest) {
    const url = new URL(req.url);
    const segments = url.pathname.split('/').filter(Boolean);
    const nsIdx = segments.indexOf('namespace');
    if (nsIdx !== -1 && segments.length > nsIdx + 2) {
      return {
        namespace: segments[nsIdx + 1],
        method: segments[nsIdx + 2],
      };
    }
    const namespace = url.searchParams.get('namespace') || 'default';
    const method = segments[segments.length - 1];
    return { namespace, method };
  }

  async function handleRequest(req: NextRequest) {
    const { namespace, method } = getParams(req);

    if (options?.validateToken) {
      const authHeader = req.headers.get('Authorization') || '';
      const token = authHeader.replace(/^Bearer\s+/i, '');
      const valid = await options.validateToken(namespace, token);
      if (!valid) {
        return new NextResponse(JSON.stringify({ error: 'Unauthorized' }), { status: 401 });
      }
    }

    try {
      if (req.method === 'POST') {
        if (method === 'push') {
          const body = await req.json();
          const sequences = await coordinator.push(namespace, body);
          return NextResponse.json(sequences);
        } else if (method === 'register') {
          const body = await req.json();
          await coordinator.register(namespace, body);
          return NextResponse.json({ status: 'ok' });
        } else if (method === 'heartbeat') {
          const url = new URL(req.url);
          const replicaId = url.searchParams.get('replica_id') || '';
          await coordinator.heartbeat(namespace, replicaId);
          return NextResponse.json({ status: 'ok' });
        }
      } else if (req.method === 'GET') {
        if (method === 'pull') {
          const url = new URL(req.url);
          const after = BigInt(url.searchParams.get('after') || '0');
          const limit = parseInt(url.searchParams.get('limit') || '100', 10);
          const mutations = await coordinator.pull(namespace, after, limit);
          
          // Stringify BigInt values safely for JSON payload
          const serialized = JSON.stringify(mutations, (_key, value) => 
            typeof value === 'bigint' ? value.toString() : value
          );
          return new NextResponse(serialized, {
            headers: { 'Content-Type': 'application/json' },
          });
        } else if (method === 'schema_version') {
          const version = await coordinator.schema_version(namespace);
          return NextResponse.json(version.toString());
        }
      }
      return new NextResponse(JSON.stringify({ error: 'Method not allowed' }), { status: 405 });
    } catch (e: any) {
      return new NextResponse(JSON.stringify({ error: e.message || 'Internal server error' }), { status: 500 });
    }
  }

  return {
    GET: handleRequest,
    POST: handleRequest,
  };
}

export async function queryServerDatabase(dbPath: string, namespace: string, docId: string) {
  const { VaultSyncClient } = await import('@vaultsync/node');
  const client = await VaultSyncClient.create({
    namespace,
    replicaId: 'nextjs-server',
    storagePath: dbPath,
  });
  try {
    return await client.find(docId);
  } finally {
    await client.shutdown();
  }
}
