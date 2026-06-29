import { VaultSyncClient } from '@vaultsync/web';

const clientPromises = new Map<string, Promise<VaultSyncClient>>();

export async function getVaultSyncClient(
  namespace: string,
  token?: string,
  replicaId?: string
): Promise<VaultSyncClient> {
  if (typeof window === 'undefined') {
    throw new Error('VaultSyncClient can only be initialized in the browser');
  }

  const existing = clientPromises.get(namespace);
  if (existing) {
    return existing;
  }

  const generatedReplicaId = replicaId || `rep_${crypto.randomUUID().slice(0, 8)}`;
  const coordinatorUrl = process.env.NEXT_PUBLIC_COORDINATOR_WS_URL || 'ws://localhost:9876';

  const config = {
    namespace,
    replicaId: generatedReplicaId,
    coordinatorUrl,
    authToken: token || null,
    dbName: `${namespace}_db`,
  };

  console.log(`[VaultSync Hub] Initializing client for namespace=${namespace}, replicaId=${generatedReplicaId}`);
  
  const promise = VaultSyncClient.create(config as any);
  clientPromises.set(namespace, promise);
  
  return promise;
}

export async function releaseVaultSyncClient(namespace: string): Promise<void> {
  const promise = clientPromises.get(namespace);
  if (promise) {
    clientPromises.delete(namespace);
    try {
      const client = await promise;
      await client.shutdown();
      console.log(`[VaultSync Hub] Shutdown client for namespace=${namespace}`);
    } catch (err) {
      console.error(`[VaultSync Hub] Error shutting down client for namespace=${namespace}:`, err);
    }
  }
}
