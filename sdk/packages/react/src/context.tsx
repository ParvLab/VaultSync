import React, { createContext, useEffect, useState } from 'react';
import { VaultSyncClient, ClientEvent } from '@vaultsync/web';
import type { VaultSyncConfig, SyncStatus } from '@vaultsync/web';

export const VaultSyncContext = createContext<VaultSyncClient | null>(null);
export const SyncStatusContext = createContext<SyncStatus>({
  connected: false,
  pendingMutations: 0,
  lastSyncedSequence: 0,
  optimisticWrites: 0,
  pushMutationsReceived: 0,
  snapshotsApplied: 0,
  activePeers: 0,
});

export interface VaultSyncProviderProps {
  config: VaultSyncConfig;
  children: React.ReactNode;
}

export function VaultSyncProvider({ config, children }: VaultSyncProviderProps) {
  const [client, setClient] = useState<VaultSyncClient | null>(null);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>({
    connected: false,
    pendingMutations: 0,
    lastSyncedSequence: 0,
    optimisticWrites: 0,
    pushMutationsReceived: 0,
    snapshotsApplied: 0,
    activePeers: 0,
  });
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function init() {
      try {
        const c = await VaultSyncClient.create(config);
        if (!cancelled) {
          setClient(c);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err as Error);
        }
      }
    }

    init();

    return () => {
      cancelled = true;
    };
  }, [config.namespace, config.replicaId, config.mode, config.coordinatorUrl, config.authToken, config.dbName, config.storageBackend]);

  // Single subscription to sync status changes (event-driven, no polling)
  useEffect(() => {
    const c: VaultSyncClient | null = client;
    if (!c) return;
    let active = true;
    let refreshScheduled = false;

    async function refresh() {
      if (!active) return;
      try {
        const s = await client!.syncStatus();
        if (!active) return;
        setSyncStatus(prev => {
          if (
            prev.connected === s.connected &&
            prev.pendingMutations === s.pendingMutations &&
            prev.lastSyncedSequence === s.lastSyncedSequence &&
            prev.optimisticWrites === s.optimisticWrites &&
            prev.pushMutationsReceived === s.pushMutationsReceived &&
            prev.snapshotsApplied === s.snapshotsApplied &&
            prev.activePeers === s.activePeers
          ) {
            return prev;
          }
          return s;
        });
      } catch (err) {
        console.error('Failed to refresh sync status:', err);
      }
    }

    // Coalescing: multiple status-dirty events collapse into one refresh
    const scheduleRefresh = () => {
      if (refreshScheduled) return;
      refreshScheduled = true;
      queueMicrotask(async () => {
        refreshScheduled = false;
        await refresh();
      });
    };

    refresh();

    const unsub = c.onEvent((event) => {
      if (event === ClientEvent.StatusDirty) {
        scheduleRefresh();
      }
    });

    return () => {
      active = false;
      unsub();
    };
  }, [client]);

  if (error) {
    return (
      <div style={{ color: 'red', padding: '1rem', border: '1px solid red' }}>
        <h4>Failed to initialize VaultSync Client</h4>
        <pre>{error.message}</pre>
      </div>
    );
  }

  if (!client) {
    return <div>Initializing sync client...</div>;
  }

  return (
    <SyncStatusContext.Provider value={syncStatus}>
      <VaultSyncContext.Provider value={client}>
        {children}
      </VaultSyncContext.Provider>
    </SyncStatusContext.Provider>
  );
}
