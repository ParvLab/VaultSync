import React from 'react';
import ReactDOM from 'react-dom/client';
import { VaultSyncProvider } from '@vaultsync/react';
import { VaultSync } from '@vaultsync/web';
import { App } from './App.tsx';

async function bootstrap() {
  const replicaId = Math.random().toString(36).substring(2, 9);
  const vaultsync = await VaultSync.create({
    namespace: 'offline-mobile',
    replicaId,
    // Start with a mock coordinator endpoint so we can simulate online/offline
    coordinatorUrl: 'ws://127.0.0.1:8080/sync',
  });

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <VaultSyncProvider client={(vaultsync as any).client}>
        <App replicaId={replicaId} vaultsync={vaultsync} />
      </VaultSyncProvider>
    </React.StrictMode>
  );
}

bootstrap();
