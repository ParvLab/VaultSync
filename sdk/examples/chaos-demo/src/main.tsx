import React from 'react';
import ReactDOM from 'react-dom/client';
import { DriftProvider } from '@drift/react';
import { Drift } from '@drift/web';
import { App } from './App.tsx';

async function bootstrap() {
  const replicaId = Math.random().toString(36).substring(2, 9);
  const drift = await Drift.create({
    namespace: 'chaos-demo',
    replicaId,
    coordinatorUrl: 'ws://127.0.0.1:8080/sync',
  });

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <DriftProvider client={(drift as any).client}>
        <App replicaId={replicaId} drift={drift} />
      </DriftProvider>
    </React.StrictMode>
  );
}

bootstrap();
