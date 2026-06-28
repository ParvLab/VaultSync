'use client';

import React from 'react';
import { useSyncStatus } from '@vaultsync/react';

export function SyncStatusBar() {
  const status = useSyncStatus();

  let dotColor = 'bg-red-500';
  let pulseColor = 'bg-red-400';
  let textClass = 'text-red-400';
  let statusText = 'Offline';

  if (status.connected) {
    if (status.pendingMutations > 0) {
      dotColor = 'bg-amber-500';
      pulseColor = 'bg-amber-400';
      textClass = 'text-amber-400';
      statusText = `Syncing (${status.pendingMutations} pending)`;
    } else {
      dotColor = 'bg-emerald-500';
      pulseColor = 'bg-emerald-400';
      textClass = 'text-emerald-400';
      statusText = 'Connected';
    }
  }

  return (
    <div className="flex items-center space-x-2 px-3 py-1.5 rounded-full bg-zinc-900/60 border border-zinc-850/50 backdrop-blur-md select-none transition-all">
      <div className="relative flex h-2 w-2">
        {status.connected && (
          <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${pulseColor} opacity-75`}></span>
        )}
        <span className={`relative inline-flex rounded-full h-2 w-2 ${dotColor}`}></span>
      </div>
      <span className={`text-[10px] font-bold uppercase tracking-wider ${textClass}`}>
        {statusText}
      </span>
      {status.activePeers > 0 && (
        <span className="text-[10px] font-medium text-zinc-550 border-l border-zinc-800 pl-2">
          👥 {status.activePeers + 1} tabs
        </span>
      )}
    </div>
  );
}
