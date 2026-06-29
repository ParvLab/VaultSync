'use client';

import React from 'react';
import { useSyncStatus } from '@vaultsync/react';

export function OfflineBanner() {
  const status = useSyncStatus();

  if (status.connected) return null;

  return (
    <div className="bg-amber-600/10 border-b border-amber-500/20 px-4 py-2.5 flex items-center justify-between text-xs text-amber-300 backdrop-blur-md z-40 select-none animate-in slide-in-from-top duration-300">
      <div className="flex items-center space-x-2.5 mx-auto font-medium">
        <span className="text-sm">⚠️</span>
        <span>
          <strong>Working Offline.</strong> Your edits are saved instantly to local OPFS cache and will sync when reconnected.
        </span>
      </div>
    </div>
  );
}
