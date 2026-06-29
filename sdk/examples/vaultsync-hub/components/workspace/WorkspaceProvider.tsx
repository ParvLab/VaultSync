'use client';

import React, { createContext, useContext, useEffect, useState } from 'react';
import { VaultSyncProvider } from '@vaultsync/react';

interface WorkspaceContextProps {
  id: string;
  name: string;
  slug: string;
  role: string;
  members: any[];
}

const WorkspaceContext = createContext<WorkspaceContextProps | null>(null);

export function useWorkspaceInfo() {
  const context = useContext(WorkspaceContext);
  if (!context) {
    throw new Error('useWorkspaceInfo must be used within a WorkspaceInfoProvider');
  }
  return context;
}

export function WorkspaceProvider({
  slug,
  children,
}: {
  slug: string;
  children: React.ReactNode;
}) {
  const [workspace, setWorkspace] = useState<any | null>(null);
  const [members, setMembers] = useState<any[]>([]);
  const [role, setRole] = useState<string>('member');
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);

    async function loadWorkspaceAndToken() {
      try {
        // 1. Fetch workspace details (resolves slug to details + members)
        const detailsRes = await fetch(`/api/workspaces/${slug}`);
        if (!detailsRes.ok) {
          throw new Error('Failed to load workspace details');
        }
        const detailsData = await detailsRes.json();
        
        if (!active) return;
        setWorkspace(detailsData.workspace);
        setMembers(detailsData.members);
        setRole(detailsData.currentUserRole);

        // 2. Fetch VaultSync workspace namespace token
        const tokenRes = await fetch(`/api/workspaces/${detailsData.workspace.id}/token`, {
          method: 'POST',
        });
        if (!tokenRes.ok) {
          throw new Error('Failed to obtain sync session token');
        }
        const tokenData = await tokenRes.json();
        
        if (!active) return;
        setToken(tokenData.token);
      } catch (err: any) {
        if (active) {
          setError(err.message || 'Error loading workspace');
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    loadWorkspaceAndToken();

    return () => {
      active = false;
    };
  }, [slug]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-zinc-950 text-zinc-400">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Connecting to workspace sync...</p>
      </div>
    );
  }

  if (error || !workspace || !token) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-zinc-950 p-4 text-center">
        <div className="text-red-500 text-5xl mb-4">⚠️</div>
        <h3 className="text-xl font-bold text-white mb-2">Workspace Access Denied</h3>
        <p className="text-zinc-400 max-w-md mb-6">
          {error || 'Ensure you are a member of this workspace and have active internet connection.'}
        </p>
        <button
          onClick={() => window.location.href = '/'}
          className="px-6 py-2.5 rounded-lg text-white font-medium gradient-btn shadow-lg cursor-pointer"
        >
          Back to Workspaces
        </button>
      </div>
    );
  }

  const vsConfig = {
    namespace: `ws_${workspace.id}`,
    replicaId: `rep_${Math.random().toString(36).substring(7)}`, // generated per session tab
    authToken: token,
    coordinatorUrl: process.env.NEXT_PUBLIC_COORDINATOR_WS_URL || 'ws://localhost:9876',
    dbName: `ws_${workspace.id}_db`,
  };

  return (
    <WorkspaceContext.Provider
      value={{
        id: workspace.id,
        name: workspace.name,
        slug: workspace.slug,
        role,
        members,
      }}
    >
      <VaultSyncProvider config={vsConfig as any}>
        {children as any}
      </VaultSyncProvider>
    </WorkspaceContext.Provider>
  );
}
