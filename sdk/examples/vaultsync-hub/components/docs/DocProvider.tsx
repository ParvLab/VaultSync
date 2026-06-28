'use client';

import React, { createContext, useContext, useEffect, useState } from 'react';
import { VaultSyncProvider } from '@vaultsync/react';
import { useWorkspaceInfo } from '../workspace/WorkspaceProvider';

interface DocContextProps {
  id: string;
  title: string;
  icon: string;
  workspaceId: string;
  projectId: string | null;
  setTitleState: (title: string) => void;
  setIconState: (icon: string) => void;
}

const DocContext = createContext<DocContextProps | null>(null);

export function useDocInfo() {
  const context = useContext(DocContext);
  if (!context) {
    throw new Error('useDocInfo must be used within a DocProvider');
  }
  return context;
}

export function DocProvider({
  docId,
  children,
}: {
  docId: string;
  children: React.ReactNode;
}) {
  const workspace = useWorkspaceInfo();
  const [doc, setDoc] = useState<any | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);

    async function loadDocAndToken() {
      try {
        // 1. Fetch document details
        const detailsRes = await fetch(`/api/workspaces/${workspace.id}/documents/${docId}`);
        if (!detailsRes.ok) {
          throw new Error('Failed to load document details');
        }
        const detailsData = await detailsRes.json();
        
        if (!active) return;
        setDoc(detailsData.document);

        // 2. Fetch VaultSync doc namespace token
        const tokenRes = await fetch(`/api/workspaces/${workspace.id}/documents/${docId}/token`, {
          method: 'POST',
        });
        if (!tokenRes.ok) {
          throw new Error('Failed to obtain sync session token for document');
        }
        const tokenData = await tokenRes.json();
        
        if (!active) return;
        setToken(tokenData.token);
      } catch (err: any) {
        if (active) {
          setError(err.message || 'Error loading document');
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    loadDocAndToken();

    return () => {
      active = false;
    };
  }, [docId, workspace.id]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-zinc-950 text-zinc-400 p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Connecting to document sync...</p>
      </div>
    );
  }

  if (error || !doc || !token) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-zinc-950 p-8 text-center">
        <div className="text-red-500 text-5xl mb-4">⚠️</div>
        <h3 className="text-xl font-bold text-white mb-2">Document Access Denied</h3>
        <p className="text-zinc-400 max-w-md mb-6">{error || 'Verify document permissions and connection.'}</p>
      </div>
    );
  }

  const vsConfig = {
    namespace: `doc_${doc.id}`,
    replicaId: `rep_${Math.random().toString(36).substring(7)}`, // generated per session tab
    authToken: token,
    coordinatorUrl: process.env.NEXT_PUBLIC_COORDINATOR_WS_URL || 'ws://localhost:9876',
    dbName: `doc_${doc.id}_db`,
  };

  const setTitleState = (newTitle: string) => {
    setDoc((prev: any) => (prev ? { ...prev, title: newTitle } : null));
  };

  const setIconState = (newIcon: string) => {
    setDoc((prev: any) => (prev ? { ...prev, icon: newIcon } : null));
  };

  return (
    <DocContext.Provider
      value={{
        id: doc.id,
        title: doc.title,
        icon: doc.icon,
        workspaceId: doc.workspace_id,
        projectId: doc.project_id,
        setTitleState,
        setIconState,
      }}
    >
      <VaultSyncProvider config={vsConfig as any}>
        {children as any}
      </VaultSyncProvider>
    </DocContext.Provider>
  );
}
