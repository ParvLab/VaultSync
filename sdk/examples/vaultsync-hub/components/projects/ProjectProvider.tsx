'use client';

import React, { createContext, useContext, useEffect, useState } from 'react';
import { VaultSyncProvider } from '@vaultsync/react';
import { useWorkspaceInfo } from '../workspace/WorkspaceProvider';

interface ProjectContextProps {
  id: string;
  name: string;
  color: string;
  workspaceId: string;
}

const ProjectContext = createContext<ProjectContextProps | null>(null);

export function useProjectInfo() {
  const context = useContext(ProjectContext);
  if (!context) {
    throw new Error('useProjectInfo must be used within a ProjectProvider');
  }
  return context;
}

export function ProjectProvider({
  projectId,
  children,
}: {
  projectId: string;
  children: React.ReactNode;
}) {
  const workspace = useWorkspaceInfo();
  const [project, setProject] = useState<any | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);

    async function loadProjectAndToken() {
      try {
        // 1. Fetch project details
        const detailsRes = await fetch(`/api/workspaces/${workspace.id}/projects/${projectId}`);
        if (!detailsRes.ok) {
          throw new Error('Failed to load project details');
        }
        const detailsData = await detailsRes.json();
        
        if (!active) return;
        setProject(detailsData.project);

        // 2. Fetch VaultSync project namespace token
        const tokenRes = await fetch(`/api/workspaces/${workspace.id}/projects/${projectId}/token`, {
          method: 'POST',
        });
        if (!tokenRes.ok) {
          throw new Error('Failed to obtain sync session token for project');
        }
        const tokenData = await tokenRes.json();
        
        if (!active) return;
        setToken(tokenData.token);
      } catch (err: any) {
        if (active) {
          setError(err.message || 'Error loading project');
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    loadProjectAndToken();

    return () => {
      active = false;
    };
  }, [projectId, workspace.id]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-zinc-950 text-zinc-400 p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Connecting to project sync...</p>
      </div>
    );
  }

  if (error || !project || !token) {
    return (
      <div className="flex flex-col items-center justify-center h-full bg-zinc-950 p-8 text-center">
        <div className="text-red-500 text-5xl mb-4">⚠️</div>
        <h3 className="text-xl font-bold text-white mb-2">Project Access Denied</h3>
        <p className="text-zinc-400 max-w-md mb-6">{error || 'Verify project membership and connection.'}</p>
      </div>
    );
  }

  const vsConfig = {
    namespace: `proj_${project.id}`,
    replicaId: `rep_${Math.random().toString(36).substring(7)}`, // generated per session tab
    authToken: token,
    coordinatorUrl: process.env.NEXT_PUBLIC_COORDINATOR_WS_URL || 'ws://localhost:9876',
    dbName: `proj_${project.id}_db`,
  };

  return (
    <ProjectContext.Provider
      value={{
        id: project.id,
        name: project.name,
        color: project.color,
        workspaceId: workspace.id,
      }}
    >
      <VaultSyncProvider config={vsConfig as any}>
        {children as any}
      </VaultSyncProvider>
    </ProjectContext.Provider>
  );
}
