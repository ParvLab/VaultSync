'use client';

import React from 'react';
import { DocSidebar } from '@/components/docs/DocSidebar';
import { useParams } from 'next/navigation';

export default function DocsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const params = useParams();
  const workspaceSlug = params.workspace as string;

  return (
    <div className="flex h-screen flex-grow min-h-0 bg-zinc-950 overflow-hidden">
      {/* Document Sidebar */}
      <DocSidebar workspaceSlug={workspaceSlug} />
      
      {/* Editor Content Area */}
      <div className="flex-grow overflow-y-auto min-w-0 bg-zinc-950/20">
        {children}
      </div>
    </div>
  );
}
