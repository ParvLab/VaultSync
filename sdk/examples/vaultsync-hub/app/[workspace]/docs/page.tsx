'use client';

import React from 'react';

export default function EmptyDocsPage() {
  return (
    <div className="flex flex-col items-center justify-center h-full text-zinc-500 bg-zinc-950 p-6 text-center select-none">
      <div className="text-6xl mb-4 opacity-40">📄</div>
      <h3 className="text-sm font-bold text-zinc-300 uppercase tracking-wider mb-2">No Document Selected</h3>
      <p className="text-xs text-zinc-550 max-w-sm">
        Select a document from the sidebar to start collaborative writing, or click "New" to create a fresh notes page.
      </p>
    </div>
  );
}
