'use client';

import React from 'react';
import { useParams } from 'next/navigation';
import { DocProvider } from '@/components/docs/DocProvider';
import { DocEditor } from '@/components/docs/DocEditor';

export default function DocIdPage() {
  const params = useParams();
  const docId = params.docId as string;

  return (
    <DocProvider docId={docId}>
      <DocEditor />
    </DocProvider>
  );
}
