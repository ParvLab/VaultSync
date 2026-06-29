import { ProjectProvider } from '@/components/projects/ProjectProvider';
import { KanbanCanvas } from '@/components/projects/KanbanCanvas';

export default async function ProjectPage({
  params,
}: {
  params: Promise<{ workspace: string; projectId: string }>;
}) {
  const { projectId } = await params;

  return (
    <ProjectProvider projectId={projectId}>
      <KanbanCanvas />
    </ProjectProvider>
  );
}
