import { WorkspaceProvider } from '@/components/workspace/WorkspaceProvider';
import { WorkspaceSwitcher } from '@/components/workspace/WorkspaceSwitcher';
import Link from 'next/link';

export default async function WorkspaceLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ workspace: string }>;
}) {
  const { workspace } = await params;

  return (
    <WorkspaceProvider slug={workspace}>
      <div className="flex h-screen bg-zinc-950 overflow-hidden text-zinc-300">
        {/* Sidebar */}
        <aside className="w-64 border-r border-zinc-900 bg-zinc-950 flex flex-col justify-between flex-shrink-0">
          <div className="flex flex-col flex-1 min-h-0">
            {/* Header / Workspace Switcher */}
            <div className="p-4 border-b border-zinc-900">
              <WorkspaceSwitcher />
            </div>

            {/* Navigation links */}
            <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
              <Link
                href={`/${workspace}`}
                className="flex items-center space-x-3 px-3 py-2.5 rounded-lg hover:bg-zinc-900 text-sm font-semibold text-zinc-300 hover:text-white transition-all"
              >
                <span>🏠</span>
                <span>Dashboard</span>
              </Link>

              <Link
                href={`/${workspace}/projects`}
                className="flex items-center space-x-3 px-3 py-2.5 rounded-lg hover:bg-zinc-900 text-sm font-semibold text-zinc-300 hover:text-white transition-all"
              >
                <span>📋</span>
                <span>Kanban Projects</span>
              </Link>

              <Link
                href={`/${workspace}/docs`}
                className="flex items-center space-x-3 px-3 py-2.5 rounded-lg hover:bg-zinc-900 text-sm font-semibold text-zinc-300 hover:text-white transition-all"
              >
                <span>📄</span>
                <span>Documents</span>
              </Link>

              <Link
                href={`/${workspace}/settings`}
                className="flex items-center space-x-3 px-3 py-2.5 rounded-lg hover:bg-zinc-900 text-sm font-semibold text-zinc-300 hover:text-white transition-all"
              >
                <span>⚙️</span>
                <span>Settings</span>
              </Link>
            </nav>
          </div>

          {/* Sidebar Footer with Sign Out */}
          <div className="p-4 border-t border-zinc-900 bg-zinc-950/80">
            <form action="/api/auth/logout" method="POST">
              <button
                type="submit"
                className="w-full flex items-center justify-center space-x-2 py-2 px-4 border border-zinc-800 hover:bg-zinc-900 text-zinc-400 hover:text-red-400 rounded-lg text-sm font-semibold transition-all cursor-pointer"
              >
                <span>🚪</span>
                <span>Sign Out</span>
              </button>
            </form>
          </div>
        </aside>

        {/* Content Area */}
        <main className="flex-1 flex flex-col min-w-0 bg-zinc-950 overflow-y-auto">
          {children}
        </main>
      </div>
    </WorkspaceProvider>
  );
}
