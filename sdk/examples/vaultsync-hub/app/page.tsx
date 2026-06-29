import { cookies } from 'next/headers';
import { redirect } from 'next/navigation';
import { getDb } from '@/lib/db';
import { verifyToken } from '@/lib/auth';

export default async function HomePage() {
  const cookieStore = await cookies();
  const token = cookieStore.get('__vs_session')?.value;

  if (!token) {
    redirect('/login');
  }

  let user;
  try {
    const secret = process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars';
    user = await verifyToken(token, secret);
  } catch (e) {
    redirect('/login');
  }

  const sql = getDb();

  // Query workspaces the user belongs to
  const members = await sql`
    SELECT workspace_id FROM workspace_members WHERE user_id = ${user.userId}
  `;

  if (members.length > 0) {
    // Redirect to the first workspace
    const workspaces = await sql`
      SELECT slug FROM workspaces WHERE id = ${members[0].workspace_id}
    `;
    if (workspaces.length > 0) {
      redirect(`/${workspaces[0].slug}`);
    }
  }

  // If no workspaces, render the workspace creation prompt
  return (
    <main className="min-h-screen flex flex-col items-center justify-center bg-zinc-950 p-4">
      <div className="w-full max-w-md p-8 rounded-2xl glassmorphism shadow-2xl text-center">
        <h1 className="text-3xl font-extrabold tracking-tight gradient-text mb-4">
          Welcome, {user.name}!
        </h1>
        <p className="text-zinc-400 mb-8">
          You are not member of any workspace yet. Create a new workspace to get started.
        </p>

        <form action="/api/workspaces" method="POST" className="space-y-4">
          <div>
            <input
              type="text"
              name="name"
              required
              className="w-full px-4 py-3 bg-zinc-900 border border-zinc-800 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all"
              placeholder="Workspace Name (e.g. Acme Corp)"
            />
          </div>

          <button
            type="submit"
            className="w-full py-3 rounded-lg text-white font-medium gradient-btn shadow-lg cursor-pointer"
          >
            Create Workspace
          </button>
        </form>

        <div className="mt-8 pt-6 border-t border-zinc-900">
          <form action="/api/auth/logout" method="POST">
            <button
              type="submit"
              className="text-zinc-500 hover:text-zinc-400 text-sm transition-colors cursor-pointer"
            >
              Sign out
            </button>
          </form>
        </div>
      </div>
    </main>
  );
}
