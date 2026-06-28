import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';
import { issueNamespaceToken } from '@/lib/tokens';

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string; pid: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id, pid } = await params;
    const sql = getDb();

    // Verify workspace membership
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    // Verify project belongs to workspace
    const projects = await sql`
      SELECT id FROM projects WHERE id = ${pid} AND workspace_id = ${id}
    `;

    if (projects.length === 0) {
      return NextResponse.json({ error: 'Project not found' }, { status: 404 });
    }

    const namespace = `proj_${pid}`;
    const token = await issueNamespaceToken(namespace);
    const coordinatorUrl = process.env.NEXT_PUBLIC_COORDINATOR_WS_URL || 'ws://localhost:9876';

    return NextResponse.json({
      token,
      namespace,
      coordinatorUrl,
    });
  } catch (error: any) {
    console.error('Project token error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
