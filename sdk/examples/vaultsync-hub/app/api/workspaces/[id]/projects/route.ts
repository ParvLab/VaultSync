import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id } = await params;
    const sql = getDb();

    // Verify workspace membership
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const projects = await sql`
      SELECT id, workspace_id, name, color, created_by, created_at 
      FROM projects 
      WHERE workspace_id = ${id}
      ORDER BY created_at DESC
    `;

    return NextResponse.json({ projects });
  } catch (error: any) {
    console.error('Fetch projects error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id } = await params;
    const { name, color = '#6366f1' } = await request.json();

    if (!name || !name.trim()) {
      return NextResponse.json({ error: 'Project name is required' }, { status: 400 });
    }

    const sql = getDb();

    // Verify workspace membership (only owner/admin can create projects)
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0 || !['owner', 'admin'].includes(membership[0].role)) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const projectId = crypto.randomUUID();
    const createdAt = Date.now();

    await sql`
      INSERT INTO projects (id, workspace_id, name, color, created_by, created_at)
      VALUES (${projectId}, ${id}, ${name}, ${color}, ${user.userId}, ${createdAt})
    `;

    return NextResponse.json({
      id: projectId,
      workspaceId: id,
      name,
      color,
      createdAt,
    });
  } catch (error: any) {
    console.error('Create project error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
