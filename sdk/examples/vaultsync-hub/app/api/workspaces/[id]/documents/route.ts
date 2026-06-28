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

    const documents = await sql`
      SELECT id, workspace_id, project_id, title, icon, created_by, created_at, updated_at 
      FROM documents 
      WHERE workspace_id = ${id}
      ORDER BY created_at DESC
    `;

    return NextResponse.json({ documents });
  } catch (error: any) {
    console.error('Fetch documents error:', error);
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
    const { title = 'Untitled', icon = '📄', projectId = null } = await request.json();

    const sql = getDb();

    // Verify workspace membership
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const docId = crypto.randomUUID();
    const now = Date.now();

    await sql`
      INSERT INTO documents (id, workspace_id, project_id, title, icon, created_by, created_at, updated_at)
      VALUES (${docId}, ${id}, ${projectId}, ${title}, ${icon}, ${user.userId}, ${now}, ${now})
    `;

    return NextResponse.json({
      id: docId,
      workspaceId: id,
      projectId,
      title,
      icon,
      createdAt: now,
      updatedAt: now,
    });
  } catch (error: any) {
    console.error('Create document error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
