import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string; docId: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id: workspaceId, docId } = await params;
    const sql = getDb();

    // Verify workspace membership
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${workspaceId} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const docs = await sql`
      SELECT id, workspace_id, project_id, title, icon, created_by, created_at, updated_at 
      FROM documents 
      WHERE workspace_id = ${workspaceId} AND id = ${docId}
    `;

    if (docs.length === 0) {
      return NextResponse.json({ error: 'Document not found' }, { status: 404 });
    }

    return NextResponse.json({ document: docs[0] });
  } catch (error: any) {
    console.error('Fetch document error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function PATCH(
  request: Request,
  { params }: { params: Promise<{ id: string; docId: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id: workspaceId, docId } = await params;
    const { title, icon, projectId } = await request.json();

    const sql = getDb();

    // Verify workspace membership
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${workspaceId} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const docs = await sql`
      SELECT id FROM documents 
      WHERE workspace_id = ${workspaceId} AND id = ${docId}
    `;

    if (docs.length === 0) {
      return NextResponse.json({ error: 'Document not found' }, { status: 404 });
    }

    const now = Date.now();

    // Update dynamically depending on passed body properties
    if (title !== undefined && icon !== undefined && projectId !== undefined) {
      await sql`
        UPDATE documents 
        SET title = ${title}, icon = ${icon}, project_id = ${projectId}, updated_at = ${now}
        WHERE id = ${docId}
      `;
    } else if (title !== undefined) {
      await sql`
        UPDATE documents 
        SET title = ${title}, updated_at = ${now}
        WHERE id = ${docId}
      `;
    } else if (icon !== undefined) {
      await sql`
        UPDATE documents 
        SET icon = ${icon}, updated_at = ${now}
        WHERE id = ${docId}
      `;
    }

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Update document error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ id: string; docId: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id: workspaceId, docId } = await params;
    const sql = getDb();

    // Verify workspace membership (only owner/admin can delete docs)
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${workspaceId} AND user_id = ${user.userId}
    `;

    if (membership.length === 0 || !['owner', 'admin'].includes(membership[0].role)) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    await sql`
      DELETE FROM documents 
      WHERE id = ${docId} AND workspace_id = ${workspaceId}
    `;

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Delete document error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
