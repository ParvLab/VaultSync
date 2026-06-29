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

    // Check if id is UUID. If not, treat as slug and resolve to ID
    const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
    let workspaceId = id;
    
    if (!isUuid) {
      const workspacesBySlug = await sql`
        SELECT id FROM workspaces WHERE slug = ${id.toLowerCase()}
      `;
      if (workspacesBySlug.length === 0) {
        return NextResponse.json({ error: 'Workspace not found' }, { status: 404 });
      }
      workspaceId = workspacesBySlug[0].id;
    }

    // Check if user is a member of this workspace
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${workspaceId} AND user_id = ${user.userId}
    `;

    if (membership.length === 0) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    // Get workspace details
    const workspaces = await sql`
      SELECT id, name, slug, owner_id, created_at FROM workspaces WHERE id = ${workspaceId}
    `;

    if (workspaces.length === 0) {
      return NextResponse.json({ error: 'Workspace not found' }, { status: 404 });
    }

    // Get all members
    const members = await sql`
      SELECT u.id, u.email, u.name, u.avatar_color, wm.role, wm.joined_at
      FROM users u
      JOIN workspace_members wm ON u.id = wm.user_id
      WHERE wm.workspace_id = ${workspaceId}
      ORDER BY wm.joined_at ASC
    `;

    return NextResponse.json({
      workspace: workspaces[0],
      members,
      currentUserRole: membership[0].role,
    });
  } catch (error: any) {
    console.error('Fetch workspace details error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function PATCH(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id } = await params;
    const { name, slug } = await request.json();
    const sql = getDb();

    // Check if user is owner or admin of this workspace
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0 || !['owner', 'admin'].includes(membership[0].role)) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    if (name) {
      await sql`
        UPDATE workspaces SET name = ${name} WHERE id = ${id}
      `;
    }

    if (slug) {
      // Validate slug uniqueness
      const existing = await sql`
        SELECT id FROM workspaces WHERE slug = ${slug.toLowerCase()} AND id != ${id}
      `;
      if (existing.length > 0) {
        return NextResponse.json({ error: 'Workspace slug is already taken' }, { status: 409 });
      }
      await sql`
        UPDATE workspaces SET slug = ${slug.toLowerCase()} WHERE id = ${id}
      `;
    }

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Update workspace error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id } = await params;
    const sql = getDb();

    // Check if user is the owner
    const workspace = await sql`
      SELECT owner_id FROM workspaces WHERE id = ${id}
    `;

    if (workspace.length === 0) {
      return NextResponse.json({ error: 'Workspace not found' }, { status: 404 });
    }

    if (workspace[0].owner_id !== user.userId) {
      return NextResponse.json({ error: 'Only the owner can delete a workspace' }, { status: 403 });
    }

    // Delete workspace (cascade deletes membership, invites, projects, docs)
    await sql`
      DELETE FROM workspaces WHERE id = ${id}
    `;

    // Revoke VaultSync token
    await sql`
      DELETE FROM namespace_tokens WHERE namespace = ${`ws_${id}`}
    `;

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Delete workspace error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
