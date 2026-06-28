import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string; token: string }> }
) {
  try {
    const { token } = await params;
    const sql = getDb();

    // Query invite details
    const invites = await sql`
      SELECT wi.id, wi.workspace_id, wi.uses_left, wi.expires_at, w.name as workspace_name
      FROM workspace_invites wi
      JOIN workspaces w ON wi.workspace_id = w.id
      WHERE wi.token = ${token}
    `;

    if (invites.length === 0) {
      return NextResponse.json({ error: 'Invalid invite link' }, { status: 404 });
    }

    const invite = invites[0];

    if (invite.uses_left <= 0) {
      return NextResponse.json({ error: 'This invite link has run out of uses' }, { status: 410 });
    }

    if (Date.now() > Number(invite.expires_at)) {
      return NextResponse.json({ error: 'This invite link has expired' }, { status: 410 });
    }

    return NextResponse.json({
      workspaceName: invite.workspace_name,
      workspaceId: invite.workspace_id,
      usesLeft: invite.uses_left,
      expiresAt: invite.expires_at,
    });
  } catch (error: any) {
    console.error('Fetch invite error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string; token: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { token } = await params;
    const sql = getDb();

    // Verify invite validity
    const invites = await sql`
      SELECT id, workspace_id, role, uses_left, expires_at 
      FROM workspace_invites 
      WHERE token = ${token}
    `;

    if (invites.length === 0) {
      return NextResponse.json({ error: 'Invalid invite link' }, { status: 404 });
    }

    const invite = invites[0];

    if (invite.uses_left <= 0) {
      return NextResponse.json({ error: 'Invite link has no uses left' }, { status: 410 });
    }

    if (Date.now() > Number(invite.expires_at)) {
      return NextResponse.json({ error: 'Invite link has expired' }, { status: 410 });
    }

    // Check if user is already a member
    const existingMembership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${invite.workspace_id} AND user_id = ${user.userId}
    `;

    if (existingMembership.length > 0) {
      return NextResponse.json({ 
        message: 'You are already a member of this workspace',
        workspaceId: invite.workspace_id 
      });
    }

    const joinedAt = Date.now();

    // Insert new member and decrement uses
    await sql`
      INSERT INTO workspace_members (workspace_id, user_id, role, joined_at)
      VALUES (${invite.workspace_id}, ${user.userId}, ${invite.role}, ${joinedAt})
    `;
    await sql`
      UPDATE workspace_invites 
      SET uses_left = uses_left - 1 
      WHERE id = ${invite.id}
    `;

    // Get workspace slug to redirect client
    const workspaces = await sql`
      SELECT slug FROM workspaces WHERE id = ${invite.workspace_id}
    `;

    return NextResponse.json({
      success: true,
      workspaceSlug: workspaces[0]?.slug,
      workspaceId: invite.workspace_id,
    });
  } catch (error: any) {
    console.error('Accept invite error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
