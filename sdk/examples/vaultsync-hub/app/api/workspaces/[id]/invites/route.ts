import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id } = await params;
    const { role = 'member', uses_left = 10 } = await request.json();
    const sql = getDb();

    // Verify user is owner or admin of the workspace
    const membership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (membership.length === 0 || !['owner', 'admin'].includes(membership[0].role)) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const inviteId = crypto.randomUUID();
    const inviteToken = crypto.randomUUID();
    const expiresAt = Date.now() + 7 * 24 * 60 * 60 * 1000; // 7 days in ms
    const createdAt = Date.now();

    await sql`
      INSERT INTO workspace_invites (id, workspace_id, role, invited_by, token, uses_left, expires_at, created_at)
      VALUES (${inviteId}, ${id}, ${role}, ${user.userId}, ${inviteToken}, ${uses_left}, ${expiresAt}, ${createdAt})
    `;

    return NextResponse.json({
      inviteId,
      token: inviteToken,
      expiresAt,
    });
  } catch (error: any) {
    console.error('Create invite error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
