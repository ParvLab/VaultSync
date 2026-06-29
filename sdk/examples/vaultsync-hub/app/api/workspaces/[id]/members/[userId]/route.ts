import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { getAuthenticatedUser, authRequiredResponse } from '@/lib/auth-helpers';

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ id: string; userId: string }> }
) {
  try {
    const user = await getAuthenticatedUser(request);
    if (!user) return authRequiredResponse();

    const { id, userId } = await params;
    const sql = getDb();

    // Check membership role of the requester
    const requesterMembership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${user.userId}
    `;

    if (requesterMembership.length === 0 || !['owner', 'admin'].includes(requesterMembership[0].role)) {
      return NextResponse.json({ error: 'Forbidden' }, { status: 403 });
    }

    const requesterRole = requesterMembership[0].role;

    // Check membership role of the target user to be removed
    const targetMembership = await sql`
      SELECT role FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${userId}
    `;

    if (targetMembership.length === 0) {
      return NextResponse.json({ error: 'Member not found' }, { status: 404 });
    }

    const targetRole = targetMembership[0].role;

    // Owner cannot be removed
    if (targetRole === 'owner') {
      return NextResponse.json({ error: 'The owner of the workspace cannot be removed' }, { status: 400 });
    }

    // Admin cannot remove another admin (only owner can)
    if (targetRole === 'admin' && requesterRole !== 'owner') {
      return NextResponse.json({ error: 'Only the owner can remove an administrator' }, { status: 403 });
    }

    // Delete membership
    await sql`
      DELETE FROM workspace_members 
      WHERE workspace_id = ${id} AND user_id = ${userId}
    `;

    // Revoke token for the workspace namespace to terminate active sync
    // This forces reconnect which will check membership and fail.
    const namespace = `ws_${id}`;
    await sql`
      DELETE FROM namespace_tokens WHERE namespace = ${namespace}
    `;

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('Remove member error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
