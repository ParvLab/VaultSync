import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { verifyToken } from '@/lib/auth';
import { issueNamespaceToken } from '@/lib/tokens';

function generateSlug(name: string): string {
  return name
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export async function POST(request: Request) {
  try {
    const cookieHeader = request.headers.get('cookie') || '';
    const cookies = Object.fromEntries(
      cookieHeader.split(';').map(c => c.trim().split('='))
    );
    const token = cookies['__vs_session'];

    if (!token) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const secret = process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars';
    const user = await verifyToken(token, secret);

    let name = '';
    const contentType = request.headers.get('content-type') || '';
    
    if (contentType.includes('application/json')) {
      const body = await request.json();
      name = body.name;
    } else {
      const formData = await request.formData();
      name = formData.get('name') as string;
    }

    if (!name || !name.trim()) {
      return NextResponse.json({ error: 'Workspace name is required' }, { status: 400 });
    }

    const sql = getDb();
    const workspaceId = crypto.randomUUID();
    let slug = generateSlug(name);
    
    // Ensure slug uniqueness
    const existing = await sql`SELECT id FROM workspaces WHERE slug = ${slug}`;
    if (existing.length > 0) {
      slug = `${slug}-${crypto.randomUUID().slice(0, 4)}`;
    }

    const createdAt = Date.now();

    // Execute queries sequentially
    // 1. Create workspace
    await sql`
      INSERT INTO workspaces (id, name, slug, owner_id, created_at)
      VALUES (${workspaceId}, ${name}, ${slug}, ${user.userId}, ${createdAt})
    `;
    // 2. Add owner to workspace members
    await sql`
      INSERT INTO workspace_members (workspace_id, user_id, role, joined_at)
      VALUES (${workspaceId}, ${user.userId}, 'owner', ${createdAt})
    `;

    // 3. Issue initial VaultSync namespace token for the workspace
    await issueNamespaceToken(`ws_${workspaceId}`);

    // If it's a form submit, redirect to the new workspace page
    if (!contentType.includes('application/json')) {
      return NextResponse.redirect(new URL(`/${slug}`, request.url), 303);
    }

    return NextResponse.json({ workspaceId, slug, name });
  } catch (error: any) {
    console.error('Create workspace error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}

export async function GET(request: Request) {
  try {
    const cookieHeader = request.headers.get('cookie') || '';
    const cookies = Object.fromEntries(
      cookieHeader.split(';').map(c => c.trim().split('='))
    );
    const token = cookies['__vs_session'];

    if (!token) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const secret = process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars';
    const user = await verifyToken(token, secret);

    const sql = getDb();
    const workspaces = await sql`
      SELECT w.id, w.name, w.slug, w.owner_id, wm.role, w.created_at
      FROM workspaces w
      JOIN workspace_members wm ON w.id = wm.workspace_id
      WHERE wm.user_id = ${user.userId}
      ORDER BY w.created_at DESC
    `;

    return NextResponse.json({ workspaces });
  } catch (error: any) {
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
