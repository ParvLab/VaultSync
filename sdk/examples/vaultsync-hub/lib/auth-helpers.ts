import { NextResponse } from 'next/server';
import { verifyToken, UserSession } from './auth';

export async function getAuthenticatedUser(request: Request): Promise<UserSession | null> {
  try {
    const cookieHeader = request.headers.get('cookie') || '';
    const cookies = Object.fromEntries(
      cookieHeader.split(';').map(c => c.trim().split('='))
    );
    const token = cookies['__vs_session'];

    if (!token) { 
      return null;
    }

    const secret = process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars';
    return await verifyToken(token, secret);
  } catch (error) {
    return null;
  }
}

export function authRequiredResponse() {
  return NextResponse.json({ error: 'Authentication required' }, { status: 401 });
}
