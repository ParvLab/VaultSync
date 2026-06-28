import { NextResponse } from 'next/server';
import { verifyToken } from '@/lib/auth';

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const cookieHeader = request.headers.get('cookie') || '';
    const cookies = Object.fromEntries(
      cookieHeader.split(';').map(c => c.trim().split('='))
    );
    
    const token = cookies['__vs_session'];

    if (!token) {
      return NextResponse.json(
        { error: 'Not authenticated' },
        { status: 401 }
      );
    }

    const secret = process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars';
    const decoded = await verifyToken(token, secret);

    return NextResponse.json({
      user: {
        id: decoded.userId,
        email: decoded.email,
        name: decoded.name,
        avatarColor: decoded.avatarColor,
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Not authenticated or session expired' },
      { status: 401 }
    );
  }
}
