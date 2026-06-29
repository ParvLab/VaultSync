import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { verifyPassword, signToken } from '@/lib/auth';

export async function POST(request: Request) {
  try {
    const { email, password } = await request.json();

    if (!email || !password) {
      return NextResponse.json(
        { error: 'Email and password are required' },
        { status: 400 }
      );
    }

    const sql = getDb();

    // Query user by email
    const users = await sql`
      SELECT id, email, name, avatar_color, password_hash FROM users WHERE email = ${email.toLowerCase()}
    `;

    if (users.length === 0) {
      return NextResponse.json(
        { error: 'Invalid email or password' },
        { status: 401 }
      );
    }

    const user = users[0];

    // Verify Password
    const isValid = await verifyPassword(password, user.password_hash);
    if (!isValid) {
      return NextResponse.json(
        { error: 'Invalid email or password' },
        { status: 401 }
      );
    }

    // Sign Auth Token
    const token = await signToken(
      { userId: user.id, email: user.email, name: user.name, avatarColor: user.avatar_color },
      process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars'
    );

    const response = NextResponse.json({
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        avatarColor: user.avatar_color,
      },
    });

    // Set cookie: __vs_session
    response.cookies.set('__vs_session', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24, // 24 hours in seconds
      path: '/',
    });

    return response;
  } catch (error: any) {
    console.error('Login error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
