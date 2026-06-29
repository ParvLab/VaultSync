import { NextResponse } from 'next/server';
import { getDb } from '@/lib/db';
import { hashPassword, signToken } from '@/lib/auth';

const AVATAR_COLORS = [
  '#f43f5e', // rose
  '#ec4899', // pink
  '#d946ef', // fuchsia
  '#a855f7', // purple
  '#6366f1', // indigo
  '#3b82f6', // blue
  '#0ea5e9', // sky
  '#06b6d4', // cyan
  '#14b8a6', // teal
  '#10b981', // emerald
  '#22c55e', // green
  '#84cc16', // lime
  '#eab308', // yellow
  '#f97316', // orange
];

export async function POST(request: Request) {
  try {
    const { email, name, password } = await request.json();

    if (!email || !name || !password) {
      return NextResponse.json(
        { error: 'Email, name, and password are required' },
        { status: 400 }
      );
    }

    const sql = getDb();

    // Check if email already exists
    const existingUsers = await sql`
      SELECT id FROM users WHERE email = ${email.toLowerCase()}
    `;

    if (existingUsers.length > 0) {
      return NextResponse.json(
        { error: 'User with this email already exists' },
        { status: 409 }
      );
    }

    const hashedPassword = await hashPassword(password);
    const userId = crypto.randomUUID();
    const avatarColor = AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)];
    const createdAt = Date.now();

    // Create user in DB
    await sql`
      INSERT INTO users (id, email, name, avatar_color, password_hash, created_at)
      VALUES (${userId}, ${email.toLowerCase()}, ${name}, ${avatarColor}, ${hashedPassword}, ${createdAt})
    `;

    // Sign Auth Token
    const token = await signToken(
      { userId, email: email.toLowerCase(), name, avatarColor },
      process.env.AUTH_SECRET || 'super-secret-paseto-signing-key-for-local-dev-32-chars'
    );

    const response = NextResponse.json(
      {
        user: {
          id: userId,
          email: email.toLowerCase(),
          name,
          avatarColor,
        },
      },
      { status: 201 }
    );

    // Set cookie: __vs_session (HTTP-only, Secure, SameSite=Lax)
    response.cookies.set('__vs_session', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24, // 24 hours in seconds
      path: '/',
    });

    return response;
  } catch (error: any) {
    console.error('Signup error:', error);
    return NextResponse.json(
      { error: error.message || 'Internal Server Error' },
      { status: 500 }
    );
  }
}
