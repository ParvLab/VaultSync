import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Paths that do not require authentication
  const isAuthPage = pathname.startsWith('/login') || pathname.startsWith('/signup');
  const isInvitePage = pathname.startsWith('/invite/');
  const isApiRoute = pathname.startsWith('/api/');
  const isStaticFile = pathname.includes('.') || pathname.startsWith('/_next/');

  if (isStaticFile || isApiRoute) {
    return NextResponse.next();
  }

  const token = request.cookies.get('__vs_session')?.value;

  // If user is on login/signup page and has a valid token, redirect to home
  if (isAuthPage && token) {
    try {
      // Basic structure validation (3 parts separated by dots)
      const parts = token.split('.');
      if (parts.length === 3) {
        // Base64url decode payload to check expiry
        const body = parts[1].replace(/-/g, '+').replace(/_/g, '/');
        const payload = JSON.parse(atob(body));
        if (payload.exp && Date.now() < payload.exp) {
          return NextResponse.redirect(new URL('/', request.url));
        }
      }
    } catch (e) {
      // Token is invalid, continue to auth page
    }
  }

  // If user is not authenticated and trying to access app pages, redirect to login
  if (!isAuthPage && !isInvitePage && !token) {
    return NextResponse.redirect(new URL('/login', request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - api (API routes)
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    '/((?!api|_next/static|_next/image|favicon.ico).*)',
  ],
};
