'use client';

import { use, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';

interface InvitePreview {
  workspaceName: string;
  workspaceId: string;
  usesLeft: number;
  expiresAt: number;
}

export default function InvitePage({
  params,
}: {
  params: Promise<{ token: string }>;
}) {
  const router = useRouter();
  const { token } = use(params);
  const [invite, setInvite] = useState<InvitePreview | null>(null);
  const [user, setUser] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [joining, setJoining] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function loadInviteAndUser() {
      try {
        // 1. Fetch invite preview
        const inviteRes = await fetch(`/api/workspaces/placeholder/invites/${token}`);
        if (!inviteRes.ok) {
          const errData = await inviteRes.json();
          throw new Error(errData.error || 'Failed to load invite link details');
        }
        const inviteData = await inviteRes.json();
        setInvite(inviteData);

        // 2. Fetch authenticated user if logged in
        const userRes = await fetch('/api/auth/me');
        if (userRes.ok) {
          const userData = await userRes.json();
          setUser(userData.user);
        }
      } catch (err: any) {
        setError(err.message || 'Error processing invite link');
      } finally {
        setLoading(false);
      }
    }

    loadInviteAndUser();
  }, [token]);

  const handleJoin = async () => {
    if (!user) {
      // If user is not logged in, redirect them to sign up page
      router.push(`/signup?invite=${token}`);
      return;
    }

    setJoining(true);
    setError(null);

    try {
      const res = await fetch(`/api/workspaces/${invite?.workspaceId}/invites/${token}`, {
        method: 'POST',
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || 'Failed to join workspace');
      }

      // Join successful, navigate to the workspace slug
      router.push(`/${data.workspaceSlug}`);
      router.refresh();
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
      setJoining(false);
    }
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-zinc-950 text-zinc-400">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-4"></div>
        <p className="text-sm">Processing invitation...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-zinc-950 p-4 text-center">
        <div className="text-red-500 text-5xl mb-4">⚠️</div>
        <h3 className="text-xl font-bold text-white mb-2">Invalid Invite Link</h3>
        <p className="text-zinc-400 max-w-md mb-6">{error}</p>
        <Link
          href="/"
          className="px-6 py-2.5 rounded-lg text-white font-medium gradient-btn shadow-lg"
        >
          Back to Workspaces
        </Link>
      </div>
    );
  }

  return (
    <main className="min-h-screen flex items-center justify-center bg-zinc-950 p-4">
      <div className="w-full max-w-md p-8 rounded-2xl glassmorphism shadow-2xl text-center">
        <div className="text-4xl mb-4">👋</div>
        <h1 className="text-3xl font-extrabold tracking-tight text-white mb-2">
          You&apos;re Invited!
        </h1>
        <p className="text-zinc-400 text-sm mb-8">
          Join the collaborative workspace <strong className="text-indigo-400 font-semibold">{invite?.workspaceName}</strong> to start syncing tasks and documents in real-time.
        </p>

        {user ? (
          <div className="space-y-6">
            <div className="p-4 rounded-xl bg-zinc-900 border border-zinc-850 flex items-center justify-center space-x-3">
              <div
                style={{ backgroundColor: user.avatarColor }}
                className="w-8 h-8 rounded-full flex items-center justify-center font-bold text-xs text-white"
              >
                {user.name.substring(0, 1).toUpperCase()}
              </div>
              <div className="text-left">
                <p className="text-sm font-semibold text-white">{user.name}</p>
                <p className="text-xs text-zinc-500">{user.email}</p>
              </div>
            </div>

            <button
              onClick={handleJoin}
              disabled={joining}
              className="w-full py-3 rounded-lg text-white font-medium gradient-btn shadow-lg disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
            >
              {joining ? 'Joining Workspace...' : 'Accept Invitation'}
            </button>
          </div>
        ) : (
          <div className="space-y-4">
            <button
              onClick={handleJoin}
              className="w-full py-3 rounded-lg text-white font-medium gradient-btn shadow-lg cursor-pointer"
            >
              Sign Up & Join
            </button>
            <p className="text-xs text-zinc-500">
              Already have an account?{' '}
              <Link href={`/login?invite=${token}`} className="text-indigo-400 hover:text-indigo-300 font-semibold">
                Sign in first
              </Link>
            </p>
          </div>
        )}
      </div>
    </main>
  );
}
