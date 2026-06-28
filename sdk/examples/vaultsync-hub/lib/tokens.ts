import { getDb } from './db';

async function sha256(token: string): Promise<string> {
  const msgUint8 = new TextEncoder().encode(token);
  const hashBuffer = await crypto.subtle.digest('SHA-256', msgUint8);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

export async function issueNamespaceToken(
  namespace: string,
  expiresInMs = 24 * 60 * 60 * 1000
): Promise<string> {
  const token = crypto.randomUUID();
  const tokenHash = await sha256(token);
  const createdAt = Date.now();
  const expiresAt = createdAt + expiresInMs;

  const sql = getDb();
  await sql`
    INSERT INTO namespace_tokens (namespace, token_hash, created_at, expires_at)
    VALUES (${namespace}, ${tokenHash}, ${createdAt}, ${expiresAt})
    ON CONFLICT (namespace) DO UPDATE 
    SET token_hash = EXCLUDED.token_hash, 
        created_at = EXCLUDED.created_at, 
        expires_at = EXCLUDED.expires_at
  `;

  return token;
}

export async function revokeNamespaceToken(namespace: string): Promise<void> {
  const sql = getDb();
  await sql`
    DELETE FROM namespace_tokens WHERE namespace = ${namespace}
  `;
}
