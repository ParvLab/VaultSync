// Custom Auth using Web Crypto API (HMAC-SHA256 base64url signed tokens)
// Fully compatible with Cloudflare Workers and Edge Runtime

function base64UrlEncode(str: string): string {
  return btoa(str)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function base64UrlDecode(str: string): string {
  let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) {
    base64 += '=';
  }
  return atob(base64);
}

function bufferToBase64Url(buf: ArrayBuffer): string {
  const bytes = new Uint8Array(buf);
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return base64UrlEncode(binary);
}

function base64UrlToBuffer(str: string): Uint8Array {
  const binary = base64UrlDecode(str);
  const buf = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    buf[i] = binary.charCodeAt(i);
  }
  return buf;
}

async function getHmacKey(secret: string): Promise<CryptoKey> {
  const enc = new TextEncoder();
  return crypto.subtle.importKey(
    'raw',
    enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  );
}

export interface UserSession {
  userId: string;
  email: string;
  name: string;
  avatarColor: string;
  iat: number;
  exp: number;
}

export async function signToken(payload: Omit<UserSession, 'iat' | 'exp'>, secret: string, expiresInMs = 24 * 60 * 60 * 1000): Promise<string> {
  const header = base64UrlEncode(JSON.stringify({ alg: 'HS256', typ: 'VS' }));
  
  const iat = Date.now();
  const exp = iat + expiresInMs;
  const fullPayload: UserSession = {
    ...payload,
    iat,
    exp,
  };
  
  const body = base64UrlEncode(JSON.stringify(fullPayload));
  const dataToSign = `${header}.${body}`;
  
  const key = await getHmacKey(secret);
  const signatureBuf = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(dataToSign)
  );
  
  const signature = bufferToBase64Url(signatureBuf);
  return `${dataToSign}.${signature}`;
}

export async function verifyToken(token: string, secret: string): Promise<UserSession> {
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('Invalid token structure');
  }
  
  const [header, body, signature] = parts;
  const dataToVerify = `${header}.${body}`;
  
  const key = await getHmacKey(secret);
  const signatureBuf = base64UrlToBuffer(signature);
  
  const isValid = await crypto.subtle.verify(
    'HMAC',
    key,
    signatureBuf,
    new TextEncoder().encode(dataToVerify)
  );
  
  if (!isValid) {
    throw new Error('Invalid signature');
  }
  
  const payload: UserSession = JSON.parse(base64UrlDecode(body));
  if (Date.now() > payload.exp) {
    throw new Error('Token expired');
  }
  
  return payload;
}

// Simple password hashing using PBKDF2 (Web Crypto API)
export async function hashPassword(password: string): Promise<string> {
  const encoder = new TextEncoder();
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    encoder.encode(password),
    'PBKDF2',
    false,
    ['deriveBits', 'deriveKey']
  );
  
  const pbkdf2Params = {
    name: 'PBKDF2',
    salt,
    iterations: 100000,
    hash: 'SHA-256'
  };
  
  const derivedKey = await crypto.subtle.deriveBits(
    pbkdf2Params,
    keyMaterial,
    256
  );
  
  const saltHex = Array.from(salt).map(b => b.toString(16).padStart(2, '0')).join('');
  const hashHex = Array.from(new Uint8Array(derivedKey)).map(b => b.toString(16).padStart(2, '0')).join('');
  
  return `pbkdf2:${saltHex}:${hashHex}`;
}

export async function verifyPassword(password: string, storedHash: string): Promise<boolean> {
  const parts = storedHash.split(':');
  if (parts.length !== 3 || parts[0] !== 'pbkdf2') {
    return false;
  }
  
  const salt = new Uint8Array(
    parts[1].match(/.{1,2}/g)!.map(byte => parseInt(byte, 16))
  );
  const storedBits = parts[2];
  
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    encoder.encode(password),
    'PBKDF2',
    false,
    ['deriveBits', 'deriveKey']
  );
  
  const pbkdf2Params = {
    name: 'PBKDF2',
    salt,
    iterations: 100000,
    hash: 'SHA-256'
  };
  
  const derivedKey = await crypto.subtle.deriveBits(
    pbkdf2Params,
    keyMaterial,
    256
  );
  
  const computedBits = Array.from(new Uint8Array(derivedKey)).map(b => b.toString(16).padStart(2, '0')).join('');
  return computedBits === storedBits;
}
