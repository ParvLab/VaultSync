import { neon } from '@neondatabase/serverless';

export function getDb(databaseUrl?: string) {
  const url = databaseUrl || process.env.DATABASE_URL;
  if (!url) {
    throw new Error('DATABASE_URL environment variable is not defined');
  }
  return neon(url);
}

export type DbClient = ReturnType<typeof getDb>;
