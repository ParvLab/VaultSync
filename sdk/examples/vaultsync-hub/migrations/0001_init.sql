-- Users Table
CREATE TABLE IF NOT EXISTS users (
  id            TEXT PRIMARY KEY,
  email         TEXT UNIQUE NOT NULL,
  name          TEXT NOT NULL,
  avatar_color  TEXT NOT NULL DEFAULT '#6366f1',
  password_hash TEXT NOT NULL,
  created_at    BIGINT NOT NULL
);

-- Workspaces Table
CREATE TABLE IF NOT EXISTS workspaces (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  slug       TEXT UNIQUE NOT NULL,
  owner_id   TEXT NOT NULL REFERENCES users(id),
  created_at BIGINT NOT NULL
);

-- Workspace Members Table
CREATE TABLE IF NOT EXISTS workspace_members (
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role         TEXT NOT NULL DEFAULT 'member',
  joined_at    BIGINT NOT NULL,
  PRIMARY KEY (workspace_id, user_id)
);

-- Invite links (link-based, reusable up to N times)
CREATE TABLE IF NOT EXISTS workspace_invites (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  role         TEXT NOT NULL DEFAULT 'member',
  invited_by   TEXT NOT NULL REFERENCES users(id),
  token        TEXT UNIQUE NOT NULL,
  uses_left    INT NOT NULL DEFAULT 10,
  expires_at   BIGINT NOT NULL,
  created_at   BIGINT NOT NULL
);

-- Project metadata (content lives in VaultSync OPFS)
CREATE TABLE IF NOT EXISTS projects (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,
  color        TEXT NOT NULL DEFAULT '#6366f1',
  created_by   TEXT NOT NULL REFERENCES users(id),
  created_at   BIGINT NOT NULL
);

-- Document metadata (content lives in VaultSync OPFS)
CREATE TABLE IF NOT EXISTS documents (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  project_id   TEXT REFERENCES projects(id) ON DELETE SET NULL,
  title        TEXT NOT NULL DEFAULT 'Untitled',
  icon         TEXT NOT NULL DEFAULT '📄',
  created_by   TEXT NOT NULL REFERENCES users(id),
  created_at   BIGINT NOT NULL,
  updated_at   BIGINT NOT NULL
);

-- VaultSync namespace tokens (shared with coordinator-cf via database/API bindings)
-- Coordinator reads this table to validate tokens
CREATE TABLE IF NOT EXISTS namespace_tokens (
  namespace   TEXT PRIMARY KEY,
  token_hash  TEXT NOT NULL,
  created_at  BIGINT NOT NULL,
  expires_at  BIGINT  -- null = never expires
);
