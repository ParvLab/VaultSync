CREATE TABLE IF NOT EXISTS mutations (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  replica_id TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  record_id TEXT NOT NULL,
  encrypted_blob BLOB NOT NULL,
  timestamp INTEGER NOT NULL,
  sequence INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS replicas (
  replica_id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  public_key BLOB NOT NULL,
  schema_version INTEGER NOT NULL,
  last_seen INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_versions (
  namespace TEXT PRIMARY KEY,
  version INTEGER NOT NULL
);
