use rusqlite::Connection;

pub fn initialize(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
        
        CREATE TABLE IF NOT EXISTS mutations (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT UNIQUE NOT NULL,
            namespace TEXT NOT NULL,
            replica_id TEXT NOT NULL,
            doc_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            encrypted_blob BLOB NOT NULL,
            timestamp INTEGER NOT NULL
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

        CREATE TABLE IF NOT EXISTS snapshots (
            namespace TEXT NOT NULL,
            doc_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            bytes BLOB NOT NULL,
            checksum INTEGER NOT NULL,
            PRIMARY KEY (namespace, doc_id, record_id)
        );"
    )?;
    Ok(())
}
