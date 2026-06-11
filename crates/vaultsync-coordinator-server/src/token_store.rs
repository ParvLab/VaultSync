use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn initialize(&self) -> Result<(), String>;
    async fn set_token(&self, namespace: &str, token: &str) -> Result<(), String>;
    async fn validate_token(&self, namespace: &str, token: &str) -> Result<bool, String>;
}

// Hash function helper
pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

// ----------------------------------------------------
// Memory Token Store
// ----------------------------------------------------
pub struct MemoryTokenStore {
    tokens: RwLock<HashMap<String, String>>,
}

impl MemoryTokenStore {
    pub fn new() -> Self {
        Self {
            tokens: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn initialize(&self) -> Result<(), String> {
        Ok(())
    }

    async fn set_token(&self, namespace: &str, token: &str) -> Result<(), String> {
        let hash = hash_token(token);
        self.tokens
            .write()
            .await
            .insert(namespace.to_string(), hash);
        Ok(())
    }

    async fn validate_token(&self, namespace: &str, token: &str) -> Result<bool, String> {
        let hash = hash_token(token);
        let guard = self.tokens.read().await;
        if let Some(stored_hash) = guard.get(namespace) {
            Ok(stored_hash == &hash)
        } else {
            Ok(false)
        }
    }
}

// ----------------------------------------------------
// SQLite Token Store
// ----------------------------------------------------
pub struct SqliteTokenStore {
    conn: Arc<tokio::sync::Mutex<rusqlite::Connection>>,
}

impl SqliteTokenStore {
    pub fn new(path: &str) -> Result<Self, String> {
        let conn = if path == ":memory:" {
            rusqlite::Connection::open_in_memory()
        } else {
            rusqlite::Connection::open(path)
        }
        .map_err(|e| e.to_string())?;

        Ok(Self {
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl TokenStore for SqliteTokenStore {
    async fn initialize(&self) -> Result<(), String> {
        let conn = self.conn.lock().await;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS namespace_tokens (
                namespace TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn set_token(&self, namespace: &str, token: &str) -> Result<(), String> {
        let hash = hash_token(token);
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT OR REPLACE INTO namespace_tokens (namespace, token_hash) VALUES (?1, ?2)",
            rusqlite::params![namespace, hash],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn validate_token(&self, namespace: &str, token: &str) -> Result<bool, String> {
        let hash = hash_token(token);
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT token_hash FROM namespace_tokens WHERE namespace = ?1")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query(rusqlite::params![namespace])
            .map_err(|e| e.to_string())?;
        if let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let stored_hash: String = row.get(0).map_err(|e| e.to_string())?;
            Ok(stored_hash == hash)
        } else {
            Ok(false)
        }
    }
}

// ----------------------------------------------------
// Redis Token Store
// ----------------------------------------------------
pub struct RedisTokenStore {
    client: redis::Client,
}

impl RedisTokenStore {
    pub fn new(url: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|e| e.to_string())?;
        Ok(Self { client })
    }
}

#[async_trait]
impl TokenStore for RedisTokenStore {
    async fn initialize(&self) -> Result<(), String> {
        Ok(())
    }

    async fn set_token(&self, namespace: &str, token: &str) -> Result<(), String> {
        let hash = hash_token(token);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let key = format!("vaultsync:ns_token:{}", namespace);
        redis::cmd("SET")
            .arg(&key)
            .arg(&hash)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn validate_token(&self, namespace: &str, token: &str) -> Result<bool, String> {
        let hash = hash_token(token);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let key = format!("vaultsync:ns_token:{}", namespace);
        let stored_hash: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;

        match stored_hash {
            Some(stored_hash) => Ok(stored_hash == hash),
            None => Ok(false),
        }
    }
}
