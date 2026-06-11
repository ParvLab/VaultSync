use crate::error::VaultSyncError;
use crate::storage::traits::Storage;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex, OnceLock};

pub struct MigrationDefinition {
    pub version: String,
    pub checksum: String,
    pub apply_fn: Box<dyn Fn() -> Result<(), VaultSyncError> + Send + Sync>,
    pub rollback_fn: Option<Box<dyn Fn() -> Result<(), VaultSyncError> + Send + Sync>>,
}

impl MigrationDefinition {
    pub fn new(
        version: &str,
        apply_fn: Box<dyn Fn() -> Result<(), VaultSyncError> + Send + Sync>,
    ) -> Self {
        let checksum = compute_checksum(version);
        Self {
            version: version.to_string(),
            checksum,
            apply_fn,
            rollback_fn: None,
        }
    }

    pub fn with_rollback(
        mut self,
        rollback_fn: Box<dyn Fn() -> Result<(), VaultSyncError> + Send + Sync>,
    ) -> Self {
        self.rollback_fn = Some(rollback_fn);
        self
    }

    pub fn apply(&self) -> Result<(), VaultSyncError> {
        (self.apply_fn)()
    }

    pub fn rollback(&self) -> Result<(), VaultSyncError> {
        match &self.rollback_fn {
            Some(f) => f(),
            None => Err(VaultSyncError::Schema("no rollback defined".into())),
        }
    }

    pub fn verify_checksum(&self) -> bool {
        compute_checksum(&self.version) == self.checksum
    }
}

fn compute_checksum(version: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(version.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn apply_migration(migration: &MigrationDefinition) -> Result<(), VaultSyncError> {
    migration.apply()
}

pub fn verify_checksum(code: &str, expected: &str) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(code.as_bytes());
    hex::encode(hasher.finalize()) == expected
}

pub struct MigrationRegistry {
    pub migrations: Vec<Arc<MigrationDefinition>>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    pub fn register(&mut self, migration: MigrationDefinition) {
        self.migrations.push(Arc::new(migration));
        self.migrations.sort_by(|a, b| a.version.cmp(&b.version));
    }
}

static GLOBAL_MIGRATION_REGISTRY: OnceLock<Mutex<MigrationRegistry>> = OnceLock::new();

pub fn register_global_migration(migration: MigrationDefinition) {
    let registry = GLOBAL_MIGRATION_REGISTRY.get_or_init(|| Mutex::new(MigrationRegistry::new()));
    registry.lock().unwrap().register(migration);
}

pub fn get_global_migrations() -> Vec<Arc<MigrationDefinition>> {
    let registry = GLOBAL_MIGRATION_REGISTRY.get_or_init(|| Mutex::new(MigrationRegistry::new()));
    registry.lock().unwrap().migrations.clone()
}

pub struct MigrationRunner {
    storage: Arc<dyn Storage>,
    migrations: Vec<Arc<MigrationDefinition>>,
}

impl MigrationRunner {
    pub fn new(storage: Arc<dyn Storage>, migrations: Vec<Arc<MigrationDefinition>>) -> Self {
        Self {
            storage,
            migrations,
        }
    }

    /// Run all pending migrations in version order.
    pub async fn run_pending(&self) -> Result<(), VaultSyncError> {
        let applied = self.storage.read_migrations().await?;

        for migration in &self.migrations {
            let already_applied = applied.iter().any(|m| m.version == migration.version);
            if !already_applied {
                tracing::info!(version = %migration.version, "Running pending schema migration");
                if let Err(e) = migration.apply() {
                    let _ = migration.rollback();
                    return Err(e);
                }

                let epoch = crate::time_utils::system_time_now_secs();
                let record = crate::storage::traits::MigrationRecord {
                    version: migration.version.clone(),
                    applied_at: epoch,
                    checksum: migration.checksum.clone(),
                };
                self.storage.write_migration(&record).await?;
            }
        }
        Ok(())
    }

    /// Validate checksums of already-applied migrations.
    pub async fn validate_applied(&self) -> Result<(), VaultSyncError> {
        let applied = self.storage.read_migrations().await?;

        for record in applied {
            if let Some(registered) = self.migrations.iter().find(|m| m.version == record.version) {
                if record.checksum != registered.checksum {
                    return Err(VaultSyncError::Schema(format!(
                        "Migration checksum mismatch for version {}: expected {}, found {}",
                        record.version, registered.checksum, record.checksum
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn current_version(&self) -> u64 {
        if let Ok(applied) = self.storage.read_migrations().await {
            applied
                .iter()
                .filter_map(|m| {
                    let digits: String = m
                        .version
                        .chars()
                        .skip_while(|c| !c.is_ascii_digit())
                        .take_while(|c| c.is_ascii_digit())
                        .collect();
                    digits.parse::<u64>().ok()
                })
                .max()
                .unwrap_or(0)
        } else {
            0
        }
    }
}
