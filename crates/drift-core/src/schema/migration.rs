use crate::error::DriftError;
use sha2::{Sha256, Digest};

pub struct MigrationDefinition {
    pub version: String,
    pub checksum: String,
    pub apply_fn: Box<dyn Fn() -> Result<(), DriftError> + Send>,
    pub rollback_fn: Option<Box<dyn Fn() -> Result<(), DriftError> + Send>>,
}

impl MigrationDefinition {
    pub fn new(version: &str, apply_fn: Box<dyn Fn() -> Result<(), DriftError> + Send>) -> Self {
        let checksum = compute_checksum(version);
        Self {
            version: version.to_string(),
            checksum,
            apply_fn,
            rollback_fn: None,
        }
    }

    pub fn with_rollback(mut self, rollback_fn: Box<dyn Fn() -> Result<(), DriftError> + Send>) -> Self {
        self.rollback_fn = Some(rollback_fn);
        self
    }

    pub fn apply(&self) -> Result<(), DriftError> {
        (self.apply_fn)()
    }

    pub fn rollback(&self) -> Result<(), DriftError> {
        match &self.rollback_fn {
            Some(f) => f(),
            None => Err(DriftError::Schema("no rollback defined".into())),
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

pub fn apply_migration(migration: &MigrationDefinition) -> Result<(), DriftError> {
    migration.apply()
}

pub fn verify_checksum(code: &str, expected: &str) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(code.as_bytes());
    hex::encode(hasher.finalize()) == expected
}
