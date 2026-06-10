use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use vaultsync_core::VaultSyncClient;
use vaultsync_core::VaultSyncConfig;
use vaultsync_core::storage::traits::StorageConfig;
use vaultsync_core::schema::migration::MigrationDefinition;
use vaultsync_core::VaultSyncError;

#[tokio::test]
async fn test_migration_runs_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mig_once.db").to_string_lossy().to_string();

    let mut config = VaultSyncConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path };
    let client = VaultSyncClient::new(config).await.unwrap();

    let run_count = Arc::new(AtomicU32::new(0));
    let run_count_clone = run_count.clone();

    // Define migration
    let migration1 = MigrationDefinition::new("v1.0.0", Box::new(move || {
        run_count_clone.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));

    // Apply first time
    client.apply_migration(migration1).await.unwrap();
    assert_eq!(run_count.load(Ordering::SeqCst), 1);

    // Define same migration again
    let run_count_clone2 = run_count.clone();
    let migration2 = MigrationDefinition::new("v1.0.0", Box::new(move || {
        run_count_clone2.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));

    // Apply second time: should skip execution
    client.apply_migration(migration2).await.unwrap();
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_migration_ordering() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mig_order.db").to_string_lossy().to_string();

    let mut config = VaultSyncConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path };
    let client = VaultSyncClient::new(config).await.unwrap();

    let history = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Define two migrations
    let h1 = history.clone();
    let mig1 = MigrationDefinition::new("v1.0.0", Box::new(move || {
        h1.lock().unwrap().push("v1.0.0");
        Ok(())
    }));

    let h2 = history.clone();
    let mig2 = MigrationDefinition::new("v2.0.0", Box::new(move || {
        h2.lock().unwrap().push("v2.0.0");
        Ok(())
    }));

    // Helper to apply registered migrations sorted by version
    let mut migrations = vec![mig2, mig1];
    migrations.sort_by(|a, b| a.version.cmp(&b.version));

    for mig in migrations {
        client.apply_migration(mig).await.unwrap();
    }

    // Verify ordering
    let h = history.lock().unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h[0], "v1.0.0");
    assert_eq!(h[1], "v2.0.0");
}

#[tokio::test]
async fn test_migration_failure_rollback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mig_rollback.db").to_string_lossy().to_string();

    let mut config = VaultSyncConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path };
    let client = VaultSyncClient::new(config).await.unwrap();

    let rolled_back = Arc::new(AtomicBool::new(false));
    let rb_clone = rolled_back.clone();

    // Define failing migration with rollback
    let migration = MigrationDefinition::new("v1.0.0", Box::new(|| {
        Err(VaultSyncError::Schema("migration failed intentionally".into()))
    }))
    .with_rollback(Box::new(move || {
        rb_clone.store(true, Ordering::SeqCst);
        Ok(())
    }));

    // Apply: should fail
    let res = client.apply_migration(migration).await;
    assert!(res.is_err());
    
    // Verify rollback ran
    assert!(rolled_back.load(Ordering::SeqCst));

    // Verify migration record was NOT written by trying to apply a succeeding migration with the same version
    let run_success = Arc::new(AtomicBool::new(false));
    let rs_clone = run_success.clone();
    let migration_success = MigrationDefinition::new("v1.0.0", Box::new(move || {
        rs_clone.store(true, Ordering::SeqCst);
        Ok(())
    }));

    client.apply_migration(migration_success).await.unwrap();
    assert!(run_success.load(Ordering::SeqCst), "Migration should run successfully because failed attempt did not persist record");
}

#[tokio::test]
async fn test_concurrent_migration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mig_concurrent.db").to_string_lossy().to_string();

    // Client A
    let mut config_a = VaultSyncConfig::default();
    config_a.storage = StorageConfig::Sqlite { path: db_path.clone() };
    let client_a = VaultSyncClient::new(config_a).await.unwrap();

    // Client B
    let mut config_b = VaultSyncConfig::default();
    config_b.storage = StorageConfig::Sqlite { path: db_path.clone() };
    let client_b = VaultSyncClient::new(config_b).await.unwrap();

    let run_count = Arc::new(AtomicU32::new(0));

    // Client A applies migration
    let rc_a = run_count.clone();
    let mig_a = MigrationDefinition::new("v1.0.0", Box::new(move || {
        rc_a.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    client_a.apply_migration(mig_a).await.unwrap();

    // Client B applies migration
    let rc_b = run_count.clone();
    let mig_b = MigrationDefinition::new("v1.0.0", Box::new(move || {
        rc_b.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    client_b.apply_migration(mig_b).await.unwrap();

    // Callback should only run once
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_migration_auto_execution() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mig_auto.db").to_string_lossy().to_string();

    let run_count = Arc::new(AtomicU32::new(0));
    let run_count_clone = run_count.clone();

    // Define local migrations
    let migration = MigrationDefinition::new("v9.9.9", Box::new(move || {
        run_count_clone.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    let migrations = vec![Arc::new(migration)];

    let mut config = VaultSyncConfig::default();
    config.storage = StorageConfig::Sqlite { path: db_path.clone() };
    
    let storage = Arc::new(vaultsync_core::storage::sqlite::SQLiteStorage::new(&db_path).unwrap());
    let coordinator = Arc::new(vaultsync_core::coordinator::memory::InMemoryCoordinator::new());
    let keyring = Arc::new(vaultsync_core::e2ee::keyring::KeyRing::generate());
    
    let client = VaultSyncClient::new_with_storage_and_migrations(config, coordinator, keyring, storage, migrations).await.unwrap();

    // Verify it ran automatically
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    assert_eq!(client.schema_version, 9);
}
