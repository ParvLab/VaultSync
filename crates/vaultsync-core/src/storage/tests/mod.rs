pub mod conformance;
pub mod manager_integration;

#[cfg(test)]
mod test_runners {
    use crate::storage::memory::InMemoryStorage;
    #[cfg(feature = "storage-sqlite")]
    use crate::storage::sqlite::SQLiteStorage;
    use crate::storage::tests::conformance::run_storage_conformance_suite;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_in_memory_storage_conformance() {
        let storage = Arc::new(InMemoryStorage::new());
        run_storage_conformance_suite(storage).await;
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn test_sqlite_storage_conformance() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir
            .path()
            .join("conformance.db")
            .to_string_lossy()
            .to_string();
        let storage = Arc::new(SQLiteStorage::new(&db_path).unwrap());
        run_storage_conformance_suite(storage).await;
    }
}
