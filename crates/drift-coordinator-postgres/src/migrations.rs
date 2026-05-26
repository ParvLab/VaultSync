use tokio_postgres::Client;
use drift_core::coordinator::traits::CoordinatorError;

pub async fn initialize(client: &Client) -> Result<(), CoordinatorError> {
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS mutations (
            sequence BIGSERIAL PRIMARY KEY,
            id VARCHAR(255) UNIQUE NOT NULL,
            namespace VARCHAR(255) NOT NULL,
            replica_id VARCHAR(255) NOT NULL,
            doc_id VARCHAR(255) NOT NULL,
            record_id VARCHAR(255) NOT NULL,
            encrypted_blob BYTEA NOT NULL,
            timestamp BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS replicas (
            replica_id VARCHAR(255) PRIMARY KEY,
            namespace VARCHAR(255) NOT NULL,
            public_key BYTEA NOT NULL,
            schema_version BIGINT NOT NULL,
            last_seen BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS schema_versions (
            namespace VARCHAR(255) PRIMARY KEY,
            version BIGINT NOT NULL
        );"
    ).await.map_err(|e| CoordinatorError::Internal(e.to_string()))?;
    Ok(())
}
