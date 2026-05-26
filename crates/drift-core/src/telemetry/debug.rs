use std::sync::Arc;
use serde_json::Value;
use crate::storage::traits::Storage;
use crate::telemetry::metrics::DriftMetrics;
use crate::error::DriftError;

pub struct DebugApi {
    storage: Arc<dyn Storage>,
    metrics: Arc<DriftMetrics>,
}

impl DebugApi {
    pub fn new(storage: Arc<dyn Storage>, metrics: Arc<DriftMetrics>) -> Self {
        Self { storage, metrics }
    }

    pub async fn get_state(&self, namespace: &str) -> Result<Value, DriftError> {
        let sync_state = self.storage.read_sync_state(namespace).await?;
        let metrics = self.metrics.snapshot();
        Ok(serde_json::json!({
            "sync_state": sync_state,
            "metrics": metrics,
        }))
    }
}
