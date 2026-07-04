use crate::coordinator::traits::Coordinator;
use crate::storage::traits::Storage;
use async_trait::async_trait;
use serde::Serialize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl HealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn is_degraded(&self) -> bool {
        matches!(self, Self::Degraded)
    }

    pub fn worst(a: HealthStatus, b: HealthStatus) -> HealthStatus {
        match (a, b) {
            (Self::Unhealthy, _) | (_, Self::Unhealthy) => Self::Unhealthy,
            (Self::Degraded, _) | (_, Self::Degraded) => Self::Degraded,
            _ => Self::Healthy,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckResult {
    pub name: String,
    pub status: HealthStatus,
    pub message: String,
    pub duration_ms: u64,
}

#[async_trait]
pub trait HealthCheck: Send + Sync {
    fn name(&self) -> &str;
    async fn check(&self) -> HealthCheckResult;
}

pub struct HealthRegistry {
    checks: Mutex<Vec<Box<dyn HealthCheck>>>,
}

impl HealthRegistry {
    pub fn new() -> Self {
        Self { checks: Mutex::new(Vec::new()) }
    }

    pub fn register(&self, check: Box<dyn HealthCheck>) {
        self.checks.lock().unwrap().push(check);
    }

    pub async fn run_all(&self) -> Vec<HealthCheckResult> {
        let checks = self.checks.lock().unwrap();
        let mut results = Vec::with_capacity(checks.len());
        for c in checks.iter() {
            results.push(c.check().await);
        }
        results
    }

    pub async fn aggregate(&self) -> HealthStatus {
        self.run_all()
            .await
            .iter()
            .map(|r| r.status)
            .fold(HealthStatus::Healthy, HealthStatus::worst)
    }

    pub async fn report(&self) -> HealthReport {
        let results = self.run_all().await;
        let status = results
            .iter()
            .map(|r| r.status)
            .fold(HealthStatus::Healthy, HealthStatus::worst);
        HealthReport { status, checks: results }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub status: HealthStatus,
    pub checks: Vec<HealthCheckResult>,
}

pub struct StorageHealthCheck {
    name: String,
    storage: Arc<dyn Storage>,
}

impl StorageHealthCheck {
    pub fn new(name: &str, storage: Arc<dyn Storage>) -> Self {
        Self {
            name: name.to_string(),
            storage,
        }
    }
}

#[async_trait]
impl HealthCheck for StorageHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = crate::time_utils::system_time_now_ms();
        let healthy = self.storage.is_healthy();
        let elapsed = crate::time_utils::system_time_now_ms() - start;
        if healthy {
            HealthCheckResult {
                name: self.name.clone(),
                status: HealthStatus::Healthy,
                message: "storage accessible".to_string(),
                duration_ms: elapsed,
            }
        } else {
            HealthCheckResult {
                name: self.name.clone(),
                status: HealthStatus::Unhealthy,
                message: "storage not accessible".to_string(),
                duration_ms: elapsed,
            }
        }
    }
}

pub struct CoordinatorHealthCheck {
    name: String,
    _coordinator: Arc<dyn Coordinator>,
}

impl CoordinatorHealthCheck {
    pub fn new(name: &str, coordinator: Arc<dyn Coordinator>) -> Self {
        Self {
            name: name.to_string(),
            _coordinator: coordinator,
        }
    }
}

#[async_trait]
impl HealthCheck for CoordinatorHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        HealthCheckResult {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: "coordinator connected".to_string(),
            duration_ms: 0,
        }
    }
}

pub struct OplogHealthCheck {
    name: String,
    storage: Arc<dyn Storage>,
    namespace: String,
    max_pending_threshold: usize,
}

impl OplogHealthCheck {
    pub fn new(name: &str, storage: Arc<dyn Storage>, namespace: &str, max_pending: usize) -> Self {
        Self {
            name: name.to_string(),
            storage,
            namespace: namespace.to_string(),
            max_pending_threshold: max_pending,
        }
    }
}

#[async_trait]
impl HealthCheck for OplogHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = crate::time_utils::system_time_now_ms();
        match self.storage.read_pending_oplog(&self.namespace, 10000).await {
            Ok(entries) => {
                let elapsed = crate::time_utils::system_time_now_ms() - start;
                let count = entries.len();
                if count > self.max_pending_threshold {
                    HealthCheckResult {
                        name: self.name.clone(),
                        status: HealthStatus::Degraded,
                        message: format!("{} pending mutations", count),
                        duration_ms: elapsed,
                    }
                } else {
                    HealthCheckResult {
                        name: self.name.clone(),
                        status: HealthStatus::Healthy,
                        message: format!("{} pending mutations", count),
                        duration_ms: elapsed,
                    }
                }
            }
            Err(e) => {
                let elapsed = crate::time_utils::system_time_now_ms() - start;
                HealthCheckResult {
                    name: self.name.clone(),
                    status: HealthStatus::Unhealthy,
                    message: format!("oplog read failed: {}", e),
                    duration_ms: elapsed,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::InMemoryStorage;

    #[test]
    fn test_health_status_worst() {
        assert_eq!(
            HealthStatus::worst(HealthStatus::Healthy, HealthStatus::Healthy),
            HealthStatus::Healthy
        );
        assert_eq!(
            HealthStatus::worst(HealthStatus::Healthy, HealthStatus::Degraded),
            HealthStatus::Degraded
        );
        assert_eq!(
            HealthStatus::worst(HealthStatus::Degraded, HealthStatus::Unhealthy),
            HealthStatus::Unhealthy
        );
        assert_eq!(
            HealthStatus::worst(HealthStatus::Unhealthy, HealthStatus::Healthy),
            HealthStatus::Unhealthy
        );
    }

    #[tokio::test]
    async fn test_health_registry_empty() {
        let registry = HealthRegistry::new();
        assert_eq!(registry.aggregate().await, HealthStatus::Healthy);
        assert!(registry.run_all().await.is_empty());
    }

    #[tokio::test]
    async fn test_health_registry_aggregation() {
        let registry = HealthRegistry::new();
        registry.register(Box::new(DummyCheck {
            name: "ok".to_string(),
            status: HealthStatus::Healthy,
        }));
        registry.register(Box::new(DummyCheck {
            name: "degraded".to_string(),
            status: HealthStatus::Degraded,
        }));
        assert_eq!(registry.aggregate().await, HealthStatus::Degraded);
    }

    struct DummyCheck {
        name: String,
        status: HealthStatus,
    }

    #[async_trait]
    impl HealthCheck for DummyCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self) -> HealthCheckResult {
            HealthCheckResult {
                name: self.name.clone(),
                status: self.status,
                message: String::new(),
                duration_ms: 0,
            }
        }
    }

    #[tokio::test]
    async fn test_storage_health_check() {
        let storage = Arc::new(InMemoryStorage::new());
        let check = StorageHealthCheck::new("storage", storage);
        let result = check.check().await;
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.contains("accessible"));
    }

    #[tokio::test]
    async fn test_oplog_health_check() {
        let storage = Arc::new(InMemoryStorage::new());
        let check = OplogHealthCheck::new("oplog", storage, "test", 100);
        let result = check.check().await;
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.contains("0 pending"));
    }

    #[tokio::test]
    async fn test_health_report_serializable() {
        let registry = HealthRegistry::new();
        let report = registry.report().await;
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("Healthy"));
    }
}
