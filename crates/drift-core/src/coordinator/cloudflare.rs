use async_trait::async_trait;
use crate::coordinator::traits::*;
use futures::Stream;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CloudflareConfig {
    pub worker_url: String,
    pub api_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CloudflareCoordinator {
    client: reqwest::Client,
    config: CloudflareConfig,
}

impl CloudflareCoordinator {
    pub fn new(config: CloudflareConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }
}

#[async_trait]
impl Coordinator for CloudflareCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let url = format!("{}/namespace/{}/push", self.config.worker_url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&mutations);
        if let Some(ref token) = self.config.api_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let seq_ids = resp.json::<Vec<SequenceId>>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(seq_ids)
    }

    async fn pull(&self, namespace: &str, after: SequenceId, limit: usize) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let url = format!(
            "{}/namespace/{}/pull?after={}&limit={}",
            self.config.worker_url.trim_end_matches('/'),
            namespace,
            after,
            limit
        );
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.api_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let mutations = resp.json::<Vec<PendingMutation>>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(mutations)
    }

    async fn subscribe(&self, namespace: &str, from_sequence: SequenceId) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let client = self.client.clone();
        let config = self.config.clone();
        let ns = namespace.to_string();
        
        crate::time_utils::spawn(async move {
            let mut last_seq = from_sequence;
            loop {
                let url = format!(
                    "{}/namespace/{}/pull?after={}&limit=100",
                    config.worker_url.trim_end_matches('/'),
                    ns,
                    last_seq
                );
                let mut builder = client.get(&url);
                if let Some(ref token) = config.api_token {
                    builder = builder.bearer_auth(token);
                }
                
                let res = match builder.send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            resp.json::<Vec<PendingMutation>>().await.ok()
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                };
                
                if let Some(mutations) = res {
                    for m in mutations {
                        last_seq = last_seq.max(m.sequence);
                        if tx.send(m).await.is_err() {
                            return; // receiver dropped, stop polling
                        }
                    }
                }
                
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });
        
        Ok(Box::new(CloudflareSubscription { rx }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let url = format!("{}/namespace/{}/register", self.config.worker_url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&info);
        if let Some(ref token) = self.config.api_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        Ok(())
    }

    async fn heartbeat(&self, namespace: &str, replica_id: &str) -> Result<(), CoordinatorError> {
        let url = format!(
            "{}/namespace/{}/heartbeat?replica_id={}",
            self.config.worker_url.trim_end_matches('/'),
            namespace,
            replica_id
        );
        let mut builder = self.client.post(&url);
        if let Some(ref token) = self.config.api_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        Ok(())
    }

    async fn schema_version(&self, namespace: &str) -> Result<u64, CoordinatorError> {
        let url = format!("{}/namespace/{}/schema_version", self.config.worker_url.trim_end_matches('/'), namespace);
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.api_token {
            builder = builder.bearer_auth(token);
        }
        let resp = builder.send().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CoordinatorError::Internal(format!("HTTP error status: {}", resp.status())));
        }
        let version = resp.json::<u64>().await
            .map_err(|e| CoordinatorError::Internal(e.to_string()))?;
        Ok(version)
    }
}

struct CloudflareSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for CloudflareSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
