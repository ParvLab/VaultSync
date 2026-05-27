use async_trait::async_trait;
use drift_core::coordinator::traits::*;
use futures::Stream;
use futures::StreamExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpCoordinatorConfig {
    pub url: String,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HttpCoordinator {
    client: reqwest::Client,
    config: HttpCoordinatorConfig,
}

impl HttpCoordinator {
    pub fn new(config: HttpCoordinatorConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }
}

#[async_trait]
impl Coordinator for HttpCoordinator {
    async fn push(&self, namespace: &str, mutations: Vec<EncryptedMutation>) -> Result<Vec<SequenceId>, CoordinatorError> {
        let url = format!("{}/namespace/{}/push", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&mutations);
        if let Some(ref token) = self.config.auth_token {
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
            self.config.url.trim_end_matches('/'),
            namespace,
            after,
            limit
        );
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
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
        
        tokio::spawn(async move {
            let mut after = from_sequence;
            let url_base = config.url.clone();
            
            loop {
                let url = format!(
                    "{}/namespace/{}/events?after={}",
                    url_base.trim_end_matches('/'),
                    ns,
                    after
                );
                
                let mut builder = client.get(&url).header("Accept", "text/event-stream");
                if let Some(ref token) = config.auth_token {
                    builder = builder.bearer_auth(token);
                }
                
                let resp = match builder.send().await {
                    Ok(r) if r.status().is_success() => r,
                    _ => {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                
                let mut stream = resp.bytes_stream();
                let mut buffer = Vec::new();
                
                while let Some(chunk_res) = stream.next().await {
                    let chunk = match chunk_res {
                        Ok(bytes) => bytes,
                        Err(_) => break,
                    };
                    
                    buffer.extend_from_slice(&chunk);
                    
                    while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                        let line_bytes = buffer.drain(..=pos).collect::<Vec<u8>>();
                        let line = match std::str::from_utf8(&line_bytes) {
                            Ok(s) => s.trim(),
                            Err(_) => continue,
                        };
                        
                        if line.starts_with("data:") {
                            let data_json = line["data:".len()..].trim();
                            if let Ok(mutation) = serde_json::from_str::<PendingMutation>(data_json) {
                                after = after.max(mutation.sequence);
                                if tx.send(mutation).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
                
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        
        Ok(Box::new(HttpSubscription { rx }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let url = format!("{}/namespace/{}/register", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.post(&url).json(&info);
        if let Some(ref token) = self.config.auth_token {
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
            self.config.url.trim_end_matches('/'),
            namespace,
            replica_id
        );
        let mut builder = self.client.post(&url);
        if let Some(ref token) = self.config.auth_token {
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
        let url = format!("{}/namespace/{}/schema_version", self.config.url.trim_end_matches('/'), namespace);
        let mut builder = self.client.get(&url);
        if let Some(ref token) = self.config.auth_token {
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

struct HttpSubscription {
    rx: tokio::sync::mpsc::Receiver<PendingMutation>,
}

impl Stream for HttpSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use crate::test_utils::run_mock_server;

    #[tokio::test]
    async fn test_http_coordinator_flow() {
        let (server_url, _server_handle) = run_mock_server().await;
        
        let config = HttpCoordinatorConfig {
            url: server_url,
            auth_token: Some("secret-token".to_string()),
        };
        
        let coord = HttpCoordinator::new(config);
        let ns = "test-http";
        
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 0);
        
        coord.register(ns, ReplicaInfo {
            replica_id: "rep-http".to_string(),
            namespace: ns.to_string(),
            public_key: vec![9, 8, 7],
            schema_version: 7,
        }).await.unwrap();
        
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 7);
        
        coord.heartbeat(ns, "rep-http").await.unwrap();
        
        let pulled = coord.pull(ns, 0, 10).await.unwrap();
        assert_eq!(pulled.len(), 0);
        
        let seqs = coord.push(ns, vec![
            EncryptedMutation {
                id: "m-http-1".to_string(),
                namespace: ns.to_string(),
                replica_id: "rep-http".to_string(),
                doc_id: "doc-http".to_string(),
                record_id: "rec-http".to_string(),
                encrypted_blob: vec![1, 3, 5],
                timestamp: 2000,
                schema_version: 7,
            }
        ]).await.unwrap();
        assert_eq!(seqs, vec![1]);
        
        let pulled = coord.pull(ns, 0, 10).await.unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].id, "m-http-1");
        assert_eq!(pulled[0].sequence, 1);
    }
}
