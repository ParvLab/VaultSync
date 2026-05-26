use async_trait::async_trait;
use drift_core::coordinator::traits::*;
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
        
        tokio::spawn(async move {
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
                
                // Poll interval
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn run_mock_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{}", port);
        
        let mutations = Arc::new(Mutex::new(Vec::new()));
        let schema_version = Arc::new(AtomicU64::new(0));
        
        let handle = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mutations = mutations.clone();
                let schema_version = schema_version.clone();
                tokio::spawn(async move {
                    let mut buf = [0; 8192];
                    if let Ok(n) = stream.read(&mut buf).await {
                        let request = String::from_utf8_lossy(&buf[..n]);
                        
                        if request.contains("POST ") && request.contains("/push") {
                            if let Some(body_start) = request.find("\r\n\r\n") {
                                let body = &request[body_start + 4..];
                                if let Ok(muts_list) = serde_json::from_str::<Vec<EncryptedMutation>>(body.trim_end_matches('\0')) {
                                    let seqs = {
                                        let mut db = mutations.lock().unwrap();
                                        let mut seqs = Vec::new();
                                        for m in muts_list {
                                            let seq = (db.len() + 1) as u64;
                                            db.push(PendingMutation {
                                                id: m.id,
                                                namespace: m.namespace,
                                                sequence: seq,
                                                doc_id: m.doc_id,
                                                record_id: m.record_id,
                                                encrypted_blob: m.encrypted_blob,
                                                timestamp: m.timestamp,
                                            });
                                            seqs.push(seq);
                                        }
                                        seqs
                                    };
                                    let resp_body = serde_json::to_string(&seqs).unwrap();
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                        resp_body.len(),
                                        resp_body
                                    );
                                    let _ = stream.write_all(response.as_bytes()).await;
                                }
                            }
                        } else if request.contains("GET ") && request.contains("/pull") {
                            let after = request.split("after=").nth(1)
                                .and_then(|s| s.split('&').next())
                                .and_then(|s| s.split(' ').next())
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(0);
                            
                            let filtered = {
                                let db = mutations.lock().unwrap();
                                let filtered: Vec<PendingMutation> = db.iter()
                                    .filter(|m| m.sequence > after)
                                    .cloned()
                                    .collect();
                                filtered
                            };
                            let resp_body = serde_json::to_string(&filtered).unwrap();
                            let response = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                resp_body.len(),
                                resp_body
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else if request.contains("POST ") && request.contains("/register") {
                            if let Some(body_start) = request.find("\r\n\r\n") {
                                let body = &request[body_start + 4..];
                                if let Ok(info) = serde_json::from_str::<ReplicaInfo>(body.trim_end_matches('\0')) {
                                    schema_version.store(info.schema_version, Ordering::SeqCst);
                                }
                            }
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else if request.contains("POST ") && request.contains("/heartbeat") {
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else if request.contains("GET ") && request.contains("/schema_version") {
                            let ver = schema_version.load(Ordering::SeqCst);
                            let resp_body = serde_json::to_string(&ver).unwrap();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                resp_body.len(),
                                resp_body
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else {
                            let response = "HTTP/1.1 404 NOT FOUND\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                    }
                });
            }
        });
        
        (url, handle)
    }

    #[tokio::test]
    async fn test_cloudflare_coordinator_flow() {
        let (server_url, _server_handle) = run_mock_server().await;
        
        let config = CloudflareConfig {
            worker_url: server_url,
            api_token: Some("secret-token".to_string()),
        };
        
        let coord = CloudflareCoordinator::new(config);
        let ns = "test-cf";
        
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 0);
        
        coord.register(ns, ReplicaInfo {
            replica_id: "rep-cf".to_string(),
            namespace: ns.to_string(),
            public_key: vec![9, 8, 7],
            schema_version: 7,
        }).await.unwrap();
        
        let v = coord.schema_version(ns).await.unwrap();
        assert_eq!(v, 7);
        
        coord.heartbeat(ns, "rep-cf").await.unwrap();
        
        let sub = coord.subscribe(ns, 0).await.unwrap();
        let mut sub = std::pin::Pin::from(sub);
        
        let seqs = coord.push(ns, vec![
            EncryptedMutation {
                id: "m-cf-1".to_string(),
                namespace: ns.to_string(),
                replica_id: "rep-cf".to_string(),
                doc_id: "doc-cf".to_string(),
                record_id: "rec-cf".to_string(),
                encrypted_blob: vec![1, 3, 5],
                timestamp: 2000,
                schema_version: 7,
            }
        ]).await.unwrap();
        assert_eq!(seqs, vec![1]);
        
        let pulled = coord.pull(ns, 0, 10).await.unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].id, "m-cf-1");
        assert_eq!(pulled[0].sequence, 1);
        
        let next_m = sub.next().await;
        assert!(next_m.is_some());
        assert_eq!(next_m.unwrap().id, "m-cf-1");
    }
}
