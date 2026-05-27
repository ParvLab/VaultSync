use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use drift_core::coordinator::traits::{EncryptedMutation, PendingMutation, ReplicaInfo};

pub async fn run_mock_server() -> (String, tokio::task::JoinHandle<()>) {
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
                        
                        let limit = request.split("limit=").nth(1)
                            .and_then(|s| s.split('&').next())
                            .and_then(|s| s.split(' ').next())
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(usize::MAX);
                        
                        let filtered = {
                            let db = mutations.lock().unwrap();
                            let filtered: Vec<PendingMutation> = db.iter()
                                .filter(|m| m.sequence > after)
                                .take(limit)
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
