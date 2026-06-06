use std::sync::Arc;
use drift_core::coordinator::traits::{Coordinator, EncryptedMutation, ReplicaInfo};
use drift_core::coordinator::memory::InMemoryCoordinator;
use drift_coordinator_sqlite::coordinator::SQLiteCoordinator;
use drift_coordinator_http::coordinator::{HttpCoordinator, HttpCoordinatorConfig};
use drift_coordinator_redis::coordinator::RedisCoordinator;
use std::sync::Mutex;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn run_coordinator_conformance_suite(coord: Arc<dyn Coordinator>) {
    let ns = "coord-conformance-ns";

    // 1. Register replica
    let replica = ReplicaInfo {
        replica_id: "rep-1".to_string(),
        namespace: ns.to_string(),
        public_key: vec![1, 2, 3],
        schema_version: 0,
    };
    coord.register(ns, replica).await.unwrap();

    // 2. Heartbeat
    coord.heartbeat(ns, "rep-1").await.unwrap();

    // 3. Schema Version
    let version = coord.schema_version(ns).await.unwrap();
    assert_eq!(version, 0);

    // 4. Push mutations and Pull mutations roundtrip
    let muts = vec![
        EncryptedMutation {
            id: "m-1".to_string(),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-1".to_string(),
            encrypted_blob: vec![10, 20],
            timestamp: 1000,
            schema_version: 0,
            key_version: 1,
        },
        EncryptedMutation {
            id: "m-2".to_string(),
            namespace: ns.to_string(),
            replica_id: "rep-1".to_string(),
            doc_id: "doc-1".to_string(),
            record_id: "rec-2".to_string(),
            encrypted_blob: vec![30, 40],
            timestamp: 2000,
            schema_version: 0,
            key_version: 1,
        },
    ];

    let seqs = coord.push(ns, muts).await.unwrap();
    assert_eq!(seqs.len(), 2);
    // Sequences must be strictly increasing
    assert!(seqs[1] > seqs[0]);

    // Pull after sequence 0
    let pending = coord.pull(ns, 0, 10).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, "m-1");
    assert_eq!(pending[0].sequence, seqs[0]);
    assert_eq!(pending[0].encrypted_blob, vec![10, 20]);
    assert_eq!(pending[1].id, "m-2");
    assert_eq!(pending[1].sequence, seqs[1]);
    assert_eq!(pending[1].encrypted_blob, vec![30, 40]);

    // Pull after sequence seqs[0] (should return only m-2)
    let pending_after = coord.pull(ns, seqs[0], 10).await.unwrap();
    assert_eq!(pending_after.len(), 1);
    assert_eq!(pending_after[0].id, "m-2");

    // Pull with limit 1
    let pending_limit = coord.pull(ns, 0, 1).await.unwrap();
    assert_eq!(pending_limit.len(), 1);
    assert_eq!(pending_limit[0].id, "m-1");
}

#[derive(Clone, Debug)]
struct MockMutation {
    id: String,
    _replica_id: String,
    doc_id: String,
    record_id: String,
    encrypted_blob: Vec<u8>,
    timestamp: u64,
    schema_version: u64,
    key_version: u64,
    stream_id: String,
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i+1] == b'\n' {
            return Some(i);
        }
    }
    None
}

fn parse_resp(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }
    
    let mut pos = 1;
    let crlf_pos = find_crlf(&buf[pos..])? + pos;
    let num_elements = std::str::from_utf8(&buf[pos..crlf_pos]).ok()?.parse::<usize>().ok()?;
    pos = crlf_pos + 2;
    
    let mut args = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        if pos >= buf.len() || buf[pos] != b'$' {
            return None;
        }
        pos += 1;
        let crlf = find_crlf(&buf[pos..])? + pos;
        let len = std::str::from_utf8(&buf[pos..crlf]).ok()?.parse::<isize>().ok()?;
        pos = crlf + 2;
        
        if len == -1 {
            args.push(Vec::new());
        } else {
            let len = len as usize;
            if pos + len > buf.len() {
                return None;
            }
            let arg = buf[pos..pos + len].to_vec();
            pos += len;
            if pos + 2 > buf.len() || &buf[pos..pos + 2] != b"\r\n" {
                return None;
            }
            pos += 2;
            args.push(arg);
        }
    }
    Some((args, pos))
}

fn stream_id_to_seq(id: &str) -> u64 {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    let ms: u64 = ms.parse().unwrap_or(0);
    let seq: u64 = seq.parse().unwrap_or(0);
    ms * 1_000_000 + seq
}

fn serialize_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn serialize_blob(b: &[u8]) -> Vec<u8> {
    let mut res = format!("${}\r\n", b.len()).into_bytes();
    res.extend_from_slice(b);
    res.extend_from_slice(b"\r\n");
    res
}

async fn run_mock_redis_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    
    let mutations = Arc::new(Mutex::new(Vec::<MockMutation>::new()));
    let schema_version = Arc::new(Mutex::new(0u64));
    let replicas = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let heartbeats = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    
    let handle = tokio::spawn(async move {
        let mut last_ms = 0u64;
        let mut last_seq = 0u64;
        
        while let Ok((mut socket, _)) = listener.accept().await {
            let mutations = mutations.clone();
            let schema_version = schema_version.clone();
            let replicas = replicas.clone();
            let heartbeats = heartbeats.clone();
            
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut data_len = 0;
                
                loop {
                    let n = match socket.read(&mut buf[data_len..]).await {
                        Ok(0) => break, // EOF
                        Ok(n) => n,
                        Err(_) => break,
                    };
                    data_len += n;
                    
                    let mut parse_pos = 0;
                    while parse_pos < data_len {
                        if let Some((args, bytes_consumed)) = parse_resp(&buf[parse_pos..data_len]) {
                            parse_pos += bytes_consumed;
                            if args.is_empty() {
                                continue;
                            }
                            
                            let cmd = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                            let response: Vec<u8> = match cmd.as_str() {
                                "PING" => b"+PONG\r\n".to_vec(),
                                "XADD" => {
                                    let mut id = String::new();
                                    let mut replica_id = String::new();
                                    let mut doc_id = String::new();
                                    let mut record_id = String::new();
                                    let mut encrypted_blob = Vec::new();
                                    let mut timestamp = 0;
                                    let mut s_version = 0;
                                    let mut k_version = 1;
                                    
                                    for chunk in args[3..].chunks_exact(2) {
                                        let key = std::str::from_utf8(&chunk[0]).unwrap_or("");
                                        match key {
                                            "id" => id = std::str::from_utf8(&chunk[1]).unwrap_or("").to_string(),
                                            "replica_id" => replica_id = std::str::from_utf8(&chunk[1]).unwrap_or("").to_string(),
                                            "doc_id" => doc_id = std::str::from_utf8(&chunk[1]).unwrap_or("").to_string(),
                                            "record_id" => record_id = std::str::from_utf8(&chunk[1]).unwrap_or("").to_string(),
                                            "blob" => encrypted_blob = chunk[1].clone(),
                                            "ts" => timestamp = std::str::from_utf8(&chunk[1]).unwrap_or("").parse().unwrap_or(0),
                                            "schema_version" => s_version = std::str::from_utf8(&chunk[1]).unwrap_or("").parse().unwrap_or(0),
                                            "key_version" => k_version = std::str::from_utf8(&chunk[1]).unwrap_or("").parse().unwrap_or(1),
                                            _ => {}
                                        }
                                    }
                                    
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as u64;
                                        
                                    let stream_id = if now_ms == last_ms {
                                        last_seq += 1;
                                        format!("{}-{}", now_ms, last_seq)
                                    } else {
                                        last_ms = now_ms;
                                        last_seq = 0;
                                        format!("{}-0", now_ms)
                                    };
                                    
                                    {
                                        let mut db = mutations.lock().unwrap();
                                        db.push(MockMutation {
                                            id,
                                            _replica_id: replica_id,
                                            doc_id,
                                            record_id,
                                            encrypted_blob,
                                            timestamp,
                                            schema_version: s_version,
                                            key_version: k_version,
                                            stream_id: stream_id.clone(),
                                        });
                                    }
                                    
                                    serialize_string(&stream_id)
                                }
                                "XRANGE" => {
                                    let start_seq = stream_id_to_seq(std::str::from_utf8(&args[2]).unwrap_or("0-0"));
                                    let limit = if args.len() >= 6 && std::str::from_utf8(&args[4]).unwrap_or("") == "COUNT" {
                                        std::str::from_utf8(&args[5]).unwrap_or("").parse::<usize>().unwrap_or(usize::MAX)
                                    } else {
                                        usize::MAX
                                    };
                                    
                                    let filtered: Vec<MockMutation> = {
                                        let db = mutations.lock().unwrap();
                                        db.iter()
                                            .filter(|m| stream_id_to_seq(&m.stream_id) >= start_seq)
                                            .take(limit)
                                            .cloned()
                                            .collect()
                                    };
                                    
                                    let mut entries_bytes = Vec::new();
                                    for m in &filtered {
                                        let mut entry_bytes = Vec::new();
                                        entry_bytes.extend_from_slice(b"*2\r\n");
                                        entry_bytes.extend_from_slice(&serialize_string(&m.stream_id));
                                        
                                        entry_bytes.extend_from_slice(b"*14\r\n");
                                        entry_bytes.extend_from_slice(&serialize_string("id"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.id));
                                        entry_bytes.extend_from_slice(&serialize_string("doc_id"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.doc_id));
                                        entry_bytes.extend_from_slice(&serialize_string("record_id"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.record_id));
                                        entry_bytes.extend_from_slice(&serialize_string("blob"));
                                        entry_bytes.extend_from_slice(&serialize_blob(&m.encrypted_blob));
                                        entry_bytes.extend_from_slice(&serialize_string("ts"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.timestamp.to_string()));
                                        entry_bytes.extend_from_slice(&serialize_string("schema_version"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.schema_version.to_string()));
                                        entry_bytes.extend_from_slice(&serialize_string("key_version"));
                                        entry_bytes.extend_from_slice(&serialize_string(&m.key_version.to_string()));
                                        
                                        entries_bytes.extend_from_slice(&entry_bytes);
                                    }
                                    
                                    let mut res = format!("*{}\r\n", filtered.len()).into_bytes();
                                    res.extend_from_slice(&entries_bytes);
                                    res
                                }
                                "XREAD" => {
                                    let start_seq = stream_id_to_seq(std::str::from_utf8(&args[7]).unwrap_or("0-0"));
                                    let limit = std::str::from_utf8(&args[4]).unwrap_or("").parse::<usize>().unwrap_or(100);
                                    let timeout_ms = std::str::from_utf8(&args[2]).unwrap_or("").parse::<u64>().unwrap_or(500);
                                    
                                    let mut elapsed = 0u64;
                                    let mut filtered: Vec<MockMutation>;
                                    loop {
                                        {
                                            let db = mutations.lock().unwrap();
                                            filtered = db.iter()
                                                .filter(|m| stream_id_to_seq(&m.stream_id) > start_seq)
                                                .take(limit)
                                                .cloned()
                                                .collect();
                                        }
                                        if !filtered.is_empty() || elapsed >= timeout_ms {
                                            break;
                                        }
                                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                        elapsed += 50;
                                    }
                                    
                                    if filtered.is_empty() {
                                        b"$-1\r\n".to_vec()
                                    } else {
                                        let mut res = Vec::new();
                                        res.extend_from_slice(b"*1\r\n*2\r\n");
                                        res.extend_from_slice(&serialize_string(std::str::from_utf8(&args[6]).unwrap()));
                                        
                                        let mut entries_bytes = Vec::new();
                                        for m in &filtered {
                                            let mut entry_bytes = Vec::new();
                                            entry_bytes.extend_from_slice(b"*2\r\n");
                                            entry_bytes.extend_from_slice(&serialize_string(&m.stream_id));
                                            
                                            entry_bytes.extend_from_slice(b"*14\r\n");
                                            entry_bytes.extend_from_slice(&serialize_string("id"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.id));
                                            entry_bytes.extend_from_slice(&serialize_string("doc_id"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.doc_id));
                                            entry_bytes.extend_from_slice(&serialize_string("record_id"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.record_id));
                                            entry_bytes.extend_from_slice(&serialize_string("blob"));
                                            entry_bytes.extend_from_slice(&serialize_blob(&m.encrypted_blob));
                                            entry_bytes.extend_from_slice(&serialize_string("ts"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.timestamp.to_string()));
                                            entry_bytes.extend_from_slice(&serialize_string("schema_version"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.schema_version.to_string()));
                                            entry_bytes.extend_from_slice(&serialize_string("key_version"));
                                            entry_bytes.extend_from_slice(&serialize_string(&m.key_version.to_string()));
                                            
                                            entries_bytes.extend_from_slice(&entry_bytes);
                                        }
                                        
                                        res.extend_from_slice(format!("*{}\r\n", filtered.len()).as_bytes());
                                        res.extend_from_slice(&entries_bytes);
                                        res
                                    }
                                }
                                "HSET" => {
                                    let key = std::str::from_utf8(&args[1]).unwrap_or("");
                                    let field = std::str::from_utf8(&args[2]).unwrap_or("");
                                    let val = std::str::from_utf8(&args[3]).unwrap_or("");
                                    if key.contains("replicas:heartbeats") {
                                        let mut db = heartbeats.lock().unwrap();
                                        db.insert(field.to_string(), val.to_string());
                                    } else {
                                        let mut db = replicas.lock().unwrap();
                                        db.insert(field.to_string(), val.to_string());
                                    }
                                    b":1\r\n".to_vec()
                                }
                                "HVALS" => {
                                    let db = replicas.lock().unwrap();
                                    let mut res = Vec::new();
                                    res.extend_from_slice(format!("*{}\r\n", db.len()).as_bytes());
                                    for val in db.values() {
                                        res.extend_from_slice(&serialize_string(val));
                                    }
                                    res
                                }
                                "GET" => {
                                    let key = std::str::from_utf8(&args[1]).unwrap_or("");
                                    if key.contains("schema_version") {
                                        let v = schema_version.lock().unwrap();
                                        serialize_string(&v.to_string())
                                    } else {
                                        b"$-1\r\n".to_vec()
                                    }
                                }
                                "SET" => {
                                    let key = std::str::from_utf8(&args[1]).unwrap_or("");
                                    let val = std::str::from_utf8(&args[2]).unwrap_or("");
                                    if key.contains("schema_version") {
                                        let mut v = schema_version.lock().unwrap();
                                        *v = val.parse().unwrap_or(0);
                                    }
                                    b"+OK\r\n".to_vec()
                                }
                                _ => b"-ERR unknown command\r\n".to_vec(),
                            };
                            
                            let _ = socket.write_all(&response).await;
                        } else {
                            break;
                        }
                    }
                    
                    if parse_pos > 0 {
                        buf.copy_within(parse_pos..data_len, 0);
                        data_len -= parse_pos;
                    }
                }
            });
        }
    });
    
    (format!("redis://{}", addr), handle)
}

#[tokio::test]
async fn test_in_memory_coordinator_conformance() {
    let coord = Arc::new(InMemoryCoordinator::new());
    run_coordinator_conformance_suite(coord).await;
}

#[tokio::test]
async fn test_sqlite_coordinator_conformance() {
    let coord = Arc::new(SQLiteCoordinator::new(":memory:"));
    run_coordinator_conformance_suite(coord).await;
}

#[tokio::test]
async fn test_http_coordinator_conformance() {
    let (server_url, _handle) = drift_coordinator_http::test_utils::run_mock_server().await;
    let coord = Arc::new(HttpCoordinator::new(HttpCoordinatorConfig {
        url: server_url,
        auth_token: None,
    }));
    run_coordinator_conformance_suite(coord).await;
}

#[tokio::test]
async fn test_redis_coordinator_conformance() {
    let (redis_url, _handle) = run_mock_redis_server().await;
    let coord = Arc::new(RedisCoordinator::new(&redis_url).await.unwrap());
    run_coordinator_conformance_suite(coord).await;
}
