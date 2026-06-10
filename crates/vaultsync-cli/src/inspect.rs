use clap::Args;
use colored::*;
use serde_json::Value;

#[derive(Args)]
pub struct InspectArgs {
    pub pid: Option<u32>,
    #[arg(long, default_value = "9876")]
    pub port: u16,
    #[arg(long)]
    pub stream: bool,
    #[arg(long)]
    pub state: bool,
    #[arg(long)]
    pub export_trace: Option<String>,
}

pub fn run(args: InspectArgs) {
    let port = args.port;

    if let Some(filename) = args.export_trace {
        let client = reqwest::blocking::Client::new();
        println!("Exporting traces from port {} to {}...", port, filename);
        match client.get(format!("http://127.0.0.1:{}/debug/vaultsync/traces", port)).send() {
            Ok(resp) => {
                match resp.json::<Vec<Value>>() {
                    Ok(spans) => {
                        let file = match std::fs::File::create(&filename) {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("Error creating file {}: {}", filename, e);
                                return;
                            }
                        };
                        serde_json::to_writer_pretty(file, &spans).unwrap();
                        println!("{} Traces successfully exported to {}", "✅".green(), filename);
                    }
                    Err(e) => eprintln!("Error decoding JSON response: {}", e),
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to debug API: {}", e);
            }
        }
        return;
    }

    if args.stream {
        let client = reqwest::blocking::Client::new();
        let mut seen_ids = std::collections::HashSet::new();
        println!("{}", "Streaming real-time vaultsync spans...".yellow().bold());
        loop {
            if let Ok(resp) = client.get(format!("http://127.0.0.1:{}/debug/vaultsync/traces", port)).send() {
                if let Ok(spans) = resp.json::<Vec<Value>>() {
                    for span in spans {
                        if let Some(id) = span.get("id").and_then(|v| v.as_u64()) {
                            if seen_ids.insert(id) {
                                let name = span.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
                                let timestamp = span.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
                                let fields = span.get("fields").cloned().unwrap_or(serde_json::json!({}));
                                println!("[{}] {} - {} fields: {}", timestamp.blue(), name.green().bold(), "SPAN".cyan(), fields);
                            }
                        }
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    if args.state {
        let client = reqwest::blocking::Client::new();
        println!("{}", "=============================================".cyan());
        println!("{}", "        VAULTSYNC REPLICA STATE MONITOR         ".green().bold());
        println!("{}", "=============================================".cyan());

        match client.get(format!("http://127.0.0.1:{}/debug/vaultsync/state", port)).send() {
            Ok(resp) => {
                match resp.json::<Value>() {
                    Ok(state) => {
                        let sync_state = state.get("sync_state");
                        let metrics = state.get("metrics");

                        if let Some(ss) = sync_state {
                            let ns = ss.get("namespace").and_then(|v| v.as_str()).unwrap_or("");
                            let rep = ss.get("replica_id").and_then(|v| v.as_str()).unwrap_or("");
                            let conn = ss.get("connection_status").and_then(|v| v.as_str()).unwrap_or("");
                            let leader = ss.get("leader_status").and_then(|v| v.as_bool()).unwrap_or(false);

                            println!("{:<20} {}", "Namespace:".bold(), ns.cyan());
                            println!("{:<20} {}", "Replica ID:".bold(), rep.yellow());
                            println!("{:<20} {}", "Connection:".bold(), if conn == "Connected" { "Connected".green().bold() } else { "Disconnected".red().bold() });
                            println!("{:<20} {}", "Role:".bold(), if leader { "LEADER".magenta().bold() } else { "READER".blue() });
                        }

                        if let Some(m) = metrics {
                            println!("\n{}", "--- METRICS ---".bold().yellow());
                            println!("{:<20} {}", "Mutations Uploaded:", m.get("mutations_uploaded").unwrap_or(&serde_json::json!(0)));
                            println!("{:<20} {}", "Mutations Downloaded:", m.get("mutations_downloaded").unwrap_or(&serde_json::json!(0)));
                            println!("{:<20} {}", "Pending Mutations:", m.get("pending_mutations").unwrap_or(&serde_json::json!(0)));
                            println!("{:<20} {}", "Replica Count:", m.get("replica_count").unwrap_or(&serde_json::json!(0)));
                            println!("{:<20} {}", "Sync Errors:", m.get("sync_errors").unwrap_or(&serde_json::json!(0)));
                        }
                    }
                    Err(e) => eprintln!("Error decoding JSON response: {}", e),
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to debug API: {}", e);
            }
        }

        // Fetch documents
        if let Ok(resp) = client.get(format!("http://127.0.0.1:{}/debug/vaultsync/state/documents", port)).send() {
            if let Ok(docs) = resp.json::<Vec<Value>>() {
                if !docs.is_empty() {
                    println!("\n{}", "--- DOCUMENTS ---".bold().yellow());
                    println!("{:<25} {:<15} {:<15}", "DOC ID".bold(), "RECORDS".bold(), "SIZE (BYTES)".bold());
                    for doc in docs {
                        let id = doc.get("doc_id").and_then(|v| v.as_str()).unwrap_or("");
                        let count = doc.get("record_count").and_then(|v| v.as_u64()).unwrap_or(0);
                        let size = doc.get("total_size_bytes").and_then(|v| v.as_u64()).unwrap_or(0);
                        println!("{:<25} {:<15} {:<15}", id.cyan(), count, size);
                    }
                }
            }
        }

        // Fetch keys
        if let Ok(resp) = client.get(format!("http://127.0.0.1:{}/debug/vaultsync/state/keys", port)).send() {
            if let Ok(keys) = resp.json::<Vec<Value>>() {
                if !keys.is_empty() {
                    println!("\n{}", "--- ACTIVE KEYS ---".bold().yellow());
                    println!("{:<15} {:<15}", "KEY VERSION".bold(), "KEY LENGTH".bold());
                    for key in keys {
                        let ver = key.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
                        let len = key.get("key_len").and_then(|v| v.as_u64()).unwrap_or(0);
                        println!("{:<15} {:<15}", ver.to_string().cyan(), len);
                    }
                }
            }
        }

        // Fetch replicas
        if let Ok(resp) = client.get(format!("http://127.0.0.1:{}/debug/vaultsync/state/replicas", port)).send() {
            if let Ok(reps) = resp.json::<Vec<Value>>() {
                if !reps.is_empty() {
                    println!("\n{}", "--- CLUSTER REPLICAS ---".bold().yellow());
                    println!("{:<30} {:<15}", "REPLICA ID".bold(), "SCHEMA VERSION".bold());
                    for rep in reps {
                        let id = rep.get("replica_id").and_then(|v| v.as_str()).unwrap_or("");
                        let ver = rep.get("schema_version").and_then(|v| v.as_u64()).unwrap_or(0);
                        println!("{:<30} {:<15}", id.cyan(), ver);
                    }
                }
            }
        }
        return;
    }

    println!("VaultSync inspect on port {} (stream: {}, state: {})", port, args.stream, args.state);
}
