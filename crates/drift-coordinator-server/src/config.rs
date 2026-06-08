use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "drift-coordinator-server", about = "Drift Coordinator HTTP Server")]
pub struct ServerConfig {
    #[arg(long, default_value = "127.0.0.1", env = "DRIFT_HOST")]
    pub host: String,

    #[arg(short, long, default_value_t = 9876, env = "DRIFT_PORT")]
    pub port: u16,

    #[arg(short, long, default_value = "sqlite", env = "DRIFT_BACKEND")]
    pub backend: String, // "sqlite", "redis", or "memory"

    #[arg(long, default_value = "./drift_coordinator.db", env = "DRIFT_DB_PATH")]
    pub db_path: String,

    #[arg(long, default_value = "redis://127.0.0.1:6379", env = "DRIFT_DB_URL")]
    pub db_url: String,

    #[arg(long, env = "DRIFT_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    #[arg(long, env = "DRIFT_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
}

impl ServerConfig {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
