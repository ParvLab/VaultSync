use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "vaultsync-coordinator-server",
    about = "VaultSync Coordinator HTTP Server"
)]
pub struct ServerConfig {
    #[arg(long, default_value = "127.0.0.1", env = "VAULTSYNC_HOST")]
    pub host: String,

    #[arg(short, long, default_value_t = 9876, env = "VAULTSYNC_PORT")]
    pub port: u16,

    #[arg(short, long, default_value = "sqlite", env = "VAULTSYNC_BACKEND")]
    pub backend: String, // "sqlite", "redis", or "memory"

    #[arg(
        long,
        default_value = "./vaultsync_coordinator.db",
        env = "VAULTSYNC_DB_PATH"
    )]
    pub db_path: String,

    #[arg(
        long,
        default_value = "redis://127.0.0.1:6379",
        env = "VAULTSYNC_DB_URL"
    )]
    pub db_url: String,

    #[arg(long, env = "VAULTSYNC_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    #[arg(long, env = "VAULTSYNC_ADMIN_TOKEN")]
    pub admin_token: Option<String>,

    /// Namespace to auto-compact (empty = disabled). Requires auto_compact_interval_minutes > 0.
    #[arg(long, default_value = "", env = "VAULTSYNC_AUTO_COMPACT_NS")]
    pub auto_compact_ns: String,

    /// How often to auto-compact, in minutes (0 = disabled).
    #[arg(long, default_value_t = 0, env = "VAULTSYNC_AUTO_COMPACT_INTERVAL")]
    pub auto_compact_interval_minutes: u64,
}

impl ServerConfig {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
