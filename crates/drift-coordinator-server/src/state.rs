use std::sync::Arc;
use drift_core::coordinator::traits::Coordinator;
use crate::token_store::TokenStore;
use crate::config::ServerConfig;

#[derive(Clone)]
pub struct AppState {
    pub coordinator: Arc<dyn Coordinator>,
    pub token_store: Option<Arc<dyn TokenStore>>,
    pub config: ServerConfig,
}
