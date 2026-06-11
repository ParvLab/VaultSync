use crate::config::ServerConfig;
use crate::token_store::TokenStore;
use axum::extract::ws::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use vaultsync_core::coordinator::traits::Coordinator;

#[derive(Clone, Default)]
pub struct SessionRegistry {
    pub sessions: Arc<RwLock<HashMap<(String, String), Sender<Message>>>>,
}

#[derive(Clone)]
pub struct AppState {
    pub coordinator: Arc<dyn Coordinator>,
    pub token_store: Option<Arc<dyn TokenStore>>,
    pub config: ServerConfig,
    pub sessions: SessionRegistry,
}
