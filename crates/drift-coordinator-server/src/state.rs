use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use axum::extract::ws::Message;
use drift_core::coordinator::traits::Coordinator;
use crate::token_store::TokenStore;
use crate::config::ServerConfig;

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
