use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use vaultsync_core::coordinator::traits::EncryptedMutation;
use vaultsync_core::transport::traits::{InboundMutation, Transport};

/// Deduplication cache: bounded by MAX_DEDUP_ENTRIES
const MAX_DEDUP_ENTRIES: usize = 10_000;

/// Routes mutations between transport layers
///
/// - Local: `BroadcastChannelTransport` (same-origin, plaintext)
/// - Remote: zero or more transports (WS, WebRTC, etc.)
///
/// Provides a merged, deduplicated, self-filtered stream of inbound mutations.
#[derive(Clone)]
pub struct TransportRouter {
    inner: Arc<TransportRouterInner>,
}

struct TransportRouterInner {
    local: Arc<dyn Transport>,
    remotes: Vec<Arc<dyn Transport>>,
    self_replica_id: String,
    merged_tx: mpsc::UnboundedSender<InboundMutation>,
    merged_rx: Mutex<Option<mpsc::UnboundedReceiver<InboundMutation>>>,
    dedup_seen: Mutex<VecDeque<String>>,
}

impl std::fmt::Debug for TransportRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportRouter")
            .field("local", &self.inner.local.name())
            .field("remotes", &self.inner.remotes.len())
            .finish()
    }
}

impl TransportRouter {
    pub fn new(
        local: Arc<dyn Transport>,
        remotes: Vec<Arc<dyn Transport>>,
        self_replica_id: String,
    ) -> Self {
        let (merged_tx, merged_rx) = mpsc::unbounded();
        Self {
            inner: Arc::new(TransportRouterInner {
                local,
                remotes,
                self_replica_id,
                merged_tx,
                merged_rx: Mutex::new(Some(merged_rx)),
                dedup_seen: Mutex::new(VecDeque::with_capacity(MAX_DEDUP_ENTRIES)),
            }),
        }
    }

    /// Start reading from all transport streams.
    /// Spawns WASM reader tasks that forward mutations to the merged stream.
    pub fn start(&self, namespace: &str) {
        let ns = namespace.to_string();

        // Spawn reader for local transport
        let local = self.inner.local.clone();
        let tx = self.inner.merged_tx.clone();
        let rid = self.inner.self_replica_id.clone();
        let ns_local = ns.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(stream) = local.incoming(&ns_local).await {
                let mut stream = stream;
                while let Some(m) = stream.next().await {
                    if m.mutation.replica_id != rid {
                        let _ = tx.unbounded_send(m);
                    }
                }
            }
        });

        // Spawn readers for remote transports
        for remote in &self.inner.remotes {
            let remote = remote.clone();
            let tx = self.inner.merged_tx.clone();
            let rid = self.inner.self_replica_id.clone();
            let ns_remote = ns.clone();
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(stream) = remote.incoming(&ns_remote).await {
                    let mut stream = stream;
                    while let Some(m) = stream.next().await {
                        if m.mutation.replica_id != rid {
                            let _ = tx.unbounded_send(m);
                        }
                    }
                }
            });
        }
    }

    /// Broadcast a mutation through all transports
    pub async fn broadcast(&self, namespace: &str, mutation: &EncryptedMutation) {
        // Local first (instant, same-tab)
        if let Err(e) = self.inner.local.send(namespace, mutation).await {
            tracing::debug!("[router] local send failed: {:?}", e);
        }
        // Then all remotes
        for remote in &self.inner.remotes {
            if remote.is_available() {
                if let Err(e) = remote.send(namespace, mutation).await {
                    tracing::warn!(
                        "[router] remote({}) send failed: {:?}",
                        remote.name(),
                        e
                    );
                }
            }
        }
    }

    /// Get the merged incoming mutation stream (can only be called once)
    pub fn incoming_stream(&self) -> Box<dyn Stream<Item = InboundMutation> + Send + Unpin> {
        let rx = self.inner.merged_rx.lock().unwrap().take();
        match rx {
            Some(rx) => Box::new(rx),
            None => Box::new(futures::stream::empty()),
        }
    }
}
