//! WebRTC P2P transport — WASM (browser) only.
//!
//! Native targets are stubbed and will return `CoordinatorError::NotSupported` for all operations.

use async_trait::async_trait;
use futures::Stream;
use vaultsync_core::coordinator::traits::*;

#[cfg(target_arch = "wasm32")]
use futures::Future;

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WebRtcConfig {
    pub ice_servers: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct PeerCoordinator {
    _config: WebRtcConfig,
}

#[cfg(not(target_arch = "wasm32"))]
impl PeerCoordinator {
    pub fn new(config: WebRtcConfig) -> Self {
        Self { _config: config }
    }

    pub async fn set_signal_handler<F>(&self, _handler: F)
    where
        F: Fn(String, String, String) + Send + Sync + 'static,
    {
    }

    pub async fn initiate_connection(&self, _peer_id: &str) -> Result<(), String> {
        Err("WebRTC P2P transport is only supported in WASM (browser) targets. Use libp2p for native.".into())
    }

    pub async fn handle_signaling_message(
        &self,
        _sender_id: &str,
        _signal_type: &str,
        _data: &str,
    ) -> Result<(), String> {
        Err("WebRTC P2P transport is only supported in WASM (browser) targets. Use libp2p for native.".into())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Coordinator for PeerCoordinator {
    async fn push(
        &self,
        _namespace: &str,
        _mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }

    async fn pull(
        &self,
        _namespace: &str,
        _after: SequenceId,
        _limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }

    async fn subscribe(
        &self,
        _namespace: &str,
        _from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }

    async fn register(&self, _namespace: &str, _info: ReplicaInfo) -> Result<(), CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Err(CoordinatorError::NotSupported(
            "WebRTC P2P transport is only supported in WASM (browser) targets. \
             For server-to-server P2P, use vaultsync-transport-libp2p instead."
                .into(),
        ))
    }
}

// -----------------------------------------------------------------------------
// WebAssembly (Browser-side) WebRTC implementation
// -----------------------------------------------------------------------------
#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct PeerCoordinator {
    inner: Arc<tokio::sync::Mutex<WasmPeerCoordinatorInner>>,
}

#[cfg(target_arch = "wasm32")]
impl std::fmt::Debug for PeerCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerCoordinator").finish()
    }
}

#[cfg(target_arch = "wasm32")]
struct WasmPeerCoordinatorInner {
    config: WebRtcConfig,
    replica_id: String,
    namespace: String,
    peers: std::collections::HashMap<String, PeerConnectionState>,
    mutation_tx: tokio::sync::broadcast::Sender<PendingMutation>,
    signal_tx: Option<Arc<dyn Fn(String, String, String) + Send + Sync + 'static>>,
}

#[cfg(target_arch = "wasm32")]
struct PeerConnectionState {
    pc: web_sys::RtcPeerConnection,
    dc: Option<web_sys::RtcDataChannel>,
}

#[cfg(target_arch = "wasm32")]
impl PeerCoordinator {
    pub fn new(config: WebRtcConfig) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(100);
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(WasmPeerCoordinatorInner {
                config,
                replica_id: String::new(),
                namespace: String::new(),
                peers: std::collections::HashMap::new(),
                mutation_tx: tx,
                signal_tx: None,
            })),
        }
    }

    pub async fn set_signal_handler<F>(&self, handler: F)
    where
        F: Fn(String, String, String) + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().await;
        inner.signal_tx = Some(Arc::new(handler));
    }

    pub async fn initiate_connection(&self, peer_id: &str) -> Result<(), String> {
        use wasm_bindgen::JsCast;
        let inner = self.inner.clone();
        let mut guard = inner.lock().await;

        if guard.peers.contains_key(peer_id) {
            return Ok(());
        }

        let pc = self.create_peer_connection(&mut guard, peer_id)?;
        let dc = pc.create_data_channel("vaultsync-sync-channel");

        let inner_clone = self.inner.clone();
        let onmessage = Closure::wrap(Box::new(move |msg_ev: web_sys::MessageEvent| {
            if let Ok(txt) = msg_ev.data().dyn_into::<js_sys::JsString>() {
                let s: String = txt.into();
                if let Ok(mutation) = serde_json::from_str::<PendingMutation>(&s) {
                    let inner_guard = inner_clone.try_lock();
                    if let Ok(g) = inner_guard {
                        let _ = g.mutation_tx.send(mutation);
                    }
                }
            }
        }) as Box<dyn Fn(web_sys::MessageEvent)>);
        dc.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        guard.peers.insert(
            peer_id.to_string(),
            PeerConnectionState {
                pc: pc.clone(),
                dc: Some(dc.clone()),
            },
        );

        drop(guard);

        let promise_offer = pc.create_offer();
        let offer = vaultsync_core::time_utils::SendJsFuture::from(promise_offer)
            .await
            .map_err(|e| format!("Failed to create offer: {:?}", e))?;

        let offer_sdp = offer
            .dyn_into::<web_sys::RtcSessionDescription>()
            .map_err(|_| "Failed to cast to RtcSessionDescription".to_string())?;

        let mut sdp_init = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Offer);
        sdp_init.sdp(&offer_sdp.sdp());

        let promise_local = pc.set_local_description(&sdp_init);
        vaultsync_core::time_utils::SendJsFuture::from(promise_local)
            .await
            .map_err(|e| format!("Failed to set local description: {:?}", e))?;

        self.send_signal(peer_id, "offer", &offer_sdp.sdp()).await?;
        Ok(())
    }

    pub async fn handle_signaling_message(
        &self,
        sender_id: &str,
        signal_type: &str,
        data: &str,
    ) -> Result<(), String> {
        let inner = self.inner.clone();
        let mut guard = inner.lock().await;

        let pc = if let Some(state) = guard.peers.get(sender_id) {
            state.pc.clone()
        } else {
            // Create new peer connection
            let pc = self.create_peer_connection(&mut guard, sender_id)?;
            guard.peers.insert(
                sender_id.to_string(),
                PeerConnectionState {
                    pc: pc.clone(),
                    dc: None,
                },
            );
            pc
        };

        drop(guard);

        if signal_type == "offer" {
            let mut sdp = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Offer);
            sdp.sdp(data);

            let promise = pc.set_remote_description(&sdp);
            vaultsync_core::time_utils::SendJsFuture::from(promise)
                .await
                .map_err(|e| format!("Failed to set remote description: {:?}", e))?;

            let promise_ans = pc.create_answer();
            let ans = vaultsync_core::time_utils::SendJsFuture::from(promise_ans)
                .await
                .map_err(|e| format!("Failed to create answer: {:?}", e))?;

            let ans_sdp = ans
                .dyn_into::<web_sys::RtcSessionDescription>()
                .map_err(|_| "Failed to cast to RtcSessionDescription".to_string())?;

            let mut sdp_init = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Answer);
            sdp_init.sdp(&ans_sdp.sdp());

            let promise_local = pc.set_local_description(&sdp_init);
            vaultsync_core::time_utils::SendJsFuture::from(promise_local)
                .await
                .map_err(|e| format!("Failed to set local description: {:?}", e))?;

            // Send answer back to sender (relayed via coordinator signaling)
            self.send_signal(sender_id, "answer", &ans_sdp.sdp())
                .await?;
        } else if signal_type == "answer" {
            let mut sdp = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Answer);
            sdp.sdp(data);
            let promise = pc.set_remote_description(&sdp);
            vaultsync_core::time_utils::SendJsFuture::from(promise)
                .await
                .map_err(|e| format!("Failed to set remote description: {:?}", e))?;
        } else if signal_type == "candidate" {
            let mut candidate_init = web_sys::RtcIceCandidateInit::new(data);
            candidate_init.sdp_m_line_index(Some(0));
            let candidate = web_sys::RtcIceCandidate::new(&candidate_init)
                .map_err(|e| format!("Failed to create ICE candidate: {:?}", e))?;
            let promise = pc.add_ice_candidate_with_opt_rtc_ice_candidate(Some(&candidate));
            vaultsync_core::time_utils::SendJsFuture::from(promise)
                .await
                .map_err(|e| format!("Failed to add ICE candidate: {:?}", e))?;
        }

        Ok(())
    }

    fn create_peer_connection(
        &self,
        inner: &mut WasmPeerCoordinatorInner,
        peer_id: &str,
    ) -> Result<web_sys::RtcPeerConnection, String> {
        let mut rtc_config = web_sys::RtcConfiguration::new();
        let ice_servers_json = serde_json::to_string(&inner.config.ice_servers).unwrap_or_default();
        rtc_config.ice_servers(&JsValue::from_str(&ice_servers_json));

        let pc = web_sys::RtcPeerConnection::new_with_configuration(&rtc_config)
            .map_err(|e| format!("Failed to create RtcPeerConnection: {:?}", e))?;

        // ICE Candidate Callback
        let this_clone = self.clone();
        let peer_id_str = peer_id.to_string();
        let onicecandidate =
            Closure::wrap(Box::new(move |ev: web_sys::RtcPeerConnectionIceEvent| {
                if let Some(candidate) = ev.candidate() {
                    let this = this_clone.clone();
                    let peer_id = peer_id_str.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        let _ = this
                            .send_signal(&peer_id, "candidate", &candidate.candidate())
                            .await;
                    });
                }
            })
                as Box<dyn Fn(web_sys::RtcPeerConnectionIceEvent)>);

        pc.set_onicecandidate(Some(onicecandidate.as_ref().unchecked_ref()));
        onicecandidate.forget();

        // Data Channel Callback for receiver
        let inner_clone = self.inner.clone();
        let ondatachannel = Closure::wrap(Box::new(move |ev: web_sys::RtcDataChannelEvent| {
            let dc = ev.channel();
            let inner = inner_clone.clone();

            let onmessage = Closure::wrap(Box::new(move |msg_ev: web_sys::MessageEvent| {
                if let Ok(txt) = msg_ev.data().dyn_into::<js_sys::JsString>() {
                    let s: String = txt.into();
                    if let Ok(mutation) = serde_json::from_str::<PendingMutation>(&s) {
                        let inner_guard = inner.try_lock();
                        if let Ok(g) = inner_guard {
                            let _ = g.mutation_tx.send(mutation);
                        }
                    }
                }
            }) as Box<dyn Fn(web_sys::MessageEvent)>);

            dc.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
            onmessage.forget();
        })
            as Box<dyn Fn(web_sys::RtcDataChannelEvent)>);

        pc.set_ondatachannel(Some(ondatachannel.as_ref().unchecked_ref()));
        ondatachannel.forget();

        Ok(pc)
    }

    async fn send_signal(
        &self,
        target_id: &str,
        signal_type: &str,
        data: &str,
    ) -> Result<(), String> {
        let inner = self.inner.lock().await;
        if let Some(ref cb) = inner.signal_tx {
            cb(
                target_id.to_string(),
                signal_type.to_string(),
                data.to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait]
impl Coordinator for PeerCoordinator {
    async fn push(
        &self,
        _namespace: &str,
        mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        let inner = self.inner.lock().await;
        // In WebRTC direct push, we broadcast our local mutations to all connected replica channels
        // For simplicity, we convert to PendingMutation (which is decrypted/applied on peer)
        // Or directly sync. Here we serialize mutations and send them over open data channels.
        for state in inner.peers.values() {
            if let Some(ref dc) = state.dc {
                if dc.ready_state() == web_sys::RtcDataChannelState::Open {
                    for m in &mutations {
                        let pm = PendingMutation {
                            id: m.id.clone(),
                            namespace: m.namespace.clone(),
                            sequence: 0, // transient
                            doc_id: m.doc_id.clone(),
                            record_id: m.record_id.clone(),
                            encrypted_blob: m.encrypted_blob.clone(),
                            timestamp: m.timestamp,
                            key_version: m.key_version,
                            replica_id: m.replica_id.clone(),
                        };
                        if let Ok(s) = serde_json::from_str::<serde_json::Value>(
                            &serde_json::to_string(&pm).unwrap(),
                        ) {
                            let _ = dc.send_with_str(&s.to_string());
                        }
                    }
                }
            }
        }
        Ok(vec![])
    }

    async fn pull(
        &self,
        _namespace: &str,
        _after: SequenceId,
        _limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        Ok(vec![])
    }

    async fn subscribe(
        &self,
        _namespace: &str,
        _from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let inner = self.inner.lock().await;
        let rx = inner.mutation_tx.subscribe();
        Ok(Box::new(WasmWebRtcSubscription { rx }))
    }

    async fn register(&self, namespace: &str, info: ReplicaInfo) -> Result<(), CoordinatorError> {
        let mut inner = self.inner.lock().await;
        inner.replica_id = info.replica_id;
        inner.namespace = namespace.to_string();
        Ok(())
    }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> {
        Ok(())
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(0)
    }
}

#[cfg(target_arch = "wasm32")]
struct WasmWebRtcSubscription {
    rx: tokio::sync::broadcast::Receiver<PendingMutation>,
}

#[cfg(target_arch = "wasm32")]
impl Stream for WasmWebRtcSubscription {
    type Item = PendingMutation;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Pin projection helper for Receiver
        let fut = self.rx.recv();
        tokio::pin!(fut);
        match fut.poll(cx) {
            std::task::Poll::Ready(Ok(m)) => std::task::Poll::Ready(Some(m)),
            _ => std::task::Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_native_peer_coordinator_returns_not_supported() {
        let config = WebRtcConfig {
            ice_servers: vec!["stun:stun.l.google.com:19302".to_string()],
        };
        let coord = PeerCoordinator::new(config);

        let push_res = coord.push("test", vec![]).await;
        assert!(matches!(push_res, Err(CoordinatorError::NotSupported(_))));

        let pull_res = coord.pull("test", 0, 10).await;
        assert!(matches!(pull_res, Err(CoordinatorError::NotSupported(_))));

        let sub_res = coord.subscribe("test", 0).await;
        assert!(matches!(sub_res, Err(CoordinatorError::NotSupported(_))));

        let reg_res = coord
            .register(
                "test",
                ReplicaInfo {
                    replica_id: "test".into(),
                    namespace: "test".into(),
                    public_key: vec![],
                    schema_version: 1,
                },
            )
            .await;
        assert!(matches!(reg_res, Err(CoordinatorError::NotSupported(_))));

        let init_res = coord.initiate_connection("peer").await;
        assert!(init_res.is_err());

        let sig_res = coord
            .handle_signaling_message("peer", "offer", "data")
            .await;
        assert!(sig_res.is_err());
    }
}
