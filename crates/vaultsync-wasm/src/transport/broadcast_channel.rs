use async_trait::async_trait;
use futures::channel::mpsc;
use tracing::debug;
use futures::Stream;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use vaultsync_core::coordinator::traits::{EncryptedMutation, PendingMutation};
use vaultsync_core::transport::traits::{
    InboundMutation, Transport, TransportError, TransportSource,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log_str(s: &str);
}

macro_rules! log {
    ($($t:tt)*) => (console_log_str(&format!($($t)*)));
}

/// BroadcastChannel transport for same-origin tab-to-tab communication
///
/// - Sends mutations as plaintext JSON (no encryption — same origin)
/// - Receives mutations via `onmessage` callback
/// - Always available (no connection lifecycle)
#[derive(Debug)]
pub struct BroadcastChannelTransport {
    channel_name: String,
    bc: BroadcastChannel,
    listeners: Mutex<Vec<mpsc::Sender<InboundMutation>>>,
}

impl BroadcastChannelTransport {
    /// Create a new BroadcastChannel transport
    ///
    /// The channel name is `"vaultsync::<namespace>"`.
    /// Different namespaces get different channels (isolation).
    pub fn new(namespace: &str) -> Result<Self, JsValue> {
        let channel_name = format!("vaultsync::{}", namespace);
        let bc = BroadcastChannel::new(&channel_name)?;

        let transport = Self {
            channel_name,
            bc,
            listeners: Mutex::new(Vec::new()),
        };

        debug!("[BC] created channel={}", transport.channel_name);
        Ok(transport)
    }

    fn handle_message(&self, data: &[u8]) {
        if let Ok(mutation) = serde_json::from_slice::<PendingMutation>(data) {
            let inbound = InboundMutation {
                source: TransportSource::BroadcastChannel,
                mutation,
                server_seq: None,
            };
            let mut listeners = self.listeners.lock().unwrap();
            listeners.retain_mut(|tx| tx.try_send(inbound.clone()).is_ok());
        }
    }
}

#[async_trait]
impl Transport for BroadcastChannelTransport {
    async fn send(
        &self,
        _namespace: &str,
        mutation: &EncryptedMutation,
    ) -> Result<(), TransportError> {
        // BC uses PendingMutation format (plaintext, no server sequence)
        let pending = PendingMutation {
            id: mutation.id.clone(),
            namespace: mutation.namespace.clone(),
            sequence: 0,
            doc_id: mutation.doc_id.clone(),
            record_id: mutation.record_id.clone(),
            encrypted_blob: mutation.encrypted_blob.clone(),
            timestamp: mutation.timestamp,
            key_version: mutation.key_version,
            replica_id: mutation.replica_id.clone(),
        };
        let json = serde_json::to_vec(&pending)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let array = js_sys::Uint8Array::from(&json[..]);
        self.bc
            .post_message(&array)
            .map_err(|e| TransportError::SendFailed(format!("{:?}", e)))?;
        Ok(())
    }

    async fn incoming(
        &self,
        _namespace: &str,
    ) -> Result<Box<dyn Stream<Item = InboundMutation> + Send + Unpin>, TransportError> {
        let (tx, rx) = mpsc::channel::<InboundMutation>(128);

        let bc_clone = self.bc.clone();
        let listener_tx = Rc::new(RefCell::new(tx.clone()));

        let callback = Closure::wrap(Box::new(move |event: MessageEvent| {
            if let Ok(ab) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&ab);
                let vec = array.to_vec();

                if let Ok(mutation) = serde_json::from_slice::<PendingMutation>(&vec) {
                    let inbound = InboundMutation {
                        source: TransportSource::BroadcastChannel,
                        mutation,
                        server_seq: None,
                    };
                    let _ = listener_tx.borrow_mut().try_send(inbound);
                }
            }
        }) as Box<dyn Fn(MessageEvent)>);

        bc_clone.set_onmessage(Some(callback.as_ref().unchecked_ref()));
        callback.forget();

        self.listeners.lock().unwrap().push(tx);

        Ok(Box::new(rx))
    }

    fn is_available(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "BroadcastChannel"
    }
}

impl Drop for BroadcastChannelTransport {
    fn drop(&mut self) {
        self.bc.set_onmessage(None);
        let _ = self.bc.close();
    }
}
