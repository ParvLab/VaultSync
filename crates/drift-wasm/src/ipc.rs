use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent};
use std::sync::{Arc, Mutex};

#[wasm_bindgen]
pub struct WasmIPC {
    channel: BroadcastChannel,
    last_received: Arc<Mutex<String>>,
}

#[wasm_bindgen]
impl WasmIPC {
    pub fn new(channel_name: &str) -> Result<WasmIPC, JsValue> {
        let channel = BroadcastChannel::new(channel_name)?;
        let last_received = Arc::new(Mutex::new(String::new()));
        let last_received_clone = last_received.clone();

        let onmessage = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Some(msg_str) = e.data().as_string() {
                let mut lock = last_received_clone.lock().unwrap();
                *lock = msg_str;
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        channel.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        Ok(Self {
            channel,
            last_received,
        })
    }

    pub fn send(&self, msg: &str) -> Result<(), JsValue> {
        self.channel.post_message(&JsValue::from_str(msg))?;
        Ok(())
    }

    pub fn receive(&self) -> String {
        let mut lock = self.last_received.lock().unwrap();
        let val = lock.clone();
        *lock = String::new(); // Clear after reading
        val
    }

    pub fn on_message(&self, callback: js_sys::Function) {
        let last_received_clone = self.last_received.clone();
        let closure = Closure::wrap(Box::new(move |e: MessageEvent| {
            let data = e.data();
            if let Some(msg_str) = data.as_string() {
                let mut lock = last_received_clone.lock().unwrap();
                *lock = msg_str;
            }
            let _ = callback.call1(&JsValue::NULL, &data);
        }) as Box<dyn FnMut(MessageEvent)>);
        self.channel.set_onmessage(Some(closure.as_ref().unchecked_ref()));
        closure.forget();
    }
}
