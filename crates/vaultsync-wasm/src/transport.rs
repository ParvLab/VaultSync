use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, MessageEvent, WebSocket};

#[wasm_bindgen]
pub struct WasmTransport {
    ws: WebSocket,
    on_message_callback: Arc<Mutex<Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>>>,
}

#[wasm_bindgen]
impl WasmTransport {
    pub fn connect(url: &str) -> Result<WasmTransport, JsValue> {
        let ws = WebSocket::new(url)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let on_message_callback: Arc<Mutex<Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>>> =
            Arc::new(Mutex::new(None));
        let callback_clone = on_message_callback.clone();

        let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(ab) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&ab);
                let vec = array.to_vec();
                let lock = callback_clone.lock().unwrap();
                if let Some(ref cb) = *lock {
                    cb(vec);
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
        onmessage_callback.forget();

        Ok(Self {
            ws,
            on_message_callback,
        })
    }

    pub fn send(&self, data: &[u8]) -> Result<(), JsValue> {
        self.ws.send_with_u8_array(data)?;
        Ok(())
    }

    pub fn close(&self) -> Result<(), JsValue> {
        self.ws.close()?;
        Ok(())
    }
}

impl WasmTransport {
    pub fn on_message<F>(&self, callback: F)
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        let mut lock = self.on_message_callback.lock().unwrap();
        *lock = Some(Box::new(callback));
    }
}
