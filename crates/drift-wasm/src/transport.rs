use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmTransport;

#[wasm_bindgen]
impl WasmTransport {
    pub fn connect(_url: &str) -> Self { Self }
    pub fn send(&self, _data: &[u8]) { }
    pub fn close(&self) { }
}
