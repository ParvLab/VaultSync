use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmIPC;

#[wasm_bindgen]
impl WasmIPC {
    pub fn new(_channel: &str) -> Self { Self }
    pub fn send(&self, _msg: &str) { }
    pub fn receive(&self) -> String { String::new() }
}
