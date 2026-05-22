use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

pub mod client;
pub mod storage;
pub mod ipc;
pub mod e2ee;
pub mod transport;
