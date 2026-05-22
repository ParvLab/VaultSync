use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmDriftClient;

#[wasm_bindgen]
impl WasmDriftClient {
    pub async fn new(_namespace: &str, _replica_id: &str) -> Self { Self }
    pub async fn insert(&self, _doc_id: &str, _record_id: &str, _json: &str) { }
    pub async fn update(&self, _doc_id: &str, _record_id: &str, _json: &str) { }
    pub async fn delete(&self, _doc_id: &str, _record_id: &str) { }
    pub async fn get(&self, _doc_id: &str, _record_id: &str) -> String { "null".to_string() }
    pub async fn shutdown(&self) { }
}
