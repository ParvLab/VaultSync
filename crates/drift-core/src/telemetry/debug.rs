use serde_json::Value;

pub struct DebugApi;

impl DebugApi {
    pub fn new() -> Self { Self }
    pub fn get_state(&self) -> Value { serde_json::json!({"status": "ok"}) }
}
