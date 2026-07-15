use std::collections::HashMap;
use vaultsync_core::crdt::types::CrdtValue;

/// Serialize CRDT fields to a JSON string for BC message dispatch.
pub(crate) fn fields_to_json_string(fields: &HashMap<String, CrdtValue>) -> Result<String, serde_json::Error> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        let json_val = match v {
            CrdtValue::String(s) => serde_json::Value::String(s.clone()),
            CrdtValue::Number(n) => serde_json::Value::Number(
                serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            CrdtValue::Boolean(b) => serde_json::Value::Bool(*b),
            CrdtValue::Null => serde_json::Value::Null,
            _ => serde_json::Value::Null,
        };
        map.insert(k.clone(), json_val);
    }
    serde_json::to_string(&serde_json::Value::Object(map))
}
