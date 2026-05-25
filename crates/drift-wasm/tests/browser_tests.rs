#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;
use drift_wasm::client::WasmDriftClient;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_wasm_drift_client_insert_get_update_delete() {
    let client = WasmDriftClient::new("test_ns", "replica_1")
        .await
        .expect("Failed to create WasmDriftClient");

    let doc_id = "doc_1";
    let record_id = "rec_1";
    let initial_json = r#"{"name": "Alice", "age": 30, "is_admin": true}"#;

    // 1. Insert document
    client.insert(doc_id, record_id, initial_json)
        .await
        .expect("Failed to insert document");

    // 2. Get document
    let value = client.get(doc_id, record_id)
        .await
        .expect("Failed to get document");
    
    assert!(!value.is_null(), "Expected non-null value");
    let val_str = value.as_string().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&val_str).unwrap();
    assert_eq!(parsed["name"], "Alice");
    assert_eq!(parsed["age"], 30.0);
    assert_eq!(parsed["is_admin"], true);

    // 3. Update document
    let updated_json = r#"{"name": "Bob", "age": 31, "is_admin": false}"#;
    client.update(doc_id, record_id, updated_json)
        .await
        .expect("Failed to update document");

    // 4. Get updated document
    let value_updated = client.get(doc_id, record_id)
        .await
        .expect("Failed to get updated document");
    let val_str_updated = value_updated.as_string().unwrap();
    let parsed_updated: serde_json::Value = serde_json::from_str(&val_str_updated).unwrap();
    assert_eq!(parsed_updated["name"], "Bob");
    assert_eq!(parsed_updated["age"], 31.0);
    assert_eq!(parsed_updated["is_admin"], false);

    // 5. Delete document
    client.delete(doc_id, record_id)
        .await
        .expect("Failed to delete document");

    // 6. Get deleted document (should be null)
    let value_deleted = client.get(doc_id, record_id)
        .await
        .expect("Failed to get deleted document");
    assert!(value_deleted.is_null(), "Expected document to be deleted (null)");

    // 7. Shutdown
    client.shutdown()
        .await
        .expect("Failed to shutdown client");
}
