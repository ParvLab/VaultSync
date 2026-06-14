#![cfg(target_arch = "wasm32")]

use vaultsync_wasm::client::WasmVaultSyncClient;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_wasm_vaultsync_client_insert_get_update_delete() {
    let client = WasmVaultSyncClient::new("test_ns", "replica_1", None, None)
        .await
        .expect("Failed to create WasmVaultSyncClient");

    let doc_id = "doc_1";
    let record_id = "rec_1";
    let initial_json = r#"{"name": "Alice", "age": 30, "is_admin": true}"#;

    // 1. Insert document
    client
        .insert(doc_id, record_id, initial_json)
        .await
        .expect("Failed to insert document");

    // 2. Get document
    let value = client
        .get(doc_id, record_id)
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
    client
        .update(doc_id, record_id, updated_json)
        .await
        .expect("Failed to update document");

    // 4. Get updated document
    let value_updated = client
        .get(doc_id, record_id)
        .await
        .expect("Failed to get updated document");
    let val_str_updated = value_updated.as_string().unwrap();
    let parsed_updated: serde_json::Value = serde_json::from_str(&val_str_updated).unwrap();
    assert_eq!(parsed_updated["name"], "Bob");
    assert_eq!(parsed_updated["age"], 31.0);
    assert_eq!(parsed_updated["is_admin"], false);

    // 5. Delete document
    client
        .delete(doc_id, record_id)
        .await
        .expect("Failed to delete document");

    // 6. Get deleted document (should be null)
    let value_deleted = client
        .get(doc_id, record_id)
        .await
        .expect("Failed to get deleted document");
    assert!(
        value_deleted.is_null(),
        "Expected document to be deleted (null)"
    );

    // 7. Shutdown
    client.shutdown().await.expect("Failed to shutdown client");
}

#[wasm_bindgen_test]
async fn test_find_returns_all_records() {
    let client = WasmVaultSyncClient::new("test_ns_find", "replica_find", None, None)
        .await
        .expect("Failed to create WasmVaultSyncClient");

    let doc_id = "doc_find";

    client
        .insert(doc_id, "rec_1", r#"{"val": 1}"#)
        .await
        .unwrap();
    client
        .insert(doc_id, "rec_2", r#"{"val": 2}"#)
        .await
        .unwrap();
    client
        .insert(doc_id, "rec_3", r#"{"val": 3}"#)
        .await
        .unwrap();

    let results = client.find(doc_id).await.expect("Failed to find records");
    assert_eq!(results.length(), 3);

    let mut found_vals = Vec::new();
    for i in 0..3 {
        let js_val = results.get(i);
        let val_str = js_val.as_string().expect("Expected string in find array");
        let parsed: serde_json::Value = serde_json::from_str(&val_str).unwrap();
        let rec_id = parsed["record_id"].as_str().unwrap().to_string();
        let val = parsed["val"].as_f64().unwrap() as i32;
        found_vals.push((rec_id, val));
    }

    found_vals.sort();
    assert_eq!(found_vals[0], ("rec_1".to_string(), 1));
    assert_eq!(found_vals[1], ("rec_2".to_string(), 2));
    assert_eq!(found_vals[2], ("rec_3".to_string(), 3));

    client.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn test_subscribe_fires_on_insert() {
    use std::sync::{Arc, Mutex};
    use wasm_bindgen::JsCast;

    let client = WasmVaultSyncClient::new("test_ns_sub", "replica_sub", None, None)
        .await
        .expect("Failed to create WasmVaultSyncClient");

    let doc_id = "doc_sub";
    let fired_data = Arc::new(Mutex::new(None));
    let fired_data_clone = fired_data.clone();

    let closure =
        wasm_bindgen::prelude::Closure::wrap(Box::new(move |record_id: JsValue, json: JsValue| {
            let rec_str = record_id.as_string().unwrap();
            let json_str = json.as_string().unwrap();
            let mut lock = fired_data_clone.lock().unwrap();
            *lock = Some((rec_str, json_str));
        }) as Box<dyn FnMut(JsValue, JsValue)>);

    let js_func = closure.as_ref().unchecked_ref::<js_sys::Function>().clone();

    let mut handle = client.subscribe(doc_id, js_func);

    client
        .insert(doc_id, "rec_sub_1", r#"{"hello": "world"}"#)
        .await
        .unwrap();

    let result = fired_data.lock().unwrap().take();
    assert!(result.is_some(), "Expected subscription callback to fire");

    let (rec_id, json_str) = result.unwrap();
    assert_eq!(rec_id, "rec_sub_1");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["hello"], "world");

    handle.cancel(&client).unwrap();

    client
        .insert(doc_id, "rec_sub_2", r#"{"hello": "again"}"#)
        .await
        .unwrap();

    let result_after = fired_data.lock().unwrap().take();
    assert!(
        result_after.is_none(),
        "Expected no callback after cancellation"
    );

    drop(closure);
    client.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn test_sync_status_returns_state() {
    let client = WasmVaultSyncClient::new("test_ns_sync", "replica_sync", None, None)
        .await
        .expect("Failed to create WasmVaultSyncClient");

    let status_val = client
        .sync_status()
        .await
        .expect("Failed to get sync status");
    assert!(!status_val.is_null());

    let status_str = status_val
        .as_string()
        .expect("Expected string return value");
    let parsed: serde_json::Value = serde_json::from_str(&status_str).unwrap();

    assert_eq!(parsed["namespace"], "test_ns_sync");
    assert_eq!(parsed["replica_id"], "replica_sync");
    let conn_status = parsed["connection_status"]
        .as_str()
        .expect("Expected connection_status string");
    assert!(
        conn_status == "Connected" || conn_status == "Disconnected" || conn_status == "Syncing",
        "Invalid connection status: {}",
        conn_status
    );

    client.shutdown().await.unwrap();
}
