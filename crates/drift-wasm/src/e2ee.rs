use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn encrypt(plaintext: &[u8], sender_sk: &[u8], recipient_pk: &[u8]) -> Result<Vec<u8>, JsValue> {
    if sender_sk.len() != 32 || recipient_pk.len() != 32 {
        return Err(JsValue::from_str("Invalid key lengths"));
    }
    let mut sk = [0u8; 32];
    let mut pk = [0u8; 32];
    sk.copy_from_slice(sender_sk);
    pk.copy_from_slice(recipient_pk);

    drift_core::e2ee::encrypt::encrypt(plaintext, &sk, &pk)
        .map_err(|e| JsValue::from_str(&format!("Encryption failed: {:?}", e)))
}

#[wasm_bindgen]
pub fn decrypt(ciphertext: &[u8], recipient_sk: &[u8], sender_pk: &[u8]) -> Result<Vec<u8>, JsValue> {
    if recipient_sk.len() != 32 || sender_pk.len() != 32 {
        return Err(JsValue::from_str("Invalid key lengths"));
    }
    let mut sk = [0u8; 32];
    let mut pk = [0u8; 32];
    sk.copy_from_slice(recipient_sk);
    pk.copy_from_slice(sender_pk);

    drift_core::e2ee::decrypt::decrypt(ciphertext, &sk, &pk)
        .map_err(|e| JsValue::from_str(&format!("Decryption failed: {:?}", e)))
}
