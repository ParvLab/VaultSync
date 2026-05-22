use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub async fn encrypt(plaintext: &[u8], _key: &[u8]) -> Vec<u8> { plaintext.to_vec() }

#[wasm_bindgen]
pub async fn decrypt(ciphertext: &[u8], _key: &[u8]) -> Vec<u8> { ciphertext.to_vec() }
