use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crate::error::DriftError;

pub fn decrypt(
    ciphertext: &[u8],
    sk: &[u8; 32],
    sender_pk: &[u8; 32],
) -> Result<Vec<u8>, DriftError> {
    let shared_secret = x25519_dalek::x25519(*sk, *sender_pk);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&shared_secret));
    let nonce = Nonce::from_slice(&[0u8; 12]);
    cipher.decrypt(nonce, ciphertext)
        .map_err(|e| DriftError::Encryption(format!("decrypt failed: {e}")))
}
