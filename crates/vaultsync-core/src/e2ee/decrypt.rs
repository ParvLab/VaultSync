use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crate::error::VaultSyncError;

pub fn decrypt(
    ciphertext: &[u8],
    sk: &[u8; 32],
    sender_pk: &[u8; 32],
) -> Result<Vec<u8>, VaultSyncError> {
    let shared_secret = x25519_dalek::x25519(*sk, *sender_pk);
    decrypt_with_shared_secret(ciphertext, &shared_secret)
}

pub fn decrypt_with_shared_secret(
    ciphertext: &[u8],
    shared_secret: &[u8; 32],
) -> Result<Vec<u8>, VaultSyncError> {
    if ciphertext.len() < 12 {
        return Err(VaultSyncError::Encryption("invalid ciphertext length (too short)".into()));
    }
    let cipher = ChaCha20Poly1305::new(Key::from_slice(shared_secret));
    
    let nonce_bytes = &ciphertext[..12];
    let actual_ciphertext = &ciphertext[12..];
    
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher.decrypt(nonce, actual_ciphertext)
        .map_err(|e| VaultSyncError::Encryption(format!("decrypt failed: {e}")))
}
