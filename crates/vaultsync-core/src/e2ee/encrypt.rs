use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crate::error::VaultSyncError;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);
static NONCE_PREFIX: OnceLock<[u8; 4]> = OnceLock::new();

fn get_unique_nonce() -> [u8; 12] {
    let prefix = NONCE_PREFIX.get_or_init(|| {
        let mut p = [0u8; 4];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut p);
        p
    });
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::SeqCst);
    if counter == u64::MAX {
        panic!("Nonce counter overflow");
    }
    let mut nonce = [0u8; 12];
    nonce[..4].copy_from_slice(prefix);
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

pub fn encrypt(
    plaintext: &[u8],
    sk: &[u8; 32],
    recipient_pk: &[u8; 32],
) -> Result<Vec<u8>, VaultSyncError> {
    let shared_secret = x25519_dalek::x25519(*sk, *recipient_pk);
    encrypt_with_shared_secret(plaintext, &shared_secret)
}

pub fn encrypt_with_shared_secret(
    plaintext: &[u8],
    shared_secret: &[u8; 32],
) -> Result<Vec<u8>, VaultSyncError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(shared_secret));
    
    let nonce_bytes = get_unique_nonce();
    let nonce = Nonce::from_slice(&nonce_bytes);
    
    let ciphertext = cipher.encrypt(nonce, plaintext)
        .map_err(|e| VaultSyncError::Encryption(format!("encrypt failed: {e}")))?;
        
    let mut payload = Vec::with_capacity(12 + ciphertext.len());
    payload.extend_from_slice(&nonce_bytes);
    payload.extend_from_slice(&ciphertext);
    Ok(payload)
}
