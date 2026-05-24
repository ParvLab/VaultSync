use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Sha256, Digest};
use crate::error::DriftError;

#[derive(Debug, Clone)]
pub struct NamespaceKeypair {
    pub public_key: [u8; 32],
    pub private_key: [u8; 32],
    pub version: u64,
    pub created_at: u64,
}

pub struct KeyRing {
    keys: Vec<NamespaceKeypair>,
    active_version: u64,
}

impl KeyRing {
    pub fn generate() -> Self {
        let keypair = generate_keypair(1);
        Self {
            keys: vec![keypair],
            active_version: 1,
        }
    }

    pub fn add_key(&mut self, key: NamespaceKeypair) {
        if key.version > self.active_version {
            self.active_version = key.version;
        }
        self.keys.push(key);
    }

    pub fn active_key(&self) -> &NamespaceKeypair {
        self.keys.iter()
            .find(|k| k.version == self.active_version)
            .expect("active key always present")
    }

    pub fn key_by_version(&self, version: u64) -> Option<&NamespaceKeypair> {
        self.keys.iter().find(|k| k.version == version)
    }

    pub fn rotate(&mut self) -> NamespaceKeypair {
        let new_version = self.active_version + 1;
        let keypair = generate_keypair(new_version);
        self.active_version = new_version;
        self.keys.push(keypair.clone());
        keypair
    }

    pub fn encrypt_private_key(&self, device_key: &[u8]) -> Result<Vec<u8>, DriftError> {
        let active = self.active_key();
        let key = derive_encryption_key(device_key);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(&[0u8; 12]);
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(&active.private_key);
        payload.extend_from_slice(&active.public_key);
        cipher.encrypt(nonce, payload.as_ref())
            .map_err(|e| DriftError::Encryption(format!("key encryption failed: {e}")))
    }

    pub fn decrypt_private_key(encrypted: &[u8], device_key: &[u8]) -> Result<NamespaceKeypair, DriftError> {
        let key = derive_encryption_key(device_key);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(&[0u8; 12]);
        let plaintext = cipher.decrypt(nonce, encrypted)
            .map_err(|e| DriftError::Encryption(format!("key decryption failed: {e}")))?;

        if plaintext.len() != 64 {
            return Err(DriftError::Encryption("invalid decrypted key length".into()));
        }

        let mut private_key = [0u8; 32];
        let mut public_key = [0u8; 32];
        private_key.copy_from_slice(&plaintext[..32]);
        public_key.copy_from_slice(&plaintext[32..]);

        Ok(NamespaceKeypair {
            public_key,
            private_key,
            version: 0,
            created_at: now_secs(),
        })
    }

    pub fn active_version(&self) -> u64 {
        self.active_version
    }
}

pub struct E2eeEncryptor {
    keyring: Arc<KeyRing>,
}

impl E2eeEncryptor {
    pub fn new(keyring: Arc<KeyRing>) -> Self {
        Self { keyring }
    }

    pub fn encrypt(&self, plaintext: &[u8], recipient_pk: &[u8; 32]) -> Result<Vec<u8>, DriftError> {
        super::encrypt::encrypt(plaintext, &self.keyring.active_key().private_key, recipient_pk)
    }

    pub fn keyring(&self) -> &KeyRing {
        &self.keyring
    }
}

pub struct E2eeDecryptor {
    keyring: Arc<KeyRing>,
}

impl E2eeDecryptor {
    pub fn new(keyring: Arc<KeyRing>) -> Self {
        Self { keyring }
    }

    pub fn decrypt(&self, ciphertext: &[u8], sender_pk: &[u8; 32]) -> Result<Vec<u8>, DriftError> {
        super::decrypt::decrypt(ciphertext, &self.keyring.active_key().private_key, sender_pk)
    }

    pub fn keyring(&self) -> &KeyRing {
        &self.keyring
    }
}

fn generate_keypair(version: u64) -> NamespaceKeypair {
    let mut private_key = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut private_key);
    clamp_scalar(&mut private_key);
    let public_key = x25519_dalek::x25519(private_key, x25519_dalek::X25519_BASEPOINT_BYTES);
    NamespaceKeypair {
        public_key,
        private_key,
        version,
        created_at: now_secs(),
    }
}

fn clamp_scalar(scalar: &mut [u8; 32]) {
    scalar[0] &= 248;
    scalar[31] &= 127;
    scalar[31] |= 64;
}

fn derive_encryption_key(device_key: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"drift-key-derivation-v1");
    hasher.update(device_key);
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
