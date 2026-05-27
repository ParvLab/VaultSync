use std::sync::Arc;
use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Sha256, Digest};
use hkdf::Hkdf;
use crate::error::DriftError;

#[derive(Debug, Clone)]
pub struct NamespaceKeypair {
    pub public_key: [u8; 32],
    pub private_key: [u8; 32],
    pub version: u64,
    pub created_at: u64,
}

pub struct KeyRing {
    inner: std::sync::RwLock<KeyRingInner>,
}

struct KeyRingInner {
    keys: Vec<NamespaceKeypair>,
    active_version: u64,
}

impl KeyRing {
    pub fn generate() -> Self {
        let keypair = generate_keypair(1);
        Self {
            inner: std::sync::RwLock::new(KeyRingInner {
                keys: vec![keypair],
                active_version: 1,
            }),
        }
    }

    pub fn from_key(key: NamespaceKeypair) -> Self {
        let version = key.version;
        Self {
            inner: std::sync::RwLock::new(KeyRingInner {
                keys: vec![key],
                active_version: version,
            }),
        }
    }

    pub fn add_key(&self, key: NamespaceKeypair) {
        let mut inner = self.inner.write().unwrap();
        if key.version > inner.active_version {
            inner.active_version = key.version;
        }
        inner.keys.push(key);
    }

    pub fn active_key(&self) -> NamespaceKeypair {
        let inner = self.inner.read().unwrap();
        inner.keys.iter()
            .find(|k| k.version == inner.active_version)
            .cloned()
            .expect("active key always present")
    }

    pub fn key_by_version(&self, version: u64) -> Option<NamespaceKeypair> {
        let inner = self.inner.read().unwrap();
        inner.keys.iter().find(|k| k.version == version).cloned()
    }

    pub fn rotate(&self) -> NamespaceKeypair {
        let mut inner = self.inner.write().unwrap();
        let new_version = inner.active_version + 1;
        let keypair = generate_keypair(new_version);
        inner.active_version = new_version;
        inner.keys.push(keypair.clone());
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
        let inner = self.inner.read().unwrap();
        inner.active_version
    }

    pub fn derive_namespace_key(&self, namespace: &str) -> [u8; 32] {
        let active = self.active_key();
        let hk = Hkdf::<Sha256>::new(Some(b"drift-namespace"), &active.private_key);
        let mut okm = [0u8; 32];
        hk.expand(namespace.as_bytes(), &mut okm)
            .expect("32 bytes is a valid length for HKDF output");
        okm
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

    pub fn encrypt_symmetric(&self, plaintext: &[u8], namespace: &str) -> Result<Vec<u8>, DriftError> {
        let start = crate::time_utils::PlatformInstant::now();
        let active_version = self.keyring.active_version();
        let span = tracing::info_span!("e2ee.encrypt", namespace = namespace, key_version = active_version);
        let _enter = span.enter();

        let key = self.keyring.derive_namespace_key(namespace);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        
        let mut nonce_bytes = [0u8; 12];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        
        let mut ciphertext = cipher.encrypt(nonce, plaintext)
            .map_err(|e| DriftError::Encryption(format!("symmetric encrypt failed: {e}")))?;

        let mut result = nonce_bytes.to_vec();
        result.append(&mut ciphertext);

        let duration_us = start.elapsed().as_micros() as f64;
        crate::telemetry::metrics::get_metrics().record_encryption_time("encrypt", duration_us);
        Ok(result)
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

    pub fn decrypt_symmetric(&self, ciphertext: &[u8], namespace: &str) -> Result<Vec<u8>, DriftError> {
        let start = crate::time_utils::PlatformInstant::now();
        let active_version = self.keyring.active_version();
        let span = tracing::info_span!("e2ee.decrypt", namespace = namespace, key_version = active_version);
        let _enter = span.enter();

        if ciphertext.len() < 12 {
            return Err(DriftError::Encryption("invalid ciphertext length (missing nonce)".into()));
        }
        let nonce_bytes = &ciphertext[..12];
        let actual_ciphertext = &ciphertext[12..];

        let key = self.keyring.derive_namespace_key(namespace);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(nonce_bytes);
        let res = cipher.decrypt(nonce, actual_ciphertext)
            .map_err(|e| DriftError::Encryption(format!("symmetric decrypt failed: {e}")));

        if res.is_ok() {
            let duration_us = start.elapsed().as_micros() as f64;
            crate::telemetry::metrics::get_metrics().record_encryption_time("decrypt", duration_us);
        }
        res
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
    crate::time_utils::system_time_now_secs()
}
