use crate::error::VaultSyncError;
use aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Debug, Clone, Zeroize, ZeroizeOnDrop)]
pub struct NamespaceKeypair {
    pub public_key: [u8; 32],
    pub private_key: [u8; 32],
    #[zeroize(skip)]
    pub version: u64,
    #[zeroize(skip)]
    pub created_at: u64,
}

pub struct KeyRing {
    inner: std::sync::RwLock<KeyRingInner>,
}

struct KeyRingInner {
    keys: Vec<NamespaceKeypair>,
    active_version: u64,
    derived_keys_cache: std::collections::HashMap<(String, u64), [u8; 32]>,
}

impl KeyRing {
    pub fn generate() -> Self {
        let keypair = generate_keypair(1);
        Self {
            inner: std::sync::RwLock::new(KeyRingInner {
                keys: vec![keypair],
                active_version: 1,
                derived_keys_cache: std::collections::HashMap::new(),
            }),
        }
    }

    pub fn from_key(key: NamespaceKeypair) -> Self {
        let version = key.version;
        Self {
            inner: std::sync::RwLock::new(KeyRingInner {
                keys: vec![key],
                active_version: version,
                derived_keys_cache: std::collections::HashMap::new(),
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
        inner
            .keys
            .iter()
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

    pub fn encrypt_private_key(&self, device_key: &[u8]) -> Result<Vec<u8>, VaultSyncError> {
        let active = self.active_key();
        let key = derive_encryption_key(device_key);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(&[0u8; 12]);
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(&active.private_key);
        payload.extend_from_slice(&active.public_key);
        cipher
            .encrypt(nonce, payload.as_ref())
            .map_err(|e| VaultSyncError::Encryption(format!("key encryption failed: {e}")))
    }

    pub fn decrypt_private_key(
        encrypted: &[u8],
        device_key: &[u8],
    ) -> Result<NamespaceKeypair, VaultSyncError> {
        let key = derive_encryption_key(device_key);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(&[0u8; 12]);
        let plaintext = cipher
            .decrypt(nonce, encrypted)
            .map_err(|e| VaultSyncError::Encryption(format!("key decryption failed: {e}")))?;

        if plaintext.len() != 64 {
            return Err(VaultSyncError::Encryption(
                "invalid decrypted key length".into(),
            ));
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

    pub fn all_keys(&self) -> Vec<NamespaceKeypair> {
        let inner = self.inner.read().unwrap();
        inner.keys.clone()
    }

    pub fn derive_namespace_key(&self, namespace: &str) -> [u8; 32] {
        let active_version = self.active_version();
        if let Some(key) = self.get_cached_key(namespace, active_version) {
            return key;
        }

        let active = self.active_key();
        let hk = Hkdf::<Sha256>::new(Some(b"vaultsync-namespace"), &active.private_key);
        let mut okm = [0u8; 32];
        hk.expand(namespace.as_bytes(), &mut okm)
            .expect("32 bytes is a valid length for HKDF output");

        self.insert_cached_key(namespace, active_version, okm);
        okm
    }

    pub fn prune_old_versions(&self, current_version: u64) {
        let mut inner = self.inner.write().unwrap();
        let keep_from = current_version.saturating_sub(1);
        inner.keys.retain(|k| k.version >= keep_from);
        inner.derived_keys_cache.retain(|(_, v), _| *v >= keep_from);
    }

    pub fn prune_to_keep(&self, keep_versions: u64) {
        let mut inner = self.inner.write().unwrap();
        let active = inner.active_version;
        let cutoff = active.saturating_sub(keep_versions).saturating_add(1);
        inner.keys.retain(|k| k.version >= cutoff);
        inner.derived_keys_cache.retain(|(_, v), _| *v >= cutoff);
    }

    pub fn load_keys(&self, loaded: Vec<NamespaceKeypair>) {
        if loaded.is_empty() {
            return;
        }
        let mut inner = self.inner.write().unwrap();
        inner.keys = loaded;
        inner.active_version = inner.keys.iter().map(|k| k.version).max().unwrap_or(1);
        inner.derived_keys_cache.clear(); // Clear all cache since keys are completely reloaded
    }

    pub fn derive_namespace_key_for_version(
        &self,
        namespace: &str,
        version: u64,
    ) -> Option<[u8; 32]> {
        if let Some(key) = self.get_cached_key(namespace, version) {
            return Some(key);
        }

        let keypair = self.key_by_version(version)?;
        let hk = Hkdf::<Sha256>::new(Some(b"vaultsync-namespace"), &keypair.private_key);
        let mut okm = [0u8; 32];
        hk.expand(namespace.as_bytes(), &mut okm)
            .expect("32 bytes is a valid length for HKDF output");

        self.insert_cached_key(namespace, version, okm);
        Some(okm)
    }

    fn get_cached_key(&self, namespace: &str, version: u64) -> Option<[u8; 32]> {
        let inner = self.inner.read().unwrap();
        inner
            .derived_keys_cache
            .get(&(namespace.to_string(), version))
            .cloned()
    }

    fn insert_cached_key(&self, namespace: &str, version: u64, key: [u8; 32]) {
        let mut inner = self.inner.write().unwrap();
        inner
            .derived_keys_cache
            .insert((namespace.to_string(), version), key);
    }
}

pub struct E2eeEncryptor {
    keyring: Arc<KeyRing>,
    shared_secrets: std::sync::RwLock<std::collections::HashMap<(u64, [u8; 32]), [u8; 32]>>,
}

impl E2eeEncryptor {
    pub fn new(keyring: Arc<KeyRing>) -> Self {
        Self {
            keyring,
            shared_secrets: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn encrypt(
        &self,
        plaintext: &[u8],
        recipient_pk: &[u8; 32],
    ) -> Result<Vec<u8>, VaultSyncError> {
        let active = self.keyring.active_key();
        let key_version = active.version;
        let cache_key = (key_version, *recipient_pk);

        let shared_secret = {
            let read_cache = self.shared_secrets.read().unwrap();
            read_cache.get(&cache_key).cloned()
        };

        let shared_secret = match shared_secret {
            Some(secret) => secret,
            None => {
                let secret = x25519_dalek::x25519(active.private_key, *recipient_pk);
                let mut write_cache = self.shared_secrets.write().unwrap();
                write_cache.insert(cache_key, secret);
                secret
            }
        };

        super::encrypt::encrypt_with_shared_secret(plaintext, &shared_secret)
    }

    pub fn encrypt_symmetric(
        &self,
        plaintext: &[u8],
        namespace: &str,
    ) -> Result<Vec<u8>, VaultSyncError> {
        let start = crate::time_utils::PlatformInstant::now();
        let active_version = self.keyring.active_version();
        let span = tracing::info_span!(
            "e2ee.encrypt",
            namespace = namespace,
            key_version = active_version
        );
        let _enter = span.enter();

        let key = self.keyring.derive_namespace_key(namespace);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));

        let mut nonce_bytes = [0u8; 12];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| VaultSyncError::Encryption(format!("symmetric encrypt failed: {e}")))?;

        let mut result = Vec::with_capacity(8 + 12 + ciphertext.len());
        result.extend_from_slice(&active_version.to_le_bytes());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);

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
    shared_secrets: std::sync::RwLock<std::collections::HashMap<(u64, [u8; 32]), [u8; 32]>>,
}

impl E2eeDecryptor {
    pub fn new(keyring: Arc<KeyRing>) -> Self {
        Self {
            keyring,
            shared_secrets: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        sender_pk: &[u8; 32],
    ) -> Result<Vec<u8>, VaultSyncError> {
        let active = self.keyring.active_key();
        let key_version = active.version;
        let cache_key = (key_version, *sender_pk);

        let shared_secret = {
            let read_cache = self.shared_secrets.read().unwrap();
            read_cache.get(&cache_key).cloned()
        };

        let shared_secret = match shared_secret {
            Some(secret) => secret,
            None => {
                let secret = x25519_dalek::x25519(active.private_key, *sender_pk);
                let mut write_cache = self.shared_secrets.write().unwrap();
                write_cache.insert(cache_key, secret);
                secret
            }
        };

        super::decrypt::decrypt_with_shared_secret(ciphertext, &shared_secret)
    }

    pub fn decrypt_symmetric(
        &self,
        ciphertext: &[u8],
        namespace: &str,
    ) -> Result<Vec<u8>, VaultSyncError> {
        let start = crate::time_utils::PlatformInstant::now();

        if ciphertext.len() < 20 {
            return Err(VaultSyncError::Encryption(
                "invalid ciphertext length (too short)".into(),
            ));
        }
        let key_version = u64::from_le_bytes(ciphertext[..8].try_into().unwrap());

        let span = tracing::info_span!(
            "e2ee.decrypt",
            namespace = namespace,
            key_version = key_version
        );
        let _enter = span.enter();

        let nonce_bytes = &ciphertext[8..20];
        let actual_ciphertext = &ciphertext[20..];

        let key = self
            .keyring
            .derive_namespace_key_for_version(namespace, key_version)
            .ok_or_else(|| {
                VaultSyncError::Encryption(format!("unknown key version {key_version}"))
            })?;

        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let nonce = Nonce::from_slice(nonce_bytes);
        let res = cipher
            .decrypt(nonce, actual_ciphertext)
            .map_err(|e| VaultSyncError::Encryption(format!("symmetric decrypt failed: {e}")));

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
    hasher.update(b"vaultsync-key-derivation-v1");
    hasher.update(device_key);
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

fn now_secs() -> u64 {
    crate::time_utils::system_time_now_secs()
}
