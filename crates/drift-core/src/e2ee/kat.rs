#[cfg(test)]
mod tests {
    use crate::e2ee::encrypt::encrypt;
    use crate::e2ee::decrypt::decrypt;
    use crate::e2ee::keyring::{KeyRing, NamespaceKeypair};

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let sk_alice = [42u8; 32];
        let pk_alice = x25519_dalek::x25519(sk_alice, x25519_dalek::X25519_BASEPOINT_BYTES);
        let sk_bob = [7u8; 32];
        let pk_bob = x25519_dalek::x25519(sk_bob, x25519_dalek::X25519_BASEPOINT_BYTES);

        let plaintext = b"hello drift e2ee";
        let ciphertext = encrypt(plaintext, &sk_alice, &pk_bob).unwrap();
        let decrypted = decrypt(&ciphertext, &sk_bob, &pk_alice).unwrap();

        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn encrypt_decrypt_wrong_key_fails() {
        let sk_alice = [1u8; 32];
        let pk_alice = x25519_dalek::x25519(sk_alice, x25519_dalek::X25519_BASEPOINT_BYTES);
        let sk_bob = [2u8; 32];
        let pk_bob = x25519_dalek::x25519(sk_bob, x25519_dalek::X25519_BASEPOINT_BYTES);
        let sk_evil = [99u8; 32];

        let plaintext = b"secret data";
        let ciphertext = encrypt(plaintext, &sk_alice, &pk_bob).unwrap();
        let result = decrypt(&ciphertext, &sk_evil, &pk_alice);
        assert!(result.is_err());
    }

    #[test]
    fn keyring_generates_unique_keys() {
        let kr1 = KeyRing::generate();
        let kr2 = KeyRing::generate();
        assert_ne!(kr1.active_key().private_key, kr2.active_key().private_key);
        assert_ne!(kr1.active_key().public_key, kr2.active_key().public_key);
    }

    #[test]
    fn keyring_rotate_changes_key() {
        let mut kr = KeyRing::generate();
        let first = kr.active_key().clone();
        kr.rotate();
        let second = kr.active_key();
        assert_ne!(first.private_key, second.private_key);
        assert_eq!(second.version, 2);
    }

    #[test]
    fn keyring_encrypt_decrypt_private_key() {
        let kr = KeyRing::generate();
        let device_key = b"my-device-secret-123";
        let encrypted = kr.encrypt_private_key(device_key).unwrap();
        let decrypted = NamespaceKeypair {
            public_key: kr.active_key().public_key,
            private_key: kr.active_key().private_key,
            version: kr.active_key().version,
            created_at: kr.active_key().created_at,
        };
        let loaded = KeyRing::decrypt_private_key(&encrypted, device_key).unwrap();
        assert_eq!(decrypted.private_key, loaded.private_key);
        assert_eq!(decrypted.public_key, loaded.public_key);
    }

    #[test]
    fn e2ee_roundtrip_via_encryptor_decryptor() {
        use crate::e2ee::keyring::{E2eeEncryptor, E2eeDecryptor};

        let kr_alice = KeyRing::generate();
        let kr_bob = KeyRing::generate();

        let alice_pk = kr_alice.active_key().public_key;
        let bob_pk = kr_bob.active_key().public_key;

        let encryptor = E2eeEncryptor::new(std::sync::Arc::new(kr_alice));
        let decryptor = E2eeDecryptor::new(std::sync::Arc::new(kr_bob));

        let msg = b"mutual encrypted message";
        let ciphertext = encryptor.encrypt(msg, &bob_pk).unwrap();
        let plaintext = decryptor.decrypt(&ciphertext, &alice_pk).unwrap();

        assert_eq!(&plaintext, msg);
    }

    #[test]
    fn e2ee_symmetric_roundtrip() {
        use crate::e2ee::keyring::{E2eeEncryptor, E2eeDecryptor};

        let kr_alice = KeyRing::generate();
        let alice_key = kr_alice.active_key().clone();
        let kr_bob = KeyRing::from_key(alice_key);

        let encryptor = E2eeEncryptor::new(std::sync::Arc::new(kr_alice));
        let decryptor = E2eeDecryptor::new(std::sync::Arc::new(kr_bob));

        let msg = b"symmetric encrypted message";
        let namespace = "test-namespace";
        
        let ciphertext = encryptor.encrypt_symmetric(msg, namespace).unwrap();
        let plaintext = decryptor.decrypt_symmetric(&ciphertext, namespace).unwrap();

        assert_eq!(&plaintext, msg);
    }
}
