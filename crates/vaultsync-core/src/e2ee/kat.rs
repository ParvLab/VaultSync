#[cfg(test)]
mod tests {
    use crate::e2ee::decrypt::decrypt;
    use crate::e2ee::encrypt::encrypt;
    use crate::e2ee::keyring::{KeyRing, NamespaceKeypair};

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let sk_alice = [42u8; 32];
        let pk_alice = x25519_dalek::x25519(sk_alice, x25519_dalek::X25519_BASEPOINT_BYTES);
        let sk_bob = [7u8; 32];
        let pk_bob = x25519_dalek::x25519(sk_bob, x25519_dalek::X25519_BASEPOINT_BYTES);

        let plaintext = b"hello vaultsync e2ee";
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
        let kr = KeyRing::generate();
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
        use crate::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor};

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
        use crate::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor};

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

    #[test]
    fn test_versioned_blob_roundtrip() {
        use crate::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor};
        let keyring = std::sync::Arc::new(KeyRing::generate());
        let encryptor = E2eeEncryptor::new(keyring.clone());
        let decryptor = E2eeDecryptor::new(keyring.clone());

        let msg = b"test message for versioning";
        let ciphertext = encryptor.encrypt_symmetric(msg, "ns").unwrap();

        assert!(ciphertext.len() >= 20);
        let version = u64::from_le_bytes(ciphertext[..8].try_into().unwrap());
        assert_eq!(version, 1);

        let decrypted = decryptor.decrypt_symmetric(&ciphertext, "ns").unwrap();
        assert_eq!(&decrypted, msg);
    }

    #[test]
    fn test_rotation_new_version_decryptable() {
        use crate::e2ee::keyring::{E2eeDecryptor, E2eeEncryptor};
        let keyring_alice = std::sync::Arc::new(KeyRing::generate());
        let alice_key_v1 = keyring_alice.active_key();

        // Bob starts with only Alice's v1 key
        let keyring_bob = std::sync::Arc::new(KeyRing::from_key(alice_key_v1));

        let encryptor_alice = E2eeEncryptor::new(keyring_alice.clone());
        let decryptor_bob = E2eeDecryptor::new(keyring_bob.clone());

        let msg1 = b"message 1";
        let ct1 = encryptor_alice.encrypt_symmetric(msg1, "ns").unwrap();

        // Alice rotates to v2
        let alice_key_v2 = keyring_alice.rotate();

        let msg2 = b"message 2";
        let ct2 = encryptor_alice.encrypt_symmetric(msg2, "ns").unwrap();

        // With deterministic symmetric keys, Bob can decrypt ct2 even without Alice's v2 key
        let decrypt_res = decryptor_bob.decrypt_symmetric(&ct2, "ns");
        assert!(decrypt_res.is_ok());

        // Now Bob receives Alice's updated key and loads it
        keyring_bob.add_key(alice_key_v2);

        // Bob should now be able to decrypt both messages successfully
        let dec1 = decryptor_bob.decrypt_symmetric(&ct1, "ns").unwrap();
        let dec2 = decryptor_bob.decrypt_symmetric(&ct2, "ns").unwrap();
        assert_eq!(&dec1, msg1);
        assert_eq!(&dec2, msg2);

        // Prune Alice's old keys to only keep v2 (since current is v2, saturating_sub(1) keeps v1 too)
        keyring_alice.prune_old_versions(2);
        assert!(keyring_alice.key_by_version(1).is_some()); // still kept
        keyring_alice.prune_old_versions(3); // should drop v1
        assert!(keyring_alice.key_by_version(1).is_none());
    }

    #[test]
    fn test_blob_too_short_rejected() {
        use crate::e2ee::keyring::E2eeDecryptor;
        let keyring = std::sync::Arc::new(KeyRing::generate());
        let decryptor = E2eeDecryptor::new(keyring.clone());

        let short_ct = vec![0u8; 19];
        let res = decryptor.decrypt_symmetric(&short_ct, "ns");
        assert!(res.is_err());
    }
}
