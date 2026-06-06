# Security Policy

## Threat Model

Drift is designed with a **Zero-Trust Coordinator** architecture:
- **Untrusted Coordinator**: The coordinator is treated as an untrusted third party. It only sees encrypted document mutations and cannot read the underlying CRDT plaintext.
- **End-to-End Encryption (E2EE)**: All data is encrypted on the client using X25519 for key exchange and ChaCha20Poly1305 for symmetric encryption before leaving the device.
- **Nonce Uniqueness**: Nonces are counter-based with a unique process prefix, guaranteeing that a nonce is never reused with the same key.

## Key Rotation Procedure

Keys can be rotated at any time by calling `DriftClient::rotate_keys()`.
1. A new `NamespaceKeypair` version is generated.
2. All pending oplog entries in the local database are decrypted with the old key and re-encrypted with the new key version.
3. The new key version is persisted to the local storage.
4. The client updates the replica public key on the coordinator and re-registers.
5. All future mutations use the new key version.

## Audit Scope

Critical cryptographic modules are located under `crates/drift-core/src/e2ee/`:
- `keyring.rs` - Public/private key management, derivation of symmetric keys, and zeroization of secret key material on drop using the `zeroize` crate.
- `encrypt.rs` - Asymmetric encryption using unique counter-based nonces.
- `decrypt.rs` - Decryption logic reading nonces directly from payload.

## Known Limitations

- **TOFU (Trust On First Use)**: Pinning of replica public keys is currently not enforced, which is scheduled for future SDK enhancements.
- **Metadata Leakage**: While document contents are fully encrypted, the coordinator can see metadata fields (e.g., document ID, record ID, and replica ID).
