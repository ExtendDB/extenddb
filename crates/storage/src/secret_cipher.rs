// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! AES-256-GCM encryption of the secrets stored for access keys and sessions.
//!
//! Every storage backend stores a secret as `nonce(12) || ciphertext || tag(16)`,
//! encrypted under the catalog encryption key: 32 bytes, base64 with the
//! standard alphabet, held in the catalog's `settings` table (a collection on
//! `MongoDB`). The access key id is the additional authenticated data (AAD), so
//! a stored secret decrypts only for the key id it was written with. There is
//! no decryption without the AAD: a ciphertext written without it fails
//! authentication like any corrupted value. An empty AAD is refused, because
//! AES-GCM treats an empty AAD and no AAD the same way.
//!
//! The cipher is AWS-LC through `aws-lc-rs`, which draws a fresh random 96-bit
//! nonce for each encryption.

use aws_lc_rs::aead::{AES_256_GCM, Aad, NONCE_LEN, Nonce, RandomizedNonceKey};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use zeroize::Zeroizing;

/// Length of the decoded encryption key.
const KEY_LEN: usize = 32;

/// Length of the GCM authentication tag that follows the ciphertext.
const TAG_LEN: usize = 16;

/// Encrypt `plaintext` under the base64 key `key_b64`, binding `aad`.
///
/// Returns `nonce(12) || ciphertext || tag(16)`.
///
/// # Errors
///
/// Returns an error if `aad` is empty, `key_b64` is not base64 or does not
/// decode to 32 bytes, or the encryption fails.
pub fn encrypt_secret(plaintext: &str, key_b64: &str, aad: &str) -> Result<Vec<u8>, String> {
    require_aad(aad)?;
    let key = cipher_key(key_b64)?;

    // Zeroizing, so the plaintext copy is wiped if the seal fails. The
    // capacity is exact, so the buffer never reallocates.
    let mut sealed = Zeroizing::new(Vec::with_capacity(NONCE_LEN + plaintext.len() + TAG_LEN));
    sealed.extend_from_slice(&[0; NONCE_LEN]);
    sealed.extend_from_slice(plaintext.as_bytes());
    let (nonce, tag) = key
        .seal_in_place_separate_tag(Aad::from(aad.as_bytes()), &mut sealed[NONCE_LEN..])
        .map_err(|_| "encrypt: AES-256-GCM seal failed".to_owned())?;
    sealed[..NONCE_LEN].copy_from_slice(nonce.as_ref());
    sealed.extend_from_slice(tag.as_ref());
    Ok(std::mem::take(&mut *sealed))
}

/// Decrypt `nonce(12) || ciphertext || tag(16)` under the base64 key
/// `key_b64`, authenticating `aad`.
///
/// # Errors
///
/// Returns an error if `aad` is empty, `encrypted` is shorter than 28 bytes,
/// `key_b64` is not base64 or does not decode to 32 bytes, authentication
/// fails (wrong key, wrong `aad`, or altered bytes), or the plaintext is not
/// UTF-8.
pub fn decrypt_secret(encrypted: &[u8], key_b64: &str, aad: &str) -> Result<String, String> {
    require_aad(aad)?;
    let (nonce, sealed) = match encrypted.split_first_chunk::<NONCE_LEN>() {
        Some((nonce, sealed)) if sealed.len() >= TAG_LEN => (nonce, sealed),
        _ => {
            return Err(format!(
                "ciphertext too short: {} bytes (need at least 12-byte nonce + 16-byte auth tag)",
                encrypted.len()
            ));
        }
    };
    let key = cipher_key(key_b64)?;

    let mut in_out = Zeroizing::new(sealed.to_vec());
    let plaintext = key
        .open_in_place(
            Nonce::from(nonce),
            Aad::from(aad.as_bytes()),
            &mut in_out[..],
        )
        .map_err(|_| "decrypt: authentication failed".to_owned())?;
    std::str::from_utf8(plaintext)
        .map(str::to_owned)
        .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"))
}

/// Refuse an empty AAD. A ciphertext sealed with an empty AAD is the same as
/// one sealed with none, so accepting it would reopen the path without the
/// access key id binding.
fn require_aad(aad: &str) -> Result<(), String> {
    if aad.is_empty() {
        return Err("AAD must not be empty".to_owned());
    }
    Ok(())
}

/// Decode the base64 encryption key and load it into an AES-256-GCM key.
pub(crate) fn cipher_key(key_b64: &str) -> Result<RandomizedNonceKey, String> {
    // The capacity is the decoder's own size estimate, so `decode_vec` never
    // reallocates, and the decoded bytes, partial ones included, are wiped on
    // every return path.
    let mut key_bytes = Zeroizing::new(Vec::with_capacity(key_b64.len().div_ceil(4) * 3));
    BASE64
        .decode_vec(key_b64, &mut key_bytes)
        .map_err(|e| format!("decode encryption key: {e}"))?;
    if key_bytes.len() != KEY_LEN {
        return Err("encryption key must be 32 bytes".to_owned());
    }
    RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)
        .map_err(|_| "load encryption key: AES-256-GCM key setup failed".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    const AAD: &str = "AKIAEXTENDDBCOMPAT01";
    const PLAINTEXT: &str = "extenddbCompatibilityVectorPlaintext0001";

    /// `PLAINTEXT` encrypted under `KEY_B64` with `AAD` and a random nonce by
    /// the aes-gcm 0.10.3 code that encrypted secrets before this module, laid
    /// out as `nonce(12) || ciphertext || tag(16)`.
    const AAD_VECTOR_HEX: &str = "06529a648732e136c8641182e54dc913f77542c595fd935fc84fbb3f\
                                  9eccb80eb543fc2d5d93dcd1f77b50b12388a02a39940964151c9fab\
                                  570f0cffb74ccff7285645f3";

    /// `PLAINTEXT` encrypted under `KEY_B64` by the same aes-gcm 0.10 code
    /// with no AAD. Before this module the decrypt path accepted it.
    const NO_AAD_VECTOR_HEX: &str = "ad80538e9fa81ea6d3b567ad90bbc4c65766d7549b814e8b9299b9e5\
                                     e8a7357c19cbf0de6af9ebe197eaee64fb0bbced3b5aeec96fe41928\
                                     15ac754ba11c156e3110a7ee";

    fn unhex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "odd-length hex");
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit"))
            .collect()
    }

    #[test]
    fn round_trip() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        assert_eq!(sealed.len(), NONCE_LEN + PLAINTEXT.len() + TAG_LEN);
        assert_eq!(
            decrypt_secret(&sealed, KEY_B64, AAD).expect("decrypt"),
            PLAINTEXT
        );

        let again = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        assert_ne!(sealed[..NONCE_LEN], again[..NONCE_LEN], "nonce reused");
        assert_eq!(
            decrypt_secret(&again, KEY_B64, AAD).expect("decrypt"),
            PLAINTEXT
        );

        let empty = encrypt_secret("", KEY_B64, AAD).expect("encrypt");
        assert_eq!(empty.len(), NONCE_LEN + TAG_LEN);
        assert_eq!(decrypt_secret(&empty, KEY_B64, AAD).expect("decrypt"), "");
    }

    #[test]
    fn wrong_aad_is_rejected() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        for aad in ["AKIAEXTENDDBCOMPAT02", "akiaextenddbcompat01"] {
            assert_eq!(
                decrypt_secret(&sealed, KEY_B64, aad).unwrap_err(),
                "decrypt: authentication failed",
                "aad {aad:?}"
            );
        }
    }

    #[test]
    fn empty_aad_is_refused() {
        assert_eq!(
            encrypt_secret(PLAINTEXT, KEY_B64, "").unwrap_err(),
            "AAD must not be empty"
        );
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        assert_eq!(
            decrypt_secret(&sealed, KEY_B64, "").unwrap_err(),
            "AAD must not be empty"
        );
    }

    #[test]
    fn wrong_key_is_rejected() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        let other_key = BASE64.encode([0x5a; KEY_LEN]);
        assert_eq!(
            decrypt_secret(&sealed, &other_key, AAD).unwrap_err(),
            "decrypt: authentication failed"
        );
    }

    #[test]
    fn altered_ciphertext_is_rejected() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        for i in [0, NONCE_LEN, sealed.len() - 1] {
            let mut altered = sealed.clone();
            altered[i] ^= 0x01;
            assert_eq!(
                decrypt_secret(&altered, KEY_B64, AAD).unwrap_err(),
                "decrypt: authentication failed",
                "byte {i} flipped"
            );
        }
    }

    #[test]
    fn truncated_input_is_rejected() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        for len in [0, 1, NONCE_LEN, NONCE_LEN + TAG_LEN - 1] {
            let err = decrypt_secret(&sealed[..len], KEY_B64, AAD).unwrap_err();
            assert_eq!(
                err,
                format!(
                    "ciphertext too short: {len} bytes (need at least 12-byte nonce + 16-byte auth tag)"
                )
            );
        }
        // Long enough to parse, but the tag no longer matches.
        for len in [NONCE_LEN + TAG_LEN, sealed.len() - 1] {
            assert_eq!(
                decrypt_secret(&sealed[..len], KEY_B64, AAD).unwrap_err(),
                "decrypt: authentication failed",
                "length {len}"
            );
        }
    }

    #[test]
    fn key_that_is_not_32_bytes_is_rejected() {
        let sealed = encrypt_secret(PLAINTEXT, KEY_B64, AAD).expect("encrypt");
        for len in [0, 16, 24, 31, 33, 64] {
            let key = BASE64.encode(vec![0x11; len]);
            assert_eq!(
                encrypt_secret(PLAINTEXT, &key, AAD).unwrap_err(),
                "encryption key must be 32 bytes",
                "encrypt, {len}-byte key"
            );
            assert_eq!(
                decrypt_secret(&sealed, &key, AAD).unwrap_err(),
                "encryption key must be 32 bytes",
                "decrypt, {len}-byte key"
            );
        }
        // A trailing newline, from a key edited by hand, and a key without its
        // padding are not base64.
        for key in [
            "not base64!".to_owned(),
            format!("{KEY_B64}\n"),
            KEY_B64.trim_end_matches('=').to_owned(),
        ] {
            let err = encrypt_secret(PLAINTEXT, &key, AAD).unwrap_err();
            assert!(err.starts_with("decode encryption key: "), "{key:?}: {err}");
            let err = decrypt_secret(&sealed, &key, AAD).unwrap_err();
            assert!(err.starts_with("decode encryption key: "), "{key:?}: {err}");
        }
    }

    #[test]
    fn non_utf8_plaintext_is_rejected() {
        let key = cipher_key(KEY_B64).expect("key");
        let mut in_out = vec![0xff, 0xfe];
        let (nonce, tag) = key
            .seal_in_place_separate_tag(Aad::from(AAD.as_bytes()), &mut in_out)
            .expect("seal");
        let mut sealed = nonce.as_ref().to_vec();
        sealed.extend_from_slice(&in_out);
        sealed.extend_from_slice(tag.as_ref());
        let err = decrypt_secret(&sealed, KEY_B64, AAD).unwrap_err();
        assert!(
            err.starts_with("decrypted secret is not valid UTF-8: "),
            "{err}"
        );
    }

    /// The access key secret in the 0.1.14 `MongoDB` release fixture,
    /// `crates/storage-mongodb/tests/fixtures/released/0.1.14.json`, written
    /// by release 0.1.14 under that deployment's encryption key.
    #[test]
    fn decrypts_secret_from_release_0_1_14_fixture() {
        const KEY: &str = "fDE8azYPDmQortu3b+HIGpfOIwUBlJ3+vxQYBnfXmnQ=";
        const ACCESS_KEY_ID: &str = "AKIAEXTENDDBEAT40RLC";
        const STORED: &str = "d2suZTVYf0N8J/1KS5y0HabRQpzkPtlYxCtrjfJGUeKwId6YuBTC4AP6B7Ysy+FBUvTqsIU+TTjuVmMGw2fYHayvRo8=";

        let stored = BASE64.decode(STORED).expect("fixture base64");
        // The tag authenticates the plaintext, so a successful open is the
        // check; the secret has the generated shape, `extenddb` and 32 more
        // characters. The assertions do not print the secret.
        let secret = decrypt_secret(&stored, KEY, ACCESS_KEY_ID).expect("decrypt");
        assert_eq!(secret.len(), 40);
        assert!(secret.starts_with("extenddb"));
        assert_eq!(
            decrypt_secret(&stored, KEY, "AKIAEXTENDDBEAT40RLD").unwrap_err(),
            "decrypt: authentication failed"
        );
    }

    /// A secret written by the aes-gcm 0.10 code decrypts unchanged.
    #[test]
    fn decrypts_secret_written_by_previous_implementation() {
        let stored = unhex(AAD_VECTOR_HEX);
        assert_eq!(stored.len(), NONCE_LEN + PLAINTEXT.len() + TAG_LEN);
        assert_eq!(
            decrypt_secret(&stored, KEY_B64, AAD).expect("decrypt"),
            PLAINTEXT
        );
    }

    /// A secret encrypted without the access key id as AAD does not decrypt.
    #[test]
    fn rejects_secret_encrypted_without_aad() {
        let stored = unhex(NO_AAD_VECTOR_HEX);
        assert_eq!(stored.len(), NONCE_LEN + PLAINTEXT.len() + TAG_LEN);
        assert_eq!(
            decrypt_secret(&stored, KEY_B64, AAD).unwrap_err(),
            "decrypt: authentication failed"
        );
        assert_eq!(
            decrypt_secret(&stored, KEY_B64, "").unwrap_err(),
            "AAD must not be empty"
        );

        // The vector is a valid ciphertext: opened directly with an empty AAD,
        // which AES-GCM treats as no AAD, it yields the plaintext. The
        // rejections above come from the access key id binding, not from
        // damaged bytes.
        let key = cipher_key(KEY_B64).expect("key");
        let (nonce, sealed) = stored.split_first_chunk::<NONCE_LEN>().expect("nonce");
        let mut in_out = sealed.to_vec();
        let opened = key
            .open_in_place(Nonce::from(nonce), Aad::empty(), &mut in_out[..])
            .expect("open without AAD");
        assert_eq!(opened, PLAINTEXT.as_bytes());
    }
}
