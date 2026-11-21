// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic primitives shared by the SigV4 verifier and the stored-secret
//! cipher, gathered here so the one target-specific backend choice lives in a
//! single module and nowhere else.
//!
//! On every target except `wasm32` the backend is AWS-LC (through `aws-lc-rs`),
//! the implementation the server has always used. On `wasm32`, where AWS-LC's C
//! and assembly do not build, the backend is the pure-Rust RustCrypto stack
//! (`sha2`, `hmac`, `aes-gcm`), constant-time comparison from `subtle`, and
//! randomness from `getrandom`'s browser backend.
//!
//! Both backends compute the same standard algorithms — SHA-256, HMAC-SHA256
//! and AES-256-GCM — so a value sealed on one target opens on the other. This
//! module is `#[doc(hidden)]`: it carries no public API-stability promise and
//! exists only to serve `extenddb-auth` and `extenddb-storage`.

use zeroize::Zeroizing;

/// Length in bytes of an AES-256-GCM key.
pub const KEY_LEN: usize = 32;

/// Length in bytes of the AES-GCM nonce that prefixes a sealed value.
pub const NONCE_LEN: usize = 12;

/// Length in bytes of the AES-GCM authentication tag that follows the ciphertext.
pub const TAG_LEN: usize = 16;

/// SHA-256 digest of `data`.
#[must_use]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    imp::sha256(data)
}

/// HMAC-SHA256 of `data` under `key`. `key` may be any length, as HMAC allows.
#[must_use]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    imp::hmac_sha256(key, data)
}

/// Compare `a` and `b` in time that does not depend on their contents. Returns
/// `false` for slices of different lengths.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    imp::constant_time_eq(a, b)
}

/// Fill `dst` with cryptographically secure random bytes.
///
/// # Errors
///
/// Returns an error if the platform random source fails.
pub fn fill_random(dst: &mut [u8]) -> Result<(), String> {
    imp::fill_random(dst)
}

/// Seal `plaintext` under the 32-byte AES-256-GCM `key`, binding `aad`, with a
/// fresh random nonce. Returns `nonce(12) || ciphertext || tag(16)`.
///
/// # Errors
///
/// Returns an error if the random nonce cannot be drawn or the AEAD seal fails.
pub fn seal(key: &[u8; KEY_LEN], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    imp::seal(key, plaintext, aad)
}

/// Open `nonce(12) || ciphertext || tag(16)` under the 32-byte AES-256-GCM
/// `key`, authenticating `aad`. The plaintext is wiped when the result drops.
///
/// # Errors
///
/// Returns an error if `sealed` is shorter than a nonce plus tag, or if
/// authentication fails (wrong key, wrong `aad`, or altered bytes).
pub fn open(key: &[u8; KEY_LEN], sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
    imp::open(key, sealed, aad)
}

/// Split a sealed value into its nonce prefix and the ciphertext-plus-tag
/// remainder, refusing anything too short to hold both.
fn split_nonce(sealed: &[u8]) -> Result<(&[u8; NONCE_LEN], &[u8]), String> {
    match sealed.split_first_chunk::<NONCE_LEN>() {
        Some((nonce, rest)) if rest.len() >= TAG_LEN => Ok((nonce, rest)),
        _ => Err("sealed value is shorter than a 12-byte nonce plus 16-byte tag".to_owned()),
    }
}

/// Native backend: AWS-LC through `aws-lc-rs`, unchanged from the server's
/// historical implementation.
#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use super::{KEY_LEN, NONCE_LEN, TAG_LEN, split_nonce};
    use aws_lc_rs::aead::{AES_256_GCM, Aad, Nonce, RandomizedNonceKey};
    use zeroize::Zeroizing;

    pub(super) fn sha256(data: &[u8]) -> [u8; 32] {
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data);
        let mut out = [0u8; 32];
        out.copy_from_slice(digest.as_ref());
        out
    }

    pub(super) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key);
        let tag = aws_lc_rs::hmac::sign(&key, data);
        let mut out = [0u8; 32];
        out.copy_from_slice(tag.as_ref());
        out
    }

    pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        aws_lc_rs::constant_time::verify_slices_are_equal(a, b).is_ok()
    }

    pub(super) fn fill_random(dst: &mut [u8]) -> Result<(), String> {
        aws_lc_rs::rand::fill(dst).map_err(|_| "random fill failed".to_owned())
    }

    pub(super) fn seal(
        key: &[u8; KEY_LEN],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, String> {
        let key = RandomizedNonceKey::new(&AES_256_GCM, key)
            .map_err(|_| "load encryption key: AES-256-GCM key setup failed".to_owned())?;
        // Zeroizing so the plaintext copy is wiped if the seal fails. The
        // capacity is exact, so the buffer never reallocates.
        let mut sealed = Zeroizing::new(Vec::with_capacity(NONCE_LEN + plaintext.len() + TAG_LEN));
        sealed.extend_from_slice(&[0; NONCE_LEN]);
        sealed.extend_from_slice(plaintext);
        let (nonce, tag) = key
            .seal_in_place_separate_tag(Aad::from(aad), &mut sealed[NONCE_LEN..])
            .map_err(|_| "encrypt: AES-256-GCM seal failed".to_owned())?;
        sealed[..NONCE_LEN].copy_from_slice(nonce.as_ref());
        sealed.extend_from_slice(tag.as_ref());
        Ok(std::mem::take(&mut *sealed))
    }

    pub(super) fn open(
        key: &[u8; KEY_LEN],
        sealed: &[u8],
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, String> {
        let (nonce, rest) = split_nonce(sealed)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, key)
            .map_err(|_| "load encryption key: AES-256-GCM key setup failed".to_owned())?;
        let mut in_out = Zeroizing::new(rest.to_vec());
        let len = key
            .open_in_place(Nonce::from(nonce), Aad::from(aad), &mut in_out[..])
            .map_err(|_| "decrypt: authentication failed".to_owned())?
            .len();
        in_out.truncate(len);
        Ok(in_out)
    }
}

/// wasm32 backend: the pure-Rust RustCrypto stack. AES-256-GCM, SHA-256 and
/// HMAC-SHA256 are standard, so the bytes match the native backend.
#[cfg(target_arch = "wasm32")]
mod imp {
    use super::{KEY_LEN, split_nonce};
    use aes_gcm::aead::{Aead, KeyInit, Payload};
    use aes_gcm::{Aes256Gcm, Nonce};
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    use zeroize::Zeroizing;

    pub(super) fn sha256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    pub(super) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
        // HMAC accepts a key of any length, so this never errors.
        let mut mac =
            <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }

    pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        use subtle::ConstantTimeEq;
        a.ct_eq(b).into()
    }

    pub(super) fn fill_random(dst: &mut [u8]) -> Result<(), String> {
        getrandom::fill(dst).map_err(|_| "random fill failed".to_owned())
    }

    pub(super) fn seal(
        key: &[u8; KEY_LEN],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, String> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| "load encryption key: AES-256-GCM key setup failed".to_owned())?;
        let mut nonce = [0u8; super::NONCE_LEN];
        fill_random(&mut nonce)?;
        // aes-gcm returns ciphertext with the 16-byte tag appended, matching
        // the native backend's `ciphertext || tag` layout.
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| "encrypt: AES-256-GCM seal failed".to_owned())?;
        let mut sealed = Vec::with_capacity(super::NONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    pub(super) fn open(
        key: &[u8; KEY_LEN],
        sealed: &[u8],
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, String> {
        let (nonce, rest) = split_nonce(sealed)?;
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| "load encryption key: AES-256-GCM key setup failed".to_owned())?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: rest, aad })
            .map_err(|_| "decrypt: authentication failed".to_owned())?;
        Ok(Zeroizing::new(plaintext))
    }
}

/// One test set for both backends: each case runs as a native `#[test]`
/// (AWS-LC) and, on `wasm32`, as a `wasm_bindgen_test` under Node (RustCrypto).
#[cfg(test)]
mod tests {
    use super::*;

    // SHA-256 of "abc" (FIPS 180-2 example).
    const ABC_SHA256: [u8; 32] = [
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22,
        0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00,
        0x15, 0xad,
    ];

    /// Decode a hex string written in a test constant.
    fn unhex(s: &str) -> Vec<u8> {
        hex::decode(s).expect("test vector is hex")
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn sha256_known_vector() {
        assert_eq!(sha256(b"abc"), ABC_SHA256);
        // SHA-256 of the empty string.
        assert_eq!(
            hex::encode(sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn hmac_sha256_rfc4231_case2() {
        // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?".
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn constant_time_eq_compares_contents_and_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn fill_random_produces_distinct_buffers() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        fill_random(&mut a).expect("fill");
        fill_random(&mut b).expect("fill");
        assert_ne!(a, b, "two random fills collided");
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn seal_open_round_trip_with_fresh_nonce() {
        let key = [7u8; KEY_LEN];
        let aad = b"context-01";
        let sealed = seal(&key, b"hello world", aad).expect("seal");
        assert_eq!(sealed.len(), NONCE_LEN + "hello world".len() + TAG_LEN);
        assert_eq!(&*open(&key, &sealed, aad).expect("open"), b"hello world");

        let again = seal(&key, b"hello world", aad).expect("seal");
        assert_ne!(sealed[..NONCE_LEN], again[..NONCE_LEN], "nonce reused");
        assert_eq!(&*open(&key, &again, aad).expect("open"), b"hello world");

        let empty = seal(&key, b"", aad).expect("seal");
        assert_eq!(empty.len(), NONCE_LEN + TAG_LEN);
        assert_eq!(&*open(&key, &empty, aad).expect("open"), b"");
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn open_rejects_wrong_aad_wrong_key_and_tampering() {
        let key = [7u8; KEY_LEN];
        let sealed = seal(&key, b"secret", b"aad-a").expect("seal");
        assert_eq!(
            open(&key, &sealed, b"aad-b").unwrap_err(),
            "decrypt: authentication failed"
        );
        assert_eq!(
            open(&[9u8; KEY_LEN], &sealed, b"aad-a").unwrap_err(),
            "decrypt: authentication failed"
        );
        let mut altered = sealed.clone();
        *altered.last_mut().unwrap() ^= 0x01;
        assert_eq!(
            open(&key, &altered, b"aad-a").unwrap_err(),
            "decrypt: authentication failed"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn open_rejects_input_too_short() {
        let key = [7u8; KEY_LEN];
        for len in [0, NONCE_LEN, NONCE_LEN + TAG_LEN - 1] {
            let short = vec![0u8; len];
            assert_eq!(
                open(&key, &short, b"aad").unwrap_err(),
                "sealed value is shorter than a 12-byte nonce plus 16-byte tag"
            );
        }
    }

    /// A secret sealed by the aes-gcm 0.10.3 code that encrypted secrets before
    /// the storage cipher existed opens on both backends.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn open_decrypts_historical_aes_gcm_vector() {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;

        const KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        const AAD: &str = "AKIAEXTENDDBCOMPAT01";
        const PLAINTEXT: &str = "extenddbCompatibilityVectorPlaintext0001";
        const VECTOR_HEX: &str = "06529a648732e136c8641182e54dc913f77542c595fd935fc84fbb3f\
                                  9eccb80eb543fc2d5d93dcd1f77b50b12388a02a39940964151c9fab\
                                  570f0cffb74ccff7285645f3";

        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&BASE64.decode(KEY_B64).expect("key b64"));
        let sealed = unhex(VECTOR_HEX);
        assert_eq!(sealed.len(), NONCE_LEN + PLAINTEXT.len() + TAG_LEN);
        assert_eq!(
            &*open(&key, &sealed, AAD.as_bytes()).expect("open"),
            PLAINTEXT.as_bytes()
        );
    }

    /// A value sealed by this module's native backend (AWS-LC through
    /// `aws-lc-rs`, with its random nonce) and stored here opens on both
    /// backends, so a secret sealed by a server opens in the wasm build.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn open_decrypts_aws_lc_sealed_vector() {
        // Key bytes 0x00..=0x1f.
        const KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        const AAD: &str = "extenddb-aws-lc-vector";
        const PLAINTEXT: &str = "sealed by AWS-LC on the native backend";
        const VECTOR_HEX: &str = "f31394933b8b764dbe2413878d77f301e2fb4beecc26905f9fc70fd1\
                                  78a544f5f30a27b5c516e07801b8200a81ca171250d2850a41b3062a\
                                  e36a6339783633018b70";

        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&unhex(KEY_HEX));
        let sealed = unhex(VECTOR_HEX);
        assert_eq!(sealed.len(), NONCE_LEN + PLAINTEXT.len() + TAG_LEN);
        assert_eq!(
            &*open(&key, &sealed, AAD.as_bytes()).expect("open"),
            PLAINTEXT.as_bytes()
        );
        assert_eq!(
            open(&key, &sealed, b"another aad").unwrap_err(),
            "decrypt: authentication failed"
        );
    }
}
