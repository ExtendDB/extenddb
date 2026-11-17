// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared cryptographic helpers for the management API.
//!
//! Provides secret key generation for the temporary session credentials that
//! `assume_role` issues, and re-exports the AES-256-GCM secret encryption that
//! every storage backend uses, from `extenddb_storage::secret_cipher`.

use rand::Rng;

/// Encrypt a secret key under the base64 catalog encryption key, with the
/// access key id as the AAD. Returns `nonce || ciphertext || tag`.
pub use extenddb_storage::secret_cipher::encrypt_secret;

/// Generate a 40-character secret access key branded with `extenddb` prefix.
///
/// Format: `extenddb` + 32 random chars = 40 total.
pub fn generate_secret_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

/// Generate a 20-character session access key ID branded with `ASIAEXTENDDB` prefix.
///
/// Format: `ASIAEXTENDDB` + 8 random alphanumeric chars = 20 total.
/// Used by `assume_role` for temporary credentials.
pub fn generate_session_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("ASIAEXTENDDB{suffix}")
}

/// Generate a session token (64-character random string).
///
/// Used by `assume_role` for temporary session credentials.
pub fn generate_session_token() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    (0..64)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}
