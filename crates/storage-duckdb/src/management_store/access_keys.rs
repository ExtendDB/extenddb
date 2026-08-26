// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Access-key management for `DuckDbCatalogStore`, including key generation
//! and AES-256-GCM secret encryption.
//!
//! Secrets are encrypted with the catalog encryption key before storage. The
//! ciphertext layout is `nonce(12) || aes_gcm_ciphertext`, with the
//! `access_key_id` bound as additional authenticated data (AAD) so a stored
//! secret cannot be transplanted onto a different key id.

use crate::db;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_storage::management_store::{AccessKeyCreated, OpError, OpResult};
use time::OffsetDateTime;

use crate::catalog_store::DuckDbCatalogStore;
use crate::duckdb_util::{is_fk_violation, is_unique_violation, parse_timestamp};

impl DuckDbCatalogStore {
    /// Resolve the AES-256-GCM encryption key (base64), preferring the cached
    /// value and falling back to the `settings` table.
    async fn resolve_encryption_key(&self) -> OpResult<String> {
        if let Some(k) = self.encryption_key() {
            return Ok(k.to_string());
        }
        let row: Option<(String,)> =
            db::query_as("SELECT value FROM settings WHERE key = 'encryption_key'")
                .fetch_optional(self.pool())
                .await
                .map_err(|e| OpError::Internal(format!("fetch encryption key: {e}")))?;
        row.map(|(v,)| v)
            .ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))
    }

    pub(crate) async fn create_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<AccessKeyCreated> {
        let enc_key = self.resolve_encryption_key().await?;
        let access_key_id = generate_access_key_id();
        let secret_access_key = generate_secret_key();
        let encrypted = encrypt_secret(&secret_access_key, &enc_key, &access_key_id)
            .map_err(|e| OpError::Internal(format!("encrypt secret: {e}")))?;

        crate::referential::ensure_user_exists(self.pool(), account_id, user_name).await?;
        db::query(
            "INSERT INTO access_keys \
             (access_key_id, secret_key_encrypted, account_id, user_name) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&access_key_id)
        .bind(&encrypted)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                OpError::NotFound("IAM user not found".to_owned())
            } else {
                tracing::error!("create_access_key: {e}");
                OpError::Internal("Database error".to_owned())
            }
        })?;

        Ok(AccessKeyCreated {
            access_key_id,
            secret_access_key,
        })
    }

    pub(crate) async fn delete_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> OpResult<()> {
        let result = db::query(
            "DELETE FROM access_keys \
             WHERE access_key_id = ? AND account_id = ? AND user_name = ?",
        )
        .bind(key_id)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("delete_access_key: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        if result.rows_affected() == 0 {
            return Err(OpError::NotFound("Access key not found".to_owned()));
        }
        Ok(())
    }

    pub(crate) async fn list_access_keys_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Vec<(String, bool, OffsetDateTime)>> {
        let rows: Vec<(String, bool, String)> = db::query_as(
            "SELECT access_key_id, is_active, created_at FROM access_keys \
             WHERE account_id = ? AND user_name = ? ORDER BY created_at",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_access_keys: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        rows.into_iter()
            .map(|(id, active, ts)| {
                Ok((
                    id,
                    active,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn import_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> OpResult<()> {
        let enc_key = self.resolve_encryption_key().await?;
        let encrypted = encrypt_secret(secret_access_key, &enc_key, access_key_id)
            .map_err(|e| OpError::Internal(format!("encrypt secret: {e}")))?;

        crate::referential::ensure_user_exists(self.pool(), account_id, user_name).await?;
        db::query(
            "INSERT INTO access_keys \
             (access_key_id, secret_key_encrypted, account_id, user_name) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(access_key_id)
        .bind(&encrypted)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            if is_unique_violation(&e) {
                OpError::AlreadyExists("Access key ID already exists".to_owned())
            } else if is_fk_violation(&e) {
                OpError::NotFound("IAM user not found".to_owned())
            } else {
                tracing::error!("import_access_key: {e}");
                OpError::Internal("Database error".to_owned())
            }
        })
    }
}

// ── Key generation & crypto helpers ─────────────────────────────────────

/// Generate a 20-character access key id (`AKIAEXTENDDB` + 8 uppercase
/// alphanumerics), matching the prefix convention used across ExtendDB backends
/// so generated keys are distinguishable from real AWS keys.
fn generate_access_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("AKIAEXTENDDB{suffix}")
}

/// Generate a 40-character secret access key, matching the AWS secret shape.
fn generate_secret_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

/// Encrypt a secret with AES-256-GCM. Output is `nonce(12) || ciphertext`,
/// with `aad` (the access key id) bound as additional authenticated data.
fn encrypt_secret(plaintext: &str, key_b64: &str, aad: &str) -> Result<Vec<u8>, String> {
    let key_bytes = BASE64
        .decode(key_b64)
        .map_err(|e| format!("decode encryption key: {e}"))?;
    if key_bytes.len() != 32 {
        return Err("encryption key must be 32 bytes".to_owned());
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext.as_bytes(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|e| format!("encrypt: {e}"))?;

    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}
