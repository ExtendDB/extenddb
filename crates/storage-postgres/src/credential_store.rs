// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Database-backed credential store for SigV4 authentication.
//!
//! Implements `extenddb_auth::CredentialStore` by looking up access keys and
//! session credentials from the catalog database, decrypting secrets with
//! AES-256-GCM.

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use sqlx::PgPool;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Decrypt a secret key from `nonce || ciphertext` using the base64-encoded encryption key.
///
/// `aad` must match the value used during encryption (CB-11). Falls back to
/// decryption without AAD for secrets encrypted before the CB-11 fix.
fn decrypt_secret(encrypted: &[u8], key_b64: &str, aad: &str) -> Result<String, String> {
    use aes_gcm::Aes256Gcm;
    use aes_gcm::KeyInit;
    use aes_gcm::aead::Aead;
    use aes_gcm::aead::Payload;
    use base64::Engine;

    if encrypted.len() < 28 {
        return Err(
            "ciphertext too short (need at least 12-byte nonce + 16-byte auth tag)".to_owned(),
        );
    }

    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|e| format!("decode encryption key: {e}"))?;

    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);
    let nonce = aes_gcm::Nonce::from_slice(&encrypted[..12]);

    // Try with AAD first (CB-11 format).
    let payload_with_aad = Payload {
        msg: &encrypted[12..],
        aad: aad.as_bytes(),
    };
    if let Ok(plaintext_bytes) = cipher.decrypt(nonce, payload_with_aad) {
        return String::from_utf8(plaintext_bytes)
            .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"));
    }

    // Fall back to without AAD (pre-CB-11 format).
    tracing::debug!("Decrypting secret without AAD (pre-CB-11 format) for {aad}");
    let plaintext_bytes = cipher
        .decrypt(nonce, &encrypted[12..])
        .map_err(|e| format!("decrypt: {e}"))?;

    String::from_utf8(plaintext_bytes)
        .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"))
}

/// Credential store backed by the catalog PostgreSQL database.
///
/// The `encryption_key` is zeroed from memory on drop.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct DbCredentialStore {
    #[zeroize(skip)]
    pool: PgPool,
    /// Base64-encoded AES-256-GCM encryption key from the settings table.
    encryption_key: String,
}

impl DbCredentialStore {
    /// Create a new credential store.
    ///
    /// `encryption_key` is the base64-encoded 32-byte key from the `settings` table.
    pub fn new(pool: PgPool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key,
        }
    }
}

#[async_trait::async_trait]
impl CredentialStore for DbCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        // Try long-lived access key first (AKIA*).
        if access_key_id.starts_with("AKIA") {
            return self.lookup_user_credential(access_key_id).await;
        }

        // Try session credential (ASIA*).
        if access_key_id.starts_with("ASIA") {
            return self.lookup_session_credential(access_key_id).await;
        }

        // S-4: Normalize error for all unrecognized access key prefixes.
        Ok(None)
    }
}

impl DbCredentialStore {
    async fn lookup_user_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let row: Option<(Vec<u8>, String, String, bool)> = sqlx::query_as(
            "SELECT secret_key_encrypted, account_id, user_name, is_active \
             FROM access_keys WHERE access_key_id = $1",
        )
        .bind(access_key_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("Credential lookup failed for access key {access_key_id}: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let Some((encrypted, account_id, user_name, is_active)) = row else {
            return Ok(None);
        };

        let secret_key =
            decrypt_secret(&encrypted, &self.encryption_key, access_key_id).map_err(|e| {
                tracing::error!("Secret key decryption failed for access key {access_key_id}: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        Ok(Some(StoredCredential {
            secret_key,
            account_id,
            principal_name: user_name,
            session_name: None,
            is_session: false,
            session_token: None,
            is_active,
            expires_at: None,
        }))
    }

    async fn lookup_session_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let row: Option<(
            Vec<u8>,
            String,
            String,
            String,
            String,
            time::OffsetDateTime,
        )> = sqlx::query_as(
            "SELECT secret_key_encrypted, account_id, role_name, session_name, \
                 session_token, expires_at \
                 FROM iam_sessions WHERE access_key_id = $1",
        )
        .bind(access_key_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("Session credential lookup failed for access key {access_key_id}: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let Some((encrypted, account_id, role_name, session_name, session_token, expires_at)) = row
        else {
            return Ok(None);
        };

        // CB-12: Fail-closed on expired sessions.
        if expires_at < time::OffsetDateTime::now_utc() {
            return Err(DynamoDbError::ExpiredTokenException(
                "The security token included in the request is expired".to_owned(),
            ));
        }

        let secret_key =
            decrypt_secret(&encrypted, &self.encryption_key, access_key_id).map_err(|e| {
                tracing::error!(
                    "Session secret key decryption failed for access key {access_key_id}: {e}"
                );
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        Ok(Some(StoredCredential {
            secret_key,
            account_id,
            principal_name: role_name,
            session_name: Some(session_name),
            is_session: true,
            session_token: Some(session_token),
            is_active: true,
            expires_at: Some(expires_at),
        }))
    }
}
