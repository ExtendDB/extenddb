// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite-backed credential store for SigV4 authentication.

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use sqlx::SqlitePool;
use zeroize::{Zeroize, ZeroizeOnDrop};

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

    let payload_with_aad = Payload {
        msg: &encrypted[12..],
        aad: aad.as_bytes(),
    };
    if let Ok(plaintext_bytes) = cipher.decrypt(nonce, payload_with_aad) {
        return String::from_utf8(plaintext_bytes)
            .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"));
    }

    tracing::debug!("Decrypting secret without AAD (pre-CB-11 format) for {aad}");
    let plaintext_bytes = cipher
        .decrypt(nonce, &encrypted[12..])
        .map_err(|e| format!("decrypt: {e}"))?;

    String::from_utf8(plaintext_bytes)
        .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"))
}

/// Credential store backed by the SQLite catalog database.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct SqliteCredentialStore {
    #[zeroize(skip)]
    pool: SqlitePool,
    encryption_key: String,
}

impl SqliteCredentialStore {
    pub fn new(pool: SqlitePool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key,
        }
    }
}

#[async_trait::async_trait]
impl CredentialStore for SqliteCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        if access_key_id.starts_with("AKIA") {
            return self.lookup_user_credential(access_key_id).await;
        }
        if access_key_id.starts_with("ASIA") {
            return self.lookup_session_credential(access_key_id).await;
        }
        Ok(None)
    }
}

impl SqliteCredentialStore {
    async fn lookup_user_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let row: Option<(Vec<u8>, String, String, bool)> = sqlx::query_as(
            "SELECT secret_key_encrypted, account_id, user_name, is_active \
             FROM access_keys WHERE access_key_id = ?",
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
        }))
    }

    async fn lookup_session_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let row: Option<(Vec<u8>, String, String, String, String, String)> = sqlx::query_as(
            "SELECT secret_key_encrypted, account_id, role_name, session_name, \
                 session_token, expires_at \
                 FROM iam_sessions WHERE access_key_id = ?",
        )
        .bind(access_key_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(
                "Session credential lookup failed for access key {access_key_id}: {e}"
            );
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let Some((encrypted, account_id, role_name, session_name, session_token, expires_at_str)) =
            row
        else {
            return Ok(None);
        };

        let expires_at = crate::sqlite_util::parse_timestamp(&expires_at_str).map_err(|e| {
            tracing::error!("Session expiry parse error: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

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
        }))
    }
}
