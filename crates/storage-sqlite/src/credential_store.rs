// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite-backed `CredentialStore` for SigV4 verification.
//!
//! `AKIA…` keys resolve to long-lived IAM user access keys; `ASIA…` keys
//! resolve to temporary `AssumeRole` sessions (with expiry enforcement).
//! Secrets are decrypted with AES-256-GCM through
//! `extenddb_storage::secret_cipher`; the `access_key_id` is the AAD.

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::secret_cipher::decrypt_secret;
use sqlx::SqlitePool;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::sqlite_util::parse_timestamp;

/// Credential store over the catalog SQLite pool.
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

    async fn lookup_user(
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
            tracing::error!("credential lookup failed for {access_key_id}: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let Some((encrypted, account_id, user_name, is_active)) = row else {
            return Ok(None);
        };
        let secret_key =
            decrypt_secret(&encrypted, &self.encryption_key, access_key_id).map_err(|e| {
                tracing::error!("secret decryption failed for {access_key_id}: {e}");
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

    async fn lookup_session(
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
            tracing::error!("session lookup failed for {access_key_id}: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let Some((encrypted, account_id, role_name, session_name, session_token, expires_at)) = row
        else {
            return Ok(None);
        };

        let expires = parse_timestamp(&expires_at).map_err(|e| {
            tracing::error!("session expiry parse error: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        if expires < time::OffsetDateTime::now_utc() {
            return Err(DynamoDbError::ExpiredTokenException(
                "The security token included in the request is expired".to_owned(),
            ));
        }

        let secret_key =
            decrypt_secret(&encrypted, &self.encryption_key, access_key_id).map_err(|e| {
                tracing::error!("session secret decryption failed for {access_key_id}: {e}");
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
            expires_at: Some(expires),
        }))
    }
}

#[async_trait::async_trait]
impl CredentialStore for SqliteCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        if access_key_id.starts_with("ASIA") {
            self.lookup_session(access_key_id).await
        } else if access_key_id.starts_with("AKIA") {
            self.lookup_user(access_key_id).await
        } else {
            Ok(None)
        }
    }
}
