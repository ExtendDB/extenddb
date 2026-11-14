// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Database-backed credential store for `SigV4` authentication.
//!
//! Implements `extenddb_auth::CredentialStore` by looking up access keys and
//! session credentials from the catalog database, decrypting secrets with
//! AES-256-GCM through `extenddb_storage::secret_cipher`. The access key id is
//! the AAD; a secret that does not authenticate under it is not returned.

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::secret_cipher::decrypt_secret;
use sqlx::PgPool;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Credential store backed by the catalog `PostgreSQL` database.
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
    #[must_use]
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
