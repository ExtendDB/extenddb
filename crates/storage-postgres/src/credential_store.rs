// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Database-backed credential store for `SigV4` authentication.
//!
//! Implements `extenddb_auth::CredentialStore` by looking up access keys and
//! session credentials from the catalog database, decrypting secrets with
//! the key holder the catalog store shares (`extenddb_storage::secret_keys`).
//! The row a secret was read from is its AAD; a secret that does not
//! authenticate under it is not returned.

use std::sync::Arc;

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::secret_keys::{SecretKeys, SecretRow};
use sqlx::PgPool;

/// Credential store backed by the catalog `PostgreSQL` database.
///
/// With no key holder (a factory caller that passed `KeySource::None`) every
/// lookup of a stored secret fails.
pub struct DbCredentialStore {
    pool: PgPool,
    keys: Option<Arc<SecretKeys>>,
}

impl DbCredentialStore {
    /// Create a new credential store over the key holder the catalog store
    /// shares.
    #[must_use]
    pub fn new(pool: PgPool, keys: Option<Arc<SecretKeys>>) -> Self {
        Self { pool, keys }
    }

    /// Decrypt the secret of `row`, logging a failure under `what`.
    fn open(
        &self,
        row: &SecretRow<'_>,
        encrypted: &[u8],
        what: &str,
    ) -> Result<String, DynamoDbError> {
        let opened = match &self.keys {
            Some(keys) => keys.decrypt(row, encrypted),
            None => Err("the store was opened without an encryption key".to_owned()),
        };
        let mut secret = opened.map_err(|e| {
            tracing::error!(
                "{what} decryption failed for access key {}: {e}",
                row.access_key_id
            );
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        Ok(std::mem::take(&mut *secret))
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

        let secret_key = self.open(
            &SecretRow::access_key(access_key_id, &account_id, &user_name),
            &encrypted,
            "Secret key",
        )?;

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

        // `expires_at` is stored in microseconds; the AAD carries whole
        // seconds, which `unix_timestamp` gives by flooring.
        let secret_key = self.open(
            &SecretRow::session(
                access_key_id,
                &account_id,
                &role_name,
                &session_name,
                expires_at.unix_timestamp(),
            ),
            &encrypted,
            "Session secret key",
        )?;

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
