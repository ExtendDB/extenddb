// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite-backed `CredentialStore` for SigV4 verification.
//!
//! `AKIA…` keys resolve to long-lived IAM user access keys; `ASIA…` keys
//! resolve to temporary `AssumeRole` sessions (with expiry enforcement).
//! Secrets are decrypted with the key holder the catalog store shares, with
//! the row the secret was read from as the AAD
//! (`extenddb_storage::secret_keys`).

use std::sync::Arc;

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::secret_keys::{SecretKeys, SecretRow};
use sqlx::SqlitePool;

use crate::sqlite_util::parse_timestamp;

/// Credential store over the catalog SQLite pool. With no key holder (a
/// factory caller that passed `KeySource::None`) every lookup of a stored
/// secret fails.
pub struct SqliteCredentialStore {
    pool: SqlitePool,
    keys: Option<Arc<SecretKeys>>,
}

impl SqliteCredentialStore {
    pub fn new(pool: SqlitePool, keys: Option<Arc<SecretKeys>>) -> Self {
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
            tracing::error!("{what} decryption failed for {}: {e}", row.access_key_id);
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        Ok(std::mem::take(&mut *secret))
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
        let secret_key = self.open(
            &SecretRow::access_key(access_key_id, &account_id, &user_name),
            &encrypted,
            "secret",
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

        let secret_key = self.open(
            &SecretRow::session(
                access_key_id,
                &account_id,
                &role_name,
                &session_name,
                expires.unix_timestamp(),
            ),
            &encrypted,
            "session secret",
        )?;
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
