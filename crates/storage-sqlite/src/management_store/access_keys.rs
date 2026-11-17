// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Access-key management for `SqliteCatalogStore`, including key generation.
//!
//! Secrets are encrypted before storage with the store's key holder
//! (`extenddb_storage::secret_keys`). The ciphertext layout is
//! `nonce(12) || ciphertext || tag(16)`, with the row bound as additional
//! authenticated data (AAD) so a stored secret cannot be transplanted onto
//! another row.

use extenddb_storage::CatalogStore as _;
use extenddb_storage::management_store::{AccessKeyCreated, OpError, OpResult};
use extenddb_storage::secret_keys::{SecretRow, require_keys};
use time::OffsetDateTime;

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::{is_fk_violation, is_unique_violation, parse_timestamp};

impl SqliteCatalogStore {
    pub(crate) async fn create_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<AccessKeyCreated> {
        let keys = require_keys(self.secret_keys())?;
        let access_key_id = generate_access_key_id();
        let secret_access_key = generate_secret_key();
        let encrypted = keys
            .encrypt(
                &SecretRow::access_key(&access_key_id, account_id, user_name),
                &secret_access_key,
            )
            .map_err(|e| OpError::Internal(format!("encrypt secret: {e}")))?;

        // D1: every writer holds the write lock. Key generation and
        // encryption above run before it.
        let _writer = self.lock_writes().await;
        sqlx::query(
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
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        let result = sqlx::query(
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
        let rows: Vec<(String, bool, String)> = sqlx::query_as(
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
        let keys = require_keys(self.secret_keys())?;
        let encrypted = keys
            .encrypt(
                &SecretRow::access_key(access_key_id, account_id, user_name),
                secret_access_key,
            )
            .map_err(|e| OpError::Internal(format!("encrypt secret: {e}")))?;

        // D1: every writer holds the write lock. Encryption above runs
        // before it.
        let _writer = self.lock_writes().await;
        sqlx::query(
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
