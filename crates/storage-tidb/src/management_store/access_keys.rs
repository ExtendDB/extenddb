// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Access key, session, and caller-tag operations for `TidbCatalogStore`.

use extenddb_storage::management_store::{AccessKeyCreated, OpError, OpResult};

use crate::catalog_store::TidbCatalogStore;
use crate::tidb_util::{is_fk_violation, is_unique_violation};

impl TidbCatalogStore {
    // ── Access keys ────────────────────────────────────────────────

    pub(crate) async fn create_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<AccessKeyCreated> {
        // P119: Use cached encryption key if available, fall back to DB query.
        let enc_key: String = if let Some(cached) = self.encryption_key() {
            cached.to_string()
        } else {
            let row: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE `key` = 'encryption_key'")
                    .fetch_optional(self.pool())
                    .await
                    .map_err(|e| {
                        tracing::error!("create_access_key fetch encryption key: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            row.ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))?
        };

        let access_key_id = generate_access_key_id();
        let secret_key = generate_secret_key();
        let encrypted = encrypt_secret(&secret_key, &enc_key, &access_key_id).map_err(|e| {
            tracing::error!("create_access_key encryption: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        sqlx::query(
            "INSERT INTO access_keys (access_key_id, account_id, user_name, secret_key_encrypted) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&access_key_id)
        .bind(account_id)
        .bind(user_name)
        .bind(&encrypted)
        .execute(self.pool())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                OpError::NotFound("User not found".to_owned())
            } else {
                tracing::error!("create_access_key failed: {e}");
                OpError::Internal("Database error".to_owned())
            }
        })?;

        Ok(AccessKeyCreated {
            access_key_id,
            secret_access_key: secret_key,
        })
    }

    pub(crate) async fn delete_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "DELETE FROM access_keys WHERE access_key_id = ? AND account_id = ? AND user_name = ?",
        )
        .bind(key_id)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("Access key not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_access_key failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn list_access_keys_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Vec<(String, bool, time::OffsetDateTime)>> {
        sqlx::query_as(
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
        })
    }

    pub(crate) async fn import_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> OpResult<()> {
        // P119: Use cached encryption key if available, fall back to DB query.
        let enc_key: String = if let Some(cached) = self.encryption_key() {
            cached.to_string()
        } else {
            let row: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE `key` = 'encryption_key'")
                    .fetch_optional(self.pool())
                    .await
                    .map_err(|e| {
                        tracing::error!("import_access_key fetch encryption key: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            row.ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))?
        };

        let encrypted =
            encrypt_secret(secret_access_key, &enc_key, access_key_id).map_err(|e| {
                tracing::error!("import_access_key encryption: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        let result = sqlx::query(
            "INSERT INTO access_keys (access_key_id, secret_key_encrypted, account_id, user_name) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(access_key_id)
        .bind(&encrypted)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_fk_violation(&e) => {
                Err(OpError::NotFound("IAM user not found".to_owned()))
            }
            Err(e) if is_unique_violation(&e) => Err(OpError::AlreadyExists(
                "Access key ID already exists".to_owned(),
            )),
            Err(e) => {
                tracing::error!("import_access_key failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    // ── Sessions ───────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store_session_impl(
        &self,
        session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        session_tags: &Option<serde_json::Value>,
        session_policy: &Option<serde_json::Value>,
        expires_at: time::OffsetDateTime,
    ) -> OpResult<()> {
        sqlx::query(
            "INSERT INTO iam_sessions \
             (session_token, access_key_id, secret_key_encrypted, account_id, role_name, \
              session_name, session_tags, session_policy, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(session_token)
        .bind(access_key_id)
        .bind(secret_key_encrypted)
        .bind(account_id)
        .bind(role_name)
        .bind(session_name)
        .bind(session_tags)
        .bind(session_policy)
        .bind(expires_at)
        .execute(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("store_session failed: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        Ok(())
    }

    // ── Caller tags ────────────────────────────────────────────────

    pub(crate) async fn fetch_caller_tags_impl(
        &self,
        account_id: &str,
        resource: &str,
    ) -> OpResult<Vec<(String, String)>> {
        if let Some(user_name) = resource.strip_prefix("user/") {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_user_tags \
                 WHERE account_id = ? AND user_name = ?",
            )
            .bind(account_id)
            .bind(user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_caller_tags user: {e}");
                OpError::Internal("Database error".to_owned())
            })
        } else if let Some(role_name) = resource.strip_prefix("role/") {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = ? AND role_name = ?",
            )
            .bind(account_id)
            .bind(role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_caller_tags role: {e}");
                OpError::Internal("Database error".to_owned())
            })
        } else if let Some(rest) = resource.strip_prefix("assumed-role/") {
            let role_name = rest.split('/').next().unwrap_or("");
            if role_name.is_empty() {
                return Ok(Vec::new());
            }
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = ? AND role_name = ?",
            )
            .bind(account_id)
            .bind(role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_caller_tags assumed-role: {e}");
                OpError::Internal("Database error".to_owned())
            })
        } else {
            Ok(Vec::new())
        }
    }
}

// ── Crypto helpers (duplicated from server::crypto to avoid circular dep) ──

fn generate_access_key_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..8)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("AKIAEXTENDDB{suffix}")
}

fn generate_secret_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    let suffix: String = (0..32)
        .map(|_| CHARSET[rand::Rng::random_range(&mut rng, 0..CHARSET.len())] as char)
        .collect();
    format!("extenddb{suffix}")
}

fn encrypt_secret(plaintext: &str, key_b64: &str, aad: &str) -> Result<Vec<u8>, String> {
    use aes_gcm::Aes256Gcm;
    use aes_gcm::KeyInit;
    use aes_gcm::aead::Aead;
    use aes_gcm::aead::Payload;
    use base64::Engine;

    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|e| format!("decode encryption key: {e}"))?;

    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

    let payload = Payload {
        msg: plaintext.as_bytes(),
        aad: aad.as_bytes(),
    };
    let ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| format!("encrypt: {e}"))?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}
