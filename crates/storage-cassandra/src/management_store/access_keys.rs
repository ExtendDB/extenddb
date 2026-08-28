// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Access key, session, and caller-tag operations for `CassandraCatalogStore`.

use crate::catalog_store::CassandraCatalogStore;
use cdrs_tokio::types::blob::Blob;
use extenddb_storage::management_store::{AccessKeyCreated, OpError, OpResult};

impl CassandraCatalogStore {
    // ── Access keys ────────────────────────────────────────────────

    pub(crate) async fn create_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<AccessKeyCreated> {
        // Check if user exists (emulate FK constraint)
        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("User not found".to_owned()));
        }

        // P119: Use cached encryption key if available, fall back to DB query.
        let enc_key: String = if let Some(cached) = self.encryption_key() {
            cached.to_string()
        } else {
            let catalog_keyspace = self.catalog_keyspace();
            let query = format!(
                "SELECT value FROM {catalog_keyspace}.settings WHERE key = 'encryption_key'"
            );

            let row = crate::cassandra_util::query_optional(
                self.session(),
                &query,
                cdrs_tokio::query_values!(),
                "create_access_key",
            )
            .await?
            .ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))?;

            crate::cassandra_util::get_column(&row, "value", "create_access_key")?
        };

        let access_key_id = generate_access_key_id();
        let secret_key = generate_secret_key();
        let encrypted = encrypt_secret(&secret_key, &enc_key, &access_key_id).map_err(|e| {
            tracing::error!("create_access_key encryption: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let catalog_keyspace = self.catalog_keyspace();
        let insert_query = format!(
            "INSERT INTO {catalog_keyspace}.access_keys (access_key_id, account_id, user_name, secret_key_encrypted, is_active, created_at) \
             VALUES (?, ?, ?, ?, true, toTimestamp(now()))"
        );

        let encrypted_blob = cdrs_tokio::types::blob::Blob::new(encrypted);

        self.session()
            .query_with_values(
                &insert_query,
                cdrs_tokio::query_values!(
                    access_key_id.as_str(),
                    account_id,
                    user_name,
                    encrypted_blob
                ),
            )
            .await
            .map_err(|e| {
                tracing::error!("create_access_key insert failed: {e}");
                OpError::Internal("Database error".to_owned())
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
        let catalog_keyspace = self.catalog_keyspace();

        // Check if key exists and belongs to the correct account/user
        let check_query = format!(
            "SELECT access_key_id, account_id, user_name FROM {catalog_keyspace}.access_keys \
             WHERE access_key_id = ?"
        );

        let row = crate::cassandra_util::query_optional(
            self.session(),
            &check_query,
            cdrs_tokio::query_values!(key_id),
            "delete_access_key",
        )
        .await?;

        match row {
            None => return Err(OpError::NotFound("Access key not found".to_owned())),
            Some(r) => {
                // Verify it belongs to the specified account and user
                let key_account: String =
                    crate::cassandra_util::get_column(&r, "account_id", "delete_access_key")?;
                let key_user: String =
                    crate::cassandra_util::get_column(&r, "user_name", "delete_access_key")?;

                if key_account != account_id || key_user != user_name {
                    return Err(OpError::NotFound("Access key not found".to_owned()));
                }
            }
        }

        // Delete the key (by PRIMARY KEY only)
        let delete_query = format!(
            "DELETE FROM {catalog_keyspace}.access_keys WHERE access_key_id = ?"
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(key_id),
            "delete_access_key",
        )
        .await
    }

    pub(crate) async fn list_access_keys_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Vec<(String, bool, time::OffsetDateTime)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT access_key_id, is_active, created_at FROM {catalog_keyspace}.access_keys \
             WHERE account_id = ? AND user_name = ? ALLOW FILTERING"
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, user_name),
            "list_access_keys",
        )
        .await?;

        let mut keys = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::{get_column, get_timestamp};
                Ok::<_, extenddb_storage::management_store::OpError>((
                    get_column::<String, _>(row, "access_key_id", "list_access_keys")?,
                    get_column::<bool, _>(row, "is_active", "list_access_keys")?,
                    get_timestamp(row, "created_at", "list_access_keys")?,
                ))
            },
            "list_access_keys",
        )?;

        // Sort by created_at to match PostgreSQL ORDER BY behavior
        keys.sort_by_key(|(_, _, created_at)| *created_at);

        Ok(keys)
    }

    pub(crate) async fn import_access_key_impl(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> OpResult<()> {
        // Check if user exists (emulate FK constraint)
        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("IAM user not found".to_owned()));
        }

        // P119: Use cached encryption key if available, fall back to DB query.
        let enc_key: String = if let Some(cached) = self.encryption_key() {
            cached.to_string()
        } else {
            let catalog_keyspace = self.catalog_keyspace();
            let query = format!(
                "SELECT value FROM {catalog_keyspace}.settings WHERE key = 'encryption_key'"
            );

            let row = crate::cassandra_util::query_optional(
                self.session(),
                &query,
                cdrs_tokio::query_values!(),
                "import_access_key",
            )
            .await?
            .ok_or_else(|| OpError::Internal("Encryption key not configured".to_owned()))?;

            crate::cassandra_util::get_column(&row, "value", "import_access_key")?
        };

        let encrypted =
            encrypt_secret(secret_access_key, &enc_key, access_key_id).map_err(|e| {
                tracing::error!("import_access_key encryption: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        let insert_query = format!(
            "INSERT INTO {}.access_keys (access_key_id, secret_key_encrypted, account_id, user_name, is_active, created_at) \
             VALUES (?, ?, ?, ?, true, toTimestamp(now())) IF NOT EXISTS",
            self.catalog_keyspace()
        );

        let encrypted_blob = cdrs_tokio::types::blob::Blob::new(encrypted);

        let applied = crate::cassandra_util::apply_lwt(
            self.session(),
            &insert_query,
            cdrs_tokio::query_values!(access_key_id, encrypted_blob, account_id, user_name),
            "import_access_key",
        )
        .await?;

        if !applied {
            return Err(OpError::AlreadyExists(
                "Access key ID already exists".to_owned(),
            ));
        }

        Ok(())
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
        let query = format!(
            "INSERT INTO {}.iam_sessions \
             (session_token, access_key_id, secret_key_encrypted, account_id, role_name, \
              session_name, session_tags, session_policy, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))",
            self.catalog_keyspace()
        );

        let session_tags_json = session_tags.as_ref().map(std::string::ToString::to_string);
        let session_policy_json = session_policy.as_ref().map(std::string::ToString::to_string);
        let expires_ms = expires_at.unix_timestamp() * 1000 + i64::from(expires_at.millisecond());

        crate::cassandra_util::execute(
            self.session(),
            &query,
            cdrs_tokio::query_values!(
                session_token,
                access_key_id,
                Blob::new(secret_key_encrypted.to_vec()),
                account_id,
                role_name,
                session_name,
                session_tags_json.as_deref(),
                session_policy_json.as_deref(),
                expires_ms
            ),
            "store_session",
        )
        .await
    }

    // ── Caller tags ────────────────────────────────────────────────

    pub(crate) async fn fetch_caller_tags_impl(
        &self,
        account_id: &str,
        resource: &str,
    ) -> OpResult<Vec<(String, String)>> {
        if let Some(user_name) = resource.strip_prefix("user/") {
            let query = format!(
                "SELECT tag_key, tag_value FROM {}.iam_user_tags \
                 WHERE account_id = ? AND user_name = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows(
                self.session(),
                &query,
                cdrs_tokio::query_values!(account_id, user_name),
                "fetch_caller_tags user",
            )
            .await?;
            crate::cassandra_util::map_rows(
                rows,
                |row| {
                    Ok((
                        crate::cassandra_util::get_column(row, "tag_key", "fetch_caller_tags")?,
                        crate::cassandra_util::get_column(row, "tag_value", "fetch_caller_tags")?,
                    ))
                },
                "fetch_caller_tags",
            )
        } else if let Some(role_name) = resource.strip_prefix("role/") {
            let query = format!(
                "SELECT tag_key, tag_value FROM {}.iam_role_tags \
                 WHERE account_id = ? AND role_name = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows(
                self.session(),
                &query,
                cdrs_tokio::query_values!(account_id, role_name),
                "fetch_caller_tags role",
            )
            .await?;
            crate::cassandra_util::map_rows(
                rows,
                |row| {
                    Ok((
                        crate::cassandra_util::get_column(row, "tag_key", "fetch_caller_tags")?,
                        crate::cassandra_util::get_column(row, "tag_value", "fetch_caller_tags")?,
                    ))
                },
                "fetch_caller_tags",
            )
        } else if let Some(rest) = resource.strip_prefix("assumed-role/") {
            let role_name = rest.split('/').next().unwrap_or("");
            if role_name.is_empty() {
                return Ok(Vec::new());
            }
            let query = format!(
                "SELECT tag_key, tag_value FROM {}.iam_role_tags \
                 WHERE account_id = ? AND role_name = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows(
                self.session(),
                &query,
                cdrs_tokio::query_values!(account_id, role_name),
                "fetch_caller_tags assumed-role",
            )
            .await?;
            crate::cassandra_util::map_rows(
                rows,
                |row| {
                    Ok((
                        crate::cassandra_util::get_column(row, "tag_key", "fetch_caller_tags")?,
                        crate::cassandra_util::get_column(row, "tag_value", "fetch_caller_tags")?,
                    ))
                },
                "fetch_caller_tags",
            )
        } else {
            Ok(Vec::new())
        }
    }
}

// ── Crypto helpers (duplicated from server::crypto to avoid circular dep) ──
// TODO - These should be lifted to extenddb-storage or extenddb-auth
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
