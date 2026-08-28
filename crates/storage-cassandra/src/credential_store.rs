// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Database-backed credential store for SigV4 authentication.
//!
//! Implements `extenddb_auth::CredentialStore` by looking up access keys and
//! session credentials from Cassandra, decrypting secrets with AES-256-GCM.

use std::sync::Arc;

use async_trait::async_trait;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::IntoRustByName;
use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use zeroize::{Zeroize, ZeroizeOnDrop};

type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Decrypt a secret key from `nonce || ciphertext` using the base64-encoded encryption key.
fn decrypt_secret(encrypted: &[u8], key_b64: &str, aad: &str) -> Result<String, String> {
    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, KeyInit};
    use base64::Engine;

    if encrypted.len() < 28 {
        return Err(format!(
            "ciphertext too short: {} bytes (need at least 12-byte nonce + 16-byte auth tag)",
            encrypted.len()
        ));
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
        .map_err(|e| format!("decrypt failed both with AAD and without AAD: {e}"))?;

    String::from_utf8(plaintext_bytes)
        .map_err(|e| format!("decrypted secret is not valid UTF-8: {e}"))
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct CassandraCredentialStore {
    #[zeroize(skip)]
    session: Arc<CassandraSession>,
    #[zeroize(skip)]
    keyspace_prefix: String,
    encryption_key: String,
}

impl CassandraCredentialStore {
    pub fn new(
        session: Arc<CassandraSession>,
        keyspace_prefix: String,
        encryption_key: String,
    ) -> Self {
        Self {
            session,
            keyspace_prefix,
            encryption_key,
        }
    }

    fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }
}

#[async_trait]
impl CredentialStore for CassandraCredentialStore {
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

impl CassandraCredentialStore {
    async fn lookup_user_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT secret_key_encrypted, account_id, user_name, is_active \
             FROM {catalog_keyspace}.access_keys WHERE access_key_id = ?"
        );

        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(access_key_id))
            .await
            .map_err(|e| {
                tracing::error!("Credential lookup failed for access key {access_key_id}: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        let body = result.response_body().map_err(|e| {
            tracing::error!("Credential lookup response_body failed: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let rows = match body.into_rows() {
            Some(r) if !r.is_empty() => r,
            _ => return Ok(None),
        };

        let row = &rows[0];
        let encrypted_blob: cdrs_tokio::types::blob::Blob =
            row.get_r_by_name("secret_key_encrypted").map_err(|e| {
                tracing::error!("Failed to parse secret_key_encrypted: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;
        let encrypted = encrypted_blob.into_vec();

        let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
            tracing::error!("Failed to parse account_id: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let user_name: String = row.get_r_by_name("user_name").map_err(|e| {
            tracing::error!("Failed to parse user_name: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let is_active: bool = row.get_r_by_name("is_active").map_err(|e| {
            tracing::error!("Failed to parse is_active: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

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
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT secret_key_encrypted, account_id, role_name, session_name, \
                 session_token, expires_at \
                 FROM {catalog_keyspace}.iam_sessions WHERE access_key_id = ? ALLOW FILTERING"
        );

        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(access_key_id))
            .await
            .map_err(|e| {
                tracing::error!(
                    "Session credential lookup failed for access key {access_key_id}: {e}"
                );
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        let body = result.response_body().map_err(|e| {
            tracing::error!("Session credential lookup response_body failed: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let rows = match body.into_rows() {
            Some(r) if !r.is_empty() => r,
            _ => return Ok(None),
        };

        let row = &rows[0];
        let encrypted_blob: cdrs_tokio::types::blob::Blob =
            row.get_r_by_name("secret_key_encrypted").map_err(|e| {
                tracing::error!("Failed to parse secret_key_encrypted: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;
        let encrypted = encrypted_blob.into_vec();

        let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
            tracing::error!("Failed to parse account_id: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let role_name: String = row.get_r_by_name("role_name").map_err(|e| {
            tracing::error!("Failed to parse role_name: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let session_name: String = row.get_r_by_name("session_name").map_err(|e| {
            tracing::error!("Failed to parse session_name: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let session_token: String = row.get_r_by_name("session_token").map_err(|e| {
            tracing::error!("Failed to parse session_token: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let expires_at_ms: i64 = row.get_r_by_name("expires_at").map_err(|e| {
            tracing::error!("Failed to parse expires_at: {e}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;
        let expires_at =
            time::OffsetDateTime::from_unix_timestamp(expires_at_ms / 1000).map_err(|e| {
                tracing::error!("Invalid expires_at timestamp: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
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
            expires_at: Some(expires_at),
        }))
    }
}
