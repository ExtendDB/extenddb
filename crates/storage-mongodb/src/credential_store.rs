// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Credential store implementation for `MongoDB`.

use mongodb::bson::{Document, doc};
use zeroize::{Zeroize, ZeroizeOnDrop};

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;

/// `MongoDB` credential store for authentication.
///
/// The `encryption_key` is zeroed from memory on drop.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct MongoCredentialStore {
    #[zeroize(skip)]
    client: mongodb::Client,
    encryption_key: String,
}

impl MongoCredentialStore {
    #[must_use]
    pub fn new(client: mongodb::Client, encryption_key: String) -> Self {
        Self {
            client,
            encryption_key,
        }
    }

    fn catalog_db(&self) -> mongodb::Database {
        self.client.database("extenddb_catalog")
    }

    async fn lookup_user_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let coll = self.catalog_db().collection::<Document>("access_keys");
        let doc = coll
            .find_one(doc! { "access_key_id": access_key_id })
            .await
            .map_err(|e| {
                tracing::error!("Credential lookup failed for access key {access_key_id}: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        let Some(key_doc) = doc else {
            return Ok(None);
        };

        let encrypted = match key_doc.get_binary_generic("secret_key_encrypted") {
            Ok(bytes) => bytes.clone(),
            Err(_) => return Ok(None),
        };
        let account_id = key_doc.get_str("account_id").unwrap_or_default().to_owned();
        let user_name = key_doc.get_str("user_name").unwrap_or_default().to_owned();
        // Fail closed: treat a missing or malformed is_active as inactive so
        // a corrupted or partially written access-key record cannot silently
        // authenticate.
        let is_active = key_doc.get_bool("is_active").unwrap_or(false);

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
        let coll = self.catalog_db().collection::<Document>("iam_sessions");
        let doc = coll
            .find_one(doc! { "access_key_id": access_key_id })
            .await
            .map_err(|e| {
                tracing::error!(
                    "Session credential lookup failed for access key {access_key_id}: {e}"
                );
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        let Some(session_doc) = doc else {
            return Ok(None);
        };

        let encrypted = match session_doc.get_binary_generic("secret_key_encrypted") {
            Ok(bytes) => bytes.clone(),
            Err(_) => return Ok(None),
        };
        let account_id = session_doc
            .get_str("account_id")
            .unwrap_or_default()
            .to_owned();
        let role_name = session_doc
            .get_str("role_name")
            .unwrap_or_default()
            .to_owned();
        let session_name = session_doc
            .get_str("session_name")
            .unwrap_or_default()
            .to_owned();
        let session_token = session_doc
            .get_str("session_token")
            .unwrap_or_default()
            .to_owned();

        let expires_at = session_doc.get_datetime("expires_at").map_err(|_| {
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })?;

        let expires_ts = time::OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(expires_at.timestamp_millis()) * 1_000_000,
        )
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);

        if expires_ts < time::OffsetDateTime::now_utc() {
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
            expires_at: Some(expires_ts),
        }))
    }
}

#[async_trait::async_trait]
impl CredentialStore for MongoCredentialStore {
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

// ── Crypto helpers ──────────────────────────────────────────────────────

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
