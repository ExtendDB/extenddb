// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Credential store implementation for `MongoDB`.
//!
//! Secrets are decrypted through the key holder
//! (`extenddb_storage::secret_keys`), which binds each secret to the fields
//! of its row.

use std::sync::Arc;

use mongodb::bson::{Document, doc};

use extenddb_auth::{CredentialStore, StoredCredential};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::secret_keys::{SecretKeys, SecretRow, require_keys};

/// `MongoDB` credential store for authentication.
pub struct MongoCredentialStore {
    catalog_db: mongodb::Database,
    /// The key holder, shared with the catalog store; `None` for a store
    /// opened without a key, which opens no stored secret.
    keys: Option<Arc<SecretKeys>>,
}

impl MongoCredentialStore {
    #[must_use]
    pub fn new(client: mongodb::Client, keys: Option<Arc<SecretKeys>>) -> Self {
        Self {
            catalog_db: client.database("extenddb_catalog"),
            keys,
        }
    }

    /// A store on `catalog_db`, for the crate's integration tests, which
    /// work in scratch databases of their own.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn on_database_for_tests(
        catalog_db: mongodb::Database,
        keys: Option<Arc<SecretKeys>>,
    ) -> Self {
        Self { catalog_db, keys }
    }

    fn catalog_db(&self) -> &mongodb::Database {
        &self.catalog_db
    }

    /// The key holder, or the refusal of a store opened without one.
    fn keys(&self) -> Result<&SecretKeys, DynamoDbError> {
        require_keys(self.keys.as_deref()).map_err(|e| {
            tracing::error!("Credential lookup: {e:?}");
            DynamoDbError::InternalServerError("Internal error during authentication".to_owned())
        })
    }

    /// Whether `account_id` has a document in `accounts`. Before account
    /// deletes removed them, a deleted account's access keys and role
    /// sessions stayed in the catalog; a credential of an account that no
    /// longer exists is unknown.
    async fn account_exists(&self, account_id: &str) -> Result<bool, DynamoDbError> {
        self.catalog_db()
            .collection::<Document>("accounts")
            .find_one(doc! { "account_id": account_id })
            .await
            .map(|d| d.is_some())
            .map_err(|e| {
                tracing::error!("Account lookup failed for account {account_id}: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })
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
        if !self.account_exists(&account_id).await? {
            return Ok(None);
        }

        let mut secret_key = self
            .keys()?
            .decrypt(
                &SecretRow::access_key(access_key_id, &account_id, &user_name),
                &encrypted,
            )
            .map_err(|e| {
                tracing::error!("Secret key decryption failed for access key {access_key_id}: {e}");
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        Ok(Some(StoredCredential {
            // Moved out, not copied: the plaintext has one allocation, wiped
            // when the credential drops.
            secret_key: std::mem::take(&mut *secret_key),
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
        if !self.account_exists(&account_id).await? {
            return Ok(None);
        }

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

        let mut secret_key = self
            .keys()?
            .decrypt(
                &SecretRow::session(
                    access_key_id,
                    &account_id,
                    &role_name,
                    &session_name,
                    expires_ts.unix_timestamp(),
                ),
                &encrypted,
            )
            .map_err(|e| {
                tracing::error!(
                    "Session secret key decryption failed for access key {access_key_id}: {e}"
                );
                DynamoDbError::InternalServerError(
                    "Internal error during authentication".to_owned(),
                )
            })?;

        Ok(Some(StoredCredential {
            // Moved out, not copied: the plaintext has one allocation, wiped
            // when the credential drops.
            secret_key: std::mem::take(&mut *secret_key),
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
