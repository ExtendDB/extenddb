// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authentication and authorization for extenddb.
//!
//! Defines the `AuthProvider` trait for pluggable auth backends. Ships with
//! `BuiltinAuthProvider` (full `SigV4` verification with local credential store).

pub mod cache_registry;
pub mod credential_cache;
pub mod policy;
pub mod sigv4;

pub use cache_registry::{AuthCacheRegistry, AuthzCacheInvalidator, TableKeyInfoCacheInvalidator};
pub use credential_cache::CachedCredentialStore;

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Auth provider trait — pluggable authentication.
///
/// `BuiltinAuthProvider` performs `SigV4` verification.
/// Fix #11: Accept `&HeaderMap` directly to avoid per-request `HashMap` allocation.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError>;
}

/// The resolved identity after successful authentication.
#[derive(Debug, Clone)]
pub enum AuthIdentity {
    /// Authenticated IAM user via long-lived access key (AKIA*).
    User {
        account_id: String,
        user_name: String,
    },
    /// Authenticated role session via temporary credentials (ASIA*).
    RoleSession {
        account_id: String,
        role_name: String,
        session_name: String,
    },
}

/// A stored credential retrieved from the database.
///
/// Implemented by the server crate to bridge the auth crate (no DB dependency)
/// with the storage layer. The `secret_key` and `session_token` fields are
/// zeroed from memory on drop to limit exposure of sensitive material.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct StoredCredential {
    /// The plaintext secret access key (already decrypted).
    pub secret_key: String,
    /// The account ID that owns this credential.
    #[zeroize(skip)]
    pub account_id: String,
    /// The user name (for AKIA* keys) or role name (for ASIA* session keys).
    #[zeroize(skip)]
    pub principal_name: String,
    /// For session credentials: the session name.
    #[zeroize(skip)]
    pub session_name: Option<String>,
    /// Whether this is a session credential (ASIA*).
    #[zeroize(skip)]
    pub is_session: bool,
    /// For session credentials: the session token value for validation.
    pub session_token: Option<String>,
    /// Whether the credential is active. Inactive keys are returned from
    /// storage so the auth layer can produce the correct error response.
    #[zeroize(skip)]
    pub is_active: bool,
    /// For session credentials: the absolute expiry time. Long-lived AKIA*
    /// credentials set this to `None`. Auth checks this on every cache hit
    /// so cached sessions don't outlive their `expires_at` window — the
    /// storage layer's expiry check only fires on the cache-miss path.
    #[zeroize(skip)]
    pub expires_at: Option<time::OffsetDateTime>,
}

/// Trait for looking up credentials from storage.
///
/// The auth crate defines this trait; the server crate implements it with
/// database access. This keeps the auth crate free of storage dependencies.
#[async_trait::async_trait]
pub trait CredentialStore: Send + Sync {
    /// Look up a credential by access key ID.
    ///
    /// Returns `Ok(None)` if the key doesn't exist.
    /// Returns `Ok(Some(...))` with the decrypted credential on success.
    /// Returns `Err(...)` on database or decryption errors.
    ///
    /// Inactive keys are returned as `Ok(Some(...))` with `is_active = false`.
    /// The auth layer decides the error response.
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError>;
}

/// `SigV4` auth provider with local credential store.
///
/// Parses the `Authorization` header, validates the request timestamp, checks
/// the credential scope, looks up the access key, decrypts the secret and
/// verifies the `SigV4` signature, in that order (the service's order).
/// Handles both long-lived (AKIA*) and temporary (ASIA* + X-Amz-Security-Token)
/// credentials. The credential scope must name this server's region.
pub struct BuiltinAuthProvider<C: CredentialStore> {
    credential_store: C,
    region: String,
}

impl<C: CredentialStore> BuiltinAuthProvider<C> {
    /// Create a new `BuiltinAuthProvider` over `credential_store` for a server
    /// deployed as `region`. A request whose credential scope names another
    /// region is rejected, as the service rejects a scope for a region other
    /// than the endpoint's.
    pub fn new(credential_store: C, region: impl Into<String>) -> Self {
        Self {
            credential_store,
            region: region.into(),
        }
    }
}

#[async_trait::async_trait]
impl<C: CredentialStore + 'static> AuthProvider for BuiltinAuthProvider<C> {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        // Extract Authorization header
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                DynamoDbError::MissingAuthenticationToken("Missing Authentication Token".to_owned())
            })?;

        // Parse Authorization header
        let parsed = sigv4::parse::parse_authorization(auth_header)?;

        // The service checks the request timestamp, then the credential
        // scope, then looks the key up (measured 2026-10-23: an unknown key
        // with a stale date answers the timestamp error, an unknown key with
        // a wrong region answers the scope message, and a stale date with a
        // wrong region answers the timestamp error). Neither check reads the
        // stored credential, so running them first leaves the S-5 timing
        // argument below unchanged.
        //
        // Validate timestamp (±15 minute window).
        sigv4::verify::validate_timestamp(headers)?;

        // The credential scope must name this server's region and the
        // dynamodb service; the service reports both complaints in one
        // message when both are wrong.
        sigv4::verify::check_scope(&parsed, &self.region)?;

        let credential = self
            .credential_store
            .lookup_credential(&parsed.access_key_id)
            .await?
            .ok_or_else(|| {
                DynamoDbError::UnrecognizedClientException(
                    "The security token included in the request is invalid.".to_owned(),
                )
            })?;

        // S-5: Track inactive status but do NOT return early. Continue through
        // signature verification to ensure constant-time failure paths — no
        // timing difference between inactive, invalid, and absent keys.
        let is_inactive = !credential.is_active;

        // Session tokens. For a session credential the presented token must
        // match the stored one. For a long-term key no token may be presented:
        // the service validates any presented token and a long-term key has
        // none, so the request fails as an invalid token.
        // CB-12: Session expiration is enforced at the credential store layer (fail-closed).
        // Expired sessions are never returned — the storage layer returns
        // ExpiredTokenException on the cache-miss path, and CachedCredentialStore
        // re-validates `expires_at` on every cache hit before returning so cached
        // entries cannot survive past their issued lifetime.
        // S-5: Compare the session token in constant time and defer the
        // rejection until after signature verification, so neither the token's
        // byte contents nor its presence changes the failure-path timing.
        // A header value that is not visible ASCII cannot be compared with
        // the stored token, and it is still a token the client presented, so
        // presence is read from the header map and the value from its text.
        let token_header_present = headers.contains_key("x-amz-security-token");
        let presented_token = headers
            .get("x-amz-security-token")
            .and_then(|v| v.to_str().ok());
        let session_token_ok = if credential.is_session {
            let token = presented_token.unwrap_or("");
            let expected = credential.session_token.as_deref().unwrap_or("");
            sigv4::verify::constant_time_eq(token.as_bytes(), expected.as_bytes())
        } else {
            !token_header_present
        };

        // Verify SigV4 signature — always, even for inactive keys (S-5).
        sigv4::verify::verify_signature(
            &parsed,
            &credential.secret_key,
            "POST", // DynamoDB is always POST
            "/",    // DynamoDB is always /
            "",     // DynamoDB has no query string
            headers,
            body,
        )?;

        // S-5: Reject inactive keys or a mismatched session token only after
        // full signature verification, to prevent timing side-channels.
        // The service's message for a presented token that does not resolve
        // has no trailing period; its message for an unknown key or a missing
        // token does (both measured 2026-09-16).
        if is_inactive || (credential.is_session && presented_token.is_none()) {
            return Err(DynamoDbError::UnrecognizedClientException(
                "The security token included in the request is invalid.".to_owned(),
            ));
        }
        if !session_token_ok {
            return Err(DynamoDbError::UnrecognizedClientException(
                "The security token included in the request is invalid".to_owned(),
            ));
        }

        // Build identity from credential (clone fields because ZeroizeOnDrop
        // prevents moving out of the struct).
        if credential.is_session {
            Ok(AuthIdentity::RoleSession {
                account_id: credential.account_id.clone(),
                role_name: credential.principal_name.clone(),
                session_name: credential.session_name.clone().unwrap_or_default(),
            })
        } else {
            Ok(AuthIdentity::User {
                account_id: credential.account_id.clone(),
                user_name: credential.principal_name.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests;
