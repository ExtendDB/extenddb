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
/// Parses the `Authorization` header, looks up the access key, decrypts the
/// secret, verifies the `SigV4` signature, and validates the request timestamp.
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

        // Look up credential before timestamp validation — real DynamoDB returns
        // an invalid-key error even when the timestamp is also expired.
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
        // timestamp validation and signature verification to ensure constant-time
        // failure paths — no timing difference between inactive, invalid, and
        // absent keys.
        let is_inactive = !credential.is_active;

        // Validate timestamp (±15 minute window) after credential lookup.
        sigv4::verify::validate_timestamp(headers)?;

        // The credential scope must name this server's region and the
        // dynamodb service; the service reports both complaints in one
        // message when both are wrong.
        sigv4::verify::check_scope(&parsed, &self.region)?;

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
        let presented_token = headers
            .get("x-amz-security-token")
            .and_then(|v| v.to_str().ok());
        let session_token_ok = if credential.is_session {
            let token = presented_token.unwrap_or("");
            let expected = credential.session_token.as_deref().unwrap_or("");
            sigv4::verify::constant_time_eq(token.as_bytes(), expected.as_bytes())
        } else {
            presented_token.is_none()
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
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    /// In-memory credential store for unit tests.
    struct MockCredentialStore {
        /// `Some(credential)` for found credentials, `None` for not found.
        credential: Option<StoredCredential>,
        /// If set, `lookup_credential` returns this error instead.
        error: Option<&'static str>,
    }

    #[async_trait::async_trait]
    impl CredentialStore for MockCredentialStore {
        async fn lookup_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<StoredCredential>, DynamoDbError> {
            if let Some(msg) = self.error {
                return Err(DynamoDbError::ExpiredTokenException(msg.to_owned()));
            }
            Ok(self.credential.clone())
        }
    }

    fn make_headers_with_auth(access_key: &str, token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        // Use current time for the date to pass timestamp validation.
        let now = time::OffsetDateTime::now_utc();
        let date_str = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            now.month() as u8,
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
        );
        let date_short = &date_str[..8];
        // Signature is fake — these tests exercise error paths (expired token,
        // unknown key) that trigger before signature verification in
        // authenticate().
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, \
             Signature=0000000000000000000000000000000000000000000000000000000000000000",
            access_key, date_short
        );
        headers.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        headers.insert("x-amz-date", HeaderValue::from_str(&date_str).unwrap());
        headers.insert("host", HeaderValue::from_static("localhost:18443"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.ListTables"),
        );
        if let Some(t) = token {
            headers.insert("x-amz-security-token", HeaderValue::from_str(t).unwrap());
        }
        headers
    }

    #[tokio::test]
    async fn expired_session_returns_expired_token_exception() {
        // CB-12: The credential store returns ExpiredTokenException directly
        // for expired sessions (fail-closed). The auth layer propagates it.
        let store = MockCredentialStore {
            credential: None,
            error: Some("The security token included in the request is expired"),
        };
        let provider = BuiltinAuthProvider::new(store, "us-east-1");
        let headers = make_headers_with_auth("ASIAEXTENDDB00000000", Some("test-token-value"));

        let result = provider.authenticate(&headers, b"{}").await;
        match result {
            Err(DynamoDbError::ExpiredTokenException(msg)) => {
                assert!(
                    msg.contains("expired"),
                    "Expected 'expired' in message: {msg}"
                );
            }
            other => panic!("Expected ExpiredTokenException, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn nonexistent_key_returns_unrecognized_client() {
        let store = MockCredentialStore {
            credential: None,
            error: None,
        };
        let provider = BuiltinAuthProvider::new(store, "us-east-1");
        let headers = make_headers_with_auth("AKIAXXXXXXXXXXXXXXXX", None);

        let result = provider.authenticate(&headers, b"{}").await;
        match result {
            Err(DynamoDbError::UnrecognizedClientException(msg)) => {
                assert!(
                    msg.contains("invalid"),
                    "Expected 'invalid' in message: {msg}"
                );
            }
            other => panic!("Expected UnrecognizedClientException, got: {other:?}"),
        }
    }

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn credential(is_session: bool) -> StoredCredential {
        StoredCredential {
            secret_key: SECRET.to_owned(),
            account_id: "123456789012".to_owned(),
            principal_name: "alice".to_owned(),
            session_name: is_session.then(|| "sess".to_owned()),
            is_session,
            session_token: is_session.then(|| "stored-token".to_owned()),
            is_active: true,
            expires_at: None,
        }
    }

    /// Headers for a request correctly signed by a client whose credential
    /// scope names `region`. `token`, when present, is sent and signed.
    fn signed_headers(
        access_key: &str,
        region: &str,
        token: Option<&str>,
        body: &[u8],
    ) -> HeaderMap {
        let mut headers = make_headers_with_auth(access_key, token);
        headers.remove("authorization");
        let date_str = headers["x-amz-date"].to_str().unwrap().to_owned();
        let date_short = date_str[..8].to_owned();
        let mut names = vec!["content-type", "host", "x-amz-date", "x-amz-target"];
        if token.is_some() {
            names.push("x-amz-security-token");
        }
        let signed = names.join(";");
        let creq = sigv4::canonical::canonical_request("POST", "/", "", &headers, &signed, body);
        let scope = format!("{date_short}/{region}/dynamodb/aws4_request");
        let sts = sigv4::canonical::string_to_sign(&date_str, &scope, &creq);
        let key = sigv4::signing_key::derive_signing_key(SECRET, &date_short, region, "dynamodb");
        let sig = sigv4::signing_key::compute_signature(&key, &sts);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, Signature={sig}"
        );
        headers.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        headers
    }

    fn provider(is_session: bool) -> BuiltinAuthProvider<MockCredentialStore> {
        BuiltinAuthProvider::new(
            MockCredentialStore {
                credential: Some(credential(is_session)),
                error: None,
            },
            "us-east-1",
        )
    }

    #[tokio::test]
    async fn correctly_signed_long_term_key_authenticates() {
        let headers = signed_headers("AKIAEXTENDDB00000000", "us-east-1", None, b"{}");
        let id = provider(false).authenticate(&headers, b"{}").await.unwrap();
        assert!(matches!(id, AuthIdentity::User { .. }));
    }

    /// Measured 2026-09-16: a scope for another region is refused with this
    /// exact message; the region is not named and the message ends with a space.
    #[tokio::test]
    async fn scope_for_another_region_is_rejected() {
        let headers = signed_headers("AKIAEXTENDDB00000000", "us-west-2", None, b"{}");
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(msg, "Credential should be scoped to a valid region. ");
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }

    /// A long-term key has no session token; presenting one is an invalid token.
    #[tokio::test]
    async fn session_token_with_a_long_term_key_is_rejected() {
        let headers = signed_headers(
            "AKIAEXTENDDB00000000",
            "us-east-1",
            Some("any-token"),
            b"{}",
        );
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::UnrecognizedClientException(msg)) => {
                assert_eq!(msg, "The security token included in the request is invalid");
            }
            other => panic!("Expected UnrecognizedClientException, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn session_key_with_matching_token_authenticates() {
        let headers = signed_headers(
            "ASIAEXTENDDB00000000",
            "us-east-1",
            Some("stored-token"),
            b"{}",
        );
        let id = provider(true).authenticate(&headers, b"{}").await.unwrap();
        assert!(matches!(id, AuthIdentity::RoleSession { .. }));
    }

    /// Measured 2026-09-16: a presented token that does not resolve has no
    /// trailing period; a missing token does.
    #[tokio::test]
    async fn session_key_token_messages_match_the_service() {
        let wrong = signed_headers("ASIAEXTENDDB00000000", "us-east-1", Some("bogus"), b"{}");
        match provider(true).authenticate(&wrong, b"{}").await {
            Err(DynamoDbError::UnrecognizedClientException(msg)) => {
                assert_eq!(msg, "The security token included in the request is invalid");
            }
            other => panic!("Expected UnrecognizedClientException, got: {other:?}"),
        }
        let missing = signed_headers("ASIAEXTENDDB00000000", "us-east-1", None, b"{}");
        match provider(true).authenticate(&missing, b"{}").await {
            Err(DynamoDbError::UnrecognizedClientException(msg)) => {
                assert_eq!(
                    msg,
                    "The security token included in the request is invalid."
                );
            }
            other => panic!("Expected UnrecognizedClientException, got: {other:?}"),
        }
    }

    /// The scope region is compared byte for byte: region names are lowercase
    /// and the service does not fold case either.
    #[tokio::test]
    async fn scope_region_comparison_is_exact() {
        for other in ["US-EAST-1", "us-east-1 ", ""] {
            let headers = signed_headers("AKIAEXTENDDB00000000", other, None, b"{}");
            match provider(false).authenticate(&headers, b"{}").await {
                Err(DynamoDbError::InvalidSignatureException(msg)) => {
                    assert_eq!(
                        msg, "Credential should be scoped to a valid region. ",
                        "{other:?}"
                    );
                }
                other_result => {
                    panic!("{other:?}: expected InvalidSignatureException, got {other_result:?}")
                }
            }
        }
    }

    /// An empty token header on a long-term key is a presented token, and is
    /// rejected like any other; only an absent header passes.
    #[tokio::test]
    async fn empty_session_token_header_with_a_long_term_key_is_rejected() {
        let headers = signed_headers("AKIAEXTENDDB00000000", "us-east-1", Some(""), b"{}");
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::UnrecognizedClientException(msg)) => {
                assert_eq!(msg, "The security token included in the request is invalid");
            }
            other => panic!("Expected UnrecognizedClientException, got: {other:?}"),
        }
    }

    /// Measured 2026-09-16: a scope wrong in both region and service reports
    /// both sentences in one message, region first.
    #[tokio::test]
    async fn scope_wrong_in_region_and_service_reports_both() {
        let mut headers = make_headers_with_auth("AKIAEXTENDDB00000000", None);
        headers.remove("authorization");
        let date_str = headers["x-amz-date"].to_str().unwrap().to_owned();
        let date_short = date_str[..8].to_owned();
        let signed = "content-type;host;x-amz-date;x-amz-target";
        let creq = sigv4::canonical::canonical_request("POST", "/", "", &headers, signed, b"{}");
        let scope = format!("{date_short}/us-west-2/s3/aws4_request");
        let sts = sigv4::canonical::string_to_sign(&date_str, &scope, &creq);
        let key = sigv4::signing_key::derive_signing_key(SECRET, &date_short, "us-west-2", "s3");
        let sig = sigv4::signing_key::compute_signature(&key, &sts);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential=AKIAEXTENDDB00000000/{scope}, SignedHeaders={signed}, Signature={sig}"
        );
        headers.insert("authorization", HeaderValue::from_str(&auth).unwrap());
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(
                    msg,
                    "Credential should be scoped to a valid region. Credential should be scoped to correct service: 'dynamodb'. "
                );
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }
}
