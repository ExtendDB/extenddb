// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `BuiltinAuthProvider::authenticate`.

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
fn signed_headers(access_key: &str, region: &str, token: Option<&str>, body: &[u8]) -> HeaderMap {
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

/// A token header whose value is not visible ASCII is a presented token
/// too. It cannot be compared with a stored token, and for a long-term key
/// the rule is that only an absent header passes, so the request is
/// rejected rather than served as an unauthenticated-token request.
#[tokio::test]
async fn non_ascii_session_token_header_with_a_long_term_key_is_rejected() {
    // Signed without a token, then the header is added unsigned: SigV4
    // covers the signed headers only, so the signature stays valid.
    let mut headers = signed_headers("AKIAEXTENDDB00000000", "us-east-1", None, b"{}");
    headers.insert(
        "x-amz-security-token",
        HeaderValue::from_bytes(&[0xc3, 0xa9]).expect("a header value with a byte above 0x7f"),
    );
    assert!(
        headers["x-amz-security-token"].to_str().is_err(),
        "the test header must not be readable as a string"
    );
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

/// A provider whose store holds no credential at all.
fn provider_without_credentials() -> BuiltinAuthProvider<MockCredentialStore> {
    BuiltinAuthProvider::new(
        MockCredentialStore {
            credential: None,
            error: None,
        },
        "us-east-1",
    )
}

/// Move the request date one hour into the past, re-signing the scope
/// date in the `Authorization` header to match.
fn make_date_stale(headers: &mut HeaderMap) {
    let fresh = headers["x-amz-date"].to_str().unwrap().to_owned();
    let stale = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let stale_str = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        stale.year(),
        stale.month() as u8,
        stale.day(),
        stale.hour(),
        stale.minute(),
        stale.second(),
    );
    let auth = headers["authorization"]
        .to_str()
        .unwrap()
        .replacen(&fresh[..8], &stale_str[..8], 1);
    headers.insert("authorization", HeaderValue::from_str(&auth).unwrap());
    headers.insert("x-amz-date", HeaderValue::from_str(&stale_str).unwrap());
}

/// Measured 2026-10-23 (R2-1 case F): an unknown access key scoped to
/// another region answers the scope message, so the scope check runs
/// before the key lookup.
#[tokio::test]
async fn unknown_key_with_a_wrong_region_reports_the_scope() {
    let headers = signed_headers("AKIAEXAMPLEEXAMPLE01", "us-west-2", None, b"{}");
    match provider_without_credentials()
        .authenticate(&headers, b"{}")
        .await
    {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert_eq!(msg, "Credential should be scoped to a valid region. ");
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// Measured 2026-10-23 (R2-2 case V): an unknown access key with a date
/// one hour old answers "Signature expired", so the timestamp check runs
/// before the key lookup. The service's message continues with the dates
/// and its type is InvalidSignatureException; the server's timestamp
/// error shape predates this change and is not altered here.
#[tokio::test]
async fn unknown_key_with_a_stale_date_reports_the_timestamp() {
    let mut headers = make_headers_with_auth("AKIAEXAMPLEEXAMPLE01", None);
    make_date_stale(&mut headers);
    match provider_without_credentials()
        .authenticate(&headers, b"{}")
        .await
    {
        Err(DynamoDbError::UnrecognizedClientException(msg)) => {
            assert!(msg.starts_with("Signature expired: "), "{msg}");
        }
        other => panic!("Expected the timestamp error, got: {other:?}"),
    }
}

/// Measured 2026-10-23 (R2-2 case D): a stale date scoped to another
/// region answers "Signature expired", so the timestamp check runs
/// before the scope check.
#[tokio::test]
async fn stale_date_with_a_wrong_region_reports_the_timestamp() {
    let mut headers = signed_headers("AKIAEXTENDDB00000000", "us-west-2", None, b"{}");
    make_date_stale(&mut headers);
    match provider(false).authenticate(&headers, b"{}").await {
        Err(DynamoDbError::UnrecognizedClientException(msg)) => {
            assert!(msg.starts_with("Signature expired: "), "{msg}");
        }
        other => panic!("Expected the timestamp error, got: {other:?}"),
    }
}
