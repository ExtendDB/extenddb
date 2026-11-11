// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the request timestamp and credential scope checks of
//! `BuiltinAuthProvider::authenticate`, and for the order of its checks.

use super::tests::{MockCredentialStore, SECRET, make_headers_with_auth, provider, signed_headers};
use super::*;
use axum::http::{HeaderMap, HeaderValue};

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

/// The scope date the service sends back for a mismatch, with the trailing
/// period and space (measured 2026-10-23, M4-R7b-1 cases 1e and 2e).
const SCOPE_DATE_MESSAGE: &str =
    "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP. ";

/// The previous day as `YYYYMMDD`, for a scope date that does not match a
/// fresh `X-Amz-Date`.
fn yesterday() -> String {
    let d = time::OffsetDateTime::now_utc() - time::Duration::days(1);
    format!("{:04}{:02}{:02}", d.year(), d.month() as u8, d.day())
}

/// Headers for a request whose credential scope names `region`, `service`
/// and `scope_date`, with `X-Amz-Date` at the current time. `scope_date`
/// of `None` uses the date of `X-Amz-Date`. The signature is computed over
/// the scope as stated, so the only fault is the one the caller set.
fn signed_with_scope(
    access_key: &str,
    region: &str,
    service: &str,
    scope_date: Option<&str>,
) -> HeaderMap {
    let mut headers = make_headers_with_auth(access_key, None);
    headers.remove("authorization");
    let date_str = headers["x-amz-date"].to_str().unwrap().to_owned();
    let scope_date = scope_date.unwrap_or(&date_str[..8]).to_owned();
    let signed = "content-type;host;x-amz-date;x-amz-target";
    let creq = sigv4::canonical::canonical_request("POST", "/", "", &headers, signed, b"{}");
    let scope = format!("{scope_date}/{region}/{service}/aws4_request");
    let sts = sigv4::canonical::string_to_sign(&date_str, &scope, &creq);
    let key = sigv4::signing_key::derive_signing_key(SECRET, &scope_date, region, service);
    let sig = sigv4::signing_key::compute_signature(&key, &sts);
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, Signature={sig}"
    );
    headers.insert("authorization", HeaderValue::from_str(&auth).unwrap());
    headers
}

/// The status, error type and message a client sees for a refusal: the
/// three parts of the response body and status line that a caller can read
/// without a credential.
fn refusal(result: Result<AuthIdentity, DynamoDbError>) -> (u16, String, String) {
    match result {
        Err(e) => (e.status_code(), e.full_error_type(), e.message().to_owned()),
        Ok(_) => panic!("expected a refusal, the request authenticated"),
    }
}

/// Measured 2026-10-23 (M4-R7b-1 case 1e): an unknown access key whose
/// credential scope carries the previous day's date answers the scope date
/// message, so the scope date is compared before the key lookup.
#[tokio::test]
async fn unknown_key_with_a_stale_scope_date_reports_the_scope_date() {
    let headers = signed_with_scope(
        "AKIAEXAMPLEEXAMPLE01",
        "us-east-1",
        "dynamodb",
        Some(&yesterday()),
    );
    match provider_without_credentials()
        .authenticate(&headers, b"{}")
        .await
    {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert_eq!(msg, SCOPE_DATE_MESSAGE);
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// Every fault the service answers before it looks the access key up gives
/// the same status, error type and message for a key that exists and one
/// that does not (measured 2026-10-23, M4-R7b-1: 1b against 2b for the
/// timestamp, 1c against 2c for the region, 1d against 2d for the service,
/// 1e against 2e for the scope date). The timestamp error's own type and
/// message differ from the service's and are not changed here.
#[tokio::test]
async fn pre_lookup_faults_answer_alike_for_a_known_and_an_unknown_key() {
    let stale = yesterday();
    let mut cases: Vec<(&str, HeaderMap)> = vec![
        (
            "scope region us-west-2",
            signed_with_scope("AKIAEXTENDDB00000000", "us-west-2", "dynamodb", None),
        ),
        (
            "scope service s3",
            signed_with_scope("AKIAEXTENDDB00000000", "us-east-1", "s3", None),
        ),
        (
            "scope date the previous day",
            signed_with_scope(
                "AKIAEXTENDDB00000000",
                "us-east-1",
                "dynamodb",
                Some(&stale),
            ),
        ),
        (
            "scope date eight letters",
            signed_with_scope(
                "AKIAEXTENDDB00000000",
                "us-east-1",
                "dynamodb",
                Some("ABCDEFGH"),
            ),
        ),
        (
            "scope date empty",
            signed_with_scope("AKIAEXTENDDB00000000", "us-east-1", "dynamodb", Some("")),
        ),
    ];
    let mut timestamp = make_headers_with_auth("AKIAEXTENDDB00000000", None);
    make_date_stale(&mut timestamp);
    cases.push(("X-Amz-Date one hour old", timestamp));

    for (name, headers) in cases {
        let known = refusal(provider(false).authenticate(&headers, b"{}").await);
        let unknown = refusal(
            provider_without_credentials()
                .authenticate(&headers, b"{}")
                .await,
        );
        assert_eq!(
            known, unknown,
            "{name}: a key that exists and one that does not answered differently"
        );
    }
}

/// The scope date is compared as bytes against the `YYYYMMDD` prefix of
/// `X-Amz-Date`, so a date of another length, a date carrying letters and a
/// date naming a day that does not exist are each a mismatch rather than a
/// parse error or an accepted value.
#[tokio::test]
async fn scope_date_comparison_is_exact() {
    let stale = yesterday();
    let today = time::OffsetDateTime::now_utc();
    let full = format!(
        "{:04}{:02}{:02}",
        today.year(),
        today.month() as u8,
        today.day()
    );
    let seven = &full[..7];
    let nine = format!("{full}0");
    for date in [
        stale.as_str(),
        seven,
        &nine,
        "ABCDEFGH",
        "20261332",
        "",
        "2026102X",
    ] {
        let headers =
            signed_with_scope("AKIAEXTENDDB00000000", "us-east-1", "dynamodb", Some(date));
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(msg, SCOPE_DATE_MESSAGE, "scope date {date:?}");
            }
            other => panic!("scope date {date:?}: expected the date message, got {other:?}"),
        }
    }
}

/// A scope whose date matches is accepted, so the check does not refuse a
/// correctly scoped request.
#[tokio::test]
async fn matching_scope_date_authenticates() {
    let headers = signed_with_scope("AKIAEXTENDDB00000000", "us-east-1", "dynamodb", None);
    let id = provider(false).authenticate(&headers, b"{}").await.unwrap();
    assert!(matches!(id, AuthIdentity::User { .. }));
}

/// Measured 2026-10-24 (B18-1 cases 1c, 1d, 1e, 2e, 2f): the date complaint
/// joins the region and service complaints in one message and comes last.
#[tokio::test]
async fn scope_wrong_in_more_than_one_part_reports_each_complaint() {
    let stale = yesterday();
    let region_then_date = format!(
        "Credential should be scoped to a valid region. {}",
        SCOPE_DATE_MESSAGE
    );
    let service_then_date = format!(
        "Credential should be scoped to correct service: 'dynamodb'. {}",
        SCOPE_DATE_MESSAGE
    );
    let all_three = format!(
        "Credential should be scoped to a valid region. Credential should be scoped to correct service: 'dynamodb'. {}",
        SCOPE_DATE_MESSAGE
    );
    let cases = [
        ("us-west-2", "dynamodb", region_then_date),
        ("us-east-1", "s3", service_then_date),
        ("us-west-2", "s3", all_three),
    ];
    for (region, service, expected) in cases {
        let headers = signed_with_scope("AKIAEXTENDDB00000000", region, service, Some(&stale));
        match provider(false).authenticate(&headers, b"{}").await {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(msg, expected, "scope {region}/{service} with a stale date");
            }
            other => panic!("scope {region}/{service}: expected the scope message, got {other:?}"),
        }
    }
}
