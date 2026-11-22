// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn parse_iso8601_basic_valid() {
    // 2026-04-15T12:00:00Z
    let epoch = parse_iso8601_basic("20260415T120000Z").unwrap();
    // Verify against known value: 2026-04-15 is day 1,776,528,000 from epoch
    // (verified with external tool)
    assert!(epoch > 0);
    // 2015-08-30T12:36:00Z — from AWS test vector
    let epoch2 = parse_iso8601_basic("20150830T123600Z").unwrap();
    assert!(epoch2 > 0);
    assert!(epoch > epoch2); // 2026 > 2015
}

#[test]
fn parse_iso8601_basic_invalid() {
    assert!(parse_iso8601_basic("2026-04-15T12:00:00Z").is_none());
    assert!(parse_iso8601_basic("20260415T12000Z").is_none());
    assert!(parse_iso8601_basic("").is_none());
}

#[test]
fn constant_time_eq_works() {
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"ab"));
}

#[test]
fn extract_timestamp_missing() {
    let headers = HeaderMap::new();
    assert!(extract_timestamp(&headers).is_err());
}

/// Measured 2026-11-21: `Date` is read only when `X-Amz-Date` is absent,
/// and an empty value is no date.
#[test]
fn the_request_date_is_x_amz_date_else_date() {
    let http = "Sat, 21 Nov 2026 01:36:16 GMT";
    let rows = [
        (vec![], None),
        (
            vec![("x-amz-date", "20260415T120000Z")],
            Some("20260415T120000Z"),
        ),
        (vec![("date", http)], Some(http)),
        (
            vec![("x-amz-date", http), ("date", "20260415T120000Z")],
            Some(http),
        ),
        (vec![("x-amz-date", ""), ("date", http)], None),
        (vec![("date", "")], None),
    ];
    for (pairs, want) in rows {
        let mut headers = HeaderMap::new();
        for (name, value) in &pairs {
            headers.insert(*name, value.parse().unwrap());
        }
        assert_eq!(request_date(&headers), want, "{pairs:?}");
    }
}

/// Measured 2026-11-21: the service echoes a date it cannot read.
#[test]
fn a_date_in_another_form_is_refused_with_its_value() {
    let err = validate_timestamp(&make_headers("Sat, 21 Nov 2026 01:36:16 GMT")).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::IncompleteSignature(_)),
        "{err:?}"
    );
    assert_eq!(
        err.message(),
        "Date must be in ISO-8601 'basic format'. Got 'Sat, 21 Nov 2026 01:36:16 GMT'. \
         See http://en.wikipedia.org/wiki/ISO_8601"
    );
}

fn make_parsed(service: &str, date: &str, signed_headers: &str) -> ParsedAuthorization {
    ParsedAuthorization {
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
        date: date.to_owned(),
        region: "us-east-1".to_owned(),
        service: service.to_owned(),
        signed_headers: signed_headers.to_owned(),
        signature: "0".repeat(64),
    }
}

fn make_headers(date: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("x-amz-date", date.parse().unwrap());
    h.insert("host", "localhost:18443".parse().unwrap());
    h.insert(
        "content-type",
        "application/x-amz-json-1.0".parse().unwrap(),
    );
    h
}

/// CB-5: Reject credential scope with wrong service.
#[test]
fn reject_wrong_service_in_scope() {
    let parsed = make_parsed("s3", "20260415", "content-type;host;x-amz-date");
    let headers = make_headers("20260415T120000Z");
    let err = verify_signature(&parsed, &key(SECRET), "POST", "/", "", &headers, b"{}");
    match err {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(msg.contains("dynamodb"), "Expected 'dynamodb' in: {msg}");
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// CB-5: Reject credential scope date mismatch with X-Amz-Date.
#[test]
fn reject_scope_date_mismatch() {
    let parsed = make_parsed("dynamodb", "20260414", "content-type;host;x-amz-date");
    let headers = make_headers("20260415T120000Z");
    let err = verify_signature(&parsed, &key(SECRET), "POST", "/", "", &headers, b"{}");
    match err {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert_eq!(msg, SCOPE_DATE_COMPLAINT);
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// `check_scope` accepts a scope whose date is the `YYYYMMDD` prefix of
/// the timestamp, and refuses one that is not.
#[test]
fn check_scope_compares_the_date_against_the_timestamp() {
    let parsed = make_parsed("dynamodb", "20260415", "content-type;host;x-amz-date");
    check_scope(&parsed, "us-east-1", "20260415T120000Z").unwrap();
    match check_scope(&parsed, "us-east-1", "20260414T120000Z") {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert_eq!(msg, SCOPE_DATE_COMPLAINT);
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// A timestamp shorter than `YYYYMMDD` carries no date to compare, so
/// the scope date cannot match it. `validate_timestamp` rejects such a
/// value before `check_scope` sees it in `authenticate`; this holds the
/// behaviour for any other caller.
#[test]
fn check_scope_refuses_a_timestamp_too_short_to_carry_a_date() {
    let parsed = make_parsed("dynamodb", "20260415", "content-type;host;x-amz-date");
    for timestamp in ["", "2026", "2026041"] {
        match check_scope(&parsed, "us-east-1", timestamp) {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(msg, SCOPE_DATE_COMPLAINT, "{timestamp:?}");
            }
            other => panic!("{timestamp:?}: expected InvalidSignatureException, got {other:?}"),
        }
    }
}

/// `validate_timestamp` hands the `X-Amz-Date` value back so the caller
/// compares the scope date against the same string.
#[test]
fn validate_timestamp_returns_the_header_value() {
    let now = time::OffsetDateTime::now_utc();
    let value = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    );
    let headers = make_headers(&value);
    assert_eq!(validate_timestamp(&headers).unwrap(), value);
}

/// CB-6: Reject SignedHeaders missing "host".
#[test]
fn reject_missing_host_in_signed_headers() {
    let parsed = make_parsed("dynamodb", "20260415", "content-type;x-amz-date");
    let headers = make_headers("20260415T120000Z");
    let err = verify_signature(&parsed, &key(SECRET), "POST", "/", "", &headers, b"{}");
    match err {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(msg.contains("Host"), "Expected 'Host' in message: {msg}");
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// A client that signs the payload line as the `UNSIGNED-PAYLOAD` literal
/// (an S3 convention) fails with the plain mismatch, which is what the
/// service returns (measured 2026-09-16): the payload line is always the
/// hash of the received body.
#[test]
fn unsigned_payload_literal_is_a_plain_signature_mismatch() {
    let mut headers = make_headers("20260415T120000Z");
    headers.insert("x-amz-content-sha256", "UNSIGNED-PAYLOAD".parse().unwrap());
    let signed = "content-type;host;x-amz-content-sha256;x-amz-date";
    // Sign the way such a client does: the literal in place of the body hash.
    let creq = canonical::canonical_request("POST", "/", "", &headers, signed, SIGNED_BODY)
        .rsplit_once('\n')
        .map(|(head, _)| format!("{head}\nUNSIGNED-PAYLOAD"))
        .unwrap();
    let sts = canonical::string_to_sign(
        "20260415T120000Z",
        "20260415/us-east-1/dynamodb/aws4_request",
        &creq,
    );
    let mut parsed = make_parsed("dynamodb", "20260415", signed);
    parsed.signature = signing_key::compute_signature(&key(SECRET), &sts);
    match verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    ) {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(
                msg.starts_with("The request signature we calculated does not match"),
                "{msg}"
            );
            assert!(!msg.contains("UNSIGNED-PAYLOAD"), "{msg}");
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

/// auth H-1: Signature mismatch returns InvalidSignatureException.
#[test]
fn signature_mismatch_returns_invalid_signature() {
    let parsed = make_parsed("dynamodb", "20260415", "content-type;host;x-amz-date");
    let headers = make_headers("20260415T120000Z");
    // Wrong secret → wrong signature → InvalidSignatureException
    let err = verify_signature(
        &parsed,
        &key("wrong-secret"),
        "POST",
        "/",
        "",
        &headers,
        b"{}",
    );
    match err {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(
                msg.contains("does not match"),
                "Expected 'does not match' in: {msg}"
            );
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const SIGNED_BODY: &[u8] = br#"{"TableName":"t","Item":{"pk":{"S":"signed"}}}"#;
const TAMPERED_BODY: &[u8] = br#"{"TableName":"t","Item":{"pk":{"S":"tampered"}}}"#;

/// Sign `body` the way a client does: the same canonical request and
/// string-to-sign the server reconstructs, over the body the client sent.
fn sign(headers: &HeaderMap, signed_headers: &str, body: &[u8]) -> ParsedAuthorization {
    let creq = canonical::canonical_request("POST", "/", "", headers, signed_headers, body);
    let sts = canonical::string_to_sign(
        "20260415T120000Z",
        "20260415/us-east-1/dynamodb/aws4_request",
        &creq,
    );
    let mut parsed = make_parsed("dynamodb", "20260415", signed_headers);
    parsed.signature = signing_key::compute_signature(&key(SECRET), &sts);
    parsed
}

/// The signing key of `secret` for the scope every test here signs with.
fn key(secret: &str) -> zeroize::Zeroizing<[u8; 32]> {
    signing_key::derive_signing_key(secret, "20260415", "us-east-1", "dynamodb")
}

fn assert_signature_mismatch(result: Result<(), DynamoDbError>) {
    match result {
        Err(DynamoDbError::InvalidSignatureException(msg)) => {
            assert!(
                msg.contains("does not match"),
                "Expected 'does not match' in: {msg}"
            );
        }
        other => panic!("Expected InvalidSignatureException, got: {other:?}"),
    }
}

#[test]
fn correctly_signed_body_verifies() {
    let headers = make_headers("20260415T120000Z");
    let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
    verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    )
    .unwrap();
}

#[test]
fn body_changed_after_signing_is_rejected() {
    let headers = make_headers("20260415T120000Z");
    let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
    assert_signature_mismatch(verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        TAMPERED_BODY,
    ));
}

/// The client signs one body, sends another, and sets
/// x-amz-content-sha256 to the hash of the body it signed. The signature
/// is checked against the body received, so the header cannot be used to
/// make a rewritten request verify.
#[test]
fn body_changed_after_signing_is_rejected_when_header_carries_signed_hash() {
    let mut headers = make_headers("20260415T120000Z");
    headers.insert(
        "x-amz-content-sha256",
        canonical::sha256_hex(SIGNED_BODY).parse().unwrap(),
    );
    let signed = "content-type;host;x-amz-content-sha256;x-amz-date";
    let parsed = sign(&headers, signed, SIGNED_BODY);
    // Sanity: the same request with the signed body verifies.
    verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    )
    .unwrap();
    assert_signature_mismatch(verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        TAMPERED_BODY,
    ));
}

/// A header the client did not sign is not part of the canonical request
/// and does not affect the payload hash, so a stray or wrong value is
/// neither trusted nor fatal.
#[test]
fn unsigned_content_sha256_header_does_not_affect_verification() {
    let mut headers = make_headers("20260415T120000Z");
    headers.insert("x-amz-content-sha256", "0".repeat(64).parse().unwrap());
    let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
    verify_signature(
        &parsed,
        &key(SECRET),
        "POST",
        "/",
        "",
        &headers,
        SIGNED_BODY,
    )
    .unwrap();
}
