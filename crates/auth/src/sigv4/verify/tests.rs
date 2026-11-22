// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Measured 2026-11-21 in us-east-1 with the documented example key: the
/// forms of `X-Amz-Date` the service reads, with the UTC basic form it
/// uses for each (its skew texts show it), and the forms it refuses with
/// the `basic format` text.
#[test]
fn the_request_date_is_read_in_the_basic_and_the_extended_form() {
    let rows = [
        ("20261121T220642Z", Some("20261121T220642Z")),
        ("2026-11-21T22:06:32Z", Some("20261121T220632Z")),
        ("2026-11-21T22:06:33.123Z", Some("20261121T220633Z")),
        ("2026-11-21T22:06:34.123456Z", Some("20261121T220634Z")),
        ("2026-11-21T22:06:34+00:00", Some("20261121T220634Z")),
        ("2026-11-21T23:06:35+01:00", Some("20261121T220635Z")),
        ("2026-11-21T22:06:36", Some("20261121T220636Z")),
        ("2026-11-21", Some("20261121T000000Z")),
        ("20261121T220638.123Z", Some("20261121T220638Z")),
        ("20261121T220639", Some("20261121T220639Z")),
        ("2026-11-21T220639Z", Some("20261121T220639Z")),
        ("20261121T22:06:40Z", Some("20261121T220640Z")),
        ("2026-11-22T01:07:13+03:00", Some("20261121T220713Z")),
        ("2026-11-21T17:08:02-05:00", Some("20261121T220802Z")),
        ("2026-11-21T23:08:02+0100", Some("20261121T220802Z")),
        ("2026-11-21T23:08:03+01", Some("20261121T220803Z")),
        ("2026-11-21T22:08:04.Z", Some("20261121T220804Z")),
        ("2026-11-21t22:06:36z", None),
        ("2026-11-21 22:06:37Z", None),
        ("2026-11-21T22:08Z", None),
        ("Sat, 21 Nov 2026 01:36:16 GMT", None),
        // Five digits of time (measured 22 Nov 2026 00:52Z).
        ("20261122T00524Z", None),
        ("", None),
        // Parts out of range and other forms (measured 22:56Z to 22:57Z).
        ("2026-1121T22:56:13Z", None),
        ("202611-21T22:56:14Z", None),
        ("2026-11-21T22:5615Z", None),
        ("2026-11-21Z", None),
        ("2026-11-21T", Some("20261121T000000Z")),
        ("2026-11-21T22:56:17,123Z", None),
        ("20261121T2256Z", None),
        ("2026-11-23T00:56:18+26:00", Some("20261121T225618Z")),
        ("2026-11-22T00:56:19+01:60", Some("20261121T225619Z")),
        ("20261399T000000Z", None),
        ("20261121T240000Z", Some("20261122T000000Z")),
        ("20261121T226000Z", Some("20261121T230000Z")),
        ("20261121T225660Z", Some("20261121T225700Z")),
        ("2026-02-30T22:56:22Z", Some("20260302T225622Z")),
        ("20261321T225656Z", None),
        ("20261199T225657Z", None),
        ("20261100T225657Z", Some("20261031T225657Z")),
        ("20261131T225658Z", Some("20261201T225658Z")),
        ("20260021T225659Z", Some("20260121T225659Z")),
        ("20261121T255659Z", None),
        ("20261121T995700Z", None),
        ("20261121T229901Z", None),
        ("20261121T225799Z", None),
        ("2026-11-21T22:57:02+99:00", Some("20261117T195702Z")),
        ("2026-11-21T22:57:03+00:99", Some("20261121T211803Z")),
        ("2026-11-21T24:00:00Z", Some("20261122T000000Z")),
        ("2026-11-21TZ", None),
    ];
    for (value, want) in rows {
        let got = parse_request_date(value).map(basic_form);
        assert_eq!(got.as_deref(), want, "{value:?}");
    }
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

/// `validate_timestamp` hands the request date back in the UTC basic
/// form, whatever form the header sent, so the caller compares the scope
/// date against the date the service would use (measured 2026-11-21: a
/// scope date of the UTC day passes for `2026-11-22T01:07:13+03:00`,
/// the local day does not).
#[test]
fn validate_timestamp_returns_the_utc_basic_form() {
    let now = time::OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .unwrap();
    let plus_three = now.to_offset(time::UtcOffset::from_hms(3, 0, 0).unwrap());
    let rfc3339 = time::format_description::well_known::Rfc3339;
    for value in [
        basic_form(now),
        now.format(&rfc3339).unwrap(),
        plus_three.format(&rfc3339).unwrap(),
    ] {
        let headers = make_headers(&value);
        assert_eq!(
            validate_timestamp(&headers).unwrap(),
            basic_form(now),
            "{value}"
        );
    }
}

/// Measured 2026-11-21 and 22 with signed ListTables requests: a request
/// whose `X-Amz-Date` is in the extended form, or carries a second or a
/// minute of 60, verifies when the date line of the string to sign is the
/// UTC basic form of the instant read, fraction dropped, and fails as a
/// signature mismatch when it is the value as sent or the local time. The
/// `x-amz-date` canonical header keeps the value as sent.
#[test]
fn the_string_to_sign_carries_the_utc_basic_form_of_the_request_date() {
    let rows = [
        ("2026-04-15T12:00:00Z", "20260415T120000Z", true),
        ("2026-04-15T12:00:00Z", "2026-04-15T12:00:00Z", false),
        ("2026-04-15T12:00:00.123Z", "20260415T120000Z", true),
        (
            "2026-04-15T12:00:00.123Z",
            "2026-04-15T12:00:00.123Z",
            false,
        ),
        ("2026-04-15T12:00:00+00:00", "20260415T120000Z", true),
        (
            "2026-04-15T12:00:00+00:00",
            "2026-04-15T12:00:00+00:00",
            false,
        ),
        ("2026-04-15T15:00:00+03:00", "20260415T120000Z", true),
        ("2026-04-15T15:00:00+03:00", "20260415T150000Z", false),
        // A second of 60 reads as the next minute, a minute of 60 as the
        // next hour, and the date line is that instant (22 Nov 2026 00:52Z:
        // `20261122T005260Z` signed as `20261122T005300Z` answered 200 and
        // signed as sent a mismatch; `20261122T006050Z` as `20261122T010050Z`
        // answered 200 and `20261122T006051Z` as sent a mismatch).
        ("20260415T120060Z", "20260415T120100Z", true),
        ("20260415T120060Z", "20260415T120060Z", false),
        ("20260415T126050Z", "20260415T130050Z", true),
        ("20260415T126051Z", "20260415T126051Z", false),
    ];
    for (sent, date_line, verifies) in rows {
        let headers = make_headers(sent);
        let signed = "content-type;host;x-amz-date";
        let parsed = sign_with_date(&headers, signed, SIGNED_BODY, date_line);
        let result = verify_signature(
            &parsed,
            &key(SECRET),
            "POST",
            "/",
            "",
            &headers,
            SIGNED_BODY,
        );
        if verifies {
            result.unwrap_or_else(|e| panic!("{sent} signed as {date_line}: {e:?}"));
        } else {
            assert_signature_mismatch(result);
        }
    }
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
    sign_with_date(headers, signed_headers, body, "20260415T120000Z")
}

/// `sign` with `date_line` as the date line of the string to sign.
fn sign_with_date(
    headers: &HeaderMap,
    signed_headers: &str,
    body: &[u8],
    date_line: &str,
) -> ParsedAuthorization {
    let creq = canonical::canonical_request("POST", "/", "", headers, signed_headers, body);
    let sts =
        canonical::string_to_sign(date_line, "20260415/us-east-1/dynamodb/aws4_request", &creq);
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
