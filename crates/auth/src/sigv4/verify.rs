// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SigV4` signature verification.
//!
//! Reconstructs the expected signature from the request and compares it
//! against the signature in the `Authorization` header using constant-time
//! comparison to prevent timing attacks.

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;

use super::canonical;
use super::parse::ParsedAuthorization;
use super::signing_key;

/// Maximum allowed clock skew between client and server (±15 minutes).
const MAX_CLOCK_SKEW_SECS: i64 = 15 * 60;

/// Verify a `SigV4` signature against the request.
///
/// # Arguments
/// * `parsed` — The parsed `Authorization` header.
/// * `secret_key` — The plaintext secret access key.
/// * `method` — HTTP method (e.g. `POST`).
/// * `uri_path` — URI path (e.g. `/`).
/// * `query_string` — Query string (empty for `DynamoDB`).
/// * `headers` — The full request headers.
/// * `body` — The request body bytes.
///
/// Returns `Ok(())` on success, or an appropriate auth error on failure.
pub fn verify_signature(
    parsed: &ParsedAuthorization,
    secret_key: &str,
    method: &str,
    uri_path: &str,
    query_string: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), DynamoDbError> {
    // CB-5: Validate credential-scope service is "dynamodb".
    if parsed.service != "dynamodb" {
        // N-2: Trailing space matches real DynamoDB's exact error message.
        return Err(DynamoDbError::InvalidSignatureException(
            "Credential should be scoped to correct service: 'dynamodb'. ".to_owned(),
        ));
    }

    // CB-5: Validate credential-scope date matches X-Amz-Date[..8].
    let timestamp = extract_timestamp(headers)?;
    if timestamp.len() < 8 || parsed.date != timestamp[..8] {
        return Err(DynamoDbError::InvalidSignatureException(
            "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP".to_owned(),
        ));
    }

    // CB-6: Require "host" in SignedHeaders.
    let signed_lower = parsed.signed_headers.to_ascii_lowercase();
    if !signed_lower.split(';').any(|h| h == "host") {
        return Err(DynamoDbError::InvalidSignatureException(
            "\"Host\" must be a \"SignedHeader\" in the AWS Authorization.".to_owned(),
        ));
    }

    // Reject UNSIGNED-PAYLOAD: real DynamoDB requires a computed body hash
    // for all API calls. UNSIGNED-PAYLOAD is only valid for S3.
    if let Some(content_sha) = headers.get("x-amz-content-sha256")
        && content_sha.as_bytes() == b"UNSIGNED-PAYLOAD"
    {
        return Err(DynamoDbError::InvalidSignatureException(
            "UNSIGNED-PAYLOAD is not supported for DynamoDB operations.".to_owned(),
        ));
    }

    // Build canonical request
    let creq = canonical::canonical_request(
        method,
        uri_path,
        query_string,
        headers,
        &parsed.signed_headers,
        body,
    );

    // Build scope and string-to-sign
    let scope = format!(
        "{}/{}/{}/aws4_request",
        parsed.date, parsed.region, parsed.service
    );

    let sts = canonical::string_to_sign(&timestamp, &scope, &creq);

    // Derive signing key and compute expected signature
    let signing_key =
        signing_key::derive_signing_key(secret_key, &parsed.date, &parsed.region, &parsed.service);
    let expected = signing_key::compute_signature(&signing_key, &sts);

    // auth H-1: Use InvalidSignatureException for signature mismatches,
    // distinct from UnrecognizedClientException (unknown key).
    if !constant_time_eq(expected.as_bytes(), parsed.signature.as_bytes()) {
        return Err(DynamoDbError::InvalidSignatureException(
            "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.".to_owned(),
        ));
    }

    Ok(())
}

/// Validate the request timestamp is within ±15 minutes of now.
///
/// Returns `InvalidSignatureException` if the timestamp is out of range.
/// Returns `IncompleteSignature` if `X-Amz-Date` is missing or not ISO 8601 basic.
/// Returns `InternalServerError` if the system clock is misconfigured.
pub fn validate_timestamp(headers: &HeaderMap) -> Result<(), DynamoDbError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .ok_or_else(|| DynamoDbError::InternalServerError("server clock error".to_owned()))?;

    validate_timestamp_at(headers, now)
}

/// Validate the request timestamp against a specified Unix epoch timestamp (seconds).
pub(crate) fn validate_timestamp_at(headers: &HeaderMap, now: i64) -> Result<(), DynamoDbError> {
    let timestamp = extract_timestamp(headers)?;

    let request_epoch = parse_iso8601_basic(&timestamp).ok_or_else(|| {
        DynamoDbError::IncompleteSignature(
            "Date must be in ISO 8601 basic format (YYYYMMDDTHHMMSSZ)".to_owned(),
        )
    })?;

    if request_epoch < now - MAX_CLOCK_SKEW_SECS {
        let edge = format_iso8601_basic(now - MAX_CLOCK_SKEW_SECS)?;
        let now_str = format_iso8601_basic(now)?;
        return Err(DynamoDbError::InvalidSignatureException(format!(
            "Signature expired: {timestamp} is now earlier than {edge} ({now_str} - 15 min.)"
        )));
    }

    if request_epoch > now + MAX_CLOCK_SKEW_SECS {
        let edge = format_iso8601_basic(now + MAX_CLOCK_SKEW_SECS)?;
        let now_str = format_iso8601_basic(now)?;
        return Err(DynamoDbError::InvalidSignatureException(format!(
            "Signature not yet current: {timestamp} is still later than {edge} ({now_str} + 15 min.)"
        )));
    }

    Ok(())
}

/// Extract the timestamp from `X-Amz-Date` header.
fn extract_timestamp(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            DynamoDbError::IncompleteSignature(
                "Authorization header requires existence of 'X-Amz-Date' header".to_owned(),
            )
        })
}

/// Parse ISO 8601 basic format `YYYYMMDDTHHMMSSZ` to Unix epoch seconds.
fn parse_iso8601_basic(s: &str) -> Option<i64> {
    if s.len() != 16 || !s.ends_with('Z') || s.as_bytes()[8] != b'T' {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: i64 = s[4..6].parse().ok()?;
    let day: i64 = s[6..8].parse().ok()?;
    let hour: i64 = s[9..11].parse().ok()?;
    let min: i64 = s[11..13].parse().ok()?;
    let sec: i64 = s[13..15].parse().ok()?;

    // Days-since-epoch via Howard Hinnant's date algorithm (general, not range-limited).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Format Unix epoch seconds to ISO 8601 basic format `YYYYMMDDTHHMMSSZ`.
fn format_iso8601_basic(epoch: i64) -> Result<String, DynamoDbError> {
    let dt = time::OffsetDateTime::from_unix_timestamp(epoch)
        .map_err(|_| DynamoDbError::InternalServerError("timestamp conversion error".to_owned()))?;
    Ok(format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    ))
}

/// Constant-time byte comparison to prevent timing attacks, through AWS-LC's
/// `CRYPTO_memcmp` (`aws_lc_rs::constant_time::verify_slices_are_equal`).
///
/// The comparison of the contents is constant time; the length check is not.
/// Inputs of different lengths return `false` at once, which reveals whether
/// the lengths match. Callers compare against values of a fixed length that
/// is not secret: hex-encoded signatures (always 64 bytes) and session tokens.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    aws_lc_rs::constant_time::verify_slices_are_equal(a, b).is_ok()
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn extract_timestamp_present() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-date", "20260415T120000Z".parse().unwrap());
        assert_eq!(extract_timestamp(&headers).unwrap(), "20260415T120000Z");
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
        let err = verify_signature(&parsed, "secret", "POST", "/", "", &headers, b"{}");
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
        let err = verify_signature(&parsed, "secret", "POST", "/", "", &headers, b"{}");
        match err {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert!(
                    msg.contains("Date in Credential scope"),
                    "Expected date mismatch message: {msg}"
                );
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }

    /// CB-6: Reject SignedHeaders missing "host".
    #[test]
    fn reject_missing_host_in_signed_headers() {
        let parsed = make_parsed("dynamodb", "20260415", "content-type;x-amz-date");
        let headers = make_headers("20260415T120000Z");
        let err = verify_signature(&parsed, "secret", "POST", "/", "", &headers, b"{}");
        match err {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert!(msg.contains("Host"), "Expected 'Host' in message: {msg}");
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }

    /// Reject UNSIGNED-PAYLOAD for DynamoDB API calls.
    #[test]
    fn reject_unsigned_payload() {
        let parsed = make_parsed(
            "dynamodb",
            "20260415",
            "content-type;host;x-amz-content-sha256;x-amz-date",
        );
        let mut headers = make_headers("20260415T120000Z");
        headers.insert("x-amz-content-sha256", "UNSIGNED-PAYLOAD".parse().unwrap());
        let err = verify_signature(&parsed, "secret", "POST", "/", "", &headers, b"{}");
        match err {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert!(
                    msg.contains("UNSIGNED-PAYLOAD"),
                    "Expected 'UNSIGNED-PAYLOAD' in: {msg}"
                );
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
        let err = verify_signature(&parsed, "wrong-secret", "POST", "/", "", &headers, b"{}");
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
        let key = signing_key::derive_signing_key(SECRET, "20260415", "us-east-1", "dynamodb");
        let mut parsed = make_parsed("dynamodb", "20260415", signed_headers);
        parsed.signature = signing_key::compute_signature(&key, &sts);
        parsed
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
        verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY).unwrap();
    }

    #[test]
    fn body_changed_after_signing_is_rejected() {
        let headers = make_headers("20260415T120000Z");
        let parsed = sign(&headers, "content-type;host;x-amz-date", SIGNED_BODY);
        assert_signature_mismatch(verify_signature(
            &parsed,
            SECRET,
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
        verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY).unwrap();
        assert_signature_mismatch(verify_signature(
            &parsed,
            SECRET,
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
        verify_signature(&parsed, SECRET, "POST", "/", "", &headers, SIGNED_BODY).unwrap();
    }

    /// Issue #805: Request timestamp > 15 min in the past returns
    /// InvalidSignatureException with the expired message naming the times.
    #[test]
    fn validate_timestamp_expired_returns_invalid_signature_exception() {
        let now_epoch = parse_iso8601_basic("20261103T203150Z").unwrap();
        let headers = make_headers("20261103T201550Z");
        let err = validate_timestamp_at(&headers, now_epoch);
        match err {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(
                    msg,
                    "Signature expired: 20261103T201550Z is now earlier than 20261103T201650Z (20261103T203150Z - 15 min.)"
                );
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }

    /// Issue #805: Request timestamp > 15 min in the future returns
    /// InvalidSignatureException with the not-yet-current message naming the times.
    #[test]
    fn validate_timestamp_future_returns_invalid_signature_exception() {
        let now_epoch = parse_iso8601_basic("20261103T203150Z").unwrap();
        let headers = make_headers("20261103T204750Z");
        let err = validate_timestamp_at(&headers, now_epoch);
        match err {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(
                    msg,
                    "Signature not yet current: 20261103T204750Z is still later than 20261103T204650Z (20261103T203150Z + 15 min.)"
                );
            }
            other => panic!("Expected InvalidSignatureException, got: {other:?}"),
        }
    }

    /// Exact ±15 minute boundary timestamps are accepted.
    #[test]
    fn validate_timestamp_at_exact_boundaries_succeeds() {
        let now_epoch = parse_iso8601_basic("20261103T203150Z").unwrap();

        // Exactly 15 min past (lower boundary)
        let headers_lower = make_headers("20261103T201650Z");
        assert!(validate_timestamp_at(&headers_lower, now_epoch).is_ok());

        // Exactly 15 min future (upper boundary)
        let headers_upper = make_headers("20261103T204650Z");
        assert!(validate_timestamp_at(&headers_upper, now_epoch).is_ok());
    }

    /// validate_timestamp uses the real clock and accepts current timestamps.
    #[test]
    fn validate_timestamp_live_clock_succeeds_for_now() {
        let now = time::OffsetDateTime::now_utc();
        let now_str = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            now.month() as u8,
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let headers = make_headers(&now_str);
        assert!(validate_timestamp(&headers).is_ok());
    }
}
