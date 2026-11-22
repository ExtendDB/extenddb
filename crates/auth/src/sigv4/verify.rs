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

/// The scope complaint for a date that is not the `YYYYMMDD` prefix of
/// the request date, with the trailing period and space the service sends.
pub(crate) const SCOPE_DATE_COMPLAINT: &str =
    "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP. ";

/// Whether the scope `date` is the `YYYYMMDD` prefix of `timestamp`.
///
/// The comparison is on bytes, so a date of another length, a date carrying
/// letters and a date naming a day that does not exist are all a mismatch.
fn scope_date_matches(date: &str, timestamp: &str) -> bool {
    timestamp.get(..8) == Some(date)
}

/// Check the credential scope's region, service and date against this server
/// and the request timestamp.
///
/// `timestamp` is the request date, which `validate_timestamp` returns.
///
/// The service reports every scope complaint in one message, region first,
/// then service, then the date, each sentence ending with a space (measured
/// 2026-09-16: a scope wrong in region and service gives "Credential should
/// be scoped to a valid region. Credential should be scoped to correct
/// service: 'dynamodb'. "). The region is not named in the message.
///
/// The date is compared here, before the access key is looked up, because the
/// service answers the same message for a key that exists and one that does
/// not (measured 2026-10-23 on signed ListTables requests: a scope date of the
/// previous day answers InvalidSignatureException with the date complaint for
/// an unknown key and for a known key). `verify_signature` compares it again,
/// so a caller that verifies a signature without this check keeps the same
/// refusal.
///
/// # Errors
///
/// Returns `InvalidSignatureException` carrying the accumulated complaints
/// when the region is not `expected_region`, the service is not `dynamodb`, or
/// the scope date is not the `YYYYMMDD` prefix of `timestamp`.
pub fn check_scope(
    parsed: &ParsedAuthorization,
    expected_region: &str,
    timestamp: &str,
) -> Result<(), DynamoDbError> {
    let mut complaints = String::new();
    if parsed.region != expected_region {
        complaints.push_str("Credential should be scoped to a valid region. ");
    }
    if parsed.service != "dynamodb" {
        complaints.push_str("Credential should be scoped to correct service: 'dynamodb'. ");
    }
    if !scope_date_matches(&parsed.date, timestamp) {
        complaints.push_str(SCOPE_DATE_COMPLAINT);
    }
    if complaints.is_empty() {
        Ok(())
    } else {
        Err(DynamoDbError::InvalidSignatureException(complaints))
    }
}

/// Verify a `SigV4` signature against the request.
///
/// # Arguments
/// * `parsed` - The parsed `Authorization` header.
/// * `signing_key` - The `SigV4` signing key of the access key for the
///   request's scope date, region and service, from the credential cache.
/// * `method` - HTTP method (e.g. `POST`).
/// * `uri_path` - URI path (e.g. `/`).
/// * `query_string` - Query string (empty for `DynamoDB`).
/// * `headers` - The full request headers.
/// * `body` - The request body bytes.
///
/// Returns `Ok(())` on success, or an appropriate auth error on failure.
pub fn verify_signature(
    parsed: &ParsedAuthorization,
    signing_key: &[u8; 32],
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

    // CB-5: Validate credential-scope date matches the request date[..8]. The
    // pre-lookup check_scope compares it too; this keeps the refusal for a
    // caller that verifies a signature on its own.
    let timestamp = extract_timestamp(headers)?;
    if !scope_date_matches(&parsed.date, &timestamp) {
        return Err(DynamoDbError::InvalidSignatureException(
            SCOPE_DATE_COMPLAINT.to_owned(),
        ));
    }

    // CB-6: Require "host" in SignedHeaders.
    let signed_lower = parsed.signed_headers.to_ascii_lowercase();
    if !signed_lower.split(';').any(|h| h == "host") {
        return Err(DynamoDbError::InvalidSignatureException(
            "\"Host\" must be a \"SignedHeader\" in the AWS Authorization.".to_owned(),
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

    // Compute the expected signature under the cached signing key.
    let expected = signing_key::compute_signature(signing_key, &sts);

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
/// Returns the request date (see `request_date`), which the caller passes to
/// `check_scope`.
///
/// Returns `UnrecognizedClientException` if the timestamp is missing or out of range.
/// Returns `InternalServerError` if the system clock is misconfigured.
pub fn validate_timestamp(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let timestamp = extract_timestamp(headers)?;

    // Parse X-Amz-Date: YYYYMMDDTHHMMSSZ
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .ok_or_else(|| DynamoDbError::InternalServerError("server clock error".to_owned()))?;

    let request_epoch = parse_iso8601_basic(&timestamp).ok_or_else(|| {
        // Measured 2026-11-21: the service echoes the value it refuses.
        DynamoDbError::IncompleteSignature(format!(
            "Date must be in ISO-8601 'basic format'. Got '{timestamp}'. See http://en.wikipedia.org/wiki/ISO_8601"
        ))
    })?;

    let skew = (now - request_epoch).abs();
    if skew > MAX_CLOCK_SKEW_SECS {
        return Err(DynamoDbError::UnrecognizedClientException(
            "Signature expired: the signature is too old or the request date is too far in the future.".to_owned(),
        ));
    }

    Ok(timestamp)
}

/// Extract the timestamp from `X-Amz-Date` header.
/// Without that header, the `Date` value counts (see `request_date`).
fn extract_timestamp(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    request_date(headers).map(str::to_owned).ok_or_else(|| {
        DynamoDbError::IncompleteSignature(
            "Authorization header requires existence of 'X-Amz-Date' header".to_owned(),
        )
    })
}

/// The request's date: the `X-Amz-Date` value when the request has that
/// header, else the `Date` value. Measured 2026-11-21: the service reads
/// `Date` only when `X-Amz-Date` is absent, and an empty value is no date.
pub(crate) fn request_date(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-amz-date")
        .or_else(|| headers.get("date"))
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
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
mod tests;
