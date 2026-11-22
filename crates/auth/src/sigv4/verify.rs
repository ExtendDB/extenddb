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
    let timestamp = basic_form(request_instant(headers)?);
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
/// Returns the request date in the UTC basic form (see `basic_form`), which
/// the caller passes to `check_scope`.
///
/// Returns `IncompleteSignature` if the timestamp is missing or in a form
/// the service does not read (see `request_instant`),
/// `UnrecognizedClientException` if it is out of range, and
/// `InternalServerError` if the system clock is misconfigured.
pub fn validate_timestamp(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let request = request_instant(headers)?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .ok_or_else(|| DynamoDbError::InternalServerError("server clock error".to_owned()))?;

    let request_epoch = request.unix_timestamp();

    let skew = (now - request_epoch).abs();
    if skew > MAX_CLOCK_SKEW_SECS {
        return Err(DynamoDbError::UnrecognizedClientException(
            "Signature expired: the signature is too old or the request date is too far in the future.".to_owned(),
        ));
    }

    Ok(basic_form(request))
}

/// The instant the request date names (see `request_date`).
///
/// Returns `IncompleteSignature` when the request has no date, or a date in
/// a form the service does not read (see `parse_request_date`).
fn request_instant(headers: &HeaderMap) -> Result<time::OffsetDateTime, DynamoDbError> {
    let timestamp = extract_timestamp(headers)?;
    parse_request_date(&timestamp).ok_or_else(|| {
        // Measured 2026-11-21: the service echoes the value it refuses.
        DynamoDbError::IncompleteSignature(format!(
            "Date must be in ISO-8601 'basic format'. Got '{timestamp}'. See http://en.wikipedia.org/wiki/ISO_8601"
        ))
    })
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

/// Parse the request date as the service reads it (measured 2026-11-21 in
/// us-east-1), to the instant in UTC. The date is `YYYYMMDD` or
/// `YYYY-MM-DD`; the time, after an upper-case `T`, is `HHMMSS` or
/// `HH:MM:SS`, each form chosen on its own (`2026-11-21T220639Z` and
/// `20261121T22:06:40Z` are read), then an optional fraction (a `.` and any
/// digits, none included) and an optional zone (`Z`, or an offset of two
/// digits of hours and optional two digits of minutes: `+01:00`, `+0100`,
/// `+01`); no zone is UTC. A date alone, or with a `T` and nothing after
/// it, is midnight UTC. Each part is checked against a limit (month 12, day
/// 31, hour 24, minute 60, second 60, offset 99 hours and 99 minutes) and
/// the parts are then added up with no calendar check: month 00 is January,
/// day 00 is the day before the first, `2026-02-30` is 2 March and
/// `T24:00:00` is the next midnight. A part over its limit, a lower-case
/// `t` or `z`, a space for `T`, a `,` before the fraction, a time without
/// seconds and a `T` followed by a zone alone are refused.
fn parse_request_date(s: &str) -> Option<time::OffsetDateTime> {
    let (year, month, day, rest) = three(s, 4, '-')?;
    if month > 12 || day > 31 {
        return None;
    }

    let (clock, rest) = match rest.strip_prefix('T') {
        None if !rest.is_empty() => return None,
        None | Some("") => (0, ""),
        Some(rest) => {
            let (hour, minute, second, rest) = three::<u8>(rest, 2, ':')?;
            if hour > 24 || minute > 60 || second > 60 {
                return None;
            }
            let rest = rest
                .strip_prefix('.')
                .map_or(rest, |f| f.trim_start_matches(|c: char| c.is_ascii_digit()));
            let clock = i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second);
            (clock, rest)
        }
    };

    let offset = match rest {
        "" | "Z" => 0,
        _ => {
            let (sign, rest) = match rest.strip_prefix('+') {
                Some(rest) => (1, rest),
                None => (-1, rest.strip_prefix('-')?),
            };
            let (hours, rest) = digits::<i64>(rest, 2)?;
            let (minutes, rest) =
                digits(rest.strip_prefix(':').unwrap_or(rest), 2).unwrap_or((0, rest));
            if !rest.is_empty() {
                return None;
            }
            sign * (hours * 3600 + minutes * 60)
        }
    };

    time::Date::from_calendar_date(year, time::Month::try_from(month.max(1)).ok()?, 1)
        .ok()?
        .checked_add(time::Duration::days(i64::from(day) - 1))?
        .midnight()
        .assume_utc()
        .checked_add(time::Duration::seconds(clock - offset))
}

/// Three numbers, the first of `first` digits and the others of two, with
/// `sep` between them (the extended form) or nothing (the basic form), and
/// the rest of `s`. A separator at one of the two places only is refused.
fn three<T: std::str::FromStr>(s: &str, first: usize, sep: char) -> Option<(T, u8, u8, &str)> {
    let (a, rest) = digits(s, first)?;
    let ext = rest.starts_with(sep);
    let (b, rest) = digits(rest.strip_prefix(sep).unwrap_or(rest), 2)?;
    let rest = if ext { rest.strip_prefix(sep)? } else { rest };
    let (c, rest) = digits(rest, 2)?;
    Some((a, b, c, rest))
}

/// The leading `n` ASCII digits of `s` as a number, and the rest of `s`.
fn digits<T: std::str::FromStr>(s: &str, n: usize) -> Option<(T, &str)> {
    let (head, rest) = s.split_at_checked(n)?;
    if !head.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((head.parse().ok()?, rest))
}

/// The UTC `YYYYMMDDTHHMMSSZ` form of `instant`, fraction dropped. The
/// service uses it for the skew check, the scope date and the date line of
/// the string to sign, whatever form the request sent (measured 2026-11-21);
/// the `x-amz-date` and `date` canonical headers keep the value as sent.
pub(crate) fn basic_form(instant: time::OffsetDateTime) -> String {
    let utc = instant.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        utc.year(),
        utc.month() as u8,
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second(),
    )
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
