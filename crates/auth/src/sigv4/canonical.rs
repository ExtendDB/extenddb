// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Canonical request construction for `SigV4` verification.
//!
//! Builds the canonical request string from the HTTP method, URI path,
//! query string, signed headers, and body hash per the AWS `SigV4` spec.

use axum::http::HeaderMap;
use sha2::{Digest, Sha256};

/// Build the canonical request string.
///
/// Format:
/// ```text
/// HTTPRequestMethod\n
/// CanonicalURI\n
/// CanonicalQueryString\n
/// CanonicalHeaders\n
/// SignedHeaders\n
/// HashedPayload
/// ```
///
/// For `DynamoDB`, the URI is always `/` and there is no query string.
pub fn canonical_request(
    method: &str,
    uri_path: &str,
    query_string: &str,
    headers: &HeaderMap,
    signed_headers: &str,
    body: &[u8],
) -> String {
    // CB-7: Normalize signed header names to lowercase per SigV4 spec.
    let signed_lower = signed_headers.to_ascii_lowercase();
    let canonical_headers = build_canonical_headers(headers, &signed_lower);
    // SigV4 spec: if the client sends x-amz-content-sha256, use that value
    // as the payload hash in the canonical request. UNSIGNED-PAYLOAD is
    // rejected by verify_signature() before this function is called.
    let payload_hash = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map_or_else(|| sha256_hex(body), str::to_owned);

    format!(
        "{method}\n{uri_path}\n{query_string}\n{canonical_headers}\n{signed_lower}\n{payload_hash}"
    )
}

/// Build the string-to-sign for `SigV4`.
///
/// Format:
/// ```text
/// AWS4-HMAC-SHA256\n
/// <timestamp>\n
/// <scope>\n
/// Hex(SHA256(canonical_request))
/// ```
#[must_use]
pub fn string_to_sign(timestamp: &str, scope: &str, canonical_request: &str) -> String {
    let hashed = sha256_hex(canonical_request.as_bytes());
    format!("AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashed}")
}

/// Build canonical headers from the signed header list.
///
/// Per the `SigV4` spec, header names are lowercased, values are trimmed,
/// and headers are sorted alphabetically. Each line ends with `\n`.
/// CB-7: Header names are explicitly lowercased to handle clients that
/// send mixed-case `SignedHeaders` values.
fn build_canonical_headers(headers: &HeaderMap, signed_headers: &str) -> String {
    let mut result = String::new();
    // signed_headers is already sorted and semicolon-delimited.
    // N-1: The caller (`canonical_request`) already lowercases `signed_headers`,
    // but we lowercase again here as defense-in-depth — this function's contract
    // does not require pre-lowercased input.
    for name in signed_headers.split(';') {
        let lower = name.to_ascii_lowercase();
        let value = headers
            .get(lower.as_str())
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // HTTP/2 uses :authority instead of Host. If the host header is empty
        // or missing, fall back to the :authority pseudo-header value.
        // Defense-in-depth: handler.rs injects Host from URI authority for
        // HTTP/2 requests, so this fallback should rarely activate.
        let value = if lower == "host" && value.is_empty() {
            headers
                .get(":authority")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(value)
        } else {
            value
        };
        // Trim leading/trailing whitespace, collapse internal whitespace
        let trimmed = value.split_whitespace().collect::<Vec<_>>().join(" ");
        result.push_str(&lower);
        result.push(':');
        result.push_str(&trimmed);
        result.push('\n');
    }
    result
}

/// Lowercase hex-encoded SHA-256 hash.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn canonical_request_basic() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:18443".parse().unwrap());
        headers.insert("x-amz-date", "20260415T120000Z".parse().unwrap());

        let creq = canonical_request("POST", "/", "", &headers, "host;x-amz-date", b"{}");

        let lines: Vec<&str> = creq.split('\n').collect();
        assert_eq!(lines[0], "POST");
        assert_eq!(lines[1], "/");
        assert_eq!(lines[2], ""); // empty query string
        assert_eq!(lines[3], "host:localhost:18443");
        assert_eq!(lines[4], "x-amz-date:20260415T120000Z");
        assert_eq!(lines[5], ""); // trailing newline from canonical headers
        assert_eq!(lines[6], "host;x-amz-date");
        assert_eq!(lines[7], sha256_hex(b"{}"));
    }

    #[test]
    fn string_to_sign_format() {
        let sts = string_to_sign(
            "20260415T120000Z",
            "20260415/us-east-1/dynamodb/aws4_request",
            "canonical-request-content",
        );
        let lines: Vec<&str> = sts.split('\n').collect();
        assert_eq!(lines[0], "AWS4-HMAC-SHA256");
        assert_eq!(lines[1], "20260415T120000Z");
        assert_eq!(lines[2], "20260415/us-east-1/dynamodb/aws4_request");
        assert_eq!(lines[3], sha256_hex(b"canonical-request-content"));
    }

    #[test]
    fn sha256_empty_payload() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// CB-7: Mixed-case SignedHeaders are lowercased in canonical output.
    #[test]
    fn canonical_request_lowercases_signed_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:18443".parse().unwrap());
        headers.insert("x-amz-date", "20260415T120000Z".parse().unwrap());

        // Pass mixed-case signed headers — output must be lowercase
        let creq = canonical_request("POST", "/", "", &headers, "Host;X-Amz-Date", b"{}");

        let lines: Vec<&str> = creq.split('\n').collect();
        assert_eq!(lines[3], "host:localhost:18443");
        assert_eq!(lines[4], "x-amz-date:20260415T120000Z");
        assert_eq!(lines[6], "host;x-amz-date"); // lowercased
    }
}
