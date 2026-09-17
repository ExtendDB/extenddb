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
    // The payload hash is computed from the body the server received. The
    // client's x-amz-content-sha256 header is not consulted: a request signed
    // for one body and transmitted with another must fail verification, and
    // trusting the header would let the sender choose the hash that the
    // signature is checked against. When the client includes the header in
    // SignedHeaders it is covered as an ordinary header by
    // build_canonical_headers. A client that signed a literal such as
    // UNSIGNED-PAYLOAD in place of the hash fails as a plain signature
    // mismatch, which is what the service returns.
    let payload_hash = sha256_hex(body);

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
/// Per the `SigV4` spec, header names are lowercased, values are trimmed with
/// internal whitespace collapsed, and a header that appears more than once
/// contributes its values joined by commas in the order they were sent.
/// Each line ends with `\n`. CB-7: Header names are explicitly lowercased to
/// handle clients that send mixed-case `SignedHeaders` values.
fn build_canonical_headers(headers: &HeaderMap, signed_headers: &str) -> String {
    let mut result = String::new();
    // signed_headers is already sorted and semicolon-delimited.
    // N-1: The caller (`canonical_request`) already lowercases `signed_headers`,
    // but we lowercase again here as defense-in-depth — this function's contract
    // does not require pre-lowercased input.
    for name in signed_headers.split(';') {
        let lower = name.to_ascii_lowercase();
        let mut value = headers
            .get_all(lower.as_str())
            .iter()
            // A value that is not visible ASCII cannot have been signed as
            // anything; it contributes an empty value rather than disappearing,
            // so a request carrying such a header never canonicalizes the same
            // as one without it.
            .map(|v| v.to_str().unwrap_or(""))
            // Trim leading/trailing whitespace, collapse internal whitespace
            .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join(",");
        // HTTP/2 uses :authority instead of Host. If the host header is empty
        // or missing, fall back to the :authority pseudo-header value.
        // Defense-in-depth: handler.rs injects Host from URI authority for
        // HTTP/2 requests, so this fallback should rarely activate.
        if lower == "host" && value.is_empty() {
            value = headers
                .get(":authority")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
                .unwrap_or_default();
        }
        result.push_str(&lower);
        result.push(':');
        result.push_str(&value);
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

    /// The hashed-payload line is computed from the received body. A client
    /// header claiming a different hash changes nothing about that line; when
    /// the header is signed it appears among the canonical headers instead.
    #[test]
    fn payload_hash_comes_from_body_not_header() {
        let signed_body = br#"{"pk":"signed"}"#;
        let received_body = br#"{"pk":"tampered"}"#;
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:18443".parse().unwrap());
        headers.insert("x-amz-date", "20260415T120000Z".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            sha256_hex(signed_body).parse().unwrap(),
        );

        let creq = canonical_request(
            "POST",
            "/",
            "",
            &headers,
            "host;x-amz-content-sha256;x-amz-date",
            received_body,
        );

        let lines: Vec<&str> = creq.split('\n').collect();
        assert_eq!(
            lines[4],
            format!("x-amz-content-sha256:{}", sha256_hex(signed_body))
        );
        assert_eq!(lines[8], sha256_hex(received_body));
        assert_ne!(lines[8], sha256_hex(signed_body));
    }

    /// A header sent more than once contributes its values joined by commas,
    /// which is how the service canonicalizes it.
    #[test]
    fn repeated_header_values_are_comma_joined_in_order() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:18443".parse().unwrap());
        headers.insert("x-amz-date", "20260415T120000Z".parse().unwrap());
        headers.append("x-amz-meta-dup", " a ".parse().unwrap());
        headers.append("x-amz-meta-dup", "b  c".parse().unwrap());

        let creq = canonical_request(
            "POST",
            "/",
            "",
            &headers,
            "host;x-amz-date;x-amz-meta-dup",
            b"{}",
        );

        let lines: Vec<&str> = creq.split('\n').collect();
        assert_eq!(lines[5], "x-amz-meta-dup:a,b c");
    }

    /// A duplicate value that is not visible ASCII cannot be signed as anything.
    /// It must still change the canonical form, so a request carrying it does not
    /// verify under a signature made without it.
    #[test]
    fn unsignable_duplicate_value_changes_the_canonical_form() {
        let mut plain = HeaderMap::new();
        plain.insert("host", "localhost:18443".parse().unwrap());
        plain.insert("x-amz-meta-dup", "a".parse().unwrap());
        let mut with_extra = plain.clone();
        with_extra.append(
            "x-amz-meta-dup",
            axum::http::HeaderValue::from_bytes(b"\xff\xfe").unwrap(),
        );
        let a = canonical_request("POST", "/", "", &plain, "host;x-amz-meta-dup", b"{}");
        let b = canonical_request("POST", "/", "", &with_extra, "host;x-amz-meta-dup", b"{}");
        assert_ne!(a, b);
        assert!(b.contains("x-amz-meta-dup:a,\n"), "{b}");
    }
}
