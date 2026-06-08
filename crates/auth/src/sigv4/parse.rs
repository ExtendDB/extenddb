// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Parse the AWS `SigV4` `Authorization` header.
//!
//! Format:
//! ```text
//! AWS4-HMAC-SHA256 Credential=AKID/20260415/us-east-1/dynamodb/aws4_request,
//!   SignedHeaders=content-type;host;x-amz-date;x-amz-target,
//!   Signature=<hex>
//! ```

use extenddb_core::error::DynamoDbError;

/// Parsed components of a `SigV4` `Authorization` header.
#[derive(Debug, PartialEq)]
pub struct ParsedAuthorization {
    /// The access key ID (e.g. `AKIAIOSFODNN7EXAMPLE`).
    pub access_key_id: String,
    /// The credential scope date (e.g. `20260415`).
    pub date: String,
    /// The region (e.g. `us-east-1`).
    pub region: String,
    /// The service (e.g. `dynamodb`).
    pub service: String,
    /// Sorted, semicolon-delimited signed header names.
    pub signed_headers: String,
    /// The hex-encoded signature.
    pub signature: String,
}

/// Parse a `SigV4` `Authorization` header value.
///
/// Returns `IncompleteSignature` if the header is malformed.
/// S-2: Rejects headers exceeding 8 KB to prevent heap abuse.
pub fn parse_authorization(header: &str) -> Result<ParsedAuthorization, DynamoDbError> {
    // S-2: Cap Authorization header length to prevent heap abuse from oversized headers.
    const MAX_AUTH_HEADER_LEN: usize = 8 * 1024;
    if header.len() > MAX_AUTH_HEADER_LEN {
        return Err(incomplete(
            "Authorization header exceeds maximum allowed length",
        ));
    }

    let rest = header
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| incomplete("Authorization header requires 'AWS4-HMAC-SHA256' algorithm"))?;

    // Split into key=value pairs. AWS SDKs use ", " (comma-space) but the
    // spec doesn't mandate exact whitespace, so split on ',' and trim.
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in rest.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val);
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val);
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val);
        }
    }

    let credential = credential
        .ok_or_else(|| incomplete("Authorization header requires 'Credential' parameter"))?;
    let signed_headers = signed_headers
        .ok_or_else(|| incomplete("Authorization header requires 'SignedHeaders' parameter"))?;
    let signature = signature
        .ok_or_else(|| incomplete("Authorization header requires 'Signature' parameter"))?;

    // Credential = access_key/date/region/service/aws4_request
    let parts: Vec<&str> = credential.splitn(5, '/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(incomplete(
            "Credential must follow the format: AKID/date/region/service/aws4_request",
        ));
    }

    Ok(ParsedAuthorization {
        access_key_id: parts[0].to_owned(),
        date: parts[1].to_owned(),
        region: parts[2].to_owned(),
        service: parts[3].to_owned(),
        signed_headers: signed_headers.to_owned(),
        signature: signature.to_owned(),
    })
}

fn incomplete(msg: &str) -> DynamoDbError {
    DynamoDbError::IncompleteSignature(msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_header() {
        let header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20260415/us-east-1/dynamodb/aws4_request, \
            SignedHeaders=content-type;host;x-amz-date;x-amz-target, \
            Signature=abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

        let parsed = parse_authorization(header).unwrap();
        assert_eq!(parsed.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.date, "20260415");
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.service, "dynamodb");
        assert_eq!(
            parsed.signed_headers,
            "content-type;host;x-amz-date;x-amz-target"
        );
        assert_eq!(
            parsed.signature,
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn reject_wrong_algorithm() {
        let header = "AWS4-HMAC-SHA512 Credential=AKID/20260415/us-east-1/dynamodb/aws4_request, \
            SignedHeaders=host, Signature=abc";
        assert!(parse_authorization(header).is_err());
    }

    #[test]
    fn reject_missing_credential() {
        let header = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc";
        assert!(parse_authorization(header).is_err());
    }

    #[test]
    fn reject_bad_credential_format() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20260415/us-east-1, \
            SignedHeaders=host, Signature=abc";
        assert!(parse_authorization(header).is_err());
    }

    #[test]
    fn reject_oversized_header() {
        // S-2: Headers exceeding 8 KB must be rejected.
        let header = "A".repeat(8 * 1024 + 1);
        assert!(parse_authorization(&header).is_err());
    }
}
