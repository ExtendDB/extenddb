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

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::error::DynamoDbError;
use sha2::{Digest, Sha256};

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
/// `has_date` says whether the request carries an `X-Amz-Date` or a `Date`
/// header.
///
/// Returns `MissingAuthenticationToken` if the value starts with neither
/// `AWS4-` nor `Bearer` and a space, ignoring ASCII case, or if it is a
/// well-formed `Bearer` value. Returns `IncompleteSignature` if it is otherwise
/// malformed.
/// S-2: Rejects headers exceeding 8 KB to prevent heap abuse.
pub fn parse_authorization(
    header: &[u8],
    has_date: bool,
) -> Result<ParsedAuthorization, DynamoDbError> {
    // S-2: Cap Authorization header length to prevent heap abuse from oversized headers.
    const MAX_AUTH_HEADER_LEN: usize = 8 * 1024;
    if header.len() > MAX_AUTH_HEADER_LEN {
        return Err(incomplete(
            "Authorization header exceeds maximum allowed length",
        ));
    }

    // Measured 2026-11-21: DynamoDB reads each byte as a Latin-1 character,
    // and hashes the value without the spaces and tabs at its ends.
    let header: String = header.iter().copied().map(char::from).collect();
    let header = header.trim_matches([' ', '\t']);
    let hash = || BASE64.encode(Sha256::digest(header));

    // Measured 2026-11-21: DynamoDB answers a value that starts with neither
    // `AWS4-` nor `Bearer` and a space, ignoring ASCII case, as a request with
    // no Authorization header. It refuses `AWS4-` in another case before any
    // other check.
    let starts_with = |prefix: &str| {
        header
            .get(..prefix.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
    };
    let aws4 = starts_with("AWS4-");
    if !aws4 && !starts_with("Bearer ") {
        return Err(DynamoDbError::MissingAuthenticationToken(
            "Missing Authentication Token".to_owned(),
        ));
    }
    if aws4 && !header.starts_with("AWS4-") {
        return Err(incomplete(&format!(
            "Algorithm must start with 'AWS4-'; got '{header}'"
        )));
    }

    // A tab or a comma after the algorithm ends it as a space does (measured
    // 2026-11-21).
    let (algorithm, rest) = header.split_once([' ', '\t', ',']).unwrap_or((header, ""));

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    // Measured 2026-11-21: DynamoDB splits the pairs at commas and at spaces.
    for part in rest.split([',', ' ', '\t']).filter(|part| !part.is_empty()) {
        match part.split_once('=') {
            Some(("Credential", val)) => credential = Some(val),
            Some(("SignedHeaders", val)) => signed_headers = Some(val),
            Some(("Signature", val)) => signature = Some(val),
            Some((key, _)) if !key.is_empty() => {}
            // Measured 2026-11-20 and 2026-11-21: DynamoDB refuses a part
            // with no '=' or no key wherever it sits, before it checks for
            // missing parameters.
            pair => {
                let fault = if pair.is_none() {
                    "missing equal-sign"
                } else {
                    "missing key"
                };
                return Err(incomplete(&format!(
                    "Invalid key=value pair ({fault}) in Authorization header \
                     (hashed with SHA-256 and encoded with Base64): '{}'.",
                    hash()
                )));
            }
        }
    }

    // Measured 2026-11-21: DynamoDB names every missing parameter and a
    // missing date header in one message, then the hash of the value.
    let (Some(credential), Some(signed_headers), Some(signature), true) =
        (credential, signed_headers, signature, has_date)
    else {
        let mut sentences: Vec<String> = [
            ("Credential", credential),
            ("Signature", signature),
            ("SignedHeaders", signed_headers),
        ]
        .into_iter()
        .filter(|(_, val)| val.is_none())
        .map(|(name, _)| format!("Authorization header requires '{name}' parameter."))
        .collect();
        if !has_date {
            sentences.push(
                "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header."
                    .to_owned(),
            );
        }
        return Err(incomplete(&format!(
            "{} (Hashed with SHA-256 and encoded with Base64) Authorization={}",
            sentences.join(" "),
            hash()
        )));
    };

    // Measured 2026-11-21: DynamoDB answers a `Bearer` value that passes the
    // checks above with this text. Its text for a request with no
    // Authorization header is `Request is missing Authentication Token`, which
    // ExtendDB sends after the fix for #1075. Until then the two constructions
    // build the same error.
    if !aws4 {
        return Err(DynamoDbError::MissingAuthenticationToken(
            "Missing Authentication Token".to_owned(),
        ));
    }
    // Measured 2026-11-21: then another algorithm is refused.
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(incomplete(&format!(
            "Unsupported AWS 'algorithm': '{algorithm}'. "
        )));
    }

    // Credential = access_key/date/region/service/aws4_request
    let parts: Vec<&str> = credential.splitn(5, '/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        // Measured 2026-09-16: the service names the rule and echoes the value.
        return Err(incomplete(&format!(
            "Credential must have exactly 5 slash-delimited elements, e.g. keyid/date/region/service/term, got '{credential}'"
        )));
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

        let parsed = parse_authorization(header.as_bytes(), true).unwrap();
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
        assert!(parse_authorization(header.as_bytes(), true).is_err());
    }

    #[test]
    fn reject_missing_credential() {
        let header = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc";
        assert!(parse_authorization(header.as_bytes(), true).is_err());
    }

    /// DynamoDB's text for a part with no '=' (`fault` "missing equal-sign")
    /// or with no key (`fault` "missing key").
    fn invalid_pair(fault: &str, hash: &str) -> String {
        format!(
            "Invalid key=value pair ({fault}) in Authorization header \
             (hashed with SHA-256 and encoded with Base64): '{hash}'."
        )
    }

    /// The hashes are DynamoDB's answers of 2026-11-20.
    #[test]
    fn part_without_equal_sign() {
        let cred = "Credential=AKIAIOSFODNN7EXAMPLE/20261120/us-east-1/dynamodb/aws4_request";
        let sh = "SignedHeaders=host;x-amz-target";
        let sig = format!("Signature={}", "0".repeat(64));
        let eq = |hash: &str| Some(invalid_pair("missing equal-sign", hash));
        let rows = [
            (
                "garbage".to_owned(),
                eq("zUZdJX3JKFekvINdpUsyc8qq7W43kESi0fduBwuIskY="),
            ),
            (
                format!("garbage, {cred}, {sh}, {sig}"),
                eq("qFJwfu14Kuj++dnpZ7SfZHb4qGEFK5qxVeYPABxnecw="),
            ),
            (
                format!("{cred}, {sh}, {sig}, garbage"),
                eq("vl/+vD+ZxerGScLuXRHUmQHfq+CnlTl4S/imWZQJJJY="),
            ),
            (
                format!("{cred}, garbage, {sig}"),
                eq("bQeyhzn0vhbh2yagmMjizvaUIkNJ6HgmBYLvWWGOzH0="),
            ),
            (
                "garbage, other".to_owned(),
                eq("ZA+NpPHMMD/WKwhx2PWZpccIYkMjg0H/pGF/oZm2TQ0="),
            ),
            (
                "Credential".to_owned(),
                eq("KfSoC26UaBDGXxo5AUFS45c1cmAbyLKDOxOTKcZYWt4="),
            ),
        ];
        for (pairs, want) in rows {
            let header = format!("AWS4-HMAC-SHA256 {pairs}");
            let got = parse_authorization(header.as_bytes(), false)
                .err()
                .map(|e| e.message().to_owned());
            assert_eq!(got, want, "{header}");
        }
    }

    /// DynamoDB's answers of 2026-11-21, one row per check of the parse, in
    /// DynamoDB's order. The empty value has ExtendDB's answer to a request
    /// with no Authorization header (#1075). It and the `Bearer` value with all
    /// three pairs use ExtendDB's type name. Each header is written one
    /// character per byte, as DynamoDB reads it. `true` marks a request with an
    /// `X-Amz-Date` header.
    #[test]
    fn malformed_authorization_header() {
        let alg = "AWS4-HMAC-SHA256";
        let cred = "Credential=AKIAIOSFODNN7EXAMPLE/20261121/us-east-1/dynamodb/aws4_request";
        let sh = "SignedHeaders=host;x-amz-target";
        let sig = format!("Signature={}", "0".repeat(64));
        let c = "Authorization header requires 'Credential' parameter.";
        let s = "Authorization header requires 'Signature' parameter.";
        let h = "Authorization header requires 'SignedHeaders' parameter.";
        let d =
            "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header.";
        let hashed = |sentences: &[&str], hash: &str| {
            Some(incomplete(&format!(
                "{} (Hashed with SHA-256 and encoded with Base64) Authorization={hash}",
                sentences.join(" ")
            )))
        };
        let pair = |fault: &str, hash: &str| Some(incomplete(&invalid_pair(fault, hash)));
        let (eq, key) = ("missing equal-sign", "missing key");
        let token = |text: &str| Some(DynamoDbError::MissingAuthenticationToken(text.to_owned()));
        let rows = [
            (String::new(), false, token("Missing Authentication Token")),
            (
                "aws4-x garbage".to_owned(),
                true,
                Some(incomplete(
                    "Algorithm must start with 'AWS4-'; got 'aws4-x garbage'",
                )),
            ),
            (
                "AWS4-X garbage".to_owned(),
                true,
                pair(eq, "GaxqElqQnrpBIz9pP0gKuSPKFVtB4MQg837n12gG5rk="),
            ),
            (
                format!("{alg} =x"),
                false,
                pair(key, "ZzAuviaOfeYu2+eCrfxsQ3Yn14mlJRh9b9MgRol8IVU="),
            ),
            (
                "AWS4-X".to_owned(),
                false,
                hashed(
                    &[c, s, h, d],
                    "stDJ5LzKjj3bBOLg8vQ9eVG2tWagrg6XrWkxF70MkSQ=",
                ),
            ),
            (
                "Bearer a=b".to_owned(),
                true,
                hashed(&[c, s, h], "trGvSpXeM6ZOst7PNaJ8ft/DE0LSIIrpOipbnukyQvA="),
            ),
            (
                format!("AWS4-HMAC-SHA512 {cred}, {sh}, {sig}"),
                false,
                hashed(&[d], "6itMs6xuHrY8yzHCXnl+ZI6wa5kemPWEH3R8yt0ruZ4="),
            ),
            (
                format!("AWS4-HMAC-SHA512 {cred}, {sh}, {sig}"),
                true,
                Some(incomplete(
                    "Unsupported AWS 'algorithm': 'AWS4-HMAC-SHA512'. ",
                )),
            ),
            (
                format!("Bearer {cred}, {sh}, {sig}"),
                true,
                token("Missing Authentication Token"),
            ),
            (
                format!("{alg} Credential=x, {sh}, {sig}"),
                true,
                Some(incomplete(
                    "Credential must have exactly 5 slash-delimited elements, \
                     e.g. keyid/date/region/service/term, got 'x'",
                )),
            ),
            (
                format!("{alg} garbag\u{e9}"),
                false,
                pair(eq, "a681Q7aqGyGXvf2VZ32NNzRb0Uq2DkrheM40R3meh9s="),
            ),
            (
                format!("{alg}\tgarbage"),
                false,
                pair(eq, "XUinbQeSOqQmlK+0Rlsvau5dUqd+5nD9Uuw1MpQxMDE="),
            ),
            (
                format!("{alg},{cred}, {sh}, {sig}"),
                false,
                hashed(&[d], "k9ZniCm661ym+L0+NfddBO1S3uB45sGroJr9Eyehky4="),
            ),
            (
                format!("{alg} {cred} {sh} {sig}"),
                false,
                hashed(&[d], "W/vVkMb/4skiNtLoi1bF0ceXeHOzLJ6++2unscls6fI="),
            ),
        ];
        let answer = |e: &DynamoDbError| (e.error_type().to_owned(), e.message().to_owned());
        let wrong: Vec<_> = rows
            .into_iter()
            .filter_map(|(header, has_date, want)| {
                let bytes: Vec<u8> = header.chars().map(|ch| u8::try_from(ch).unwrap()).collect();
                let got = parse_authorization(&bytes, has_date).err();
                let (got, want) = (got.as_ref().map(answer), want.as_ref().map(answer));
                (got != want).then_some((header, has_date, got, want))
            })
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    fn reject_bad_credential_format() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20260415/us-east-1, \
            SignedHeaders=host, Signature=abc";
        assert!(parse_authorization(header.as_bytes(), true).is_err());
    }

    #[test]
    fn reject_oversized_header() {
        // S-2: Headers exceeding 8 KB must be rejected.
        let header = "A".repeat(8 * 1024 + 1);
        assert!(parse_authorization(header.as_bytes(), true).is_err());
    }
}
