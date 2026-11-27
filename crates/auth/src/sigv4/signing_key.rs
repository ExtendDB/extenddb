// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SigV4` signing key derivation.
//!
//! Derives the signing key via the 4-step HMAC-SHA256 chain:
//! ```text
//! kDate    = HMAC("AWS4" + secret, date)
//! kRegion  = HMAC(kDate, region)
//! kService = HMAC(kRegion, service)
//! kSigning = HMAC(kService, "aws4_request")
//! ```

use zeroize::Zeroizing;

/// Derive the `SigV4` signing key of `secret` into `out`.
///
/// The prefixed secret and the three intermediate keys live in `Zeroizing`
/// buffers and are wiped before this returns. The caller passes `out` as a
/// slot of the locked key store, or a `Zeroizing` buffer. Copies of key
/// material this function cannot wipe are also left behind:
///
/// - the 32-byte array that `extenddb_core::crypto::hmac_sha256` returns for
///   each step, and the tag its backend computes, on the stack;
/// - copies of the key-derived state that the HMAC of each step can leave on
///   the stack, with either backend, and on `wasm32` copies of its key as well
///   (`docs/webassembly.md` in the repository this crate's manifest names,
///   in its section "What it does not do", says what the `wasm32` backend can
///   leave).
///
/// # Arguments
/// * `secret` - The plaintext secret access key.
/// * `date` - Date string in `YYYYMMDD` format.
/// * `region` - AWS region (e.g. `us-east-1`).
/// * `service` - Service name (e.g. `dynamodb`).
pub fn derive_signing_key_into(
    secret: &[u8],
    date: &str,
    region: &str,
    service: &str,
    out: &mut [u8; 32],
) {
    let mut prefixed = Zeroizing::new(Vec::with_capacity(4 + secret.len()));
    prefixed.extend_from_slice(b"AWS4");
    prefixed.extend_from_slice(secret);
    let mut k_date = Zeroizing::new([0u8; 32]);
    hmac_sha256(&prefixed, date.as_bytes(), &mut k_date);
    let mut k_region = Zeroizing::new([0u8; 32]);
    hmac_sha256(&k_date[..], region.as_bytes(), &mut k_region);
    let mut k_service = Zeroizing::new([0u8; 32]);
    hmac_sha256(&k_region[..], service.as_bytes(), &mut k_service);
    hmac_sha256(&k_service[..], b"aws4_request", out);
}

/// Derive the `SigV4` signing key from a secret access key, as
/// [`derive_signing_key_into`], into a `Zeroizing` buffer.
#[must_use]
pub fn derive_signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    derive_signing_key_into(secret.as_bytes(), date, region, service, &mut out);
    out
}

/// Compute the final signature: HMAC-SHA256(signing_key, `string_to_sign`), hex-encoded.
#[must_use]
pub fn compute_signature(signing_key: &[u8; 32], string_to_sign: &str) -> String {
    // The signature is sent on the wire, so it needs no wipe.
    let mut mac = [0u8; 32];
    hmac_sha256(signing_key, string_to_sign.as_bytes(), &mut mac);
    hex::encode(mac)
}

fn hmac_sha256(key: &[u8], data: &[u8], out: &mut [u8; 32]) {
    // HMAC-SHA256 accepts a key of any length (RFC 2104). The shared crypto
    // module chooses the backend (AWS-LC on the server, pure-Rust on wasm);
    // what each can leave on the stack is listed on `derive_signing_key_into`.
    out.copy_from_slice(&extenddb_core::crypto::hmac_sha256(key, data));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS SigV4 test vector from the AWS documentation.
    /// <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
    #[test]
    fn aws_test_vector_signing_key() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let date = "20150830";
        let region = "us-east-1";
        let service = "iam";

        let signing_key = derive_signing_key(secret, date, region, service);

        // The signing key is 32 bytes (HMAC-SHA256 output).
        assert_eq!(signing_key.len(), 32);

        // Verify end-to-end: the signing key produces the correct signature
        // for the AWS test vector string-to-sign.
        let string_to_sign = "AWS4-HMAC-SHA256\n\
            20150830T123600Z\n\
            20150830/us-east-1/iam/aws4_request\n\
            f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
        let sig = compute_signature(&signing_key, string_to_sign);
        assert_eq!(
            sig,
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    /// Full signature computation using the AWS test vector.
    #[test]
    fn aws_test_vector_signature() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let signing_key = derive_signing_key(secret, "20150830", "us-east-1", "iam");

        let string_to_sign = "AWS4-HMAC-SHA256\n\
            20150830T123600Z\n\
            20150830/us-east-1/iam/aws4_request\n\
            f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";

        let signature = compute_signature(&signing_key, string_to_sign);
        assert_eq!(
            signature,
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    /// A key derived into a caller's buffer equals the one `derive_signing_key`
    /// returns, for the AWS test vector and the documentation example pair.
    #[test]
    fn derive_into_matches_derive() {
        let cases = [
            (
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "20150830",
                "us-east-1",
                "iam",
            ),
            (
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "20260415",
                "us-east-1",
                "dynamodb",
            ),
            (
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "20260416",
                "eu-west-1",
                "dynamodb",
            ),
        ];
        for (secret, date, region, service) in cases {
            let mut out = [0u8; 32];
            derive_signing_key_into(secret.as_bytes(), date, region, service, &mut out);
            // The message names the scope alone, so a failure prints no secret
            // and no key.
            assert!(
                out == *derive_signing_key(secret, date, region, service),
                "{date} {region} {service}"
            );
        }
    }
}
