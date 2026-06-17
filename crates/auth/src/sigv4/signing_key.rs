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

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Derive the `SigV4` signing key from a secret access key.
///
/// # Arguments
/// * `secret` — The plaintext secret access key.
/// * `date` — Date string in `YYYYMMDD` format.
/// * `region` — AWS region (e.g. `us-east-1`).
/// * `service` — Service name (e.g. `dynamodb`).
#[must_use]
pub fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Compute the final signature: HMAC-SHA256(signing_key, `string_to_sign`), hex-encoded.
#[must_use]
pub fn compute_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC-SHA256 accepts any key length per RFC 2104. `new_from_slice` only
    // fails for `InvalidLength`, which cannot occur for HMAC (it hashes
    // oversized keys). Using `expect` is safe here — an empty fallback would
    // risk producing a predictable signing key.
    #[allow(clippy::expect_used)]
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
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
}
