// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire encoding for DynamoDB items stored as Route 53 TXT records.
//!
//! ## Encoding scheme
//!
//! Each item is serialized to JSON (the `item_data` JSONB column of the
//! PostgreSQL backend, but as a `String`), base64-encoded, and split into
//! [`MAX_TXT_STRING_BYTES`]-byte chunks. The chunks become the individual
//! strings of a single TXT resource record.
//!
//! Items larger than [`MAX_BYTES_PER_TXT_RECORD`] are split across multiple
//! TXT records keyed by sequence number, encoded as a subdomain label
//! (`seg00`, `seg01`, ...). The reader concatenates all segments under the
//! item's partition-key label before base64-decoding.
//!
//! ## Limits inherited from Route 53
//!
//! - 255 bytes per TXT string (DNS protocol).
//! - 65,535 bytes per RRSET (DNS protocol). In practice Route 53 caps each
//!   TXT record at ~4 KB of usable payload after segment framing.
//! - 10,000 records per hosted zone by default (raisable via support
//!   ticket; raises billed separately).
//! - 1,000 hosted zones per AWS account by default.
//!
//! ## Consistency
//!
//! Route 53 advertises strong consistency for `ChangeResourceRecordSets`
//! (the change is in-zone before the API returns) but eventual consistency
//! for resolvers (TTL-bounded). This maps cleanly onto DynamoDB's
//! `ConsistentRead=true` / `ConsistentRead=false` distinction:
//!
//! - `ConsistentRead=true` → fetch via the Route 53 management API.
//! - `ConsistentRead=false` → resolve via any DNS resolver. Cheaper, faster,
//!   bounded by the configured TTL.
//!
//! This is one of the rare cases where the underlying storage model
//! provides a stronger consistency contract than DynamoDB's documented
//! defaults.

use base64::Engine;

/// Per DNS protocol (RFC 1035, §3.3.14): each `<character-string>` in a TXT
/// RDATA section is preceded by a single octet length, which caps it at 255.
pub const MAX_TXT_STRING_BYTES: usize = 255;

/// Practical Route 53 ceiling per TXT record after segment framing.
/// Used to decide when to spill an item across multiple records.
pub const MAX_BYTES_PER_TXT_RECORD: usize = 4000;

/// Encode a JSON-serialized DynamoDB item as a sequence of base64-encoded
/// TXT strings, each no longer than [`MAX_TXT_STRING_BYTES`].
#[must_use]
pub fn item_json_to_txt_strings(item_json: &str) -> Vec<String> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(item_json.as_bytes());
    encoded
        .as_bytes()
        .chunks(MAX_TXT_STRING_BYTES)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect()
}

/// Inverse of [`item_json_to_txt_strings`].
///
/// # Errors
///
/// Returns an error if the concatenated segments do not base64-decode or
/// the result is not valid UTF-8.
pub fn txt_strings_to_item_json(strings: &[String]) -> Result<String, EncodingError> {
    let concatenated: String = strings.iter().flat_map(|s| s.chars()).collect();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(concatenated.as_bytes())
        .map_err(|e| EncodingError::Base64(e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| EncodingError::Utf8(e.to_string()))
}

#[derive(Debug)]
pub enum EncodingError {
    Base64(String),
    Utf8(String),
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base64(s) => write!(f, "base64 decode failed: {s}"),
            Self::Utf8(s) => write!(f, "utf-8 decode failed: {s}"),
        }
    }
}

impl std::error::Error for EncodingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_item_roundtrips() {
        let json = r#"{"pk":{"S":"user-1"},"name":{"S":"Alice"}}"#;
        let strs = item_json_to_txt_strings(json);
        assert!(strs.iter().all(|s| s.len() <= MAX_TXT_STRING_BYTES));
        assert_eq!(txt_strings_to_item_json(&strs).unwrap(), json);
    }

    #[test]
    fn large_item_chunks_correctly() {
        let json = format!(r#"{{"pk":{{"S":"x"}},"blob":{{"S":"{}"}}}}"#, "A".repeat(2_000));
        let strs = item_json_to_txt_strings(&json);
        assert!(strs.len() > 1);
        assert!(strs.iter().all(|s| s.len() <= MAX_TXT_STRING_BYTES));
        assert_eq!(txt_strings_to_item_json(&strs).unwrap(), json);
    }
}
