// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The idempotency request fingerprint for bulk put operations.
//!
//! When a `ClientToken` is replayed, the stored fingerprint decides between
//! returning the existing operation and
//! `IdempotentParameterMismatchException`. The fingerprint is SHA-256 over
//! a canonical JSON document of the request parameters that define the
//! operation, so two requests compare equal exactly when they ask for the
//! same work.

use std::fmt::Write as _;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// Compute the fingerprint of one start request.
///
/// The hashed document is `{"TableArn", "InputS3Configuration",
/// "OutputS3Configuration", "MaxWriteRequestUnits", "PerformanceTier"}`
/// with null members removed recursively and object keys sorted
/// recursively, so an explicit null and an absent member hash alike and
/// member order on the wire is irrelevant. `table_arn` is the target
/// table's ARN, not its bare name, so the fingerprint is stable across
/// the two ways of naming the table. The result is 64 lowercase hex
/// characters.
///
/// The caller normalizes the service defaults a retry may add or omit
/// (`strip_replayed_defaults` on each configuration, and `PerformanceTier`
/// `STANDARD` to an absent tier) before calling this, so the request as
/// submitted and the request with a default spelled out hash alike.
pub(crate) fn request_fingerprint(
    table_arn: &str,
    input: &Value,
    output: &Value,
    max_write_request_units: Option<u64>,
    performance_tier: Option<&str>,
) -> String {
    let mut document = Map::new();
    document.insert("TableArn".to_owned(), Value::from(table_arn));
    document.insert("InputS3Configuration".to_owned(), input.clone());
    document.insert("OutputS3Configuration".to_owned(), output.clone());
    if let Some(units) = max_write_request_units {
        document.insert("MaxWriteRequestUnits".to_owned(), Value::from(units));
    }
    if let Some(tier) = performance_tier {
        document.insert("PerformanceTier".to_owned(), Value::from(tier));
    }
    let mut canonical = String::new();
    write_canonical(&Value::Object(document), false, &mut canonical);
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

/// Remove the configuration members whose value equals a service default a
/// retry may add or omit without changing the operation, so the request as
/// submitted and the request with the default spelled out fingerprint alike
/// (measured on the service). The list is one entry per such default:
/// `InputCompressionType` `NONE` (input only; the output configuration has
/// no such member, so the rule is a no-op there) and `S3BucketOwner` equal
/// to the caller's own account (either configuration). `PerformanceTier`
/// `STANDARD` is the other replayed default; the caller drops it by passing
/// an absent tier. `S3SseAlgorithm` `AES256` is deliberately not here: a
/// retry that adds it mismatches (measured), so it stays in the
/// fingerprint. A member absent, or present with a non-default value, is
/// left untouched, so the fingerprint of a request that spells no default
/// is byte-identical to the pre-change fingerprint and a token issued
/// before this change still replays.
pub(crate) fn strip_replayed_defaults(config: &Value, caller_account: &str) -> Value {
    let mut config = config.clone();
    if let Some(members) = config.as_object_mut() {
        let defaults: [(&str, &str); 2] = [
            ("InputCompressionType", "NONE"),
            ("S3BucketOwner", caller_account),
        ];
        for (member, default) in defaults {
            if members.get(member).and_then(Value::as_str) == Some(default) {
                members.remove(member);
            }
        }
    }
    config
}

/// Render the canonical form of one JSON value.
///
/// Object members whose value is null are dropped and keys are written in
/// lexicographic order, both applied recursively. `OutputType` is a set on
/// the wire, so the array under that key is sorted; every other array
/// keeps its order. `sort_elements` carries that one-key decision down to
/// the array itself.
fn write_canonical(value: &Value, sort_elements: bool, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => {
            let _ = write!(out, "{number}");
        }
        Value::String(string) => write_string(string, out),
        Value::Array(elements) => {
            let mut rendered: Vec<String> = elements
                .iter()
                .map(|element| {
                    let mut buffer = String::new();
                    write_canonical(element, false, &mut buffer);
                    buffer
                })
                .collect();
            if sort_elements {
                rendered.sort();
            }
            out.push('[');
            for (index, element) in rendered.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(element);
            }
            out.push(']');
        }
        Value::Object(members) => {
            let mut keys: Vec<&String> = members
                .iter()
                .filter(|(_, member)| !member.is_null())
                .map(|(key, _)| key)
                .collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                if let Some(member) = members.get(*key) {
                    write_canonical(member, *key == "OutputType", out);
                }
            }
            out.push('}');
        }
    }
}

/// Write one JSON string literal. The escaping only needs to be
/// deterministic and injective for the hash; it escapes the quote, the
/// backslash, and control characters, which is valid JSON.
fn write_string(string: &str, out: &mut String) {
    out.push('"');
    for character in string.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            character if (character as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", character as u32);
            }
            character => out.push(character),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::request_fingerprint;
    use serde_json::json;

    const TABLE_ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/orders";

    /// Two Start requests that ask for the same work, written with their JSON
    /// members in a different order, with different whitespace, and with the
    /// OutputType set in a different order, hash to the same fingerprint;
    /// changing any value changes it. Object member order on the wire is
    /// normalised by the JSON parser before the canonical form is built, so
    /// the OutputType set order is the ordering difference the canonical form
    /// itself must absorb.
    #[test]
    fn member_order_and_whitespace_do_not_change_the_fingerprint() {
        let input_a: serde_json::Value = serde_json::from_str(
            r#"{"S3Bucket":"loads","S3KeyPrefix":"orders/","InputFormat":"DYNAMODB_JSON","InputCompressionType":"GZIP"}"#,
        )
        .unwrap();
        let input_b: serde_json::Value = serde_json::from_str(
            "{\n  \"InputCompressionType\" : \"GZIP\" ,\n  \"InputFormat\" : \"DYNAMODB_JSON\" ,\n  \"S3KeyPrefix\" : \"orders/\" ,\n  \"S3Bucket\"  :  \"loads\"\n}",
        )
        .unwrap();
        let output_a: serde_json::Value = serde_json::from_str(
            r#"{"S3KeyPrefix":"run/","OutputType":["ERROR","CONDITION_CHECK_FAILURE"]}"#,
        )
        .unwrap();
        let output_b: serde_json::Value = serde_json::from_str(
            "{ \"OutputType\" : [ \"CONDITION_CHECK_FAILURE\" , \"ERROR\" ] , \"S3KeyPrefix\" : \"run/\" }",
        )
        .unwrap();

        let fp_a =
            request_fingerprint(TABLE_ARN, &input_a, &output_a, Some(1000), Some("STANDARD"));
        let fp_b =
            request_fingerprint(TABLE_ARN, &input_b, &output_b, Some(1000), Some("STANDARD"));
        assert_eq!(
            fp_a, fp_b,
            "member order and whitespace must not change the fingerprint"
        );
        assert_eq!(fp_a.len(), 64, "the fingerprint is 64 hex characters");
        assert!(
            fp_a.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "the fingerprint is lowercase hex: {fp_a}"
        );

        let fp_units =
            request_fingerprint(TABLE_ARN, &input_a, &output_a, Some(2000), Some("STANDARD"));
        assert_ne!(
            fp_a, fp_units,
            "a changed MaxWriteRequestUnits must change the fingerprint"
        );
        let mut input_changed = input_a.clone();
        input_changed["S3Bucket"] = json!("other-bucket");
        let fp_bucket = request_fingerprint(
            TABLE_ARN,
            &input_changed,
            &output_a,
            Some(1000),
            Some("STANDARD"),
        );
        assert_ne!(
            fp_a, fp_bucket,
            "a changed input value must change the fingerprint"
        );
    }
}
