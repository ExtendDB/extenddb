// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Custom serde deserializers for `DynamoDB` input validation.
//!
//! Validate `ExpressionAttributeNames` / `ExpressionAttributeValues` map entries
//! at deserialization time, before expression parsing, matching Amazon
//! `DynamoDB`'s order: non-empty map, well-formed placeholder keys
//! (`<prefix>[A-Za-z0-9_]+`, <=255 bytes incl. prefix), non-empty mapped value.

use std::collections::HashMap;

use serde::de::{self, Deserialize, Deserializer};
use serde_json::Value;

use crate::types::AttributeValue;

/// Maximum placeholder key length in bytes, including the `#` / `:` prefix.
const MAX_PLACEHOLDER_KEY_BYTES: usize = 255;

/// A placeholder identifier (the text after the `#` / `:`) is one or more of
/// `[A-Za-z0-9_]`.
fn is_placeholder_ident(rest: &str) -> bool {
    !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Validate one placeholder key. Length is checked before syntax.
///
/// `include_size_in_too_long`: the Names too-long message appends
/// `; size of key: N`, the Values one does not (Amazon `DynamoDB` quirk).
fn validate_placeholder_key(
    key: &str,
    prefix: char,
    field_name: &str,
    include_size_in_too_long: bool,
) -> Result<(), String> {
    if key.len() > MAX_PLACEHOLDER_KEY_BYTES {
        let mut msg = format!(
            "{field_name} contains invalid key: The expression attribute map \
             contains a key that is too long;"
        );
        if include_size_in_too_long {
            use std::fmt::Write as _;
            let _ = write!(msg, " size of key: {}", key.len());
        }
        return Err(msg);
    }
    if !key.strip_prefix(prefix).is_some_and(is_placeholder_ident) {
        return Err(format!(
            "{field_name} contains invalid key: Syntax error; key: \"{key}\""
        ));
    }
    Ok(())
}

/// Deserialize a placeholder map, sharing the structure check (non-empty map,
/// well-formed keys). The caller supplies the per-type value check and
/// conversion. Values arrive as raw `serde_json::Value` so a value error can
/// name its key (serde errors do not carry the failing key).
fn deserialize_placeholder_map<'de, D, T>(
    deserializer: D,
    prefix: char,
    field_name: &str,
    include_size_in_too_long: bool,
    check_value: impl Fn(&str, &Value) -> Result<(), String>,
    convert: impl Fn(Value) -> Result<T, serde_json::Error>,
) -> Result<Option<HashMap<String, T>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(raw) = Option::<serde_json::Map<String, Value>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Err(de::Error::custom(format!("{field_name} must not be empty")));
    }
    let mut out = HashMap::with_capacity(raw.len());
    // Pass 1: validate every key first, so a malformed key is reported before
    // any value error (DynamoDB validates key syntax ahead of value contents).
    for key in raw.keys() {
        validate_placeholder_key(key, prefix, field_name, include_size_in_too_long)
            .map_err(de::Error::custom)?;
    }
    // Pass 2: per-value check and conversion.
    for (key, value) in raw {
        check_value(&key, &value).map_err(de::Error::custom)?;
        let converted = convert(value).map_err(|e| {
            let msg = e.to_string();
            // Semantic value-validation errors are wrapped with the field name
            // and the offending key (DynamoDB parity). Wire/type errors (wrong
            // JSON shape for a datatype) pass through as-is.
            if msg.starts_with("One or more parameter values were invalid:") {
                de::Error::custom(format!(
                    "{field_name} contains invalid value: {msg} for key {key}"
                ))
            } else {
                de::Error::custom(msg)
            }
        })?;
        out.insert(key, converted);
    }
    Ok(Some(out))
}

/// Deserialize `ExpressionAttributeNames`: keys are `#<ident>` (at most 255
/// bytes), the map is non-empty, and each mapped attribute name is non-empty.
///
/// # Errors
///
/// Returns the deserializer error type when a map entry is malformed.
pub fn deserialize_expression_names<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_placeholder_map(
        deserializer,
        '#',
        "ExpressionAttributeNames",
        true,
        |key, value| {
            // Only a genuine empty *string* is rejected. A non-string falls
            // through to `convert` -> type error -> SerializationException,
            // matching Amazon DynamoDB. `is_none_or` would mislabel it.
            if value.as_str().is_some_and(str::is_empty) {
                return Err(format!(
                    "ExpressionAttributeNames contains invalid value: \
                     Empty attribute name for key {key}"
                ));
            }
            Ok(())
        },
        serde_json::from_value::<String>,
    )
}

/// Deserialize `ExpressionAttributeValues`: keys are `:<ident>` (at most 255
/// bytes), the map is non-empty, and each `AttributeValue` carries a datatype.
///
/// # Errors
///
/// Returns the deserializer error type when a map entry is malformed.
pub fn deserialize_expression_values<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, AttributeValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_placeholder_map(
        deserializer,
        ':',
        "ExpressionAttributeValues",
        false,
        |key, value| {
            // Empty AttributeValue (no datatype). Caught here so the error names
            // its key. Mirrors the empty-object check in `types::attribute_value`;
            // keep in sync. Only the empty case is prefixed, matching DynamoDB.
            if value.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err(format!(
                    "ExpressionAttributeValues contains invalid value: Supplied \
                     AttributeValue is empty, must contain exactly one of the \
                     supported datatypes for key {key}"
                ));
            }
            Ok(())
        },
        serde_json::from_value::<AttributeValue>,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestNames {
        #[serde(default, deserialize_with = "deserialize_expression_names")]
        names: Option<HashMap<String, String>>,
    }

    #[derive(Debug, Deserialize)]
    struct TestValues {
        #[serde(default, deserialize_with = "deserialize_expression_values")]
        values: Option<HashMap<String, AttributeValue>>,
    }

    fn names_err(json: &str) -> String {
        serde_json::from_str::<TestNames>(json)
            .unwrap_err()
            .to_string()
    }

    fn values_err(json: &str) -> String {
        serde_json::from_str::<TestValues>(json)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn names_missing_hash_rejected() {
        assert!(names_err(r#"{"names":{"a":"real"}}"#).contains("Syntax error; key"));
    }

    #[test]
    fn names_with_hash_accepted() {
        let json = "{\"names\":{\"#a\":\"real\"}}";
        let parsed: TestNames = serde_json::from_str(json).unwrap();
        assert!(parsed.names.unwrap().contains_key("#a"));
    }

    #[test]
    fn names_empty_rejected() {
        assert!(names_err(r#"{"names":{}}"#).contains("must not be empty"));
    }

    #[test]
    fn names_prefix_only_key_rejected() {
        assert!(names_err(r##"{"names":{"#":"foo"}}"##).contains(
            r##"ExpressionAttributeNames contains invalid key: Syntax error; key: "#""##
        ));
    }

    #[test]
    fn names_invalid_char_key_rejected() {
        assert!(names_err(r##"{"names":{"#a-b":"foo"}}"##).contains(
            r##"ExpressionAttributeNames contains invalid key: Syntax error; key: "#a-b""##
        ));
    }

    #[test]
    fn names_key_too_long_reports_size() {
        let key = format!("#{}", "a".repeat(255)); // 256 bytes including '#'
        let json = format!(r#"{{"names":{{"{key}":"foo"}}}}"#);
        let msg = names_err(&json);
        assert!(
            msg.contains("contains a key that is too long; size of key: 256"),
            "{msg}"
        );
    }

    #[test]
    fn names_length_checked_before_syntax() {
        let key = format!("#{}-x", "a".repeat(255)); // too long AND invalid char
        let json = format!(r#"{{"names":{{"{key}":"foo"}}}}"#);
        let msg = names_err(&json);
        assert!(msg.contains("too long; size of key: 258"), "{msg}");
    }

    #[test]
    fn names_empty_value_rejected() {
        assert!(names_err(r##"{"names":{"#a":""}}"##).contains(
            "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a"
        ));
    }

    #[test]
    fn names_valid_ident_with_digits_and_underscore_accepted() {
        let parsed: TestNames = serde_json::from_str("{\"names\":{\"#a_1\":\"foo\"}}").unwrap();
        assert!(parsed.names.unwrap().contains_key("#a_1"));
    }

    #[test]
    fn values_missing_colon_rejected() {
        assert!(values_err(r#"{"values":{"v":{"S":"x"}}}"#).contains("Syntax error; key"));
    }

    #[test]
    fn values_with_colon_accepted() {
        let parsed: TestValues = serde_json::from_str(r#"{"values":{":v":{"S":"x"}}}"#).unwrap();
        assert!(parsed.values.is_some());
    }

    #[test]
    fn values_prefix_only_key_rejected() {
        assert!(
            values_err(r#"{"values":{":":{"S":"x"}}}"#).contains(
                r#"ExpressionAttributeValues contains invalid key: Syntax error; key: ":""#
            )
        );
    }

    #[test]
    fn values_null_non_boolean_is_validation_error() {
        // {"NULL":"no"} is a validation error on real DynamoDB, not a parse
        // (Serialization) error. Must be prefixed and name its key.
        let msg = values_err(r#"{"values":{":b":{"NULL":"no"}}}"#);
        assert!(
            msg.contains(
                "ExpressionAttributeValues contains invalid value: One or more parameter \
                 values were invalid: Null attribute value types must have the value of \
                 true for key :b"
            ),
            "{msg}"
        );
    }

    #[test]
    fn values_null_false_is_validation_error() {
        let msg = values_err(r#"{"values":{":b":{"NULL":false}}}"#);
        assert!(
            msg.contains(
                "ExpressionAttributeValues contains invalid value: One or more parameter \
                 values were invalid: Null attribute value types must have the value of \
                 true for key :b"
            ),
            "{msg}"
        );
    }

    #[test]
    fn values_null_true_accepted() {
        let parsed: TestValues =
            serde_json::from_str(r#"{"values":{":b":{"NULL":true}}}"#).unwrap();
        assert!(parsed.values.is_some());
    }

    #[test]
    fn values_empty_set_wrapped_with_key() {
        for (av, needle) in [
            (r#"{"SS":[]}"#, "An string set  may not be empty"),
            (r#"{"NS":[]}"#, "An number set  may not be empty"),
            (r#"{"BS":[]}"#, "Binary sets should not be empty"),
        ] {
            let msg = values_err(&format!(r#"{{"values":{{":b":{av}}}}}"#));
            let expected = format!(
                "ExpressionAttributeValues contains invalid value: One or more \
                 parameter values were invalid: {needle} for key :b"
            );
            assert!(msg.contains(&expected), "av={av} got: {msg}");
        }
    }

    #[test]
    fn values_duplicate_set_wrapped_with_key() {
        let ss = values_err(r#"{"values":{":b":{"SS":["a","a"]}}}"#);
        assert!(
            ss.contains(
                "ExpressionAttributeValues contains invalid value: One or more parameter \
                 values were invalid: Input collection [a, a] contains duplicates. for key :b"
            ),
            "{ss}"
        );
        // Binary duplicates carry the "of type BS" qualifier (DynamoDB parity).
        let bs = values_err(r#"{"values":{":b":{"BS":["Yg==","Yg=="]}}}"#);
        assert!(
            bs.contains(
                "ExpressionAttributeValues contains invalid value: One or more parameter \
                 values were invalid: Input collection [Yg==, Yg==]of type BS contains \
                 duplicates. for key :b"
            ),
            "{bs}"
        );
    }

    #[test]
    fn values_invalid_key_reported_before_value_error() {
        // Map has a malformed value (:b -> unknown type) AND a malformed key (b
        // without the ':' prefix). The key error must win, matching DynamoDB.
        let msg = values_err(r#"{"values":{":b":{"a":""},"b":{"S":"a"}}}"#);
        assert!(
            msg.contains(r#"ExpressionAttributeValues contains invalid key: Syntax error; key: "b""#),
            "{msg}"
        );
    }

    #[test]
    fn values_key_too_long_omits_size() {
        let key = format!(":{}", "a".repeat(255)); // 256 bytes including ':'
        let json = format!(r#"{{"values":{{"{key}":{{"S":"x"}}}}}}"#);
        let msg = values_err(&json);
        assert!(msg.contains("contains a key that is too long;"), "{msg}");
        // Quirk: the values variant omits "; size of key: N".
        assert!(!msg.contains("size of key"), "{msg}");
    }

    #[test]
    fn values_empty_attribute_value_rejected() {
        let msg = values_err(r#"{"values":{":v":{}}}"#);
        assert!(
            msg.contains(
                "ExpressionAttributeValues contains invalid value: Supplied \
                 AttributeValue is empty, must contain exactly one of the \
                 supported datatypes for key :v"
            ),
            "{msg}"
        );
    }

    #[test]
    fn values_empty_string_accepted() {
        let parsed: TestValues = serde_json::from_str(r#"{"values":{":v":{"S":""}}}"#).unwrap();
        assert!(parsed.values.is_some());
    }

    #[test]
    fn names_key_length_is_bytes_not_chars() {
        // 128 two-byte chars after '#' = 257 bytes (129 chars): too long, size in bytes.
        let key = format!("#{}", "\u{00e9}".repeat(128));
        assert_eq!(key.chars().count(), 129);
        assert_eq!(key.len(), 257);
        let json = format!(r#"{{"names":{{"{key}":"foo"}}}}"#);
        let msg = names_err(&json);
        assert!(
            msg.contains("contains a key that is too long; size of key: 257"),
            "{msg}"
        );
    }

    #[test]
    fn names_non_string_value_is_type_error_not_empty() {
        // Non-string value: type error (SerializationException), not "Empty attribute name".
        let msg = names_err(r##"{"names":{"#a":5}}"##);
        assert!(!msg.contains("Empty attribute name"), "{msg}");
        assert!(
            msg.contains("invalid type") || msg.contains("expected a string"),
            "{msg}"
        );
    }
}
