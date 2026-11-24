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

use crate::types::{AttributeValue, Item};

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
    for (key, value) in raw {
        validate_placeholder_key(&key, prefix, field_name, include_size_in_too_long)
            .map_err(de::Error::custom)?;
        check_value(&key, &value).map_err(de::Error::custom)?;
        let converted = convert(value).map_err(de::Error::custom)?;
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

/// The `AttributeValue` type descriptors ExtendDB recognizes. A value with no
/// member, or a single member outside this set, is "empty" to the service.
const KNOWN_DESCRIPTORS: &[&str] = &["S", "N", "B", "SS", "NS", "BS", "M", "L", "BOOL", "NULL"];

/// DynamoDB's message for an `ExclusiveStartKey` value that is empty or
/// carries only an unknown type descriptor: the service reports both as
/// "empty", prefixed with the starting-key sentence. A recognized but invalid
/// value (an empty number set, for one) keeps its own message with no prefix.
/// Measured on DynamoDB (us-east-1, 2026-11-24).
const INVALID_START_KEY_EMPTY: &str = "The provided starting key is invalid: Supplied \
     AttributeValue is empty, must contain exactly one of the supported datatypes";

/// Deserialize `ExclusiveStartKey` (Query and Scan). An empty value or one
/// with only an unknown type descriptor answers [`INVALID_START_KEY_EMPTY`];
/// any other malformed value keeps its own `AttributeValue` error, so an empty
/// number set still answers "An number set may not be empty" with no prefix.
/// A valid value deserializes as before.
///
/// # Errors
///
/// Returns the deserializer error type when a key value is malformed.
pub fn deserialize_exclusive_start_key<'de, D>(deserializer: D) -> Result<Option<Item>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(raw) = Option::<serde_json::Map<String, Value>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let mut out = Item::new();
    for (name, value) in raw {
        // The service reports an empty value, or one whose single member is
        // not a known type descriptor, as "empty" with the starting-key
        // prefix. A recognized descriptor with bad content (an empty number
        // set, for one) keeps its own message. Decide on the value's shape,
        // not on another layer's error text, so a member whose data merely
        // contains "descriptor" is not mistaken for an unknown descriptor.
        if let Some(obj) = value.as_object() {
            let first_key = obj.keys().next().map_or("", String::as_str);
            if obj.is_empty() || (obj.len() == 1 && !KNOWN_DESCRIPTORS.contains(&first_key)) {
                return Err(de::Error::custom(INVALID_START_KEY_EMPTY));
            }
        }
        let av = serde_json::from_value::<AttributeValue>(value).map_err(de::Error::custom)?;
        out.insert(name, av);
    }
    Ok(Some(out))
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

    #[derive(Debug, Deserialize)]
    struct TestStartKey {
        #[serde(default, deserialize_with = "deserialize_exclusive_start_key")]
        key: Option<Item>,
    }

    fn start_key_err(json: &str) -> String {
        // Match the production path (serde_json::from_value), which does not
        // append a position suffix to a custom error, unlike from_str.
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        serde_json::from_value::<TestStartKey>(value)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn start_key_empty_value_is_prefixed_empty() {
        assert_eq!(
            start_key_err(r#"{"key":{"pk":{},"sk":{"S":"x"}}}"#),
            INVALID_START_KEY_EMPTY
        );
    }

    #[test]
    fn start_key_unknown_descriptor_is_prefixed_empty() {
        // The service reports an unknown descriptor as "empty", same as {}.
        assert_eq!(
            start_key_err(r#"{"key":{"pk":{"X":"1"},"sk":{"S":"x"}}}"#),
            INVALID_START_KEY_EMPTY
        );
    }

    #[test]
    fn start_key_empty_number_set_keeps_its_own_message_unprefixed() {
        assert_eq!(
            start_key_err(r#"{"key":{"pk":{"NS":[]},"sk":{"S":"x"}}}"#),
            "One or more parameter values were invalid: An number set  may not be empty"
        );
    }

    #[test]
    fn start_key_set_member_text_is_not_mistaken_for_a_descriptor() {
        // A recognized descriptor (SS) whose data contains "descriptor" must
        // keep its own error, not the prefixed empty one.
        let msg = start_key_err(
            r#"{"key":{"pk":{"SS":["unknown AttributeValue type descriptor","unknown AttributeValue type descriptor"]}}}"#,
        );
        assert!(msg.contains("contains duplicates"), "{msg}");
        assert!(!msg.contains("provided starting key"), "{msg}");
    }

    #[test]
    fn start_key_nested_unknown_descriptor_is_passed_through() {
        // Only a top-level unknown descriptor is "empty"; a nested one keeps
        // its own error (the service behavior here was not measured).
        let msg = start_key_err(r#"{"key":{"pk":{"L":[{"X":"1"}]}}}"#);
        assert!(
            msg.contains("unknown AttributeValue type descriptor"),
            "{msg}"
        );
        assert!(!msg.contains("provided starting key"), "{msg}");
    }

    #[test]
    fn start_key_valid_value_deserializes() {
        let parsed: TestStartKey =
            serde_json::from_str(r#"{"key":{"pk":{"S":"p0"},"sk":{"S":"x"}}}"#).unwrap();
        assert_eq!(parsed.key.unwrap().len(), 2);
    }

    #[test]
    fn start_key_absent_is_none() {
        let parsed: TestStartKey = serde_json::from_str("{}").unwrap();
        assert!(parsed.key.is_none());
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
