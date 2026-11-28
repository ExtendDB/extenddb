// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Custom serde deserializers for `DynamoDB` input validation.
//!
//! Validate `ExpressionAttributeNames` / `ExpressionAttributeValues` map entries
//! at deserialization time, before expression parsing, matching Amazon
//! `DynamoDB`'s order: non-empty map, well-formed placeholder keys
//! (`<prefix>[A-Za-z0-9_]+`, <=255 bytes incl. prefix), non-empty mapped value.

use std::collections::HashMap;
use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::de::{self, Deserialize, Deserializer};
use serde_json::{Map, Value};

use crate::types::{AttributeValue, Item};

/// The refusal of an attribute value that is JSON null, the same for an
/// item attribute as for a member of a PartiQL parameter.
pub const NULL_MEMBER: &str = "The supplied Item contains a null AttributeValue";

/// The refusal of a JSON null at the attribute or map member `name`.
#[must_use]
pub fn null_member(name: &str) -> String {
    format!("{NULL_MEMBER}: {name} is null")
}

/// The refusal of an attribute name that is empty or JSON null.
pub const EMPTY_ATTRIBUTE_NAME: &str = "Empty attribute name";

/// Maximum placeholder key length in bytes, including the `#` / `:` prefix.
const MAX_PLACEHOLDER_KEY_BYTES: usize = 255;

/// A placeholder identifier (the text after the `#` / `:`) is one or more of
/// `[A-Za-z0-9_]`.
pub(crate) fn is_placeholder_ident(rest: &str) -> bool {
    !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Validate one placeholder key. Length is checked before syntax.
///
/// `include_size_in_too_long`: the Names too-long message appends
/// `; size of key: N`, the Values one does not (Amazon `DynamoDB` quirk).
pub(crate) fn validate_placeholder_key(
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
                     {EMPTY_ATTRIBUTE_NAME} for key {key}"
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

/// Deserialize an item, refusing an attribute that is JSON null by name, as
/// Amazon `DynamoDB` refuses it (measured 2026-11-14 and 2026-11-17). The
/// attributes are read in map order, and the first fault is reported unless
/// a later value has the wrong JSON type. `DynamoDB` refuses such a value
/// ahead of a null or empty value wherever it stands (measured 2026-11-17).
/// In a `serde_json::Value`, map order is name order unless serde_json's
/// `preserve_order` is on. For an item with `"c": null` and `"ba": null`,
/// `DynamoDB` names `c`, so its order of two nulls is not name order
/// (measured 2026-11-17).
///
/// # Errors
///
/// Returns the deserializer error type when the item does not deserialize or
/// holds a null attribute.
pub fn deserialize_item<'de, D>(deserializer: D) -> Result<Item, D::Error>
where
    D: Deserializer<'de>,
{
    struct ItemVisitor;

    impl<'de> de::Visitor<'de> for ItemVisitor {
        type Value = Item;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map")
        }

        fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Item, A::Error> {
            let mut item = Item::new();
            while let Some(name) = map.next_key::<String>()? {
                let fault = match map.next_value::<Option<AttributeValue>>() {
                    Ok(Some(value)) => {
                        item.insert(name, value);
                        continue;
                    }
                    Ok(None) => de::Error::custom(null_member(&name)),
                    Err(fault) => fault,
                };
                return Err(first_type_fault(map).unwrap_or(fault));
            }
            Ok(item)
        }
    }

    deserializer.deserialize_map(ItemVisitor)
}

/// The first value left in `map` that [`check_parameter_types`] refuses.
/// A null is skipped. A read error ends the search, and the caller then
/// reports the fault it holds.
fn first_type_fault<'de, A: de::MapAccess<'de>>(mut map: A) -> Option<A::Error> {
    while let Ok(Some((de::IgnoredAny, value))) = map.next_entry::<_, Option<Value>>() {
        if let Err(fault) = check_parameter_types(value.as_slice()) {
            return Some(de::Error::custom(fault));
        }
    }
    None
}

/// The members an attribute value may hold. Any other member is ignored.
const TYPES: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// Check the JSON types of a statement's parameters (wire attribute
/// values), the first of the two passes of the PartiQL parameter checks.
/// The caller checks every statement of the request this way before it
/// checks anything else of the request. [`deserialize_item`] checks the
/// values an item holds after its first fault the same way.
///
/// The walk visits every map and list. The request body's JSON nesting
/// limit bounds its depth, at most one level for each two JSON levels.
///
/// # Errors
///
/// Returns a description of the first value whose JSON the attribute value
/// model cannot read: a value that is not a JSON object, or a member with a
/// payload of the wrong JSON type.
pub fn check_parameter_types(parameters: &[Value]) -> Result<(), String> {
    parameters.iter().try_for_each(check_types)
}

fn check_types(value: &Value) -> Result<(), String> {
    let Value::Object(members) = value else {
        return Err(AttributeValue::deserialize(value).err().map_or_else(
            || "an attribute value must be a JSON object".to_owned(),
            |e| e.to_string(),
        ));
    };
    for (tag, payload) in typed_members(members) {
        match (tag, payload) {
            ("S" | "N", Value::String(_)) | ("NULL" | "BOOL", Value::Bool(_)) => {}
            ("B", Value::String(text)) => check_base64(text)?,
            ("SS" | "NS", Value::Array(items)) => {
                if items
                    .iter()
                    .any(|item| !item.is_string() && !item.is_null())
                {
                    return Err(format!("{tag} member must be a string"));
                }
            }
            ("BS", Value::Array(items)) => {
                for item in items {
                    match item {
                        Value::String(text) => check_base64(text)?,
                        Value::Null => {}
                        _ => return Err("BS member must be a base64 string".to_owned()),
                    }
                }
            }
            ("M", Value::Object(entries)) => {
                for member in entries.values().filter(|member| !member.is_null()) {
                    check_types(member)?;
                }
            }
            ("L", Value::Array(items)) => {
                for item in items.iter().filter(|item| !item.is_null()) {
                    check_types(item)?;
                }
            }
            (tag, _) => return Err(wrong_payload(tag)),
        }
    }
    Ok(())
}

/// The description of a payload of the wrong JSON type, in the attribute
/// value deserializer's words.
fn wrong_payload(tag: &str) -> String {
    let expected = match tag {
        "S" | "N" => "a string",
        "B" => "a base64 string",
        "NULL" | "BOOL" => "a boolean",
        "M" => "an object",
        _ => "an array",
    };
    format!("{tag} value must be {expected}")
}

fn check_base64(text: &str) -> Result<(), String> {
    BASE64
        .decode(text)
        .map(drop)
        .map_err(|e| format!("invalid base64: {e}"))
}

/// The members of an attribute value the model reads: those it names whose
/// value is not null, in the order the value holds them.
pub(crate) fn typed_members(members: &Map<String, Value>) -> impl Iterator<Item = (&str, &Value)> {
    members
        .iter()
        .filter(|(tag, payload)| !payload.is_null() && TYPES.contains(&tag.as_str()))
        .map(|(tag, payload)| (tag.as_str(), payload))
}

/// Deserialize `AttributesToGet`: an element that is JSON null is refused as
/// an empty attribute name (measured 2026-11-14).
///
/// # Errors
///
/// Returns the deserializer error type when the list does not deserialize or
/// holds a null element.
pub fn deserialize_attributes_to_get<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<Option<String>>>::deserialize(deserializer)?
        .map(|names| {
            names
                .into_iter()
                .map(|name| name.ok_or_else(|| de::Error::custom(EMPTY_ATTRIBUTE_NAME)))
                .collect()
        })
        .transpose()
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
