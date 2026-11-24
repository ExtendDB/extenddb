// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Custom serde deserializers for `DynamoDB` input validation.
//!
//! Validate `ExpressionAttributeNames` / `ExpressionAttributeValues` map entries
//! at deserialization time, before expression parsing, matching Amazon
//! `DynamoDB`'s order for each map (see `deserialize_placeholder_map`): a
//! non-empty map and the JSON type of each value come first. Then
//! `ExpressionAttributeValues` checks its keys (`<prefix>[A-Za-z0-9_]+`, <=255
//! bytes incl. prefix) ahead of its values, and `ExpressionAttributeNames` its
//! names ahead of its keys.

use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::de::{self, Deserialize, Deserializer};
use serde_json::{Map, Value};

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
/// well-formed keys). The caller supplies the per-type value checks and the
/// conversion. Values arrive as raw `serde_json::Value` so a value error can
/// name its key (serde errors do not carry the failing key).
///
/// The checks come in Amazon `DynamoDB`'s order (measured 2026-11-19). A
/// value of the wrong JSON type is refused first, the first one written,
/// because the service refuses it while it reads the request's JSON.
/// `wrong_type` finds such a value and `convert` gives its refusal. Then come
/// the keys and the values, the values first when `values_before_keys`. Both
/// are visited in the order a Java `HashMap` of the map yields them (see
/// [`crate::map_order::hash_map_order`]).
#[allow(clippy::too_many_arguments)]
fn deserialize_placeholder_map<'de, D, T>(
    deserializer: D,
    prefix: char,
    field_name: &str,
    include_size_in_too_long: bool,
    wrong_type: impl Fn(&Value) -> bool,
    values_before_keys: bool,
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
    for value in raw.values().filter(|value| wrong_type(value)) {
        convert(value.clone()).map_err(de::Error::custom)?;
    }
    let members = crate::map_order::hash_map_order(raw);
    if values_before_keys {
        for (key, value) in &members {
            check_value(key, value).map_err(de::Error::custom)?;
        }
    }
    for (key, _) in &members {
        validate_placeholder_key(key, prefix, field_name, include_size_in_too_long)
            .map_err(de::Error::custom)?;
    }
    let mut out = HashMap::with_capacity(members.len());
    for (key, value) in members {
        if !values_before_keys {
            check_value(&key, &value).map_err(de::Error::custom)?;
        }
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
        // A name that is not a string is refused first, and an empty name
        // ahead of a malformed key. A JSON null is neither: an empty name
        // beside it is refused (measured 2026-11-19).
        |value| !value.is_string() && !value.is_null(),
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
        // A value the attribute value model cannot read is refused first,
        // and a malformed key ahead of an empty value. A JSON null is not
        // such a value: an empty value beside it is refused (measured
        // 2026-11-19).
        |value| !value.is_null() && check_types(value).is_err(),
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

/// The members an attribute value may hold. Any other member is ignored.
const TYPES: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// Check the JSON types of a statement's parameters (wire attribute
/// values), the first of the two passes of the PartiQL parameter checks.
/// The caller checks every statement of the request this way before it
/// checks anything else of the request. [`deserialize_expression_values`]
/// checks its values the same way before it checks their keys.
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

    /// Two faults in one map, the members of `first` written ahead of those
    /// of `second` and after them, and the refusal the service gave in both
    /// orders (measured 2026-11-12 and 2026-11-19). The map's own order is
    /// name order in this crate's tests, and the order written in a build
    /// with serde_json's `preserve_order` feature.
    #[test]
    fn two_faults_in_one_map_are_refused_as_the_service_refuses_them() {
        let empty_value = |key: &str| {
            format!(
                "ExpressionAttributeValues contains invalid value: Supplied AttributeValue \
                 is empty, must contain exactly one of the supported datatypes for key {key}"
            )
        };
        let empty_name = |key: &str| {
            format!(
                "ExpressionAttributeNames contains invalid value: Empty attribute name for key {key}"
            )
        };
        let values_key =
            r#"ExpressionAttributeValues contains invalid key: Syntax error; key: "b""#;
        let s_number = "S value must be a string".to_owned();
        let name_number = "invalid type: integer `5`, expected a string".to_owned();
        let rows = [
            // Two empty values: the one in the lower bucket (see
            // `crate::map_order`). In 4 buckets `:b` is in 0, `:c` in 1 and
            // `:a` in 3. In 8, `:c` is in 1, `:f` in 4, `:p` in 6 and `:a`
            // and `:q` in 7.
            ("values", r#"":c":{}"#, r#"":b":{}"#, empty_value(":b")),
            ("values", r#"":a":{}"#, r#"":c":{}"#, empty_value(":c")),
            (
                "values",
                r#"":a":{}"#,
                r#"":f":{},":p":{"S":"p"},":q":{"S":"q"}"#,
                empty_value(":f"),
            ),
            // A value of the wrong JSON type, wherever it stands.
            ("values", r#"":b":{}"#, r#"":a":{"S":5}"#, s_number.clone()),
            ("values", r#""b":{"S":"p"}"#, r#"":c":{"S":5}"#, s_number),
            ("names", r##""#c":"""##, r##""#b":5"##, name_number.clone()),
            ("names", r#""b":"x""#, r##""#c":5"##, name_number),
            // A malformed key ahead of an empty value, but an empty name
            // ahead of a malformed key.
            ("values", r#""b":{}"#, r#"":c":{}"#, values_key.to_owned()),
            (
                "values",
                r#""b":{"S":"p"}"#,
                r#"":c":{}"#,
                values_key.to_owned(),
            ),
            ("names", r#""b":"x""#, r##""#c":"""##, empty_name("#c")),
            // A value refused for its content is met in its bucket.
            (
                "values",
                r#"":b":{"NULL":false}"#,
                r#"":a":{}"#,
                "Null attribute value types must have the value of true".to_owned(),
            ),
            // A JSON null is not a value of the wrong type.
            ("values", r#"":b":{}"#, r#"":a":null"#, empty_value(":b")),
            ("names", r##""#c":"""##, r##""#b":null"##, empty_name("#c")),
        ];
        for (map, first, second, refusal) in rows {
            for members in [format!("{first},{second}"), format!("{second},{first}")] {
                let json = format!("{{\"{map}\":{{{members}}}}}");
                let msg = if map == "names" {
                    names_err(&json)
                } else {
                    values_err(&json)
                };
                assert!(msg.contains(&refusal), "{json}: {msg}");
            }
        }
    }
}
