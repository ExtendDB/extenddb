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

/// The start of the attribute value deserializer's text for a `B` payload or
/// a `BS` member that is not base64.
const INVALID_BASE64: &str = "invalid base64: ";

/// Whether the value `members` describe has no single type descriptor, or
/// holds one that has none in an `L` or an `M` at any depth.
fn has_no_single_descriptor(members: &serde_json::Map<String, Value>) -> bool {
    match crate::types::type_descriptor(members) {
        Ok((tag, payload)) => match (tag.as_str(), payload) {
            ("L", Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_object)
                .any(has_no_single_descriptor),
            ("M", Value::Object(entries)) => entries
                .values()
                .filter_map(Value::as_object)
                .any(has_no_single_descriptor),
            _ => false,
        },
        Err(_) => true,
    }
}

/// A request member that holds legacy condition operands: the `Value` or
/// `AttributeValueList` of an `Expected` entry, or the `AttributeValueList`
/// of a `KeyConditions`, `QueryFilter` or `ScanFilter` entry. Each operand is
/// read at depth 0, as an `ExpressionAttributeValues` value is: a value with
/// two descriptors may hold one map or list more there than in an item
/// attribute or a key (measured 2026-11-24 through PutItem, UpdateItem,
/// DeleteItem, Query and Scan, each request twice). Serde reads the member's
/// JSON type and [`crate::types::from_nested`] each operand, so a member of
/// the wrong JSON type and an operand that is not an object keep serde's
/// text.
pub(crate) trait Operands: Sized {
    /// The member with each operand as JSON.
    type Json: serde::de::DeserializeOwned;
    fn read(json: Self::Json) -> Result<Self, String>;
}

impl Operands for AttributeValue {
    type Json = Value;
    fn read(json: Value) -> Result<Self, String> {
        crate::types::from_nested(json, 0)
    }
}

impl<T: Operands> Operands for Option<T> {
    type Json = Option<T::Json>;
    fn read(json: Self::Json) -> Result<Self, String> {
        json.map(T::read).transpose()
    }
}

impl<T: Operands> Operands for Vec<T> {
    type Json = Vec<T::Json>;
    fn read(json: Self::Json) -> Result<Self, String> {
        json.into_iter().map(T::read).collect()
    }
}

/// `deserialize_with` target of a member of legacy condition operands (see
/// [`Operands`]).
pub(crate) fn deserialize_operands<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Operands,
{
    T::read(T::Json::deserialize(deserializer)?).map_err(de::Error::custom)
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
            // A value that has, or holds in an `L` or an `M`, a value with no
            // single type descriptor is refused with the deserializer's text
            // named after its key, read at depth 0: such a value may hold one
            // map or list more than an item attribute (measured 2026-11-23
            // and 2026-11-24 on UpdateItem and Query, each shape twice).
            // Caught here because a serde error does not carry the key. A
            // `B` or `BS` payload that is not base64 keeps its bare text, as
            // a single `B` does. Any other value is read by `convert`, whose
            // refusals are bare.
            if let Some(members) = value.as_object()
                && has_no_single_descriptor(members)
                && let Err(refusal) = crate::types::from_nested(value.clone(), 0)
                && !refusal.starts_with(INVALID_BASE64)
            {
                return Err(format!(
                    "ExpressionAttributeValues contains invalid value: {refusal} for key {key}"
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

    const EMPTY: &str = "Supplied AttributeValue is empty, must contain exactly one of the \
                         supported datatypes";
    const MULTI: &str = "Supplied AttributeValue has more than one datatypes set, must contain \
                         exactly one of the supported datatypes";

    /// An `S` beside an `L`, `levels` lists in all.
    fn beside_s(levels: usize) -> Value {
        let lists = (1..levels).fold(
            serde_json::json!({"S": "x"}),
            |v, _| serde_json::json!({"L": [v]}),
        );
        serde_json::json!({"S": "x", "L": [lists]})
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

    /// A value that has no single type descriptor (`{}`, members the
    /// deserializer ignores only, or two or more), or holds one in an `L` or
    /// an `M`, is refused with the deserializer's text named after its key:
    /// the count's text, or that of a payload checked ahead of the count,
    /// with one map or list more allowed than in an item attribute (measured
    /// 2026-11-23 and 2026-11-24 on Query and UpdateItem, each twice). Any
    /// other value, and a payload that is not base64, keep the deserializer's
    /// bare text.
    #[test]
    fn values_with_no_single_descriptor_are_refused_by_key() {
        let wrapped = |text: &str| {
            format!("ExpressionAttributeValues contains invalid value: {text} for key :v")
        };
        let base64 = serde_json::from_value::<AttributeValue>(serde_json::json!({"B": "!!!"}))
            .unwrap_err()
            .to_string();
        let deep = beside_s(32).to_string();
        let cases = [
            ("{}", wrapped(EMPTY)),
            (r#"{"X":"1"}"#, wrapped(EMPTY)),
            (r#"{"S":null}"#, wrapped(EMPTY)),
            (r#"{"S":null,"X":"1"}"#, wrapped(EMPTY)),
            (r#"{"S":"x","N":"1"}"#, wrapped(MULTI)),
            (
                r#"{"S":"x","N":"bob"}"#,
                wrapped("The parameter cannot be converted to a numeric value: bob"),
            ),
            (
                r#"{"SS":["a","a"],"N":"bob"}"#,
                wrapped(
                    "One or more parameter values were invalid: Input collection [a, a] \
                     contains duplicates.",
                ),
            ),
            (
                r#"{"S":"x","NS":["1","1"]}"#,
                wrapped(crate::partiql::params::NUMBER_SET_DUPLICATES),
            ),
            (r#"{"S":"x","BOOL":"yes"}"#, wrapped(MULTI)),
            (
                r#"{"S":"x","NULL":false}"#,
                wrapped(
                    "One or more parameter values were invalid: Null attribute value types \
                     must have the value of true",
                ),
            ),
            (
                r#"{"S":"x","L":[null]}"#,
                wrapped("The supplied Item contains a null AttributeValue"),
            ),
            // A value with two descriptors in an `L` or an `M`.
            (
                r#"{"L":[{"S":"x","N":"bob"}]}"#,
                wrapped("The parameter cannot be converted to a numeric value: bob"),
            ),
            (
                r#"{"M":{"k":{"S":"x","N":"bob"}}}"#,
                wrapped("The parameter cannot be converted to a numeric value: bob"),
            ),
            (r#"{"L":[{"S":"x","N":"1"}]}"#, wrapped(MULTI)),
            (
                r#"{"M":{"k":{"S":"x","SS":["a","a"]}}}"#,
                wrapped(
                    "One or more parameter values were invalid: Input collection [a, a] \
                     contains duplicates.",
                ),
            ),
            (
                r#"{"L":[{"SS":[]},{"S":"x","N":"1"}]}"#,
                wrapped(
                    "One or more parameter values were invalid: An string set  may not be empty",
                ),
            ),
            (deep.as_str(), wrapped(MULTI)),
            // A `B` or `BS` payload that is not base64 keeps the bare text of
            // a single `B` (DynamoDB: a `SerializationException` with no
            // wrapper, with a text of its own, #1189).
            (r#"{"S":"x","B":"!!!"}"#, base64.clone()),
            (r#"{"S":"x","BS":["!!!"]}"#, base64.clone()),
            (r#"{"N":"bob","B":"!!!"}"#, base64),
            // ExtendDB's own text for a payload of the wrong JSON type, left
            // unwrapped (not measured; #1012 tracks these texts).
            (r#"{"S":1}"#, "S value must be a string".to_owned()),
        ];
        for (value, want) in cases {
            let msg = values_err(&format!(r#"{{"values":{{":v":{value}}}}}"#));
            // The text deserializer appends the position; the engine's
            // `from_value` path does not.
            assert!(
                msg.starts_with(&format!("{want} at line 1 column ")),
                "{value}: {msg}"
            );
        }
    }

    /// A legacy condition operand is read at depth 0, so a value with two
    /// descriptors there may hold 32 maps and lists in all, one more than an
    /// `AttributeUpdates` value or a key, before the nesting refusal (measured
    /// 2026-11-24 on a table that does not exist, each request twice; the
    /// service adds `The provided starting key is invalid: ` to the
    /// `ExclusiveStartKey` texts). The other rows pin, for each member of
    /// operands, the answers that do not depend on the depth.
    #[test]
    fn a_condition_operand_may_hold_one_map_or_list_more_than_a_key() {
        use crate::partiql::params::NESTING_MESSAGE as NESTING;
        use crate::types::{
            BatchGetItemInput, DeleteItemInput, PutItemInput, QueryInput, ScanInput,
            TransactGetItemsInput, UpdateItemInput,
        };
        use serde_json::json;
        fn read<T: serde::de::DeserializeOwned>(body: Value) -> Result<(), String> {
            serde_json::from_value::<T>(body)
                .map(drop)
                .map_err(|e| e.to_string())
        }
        let invalid_type = |what: &str| {
            format!(
                "invalid type: {what}, expected a DynamoDB AttributeValue map with exactly one \
                 type descriptor"
            )
        };
        let base64 = serde_json::from_value::<AttributeValue>(json!({"B": "!!!"}))
            .unwrap_err()
            .to_string();
        // Each place by its row name: the member it holds (`V` an `Expected`
        // `Value`, `E` an `Expected` `AttributeValueList`, `C` that of a
        // `Condition`, `K` a stored value or a key), the reader of the
        // operation's input, the lists in all it was measured with, and the
        // request around the member.
        type Read = fn(Value) -> Result<(), String>;
        type Request = fn(Value) -> Value;
        let places: [(&str, char, Read, &[usize], Request); 12] = [
            ("EV", 'V', read::<PutItemInput>, &[31, 32, 33], |m| {
                json!({"TableName": "t", "Item": {"pk": {"S": "k"}},
                       "Expected": {"a": {"Value": m}}})
            }),
            ("UE", 'V', read::<UpdateItemInput>, &[32, 33], |m| {
                json!({"TableName": "t", "Key": {"pk": {"S": "k"}},
                       "Expected": {"a": {"Value": m}}})
            }),
            ("DE", 'V', read::<DeleteItemInput>, &[32, 33], |m| {
                json!({"TableName": "t", "Key": {"pk": {"S": "k"}},
                       "Expected": {"a": {"Value": m}}})
            }),
            ("EL", 'E', read::<PutItemInput>, &[31, 32, 33], |m| {
                json!({"TableName": "t", "Item": {"pk": {"S": "k"}},
                       "Expected": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": m}}})
            }),
            ("SF", 'C', read::<ScanInput>, &[31, 32, 33], |m| {
                json!({"TableName": "t",
                       "ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": m}}})
            }),
            ("QF", 'C', read::<QueryInput>, &[32, 33], |m| {
                json!({"TableName": "t",
                       "KeyConditions": {"pk": {"ComparisonOperator": "EQ",
                                                "AttributeValueList": [{"S": "k"}]}},
                       "QueryFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": m}}})
            }),
            ("KC", 'C', read::<QueryInput>, &[31, 32, 33], |m| {
                json!({"TableName": "t",
                       "KeyConditions": {"pk": {"ComparisonOperator": "EQ",
                                                "AttributeValueList": m}}})
            }),
            ("AU", 'K', read::<UpdateItemInput>, &[31, 32, 33], |v| {
                json!({"TableName": "t", "Key": {"pk": {"S": "k"}},
                       "AttributeUpdates": {"a": {"Value": v, "Action": "PUT"}}})
            }),
            ("XK", 'K', read::<ScanInput>, &[31, 32, 33], |v| {
                json!({"TableName": "t",
                       "ExclusiveStartKey": {"pk": v}})
            }),
            ("DK", 'K', read::<DeleteItemInput>, &[32, 33], |v| {
                json!({"TableName": "t",
                       "Key": {"pk": v}})
            }),
            ("BG", 'K', read::<BatchGetItemInput>, &[32, 33], |v| {
                json!({"RequestItems":
                       {"t": {"Keys": [{"pk": v}]}}})
            }),
            ("TG", 'K', read::<TransactGetItemsInput>, &[32, 33], |v| {
                json!({"TransactItems": [{"Get": {"TableName": "t",
                                                  "Key": {"pk": v}}}]})
            }),
        ];
        for (name, member, read_body, measured, request) in places {
            // The member that holds `v` alone.
            let list = matches!(member, 'E' | 'C');
            let member_of = |v: Value| if list { json!([v]) } else { v };
            let limit = if member == 'K' { 31 } else { 32 };
            for &levels in measured {
                let want = if levels > limit { NESTING } else { MULTI };
                let body = request(member_of(beside_s(levels)));
                assert_eq!(read_body(body), Err(want.into()), "{name}-{levels}");
            }
            if member == 'K' {
                continue;
            }
            let mut rows: Vec<(Value, Result<(), String>)> = [
                (json!("x"), Err(invalid_type("string \"x\""))),
                (json!({}), Err(EMPTY.to_owned())),
                (json!({"S": "x"}), Ok(())),
                (json!({"S": "x", "B": "!!!"}), Err(base64.clone())),
                (json!({"B": "!!!"}), Err(base64.clone())),
            ]
            .into_iter()
            .map(|(v, want)| (member_of(v), want))
            .collect();
            // A null, a member of the other JSON type, and a list read in
            // order.
            if list {
                let not_a_list = |what: &str| format!("invalid type: {what}, expected a sequence");
                let null_list = if member == 'E' {
                    Ok(())
                } else {
                    Err(not_a_list("null"))
                };
                let first = Err("S value must be a string".to_owned());
                rows.extend([
                    (json!([null]), Err(invalid_type("null"))),
                    (Value::Null, null_list),
                    (json!({"S": "x", "N": "1"}), Err(not_a_list("map"))),
                    (
                        json!(["x", {"S": "x", "N": "1"}]),
                        Err(invalid_type("string \"x\"")),
                    ),
                    (json!([{"S": 1}, beside_s(33)]), first),
                ]);
            } else {
                rows.extend([
                    (Value::Null, Ok(())),
                    (json!([{"S": "x", "N": "1"}]), Err(invalid_type("sequence"))),
                ]);
            }
            for (m, want) in rows {
                assert_eq!(read_body(request(m.clone())), want, "{name} {m}");
            }
        }
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
