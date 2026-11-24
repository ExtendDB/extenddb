// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Virtual `DynamoDB` attribute value — the fundamental data type.
///
/// Each variant maps to a `DynamoDB` type descriptor. Custom Serialize/Deserialize
/// impls produce the `DynamoDB` JSON wire format: `{"S": "hello"}`, `{"N": "42"}`.
///
/// REQ-TYPE-001: exactly one type descriptor per value.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    /// String type.
    S(String),
    /// Number type — stored as string to preserve arbitrary precision (up to 38 digits).
    N(String),
    /// Binary type — raw bytes; base64-encoded on wire.
    B(Vec<u8>),
    /// String set.
    SS(BTreeSet<String>),
    /// Number set.
    NS(BTreeSet<String>),
    /// Binary set.
    BS(BTreeSet<Vec<u8>>),
    /// Boolean type.
    Bool(bool),
    /// Null type.
    Null,
    /// List type — ordered, heterogeneous.
    L(Vec<AttributeValue>),
    /// Map type — unordered, heterogeneous.
    M(BTreeMap<String, AttributeValue>),
}

impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::S(v) => map.serialize_entry("S", v)?,
            Self::N(v) => map.serialize_entry("N", v)?,
            Self::B(v) => map.serialize_entry("B", &BASE64.encode(v))?,
            Self::SS(v) => map.serialize_entry("SS", &v.iter().collect::<Vec<_>>())?,
            Self::NS(v) => map.serialize_entry("NS", &v.iter().collect::<Vec<_>>())?,
            Self::BS(v) => {
                let encoded: Vec<String> = v.iter().map(|b| BASE64.encode(b)).collect();
                map.serialize_entry("BS", &encoded)?;
            }
            Self::Bool(v) => map.serialize_entry("BOOL", v)?,
            Self::Null => map.serialize_entry("NULL", &true)?,
            Self::L(v) => map.serialize_entry("L", v)?,
            Self::M(v) => map.serialize_entry("M", v)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(AttributeValueVisitor)
    }
}

struct AttributeValueVisitor;

const EMPTY_ATTRIBUTE_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
const MULTI_ATTRIBUTE_VALUE: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

impl<'de> Visitor<'de> for AttributeValueVisitor {
    type Value = AttributeValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB AttributeValue map with exactly one type descriptor")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut members = Vec::new();
        while let Some(member) = map.next_entry::<String, serde_json::Value>()? {
            members.push(member);
        }
        // Read as an item attribute, whose item is a map around it.
        from_members(members, 1).map_err(de::Error::custom)
    }
}

/// The type descriptors `DynamoDB` defines.
const TYPE_DESCRIPTORS: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "BOOL", "NULL", "L", "M"];

/// Whether the member `name: payload` of a value object is a type descriptor
/// the value is read by: one of [`TYPE_DESCRIPTORS`] whose payload is not
/// JSON null. `DynamoDB` ignores every other member, at the top level and
/// inside an `L` or an `M` (measured 2026-11-23): `{"S": "a", "X": "1"}` and
/// `{"S": "a", "N": null}` are the string `a`, and `{"X": "1"}` and
/// `{"S": null}` have no type.
pub(crate) fn is_type_descriptor(name: &str, payload: &serde_json::Value) -> bool {
    TYPE_DESCRIPTORS.contains(&name) && !payload.is_null()
}

/// The member of a value object the value is read by: the one member
/// [`is_type_descriptor`] passes, whatever order the members come in, so a
/// build that keeps the request's member order and one that sorts the members
/// read the same descriptor. The deserializer, the readers ahead of it that
/// bind `dynamodb:LeadingKeys` and the throttling key, and the PartiQL
/// parameter checks call it, so they read the member the engine reads.
///
/// # Errors
///
/// The text the deserializer refuses the object with (REQ-TYPE-001):
/// `EMPTY_ATTRIBUTE_VALUE` when no member passes, `MULTI_ATTRIBUTE_VALUE`
/// when two or more do.
pub fn type_descriptor<K, V>(
    members: impl IntoIterator<Item = (K, V)>,
) -> Result<(K, V), &'static str>
where
    K: AsRef<str>,
    V: std::borrow::Borrow<serde_json::Value>,
{
    let mut descriptors = members
        .into_iter()
        .filter(|(name, payload)| is_type_descriptor(name.as_ref(), payload.borrow()));
    match (descriptors.next(), descriptors.next()) {
        (None, _) => Err(EMPTY_ATTRIBUTE_VALUE),
        (Some(_), Some(_)) => Err(MULTI_ATTRIBUTE_VALUE),
        (Some(member), None) => Ok(member),
    }
}

/// The value the members of one object describe, at any level: the one
/// member [`type_descriptor`] leaves, read by [`from_descriptor`]. A value
/// with two or more descriptors is refused for the first of their payloads
/// that the service checks ahead of the count, else for the count. `depth`
/// counts the maps and lists around the value. An item attribute, a key and
/// an `AttributeUpdates` value have their item around them, and a condition
/// operand (an `ExpressionAttributeValues` value, an `Expected` value or an
/// `AttributeValueList` member) nothing, so it may hold one map or list
/// more, as a PartiQL parameter may (measured 2026-11-24).
fn from_members(
    members: impl IntoIterator<Item = (String, serde_json::Value)>,
    depth: usize,
) -> Result<AttributeValue, String> {
    let members: Vec<(String, serde_json::Value)> = members.into_iter().collect();
    let counted = type_descriptor(members.iter().map(|(name, payload)| (name, payload)));
    if matches!(counted, Err(MULTI_ATTRIBUTE_VALUE)) {
        // The service refuses a `B` or `BS` payload that is not base64 first,
        // at any depth, as a single `B`. It then checks the payloads as it
        // checks a PartiQL parameter `depth` maps and lists deep, and counts
        // the descriptors last (measured 2026-11-23 and 2026-11-24 on
        // PutItem, UpdateItem and ExecuteStatement).
        let value = serde_json::Value::Object(members.into_iter().collect());
        check_base64(&value)?;
        crate::partiql::params::check_value(&value, depth)?;
        return Err(MULTI_ATTRIBUTE_VALUE.to_owned());
    }
    let (name, payload) = type_descriptor(members)?;
    from_descriptor(&name, payload, depth)
}

/// Refuse a `B` payload or `BS` member that is not base64 in `value`, or in a
/// value its `L` and `M` payloads hold, as a single `B` is refused.
fn check_base64(value: &serde_json::Value) -> Result<(), String> {
    use serde_json::Value;
    let decode = |text: &Value| from_descriptor("B", text.clone(), 0).map(drop);
    for (name, payload) in value.as_object().into_iter().flatten() {
        match (name.as_str(), payload) {
            ("B", Value::String(_)) => decode(payload)?,
            ("BS", Value::Array(set)) => {
                set.iter().filter(|m| m.is_string()).try_for_each(decode)?
            }
            ("L", Value::Array(items)) => items.iter().try_for_each(check_base64)?,
            ("M", Value::Object(entries)) => entries.values().try_for_each(check_base64)?,
            _ => {}
        }
    }
    Ok(())
}

/// A value `depth` maps and lists deep, converted from the `serde_json::Value`
/// its reader owns: an element of an `L` or a value of an `M`, or a condition
/// operand at depth 0 (`crate::serde_helpers::Operands`).
///
/// The value is consumed, not cloned and re-deserialized: the earlier
/// `serde_json::from_value(v.clone())` held one more copy of the subtree at
/// every enclosing level, so a value nested 31 deep cost 31 copies of its
/// leaves at once. A non-object goes through `serde_json::from_value`
/// unchanged (serde's "invalid type" text; nothing below it is visited), and
/// an object takes the steps `visit_map` takes, at `depth`.
pub(crate) fn from_nested(
    value: serde_json::Value,
    depth: usize,
) -> Result<AttributeValue, String> {
    match value {
        serde_json::Value::Object(members) => from_members(members, depth),
        other => serde_json::from_value(other).map_err(|e| e.to_string()),
    }
}

/// The value of one type descriptor: `key` is the descriptor (`S`, `L`, ...)
/// and `value` its payload, of a value `depth` maps and lists deep. The error
/// is the message; the caller turns it into its deserializer's error.
fn from_descriptor(
    key: &str,
    value: serde_json::Value,
    depth: usize,
) -> Result<AttributeValue, String> {
    use serde_json::Value;
    match key {
        "S" => match value {
            Value::String(s) => Ok(AttributeValue::S(s)),
            _ => Err("S value must be a string".to_owned()),
        },
        "N" => match value {
            // Normalize valid numbers; store raw string for invalid ones so the
            // validation layer can reject them with ValidationException.
            Value::String(n) => Ok(AttributeValue::N(
                crate::validation::number::validate_and_normalize_number(&n).unwrap_or(n),
            )),
            _ => Err("N value must be a string".to_owned()),
        },
        "B" => {
            let b64 = value
                .as_str()
                .ok_or_else(|| "B value must be a base64 string".to_owned())?;
            let bytes = BASE64
                .decode(b64)
                .map_err(|e| format!("invalid base64: {e}"))?;
            Ok(AttributeValue::B(bytes))
        }
        "SS" => string_set(&value),
        "NS" => number_set(&value),
        "BS" => binary_set(&value),
        "BOOL" => {
            let b = value
                .as_bool()
                .ok_or_else(|| "BOOL value must be a boolean".to_owned())?;
            Ok(AttributeValue::Bool(b))
        }
        "NULL" => {
            let n = value
                .as_bool()
                .ok_or_else(|| "NULL value must be a boolean".to_owned())?;
            if !n {
                return Err(
                    "One or more parameter values were invalid: Null attribute value types must have the value of true"
                        .to_owned(),
                );
            }
            Ok(AttributeValue::Null)
        }
        // `L` and `M` consume their payload (see `from_nested`).
        "L" => match value {
            Value::Array(arr) => Ok(AttributeValue::L(
                arr.into_iter()
                    .map(|v| from_nested(v, depth + 1))
                    .collect::<Result<_, _>>()?,
            )),
            _ => Err("L value must be an array".to_owned()),
        },
        "M" => match value {
            Value::Object(obj) => Ok(AttributeValue::M(
                obj.into_iter()
                    .map(|(k, v)| from_nested(v, depth + 1).map(|av| (k, av)))
                    .collect::<Result<_, _>>()?,
            )),
            _ => Err("M value must be an object".to_owned()),
        },
        // Not reached: `from_members` passes a member of `TYPE_DESCRIPTORS`.
        _ => Err(EMPTY_ATTRIBUTE_VALUE.to_owned()),
    }
}

/// The payload of an `SS`.
fn string_set(value: &serde_json::Value) -> Result<AttributeValue, String> {
    let arr = value
        .as_array()
        .ok_or_else(|| "SS value must be an array".to_owned())?;
    if arr.is_empty() {
        return Err(
            "One or more parameter values were invalid: An string set  may not be empty".to_owned(),
        );
    }
    let set: BTreeSet<String> = arr
        .iter()
        .map(|v| {
            v.as_str()
                .map(std::borrow::ToOwned::to_owned)
                .ok_or_else(|| {
                    "One or more parameter values were invalid: An string set may not have a null string as a member".to_owned()
                })
        })
        .collect::<Result<_, _>>()?;
    string_set_duplicates(arr)?;
    Ok(AttributeValue::SS(set))
}

/// The refusal of a string set that holds a member twice, naming the members
/// as written. A member that is no string is skipped.
fn string_set_duplicates(members: &[serde_json::Value]) -> Result<(), String> {
    let values: Vec<&str> = members
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    let distinct: BTreeSet<&str> = values.iter().copied().collect();
    if values.len() == distinct.len() {
        return Ok(());
    }
    let repr = values.join(", ");
    Err(format!(
        "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
    ))
}

/// The payload of an `NS`.
fn number_set(value: &serde_json::Value) -> Result<AttributeValue, String> {
    let arr = value
        .as_array()
        .ok_or_else(|| "NS value must be an array".to_owned())?;
    if arr.is_empty() {
        return Err(
            "One or more parameter values were invalid: An number set  may not be empty".to_owned(),
        );
    }
    let set: BTreeSet<String> = arr
        .iter()
        .map(|v| {
            let s = v.as_str().ok_or_else(|| EMPTY_ATTRIBUTE_VALUE.to_owned())?;
            Ok(crate::validation::number::validate_and_normalize_number(s)
                .unwrap_or_else(|_| s.to_owned()))
        })
        .collect::<Result<_, String>>()?;
    if set.len() != arr.len() {
        let values: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
        let repr = values.join(", ");
        return Err(format!(
            "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
        ));
    }
    Ok(AttributeValue::NS(set))
}

/// The payload of a `BS`.
fn binary_set(value: &serde_json::Value) -> Result<AttributeValue, String> {
    let arr = value
        .as_array()
        .ok_or_else(|| "BS value must be an array".to_owned())?;
    if arr.is_empty() {
        return Err(
            "One or more parameter values were invalid: Binary sets should not be empty".to_owned(),
        );
    }
    let set: BTreeSet<Vec<u8>> = arr
        .iter()
        .map(|v| {
            let b64 = v.as_str().ok_or_else(|| EMPTY_ATTRIBUTE_VALUE.to_owned())?;
            BASE64
                .decode(b64)
                .map_err(|e| format!("invalid base64: {e}"))
        })
        .collect::<Result<_, _>>()?;
    if set.len() != arr.len() {
        let values: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
        let repr = values.join(", ");
        // The binary form names the set type, with the service's
        // own missing space before 'of' ("...]of type BS...") --
        // invariant across regions; the S and N set forms do not
        // name a type.
        return Err(format!(
            "One or more parameter values were invalid: Input collection [{repr}]of type BS contains duplicates."
        ));
    }
    Ok(AttributeValue::BS(set))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_roundtrip() {
        let val = AttributeValue::S("hello".into());
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"S":"hello"}"#);
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn number_roundtrip() {
        let val = AttributeValue::N("42".into());
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"N":"42"}"#);
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn binary_roundtrip() {
        let val = AttributeValue::B(vec![1, 2, 3]);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn bool_true_roundtrip() {
        let val = AttributeValue::Bool(true);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"BOOL":true}"#);
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn bool_false_roundtrip() {
        let val = AttributeValue::Bool(false);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"BOOL":false}"#);
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn null_roundtrip() {
        let val = AttributeValue::Null;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#"{"NULL":true}"#);
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn list_roundtrip() {
        let val = AttributeValue::L(vec![
            AttributeValue::S("a".into()),
            AttributeValue::N("1".into()),
        ]);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn map_roundtrip() {
        let mut m = BTreeMap::new();
        m.insert("key".into(), AttributeValue::S("value".into()));
        let val = AttributeValue::M(m);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn string_set_roundtrip() {
        let mut set = BTreeSet::new();
        set.insert("a".into());
        set.insert("b".into());
        let val = AttributeValue::SS(set);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn number_set_roundtrip() {
        let mut set = BTreeSet::new();
        set.insert("1".into());
        set.insert("2".into());
        let val = AttributeValue::NS(set);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn binary_set_roundtrip() {
        let mut set = BTreeSet::new();
        set.insert(vec![1, 2]);
        set.insert(vec![3, 4]);
        let val = AttributeValue::BS(set);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn empty_string_set_rejected() {
        let json = r#"{"SS":[]}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("may not be empty"));
    }

    #[test]
    fn empty_number_set_rejected() {
        let json = r#"{"NS":[]}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("may not be empty"));
    }

    #[test]
    fn empty_binary_set_rejected() {
        let json = r#"{"BS":[]}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("should not be empty"));
    }

    #[test]
    fn null_false_rejected() {
        let json = r#"{"NULL":false}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(
            err.to_string()
                .contains("Null attribute value types must have the value of true")
        );
    }

    #[test]
    fn multiple_type_descriptors_rejected() {
        let json = r#"{"S":"hello","N":"42"}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("more than one datatypes"));
    }

    #[test]
    fn empty_map_rejected() {
        let json = r#"{}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    /// A member that is no type descriptor (a name `DynamoDB` does not
    /// define, or a defined one with a JSON null payload) is ignored, at the
    /// top level and inside `L` and `M`; the descriptors left are counted.
    /// The rows and their answers were measured on 2026-11-23 through Query,
    /// PutItem and GetItem, each twice. Read from a `serde_json::Value`, the
    /// engine's path.
    #[test]
    fn members_that_are_no_type_descriptor_are_ignored() {
        let s = |v: &str| Ok(AttributeValue::S(v.to_owned()));
        let n = |v: &str| Ok(AttributeValue::N(v.to_owned()));
        let empty = Err(EMPTY_ATTRIBUTE_VALUE.to_owned());
        let multi = Err(MULTI_ATTRIBUTE_VALUE.to_owned());
        let cases = [
            (r#"{"S":"a","X":"1"}"#, s("a")),
            (r#"{"X":"1","S":"a"}"#, s("a")),
            (r#"{"S":null,"N":"1"}"#, n("1")),
            (r#"{"N":"1","S":null}"#, n("1")),
            (r#"{"S":"a","N":null,"X":"1"}"#, s("a")),
            (r#"{"S":"a","N":"1"}"#, multi.clone()),
            (r#"{"X":"1"}"#, empty.clone()),
            (r#"{"S":null}"#, empty.clone()),
            (r#"{"S":null,"X":"1"}"#, empty.clone()),
            (r#"{"S":null,"N":null}"#, empty.clone()),
            ("{}", empty.clone()),
            // The number is read here and refused by the validation layer.
            (r#"{"N":"bob","X":"1"}"#, n("bob")),
            (r#"{"NULL":true,"S":null}"#, Ok(AttributeValue::Null)),
            (
                r#"{"BOOL":false,"S":null}"#,
                Ok(AttributeValue::Bool(false)),
            ),
            (
                r#"{"L":[{"S":"a","X":"1"}]}"#,
                s("a").map(|a| AttributeValue::L(vec![a])),
            ),
            (
                r#"{"L":[{"S":null,"N":"1"}]}"#,
                n("1").map(|a| AttributeValue::L(vec![a])),
            ),
            (r#"{"L":[{"X":"1"}]}"#, empty.clone()),
            (r#"{"L":[{"S":"a","N":"1"}]}"#, multi),
            (
                r#"{"M":{"k":{"S":"a","X":"1"}}}"#,
                s("a").map(|a| AttributeValue::M(BTreeMap::from([("k".to_owned(), a)]))),
            ),
            (r#"{"M":{"k":{"S":null}}}"#, empty.clone()),
            (r#"{"M":{"k":{"S":null,"N":null}}}"#, empty),
            (r#"{"L":null,"S":"a"}"#, s("a")),
        ];
        for (json, want) in cases {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            let got = serde_json::from_value::<AttributeValue>(value).map_err(|e| e.to_string());
            assert_eq!(got, want, "{json}");
        }
    }

    /// The member a reader of a value's raw JSON reads the value by is the
    /// one the deserializer reads it by, in either member order: each row
    /// gives the members as a list of pairs, so the order is the row's and
    /// not the JSON map's.
    #[test]
    fn type_descriptor_is_the_one_counted_member_in_either_order() {
        let cases = [
            (r#"[["A","alice"],["S","bob"]]"#, Ok("S")),
            (r#"[["S","bob"],["A","alice"]]"#, Ok("S")),
            (r#"[["N",null],["S","bob"]]"#, Ok("S")),
            (r#"[["S","bob"],["N",null]]"#, Ok("S")),
            (r#"[["A","alice"]]"#, Err(EMPTY_ATTRIBUTE_VALUE)),
            (r#"[["S",null]]"#, Err(EMPTY_ATTRIBUTE_VALUE)),
            ("[]", Err(EMPTY_ATTRIBUTE_VALUE)),
            (r#"[["S","a"],["N","1"]]"#, Err(MULTI_ATTRIBUTE_VALUE)),
            (r#"[["N","1"],["S","a"]]"#, Err(MULTI_ATTRIBUTE_VALUE)),
        ];
        for (json, want) in cases {
            let members: Vec<(String, serde_json::Value)> = serde_json::from_str(json).unwrap();
            let borrowed = members.iter().map(|(name, payload)| (name, payload));
            let got = type_descriptor(borrowed).map(|(name, payload)| {
                assert!(!payload.is_null(), "{json}");
                name.as_str()
            });
            assert_eq!(got, want, "{json}");
        }
    }

    /// A value with two type descriptors is refused for a `B` or `BS` payload
    /// that is not base64 at any depth, as a single `B` is, then for the
    /// first faulty payload in the order `SS`, `N`, `NS`, `BS`, `M`, `L`,
    /// `NULL` (an `N` by the PartiQL number rules), and for the count only
    /// when those payloads pass. An item attribute may hold one map or list
    /// less than a parameter. Measured on 2026-11-23 and 2026-11-24, each
    /// request twice: the number and set rows through UpdateItem, Query and
    /// PutItem, the other payloads through PutItem and UpdateItem, and every
    /// pair of two faulty payloads through PutItem in both member orders (98
    /// values). The rows give the members as a list of pairs and are run in
    /// both orders, and as the `serde_json::Value` object the engine reads.
    #[test]
    fn a_payload_fault_beside_a_second_descriptor_is_refused_before_the_count() {
        use crate::partiql::number::{
            OVERFLOW_MESSAGE, TOO_MANY_DIGITS_MESSAGE, UNCONVERTIBLE_PARAMETER_MESSAGE,
            UNDERFLOW_MESSAGE,
        };
        use crate::partiql::params::{NESTING_MESSAGE, NUMBER_SET_DUPLICATES};
        let unconvertible = |text: &str| format!("{UNCONVERTIBLE_PARAMETER_MESSAGE}: {text}");
        let invalid = |text: &str| format!("One or more parameter values were invalid: {text}");
        let with_s = |n: &str| format!(r#"[["S","x"],["N",{}]]"#, serde_json::json!(n));
        let beside_s = |tag: &str, payload: &str| format!(r#"[["S","x"],["{tag}",{payload}]]"#);
        // `{"S": "x"}` in `levels` lists.
        let lists = |levels: usize| {
            (0..levels).fold(r#"{"S":"x"}"#.to_owned(), |value, _| {
                format!(r#"{{"L":[{value}]}}"#)
            })
        };
        // DynamoDB answers a `SerializationException` with a text of its own
        // here, as it does a single `B` that is not base64 (#1189).
        let base64 = from_descriptor("B", serde_json::json!("!!!"), 0).unwrap_err();
        let mut cases = vec![
            (with_s("1"), MULTI_ATTRIBUTE_VALUE.to_owned()),
            (with_s(""), UNCONVERTIBLE_PARAMETER_MESSAGE.to_owned()),
            (with_s("1e9999999999"), unconvertible("1e9999999999")),
            (with_s("1e-9999999999"), unconvertible("1e-9999999999")),
            (with_s("1e2147483648"), unconvertible("1e2147483648")),
            (with_s("1e2147483647"), UNDERFLOW_MESSAGE.to_owned()),
            (with_s("1e126"), OVERFLOW_MESSAGE.to_owned()),
            (with_s("1e-131"), UNDERFLOW_MESSAGE.to_owned()),
            (
                with_s("123456789012345678901234567890123456789"),
                TOO_MANY_DIGITS_MESSAGE.to_owned(),
            ),
            (with_s(" 1"), unconvertible(" 1")),
            (with_s("1.5e-2147483647"), unconvertible("1.5e-2147483647")),
            (with_s("-0.0"), MULTI_ATTRIBUTE_VALUE.to_owned()),
            (with_s("0x10"), unconvertible("0x10")),
            (beside_s("B", r#""!!!""#), base64.clone()),
            (beside_s("BS", r#"["!!!"]"#), base64.clone()),
            (r#"[["N","bob"],["B","!!!"]]"#.to_owned(), base64.clone()),
            (beside_s("L", r#"[{"B":"!!!"}]"#), base64),
            (
                beside_s("BOOL", r#""yes""#),
                MULTI_ATTRIBUTE_VALUE.to_owned(),
            ),
            (beside_s("NULL", "true"), MULTI_ATTRIBUTE_VALUE.to_owned()),
            (beside_s("BOOL", "true"), MULTI_ATTRIBUTE_VALUE.to_owned()),
            (beside_s("B", r#""YQ==""#), MULTI_ATTRIBUTE_VALUE.to_owned()),
            (beside_s("L", r#"[{"N":"bob"}]"#), unconvertible("bob")),
            (beside_s("M", r#"{"k":{"N":"bob"}}"#), unconvertible("bob")),
            // A payload of the wrong JSON type leaves the others checked.
            (
                r#"[["SS",["a","a"]],["BOOL","yes"]]"#.to_owned(),
                invalid("Input collection [a, a] contains duplicates."),
            ),
            (
                r#"[["S","x"],["NS",["1","1"]],["BOOL","yes"]]"#.to_owned(),
                NUMBER_SET_DUPLICATES.to_owned(),
            ),
            // Not measured: a payload of the wrong JSON type that the order
            // names is not checked either (#1012).
            (
                beside_s("NULL", r#""yes""#),
                MULTI_ATTRIBUTE_VALUE.to_owned(),
            ),
            // Measured 2026-11-24: DynamoDB refuses the number in the set
            // with a `SerializationException` (#1012). The row pins this
            // answer until that text exists.
            (
                r#"[["SS",[1]],["N","bob"]]"#.to_owned(),
                unconvertible("bob"),
            ),
            (
                beside_s("NS", "[null]"),
                invalid("Numeric sets may not contain null"),
            ),
            (
                beside_s("BS", "[null]"),
                invalid("Binary sets may not contain null or empty values"),
            ),
            (
                beside_s("BS", r#"["AQ==","AQ=="]"#),
                invalid("Input collection [AQ==, AQ==]of type BS contains duplicates."),
            ),
            (
                beside_s("L", r#"[{"SS":[]}]"#),
                invalid("An string set  may not be empty"),
            ),
            (beside_s("L", "[{}]"), EMPTY_ATTRIBUTE_VALUE.to_owned()),
            // Refused for the nesting: 33 lists in all, 32, and 32 with the
            // value with two descriptors inside an `L`.
            (
                beside_s("L", &format!("[{}]", lists(32))),
                NESTING_MESSAGE.to_owned(),
            ),
            (
                beside_s("L", &format!("[{}]", lists(31))),
                NESTING_MESSAGE.to_owned(),
            ),
            (
                format!(r#"[["L",[{{"S":"x","L":[{}]}}]]]"#, lists(30)),
                NESTING_MESSAGE.to_owned(),
            ),
            // A fault beside a value with two descriptors in an `L`.
            (
                r#"[["L",[{"SS":[]},{"S":"x","N":"1"}]]]"#.to_owned(),
                invalid("An string set  may not be empty"),
            ),
        ];
        // Each fault beside an `S`, and every pair of faults of two
        // descriptors, refused for the descriptor that comes first in `order`.
        let null_member = "The supplied Item contains a null AttributeValue";
        let faults = [
            (
                "NULL",
                "false",
                invalid("Null attribute value types must have the value of true"),
            ),
            ("SS", "[]", invalid("An string set  may not be empty")),
            (
                "SS",
                "[null]",
                invalid("An string set may not have a null string as a member"),
            ),
            (
                "SS",
                r#"["a","a"]"#,
                invalid("Input collection [a, a] contains duplicates."),
            ),
            ("NS", "[]", invalid("An number set  may not be empty")),
            ("NS", r#"["bob"]"#, unconvertible("bob")),
            (
                "NS",
                r#"["1","1"]"#,
                crate::partiql::params::NUMBER_SET_DUPLICATES.to_owned(),
            ),
            ("BS", "[]", invalid("Binary sets should not be empty")),
            ("L", "[null]", null_member.to_owned()),
            ("M", r#"{"k":null}"#, format!("{null_member}: k is null")),
            ("N", r#""bob""#, unconvertible("bob")),
        ];
        let order = ["SS", "N", "NS", "BS", "M", "L", "NULL"];
        let rank = |tag: &str| order.iter().position(|first| *first == tag);
        for (i, (tag, payload, text)) in faults.iter().enumerate() {
            cases.push((beside_s(tag, payload), text.clone()));
            for (other, other_payload, other_text) in &faults[i + 1..] {
                if other != tag {
                    let want = if rank(tag) < rank(other) {
                        text
                    } else {
                        other_text
                    };
                    let members = format!(r#"[["{tag}",{payload}],["{other}",{other_payload}]]"#);
                    cases.push((members, want.clone()));
                }
            }
        }
        for (json, want) in cases {
            let members: Vec<(String, serde_json::Value)> = serde_json::from_str(&json).unwrap();
            let reversed: Vec<_> = members.iter().rev().cloned().collect();
            let item = |members| from_members(members, 1);
            assert_eq!(item(members.clone()), Err(want.clone()), "{json}");
            assert_eq!(item(reversed), Err(want.clone()), "{json} reversed");
            let object = serde_json::Value::Object(members.into_iter().collect());
            let got = serde_json::from_value::<AttributeValue>(object).map_err(|e| e.to_string());
            assert_eq!(got, Err(want), "{json} as an object");
        }
    }

    #[test]
    fn invalid_number_accepted_at_deserialization() {
        // Invalid numbers are accepted by the deserializer (stored raw)
        // and rejected later by the validation layer as ValidationException.
        let json = r#"{"N":"abc"}"#;
        let val: AttributeValue = serde_json::from_str(json).unwrap();
        assert_eq!(val, AttributeValue::N("abc".to_owned()));

        let json = r#"{"N":"1E999"}"#;
        let val: AttributeValue = serde_json::from_str(json).unwrap();
        assert_eq!(val, AttributeValue::N("1E999".to_owned()));
    }

    #[test]
    fn invalid_number_in_ns_accepted() {
        let json = r#"{"NS":["1","abc"]}"#;
        let val: AttributeValue = serde_json::from_str(json).unwrap();
        match val {
            AttributeValue::NS(set) => assert!(set.contains("abc")),
            _ => panic!("expected NS"),
        }
    }

    #[test]
    fn nested_map_roundtrip() {
        let mut inner = BTreeMap::new();
        inner.insert("nested".into(), AttributeValue::N("99".into()));
        let mut outer = BTreeMap::new();
        outer.insert("inner".into(), AttributeValue::M(inner));
        let val = AttributeValue::M(outer);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn empty_list_roundtrip() {
        let val = AttributeValue::L(vec![]);
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn empty_map_value_roundtrip() {
        let val = AttributeValue::M(BTreeMap::new());
        let json = serde_json::to_string(&val).unwrap();
        let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    // ─── Empty AttributeValue map and multiple-keys (DDB error parity) ─────
    //
    // The AWS Java SDK omits null-valued fields from JSON, so calls like
    // `attrVal.setBOOL(null)` serialize to a literal `{}` AttributeValue.
    // Real DynamoDB rejects these with a `ValidationException` whose
    // message starts with "Supplied AttributeValue is empty". Match that
    // wording so the engine-level error classifier promotes it to
    // `ValidationException` instead of `SerializationException`.

    #[test]
    fn empty_map_rejected_with_ddb_message() {
        let err = serde_json::from_str::<AttributeValue>("{}").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue is empty"),
            "expected DDB EMPTY_ATTRIBUTE_VALUE wording, got: {msg}"
        );
        assert!(
            msg.contains("must contain exactly one of the supported datatypes"),
            "expected DDB suffix, got: {msg}"
        );
    }

    #[test]
    fn multi_key_rejected_with_ddb_message() {
        let err = serde_json::from_str::<AttributeValue>(r#"{"S":"a","N":"1"}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue has more than one datatypes set"),
            "expected DDB MULTI_ATTRIBUTE_VALUE wording, got: {msg}"
        );
        assert!(
            msg.contains("must contain exactly one of the supported datatypes"),
            "expected DDB suffix, got: {msg}"
        );
    }

    #[test]
    fn ss_with_null_element_rejected_with_ddb_message() {
        // Real DynamoDB returns INVALID_PARAMETER_VALUE with a SS-specific
        // suffix (note the "An string set" wording is preserved from the
        // upstream service — it's grammatically awkward but verbatim).
        let err = serde_json::from_str::<AttributeValue>(r#"{"SS":["a",null]}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("One or more parameter values were invalid"),
            "expected INVALID_PARAMETER_VALUE wording, got: {msg}"
        );
        assert!(
            msg.contains("An string set may not have a null string as a member"),
            "expected SS-specific suffix, got: {msg}"
        );
    }

    #[test]
    fn ns_with_null_element_rejected_with_ddb_message() {
        let err = serde_json::from_str::<AttributeValue>(r#"{"NS":["1",null]}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue is empty"),
            "expected DDB EMPTY_ATTRIBUTE_VALUE wording for null NS element, got: {msg}"
        );
    }

    #[test]
    fn bs_with_null_element_rejected_with_ddb_message() {
        let err = serde_json::from_str::<AttributeValue>(r#"{"BS":["QQ==",null]}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue is empty"),
            "expected DDB EMPTY_ATTRIBUTE_VALUE wording for null BS element, got: {msg}"
        );
    }

    #[test]
    fn empty_map_inside_list_rejected_with_ddb_message() {
        // Common path: PutItem with a List whose element is `attrVal.setBOOL(null)`.
        let err = serde_json::from_str::<AttributeValue>(r#"{"L":[{}]}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue is empty"),
            "expected DDB EMPTY_ATTRIBUTE_VALUE wording for empty inside L, got: {msg}"
        );
    }

    /// The text of an error inside an `L` or `M` is what
    /// `serde_json::from_value` gave when each level re-deserialized a clone
    /// of its subtree; the levels now consume the value instead, and these
    /// strings (the API's error text) did not change. Checked from text and
    /// from a `serde_json::Value`, the engine's path.
    #[test]
    fn nested_error_text_is_unchanged() {
        const EXPECTING: &str =
            "expected a DynamoDB AttributeValue map with exactly one type descriptor";
        let cases = [
            (
                r#"{"L":["x"]}"#,
                format!("invalid type: string \"x\", {EXPECTING}"),
                " at line 1 column 11",
            ),
            (
                r#"{"L":[{"L":[{"Q":1}]}]}"#,
                EMPTY_ATTRIBUTE_VALUE.to_owned(),
                " at line 1 column 23",
            ),
            (
                r#"{"M":{"k":{"S":1}}}"#,
                "S value must be a string".to_owned(),
                " at line 1 column 19",
            ),
            (
                r#"{"M":{"k":{"S":"a","N":"1"}}}"#,
                MULTI_ATTRIBUTE_VALUE.to_owned(),
                " at line 1 column 29",
            ),
            (
                r#"{"L":[{"L":[{}]}]}"#,
                EMPTY_ATTRIBUTE_VALUE.to_owned(),
                " at line 1 column 18",
            ),
            (
                r#"{"M":{"a":{"L":[null]}}}"#,
                format!("invalid type: null, {EXPECTING}"),
                " at line 1 column 24",
            ),
        ];
        for (json, message, position) in cases {
            let from_text = serde_json::from_str::<AttributeValue>(json).unwrap_err();
            assert_eq!(
                from_text.to_string(),
                format!("{message}{position}"),
                "{json}"
            );
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            let from_value = serde_json::from_value::<AttributeValue>(value).unwrap_err();
            assert_eq!(from_value.to_string(), message, "{json}");
        }
    }

    #[test]
    fn empty_map_inside_map_rejected_with_ddb_message() {
        // Common path: PutItem with a Map whose value is `attrVal.setM(null)`.
        let err = serde_json::from_str::<AttributeValue>(r#"{"M":{"k":{}}}"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Supplied AttributeValue is empty"),
            "expected DDB EMPTY_ATTRIBUTE_VALUE wording for empty inside M, got: {msg}"
        );
    }
}
