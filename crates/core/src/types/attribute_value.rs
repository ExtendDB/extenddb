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

/// The deserializer's text for a value with no type descriptor.
pub const EMPTY_ATTRIBUTE_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
/// The deserializer's text for a value with more than one type descriptor.
pub const MULTI_ATTRIBUTE_VALUE: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

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
        from_members(members).map_err(de::Error::custom)
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
/// member [`type_descriptor`] leaves, read by [`from_descriptor`]. The
/// payload is read after the count.
fn from_members(
    members: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> Result<AttributeValue, String> {
    let (name, payload) = type_descriptor(members)?;
    from_descriptor(&name, payload)
}

/// An element of an `L` or a value of an `M`, converted from the
/// `serde_json::Value` the enclosing level already owns.
///
/// The value is consumed, not cloned and re-deserialized: the earlier
/// `serde_json::from_value(v.clone())` held one more copy of the subtree at
/// every enclosing level, so a value nested 31 deep cost 31 copies of its
/// leaves at once. The result and every error message are those of
/// `serde_json::from_value::<AttributeValue>(value)`: a non-object goes
/// through it unchanged (serde's "invalid type" text; nothing below it is
/// visited), and an object takes the steps `visit_map` takes.
fn from_nested(value: serde_json::Value) -> Result<AttributeValue, String> {
    match value {
        serde_json::Value::Object(members) => from_members(members),
        other => serde_json::from_value(other).map_err(|e| e.to_string()),
    }
}

/// The value of one type descriptor: `key` is the descriptor (`S`, `L`, ...)
/// and `value` its payload. The error is the message; the caller turns it
/// into its deserializer's error.
fn from_descriptor(key: &str, value: serde_json::Value) -> Result<AttributeValue, String> {
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
                arr.into_iter().map(from_nested).collect::<Result<_, _>>()?,
            )),
            _ => Err("L value must be an array".to_owned()),
        },
        "M" => match value {
            Value::Object(obj) => Ok(AttributeValue::M(
                obj.into_iter()
                    .map(|(k, v)| from_nested(v).map(|av| (k, av)))
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
    let values: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(std::borrow::ToOwned::to_owned))
        .collect();
    if values.len() != set.len() {
        let repr = values.join(", ");
        return Err(format!(
            "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
        ));
    }
    Ok(AttributeValue::SS(set))
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
