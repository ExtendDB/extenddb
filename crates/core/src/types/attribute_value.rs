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

impl<'de> Visitor<'de> for AttributeValueVisitor {
    type Value = AttributeValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB AttributeValue map with exactly one type descriptor")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (key, value): (String, serde_json::Value) = map
            .next_entry()?
            .ok_or_else(|| {
                de::Error::custom(
                    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
                )
            })?;

        // REQ-TYPE-001: reject if multiple keys
        if map.next_key::<String>()?.is_some() {
            return Err(de::Error::custom(
                "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes",
            ));
        }

        match key.as_str() {
            "S" => {
                let s = value
                    .as_str()
                    .ok_or_else(|| de::Error::custom("S value must be a string"))?;
                Ok(AttributeValue::S(s.to_owned()))
            }
            "N" => {
                let n = value
                    .as_str()
                    .ok_or_else(|| de::Error::custom("N value must be a string"))?;
                // Normalize valid numbers; store raw string for invalid ones so the
                // validation layer can reject them with ValidationException.
                let stored = crate::validation::number::validate_and_normalize_number(n)
                    .unwrap_or_else(|_| n.to_owned());
                Ok(AttributeValue::N(stored))
            }
            "B" => {
                let b64 = value
                    .as_str()
                    .ok_or_else(|| de::Error::custom("B value must be a base64 string"))?;
                let bytes = BASE64
                    .decode(b64)
                    .map_err(|e| de::Error::custom(format!("invalid base64: {e}")))?;
                Ok(AttributeValue::B(bytes))
            }
            "SS" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| de::Error::custom("SS value must be an array"))?;
                if arr.is_empty() {
                    return Err(de::Error::custom(
                        "One or more parameter values were invalid: An string set  may not be empty",
                    ));
                }
                let set: BTreeSet<String> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(std::borrow::ToOwned::to_owned)
                            .ok_or_else(|| {
                                de::Error::custom(
                                    "One or more parameter values were invalid: An string set may not have a null string as a member",
                                )
                            })
                    })
                    .collect::<Result<_, _>>()?;
                let values: Vec<String> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(std::borrow::ToOwned::to_owned))
                    .collect();
                if values.len() != set.len() {
                    let repr = values.join(", ");
                    return Err(de::Error::custom(format!(
                        "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
                    )));
                }
                Ok(AttributeValue::SS(set))
            }
            "NS" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| de::Error::custom("NS value must be an array"))?;
                if arr.is_empty() {
                    return Err(de::Error::custom(
                        "One or more parameter values were invalid: An number set  may not be empty",
                    ));
                }
                let set: BTreeSet<String> = arr
                    .iter()
                    .map(|v| {
                        let s = v
                            .as_str()
                            .ok_or_else(|| {
                                de::Error::custom(
                                    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
                                )
                            })?;
                        Ok(crate::validation::number::validate_and_normalize_number(s)
                            .unwrap_or_else(|_| s.to_owned()))
                    })
                    .collect::<Result<_, _>>()?;
                if set.len() != arr.len() {
                    let values: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                    let repr = values.join(", ");
                    return Err(de::Error::custom(format!(
                        "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
                    )));
                }
                Ok(AttributeValue::NS(set))
            }
            "BS" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| de::Error::custom("BS value must be an array"))?;
                if arr.is_empty() {
                    return Err(de::Error::custom(
                        "One or more parameter values were invalid: Binary sets should not be empty",
                    ));
                }
                let set: BTreeSet<Vec<u8>> = arr
                    .iter()
                    .map(|v| {
                        let b64 = v
                            .as_str()
                            .ok_or_else(|| {
                                de::Error::custom(
                                    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes",
                                )
                            })?;
                        BASE64
                            .decode(b64)
                            .map_err(|e| de::Error::custom(format!("invalid base64: {e}")))
                    })
                    .collect::<Result<_, _>>()?;
                if set.len() != arr.len() {
                    let values: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                    let repr = values.join(", ");
                    return Err(de::Error::custom(format!(
                        "One or more parameter values were invalid: Input collection [{repr}] contains duplicates."
                    )));
                }
                Ok(AttributeValue::BS(set))
            }
            "BOOL" => {
                let b = value
                    .as_bool()
                    .ok_or_else(|| de::Error::custom("BOOL value must be a boolean"))?;
                Ok(AttributeValue::Bool(b))
            }
            "NULL" => {
                let n = value
                    .as_bool()
                    .ok_or_else(|| de::Error::custom("NULL value must be a boolean"))?;
                if !n {
                    return Err(de::Error::custom(
                        "One or more parameter values were invalid: Null attribute value types must have the value of true",
                    ));
                }
                Ok(AttributeValue::Null)
            }
            "L" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| de::Error::custom("L value must be an array"))?;
                let list: Vec<AttributeValue> = arr
                    .iter()
                    .map(|v| serde_json::from_value(v.clone()).map_err(de::Error::custom))
                    .collect::<Result<_, _>>()?;
                Ok(AttributeValue::L(list))
            }
            "M" => {
                let obj = value
                    .as_object()
                    .ok_or_else(|| de::Error::custom("M value must be an object"))?;
                let map: BTreeMap<String, AttributeValue> = obj
                    .iter()
                    .map(|(k, v)| {
                        let av: AttributeValue =
                            serde_json::from_value(v.clone()).map_err(de::Error::custom)?;
                        Ok((k.clone(), av))
                    })
                    .collect::<Result<_, A::Error>>()?;
                Ok(AttributeValue::M(map))
            }
            other => Err(de::Error::custom(format!(
                "unknown AttributeValue type descriptor: {other}"
            ))),
        }
    }
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

    #[test]
    fn unknown_type_descriptor_rejected() {
        let json = r#"{"X":"hello"}"#;
        let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert!(err.to_string().contains("unknown"));
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
