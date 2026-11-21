// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
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

/// The service's texts for a `B` value or a `BS` member it cannot decode
/// (measured 2026-11-20), with its spelling. See [`decode_base64`].
const BASE64_TEXTS: [&str; 3] = [
    "Buffer is too short for base64 encoding",
    "Base64 encoded length is expected a multiple of 4 bytes but found: ",
    "Invalid last non-pad Base64 character dectected",
];

/// The decoder of a `B` value or a `BS` member outside a PartiQL parameter.
const UNPADDED: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Decode the text of a `B` value or a `BS` member. Measured 2026-11-21: the
/// service reads it with its `=` padding left out or cut short, in an item
/// and in an expression attribute value.
pub fn decode_base64(text: &str) -> Result<Vec<u8>, String> {
    decode_with(&UNPADDED, text)
}

/// Decode the text of a `B` value or a `BS` member of a PartiQL parameter,
/// which must carry its `=` padding (measured 2026-11-21).
pub fn decode_padded_base64(text: &str) -> Result<Vec<u8>, String> {
    decode_with(&BASE64, text)
}

fn decode_with(engine: &GeneralPurpose, text: &str) -> Result<Vec<u8>, String> {
    let [too_short, length, last_character] = BASE64_TEXTS;
    engine.decode(text).map_err(|_| match text.len() {
        1 => too_short.to_owned(),
        2 if text == "==" => String::new(),
        n if n % 4 != 0 => format!("{length}{n}"),
        _ if text
            .trim_end_matches('=')
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/') =>
        {
            last_character.to_owned()
        }
        _ => String::new(),
    })
}

/// Whether `message` is a text of [`decode_base64`], which the service sends
/// as it is, and an empty one as no message.
#[must_use]
pub fn is_base64_refusal(message: &str) -> bool {
    message.is_empty() || BASE64_TEXTS.iter().any(|text| message.starts_with(text))
}

impl<'de> Visitor<'de> for AttributeValueVisitor {
    type Value = AttributeValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a DynamoDB AttributeValue map with exactly one type descriptor")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (key, value): (String, serde_json::Value) = map
            .next_entry()?
            .ok_or_else(|| de::Error::custom(EMPTY_ATTRIBUTE_VALUE))?;

        // REQ-TYPE-001: reject if multiple keys
        if map.next_key::<String>()?.is_some() {
            return Err(de::Error::custom(MULTI_ATTRIBUTE_VALUE));
        }

        from_descriptor(&key, value).map_err(de::Error::custom)
    }
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
/// visited), and an object takes the steps `visit_map` takes; the message is
/// the one `visit_map` produces, rendered once per level as `from_value`
/// rendered it: `serde_json::Error::custom` parses a trailing
/// ` at line N column M` out of the message it is given and drops it when
/// N is 0, so each nesting level strips one such suffix, and
/// [`rendered_per_level`] repeats that here.
fn from_nested(value: serde_json::Value) -> Result<AttributeValue, String> {
    let serde_json::Value::Object(object) = value else {
        return serde_json::from_value(value).map_err(|e| e.to_string());
    };
    let mut entries = object.into_iter();
    match (entries.next(), entries.next()) {
        (None, _) => Err(EMPTY_ATTRIBUTE_VALUE.to_owned()),
        (Some(_), Some(_)) => Err(MULTI_ATTRIBUTE_VALUE.to_owned()),
        (Some((key, value)), None) => from_descriptor(&key, value),
    }
    .map_err(rendered_per_level)
}

/// The text `serde_json::from_value` gave for a message its deserializer
/// raised: the message through `serde_json::Error::custom`, displayed.
fn rendered_per_level(message: String) -> String {
    <serde_json::Error as de::Error>::custom(message).to_string()
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
            Ok(AttributeValue::B(decode_base64(b64)?))
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
        other => Err(format!("unknown AttributeValue type descriptor: {other}")),
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
            decode_base64(b64)
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
mod tests;
