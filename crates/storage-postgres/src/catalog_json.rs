// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Serialization for the catalog's `jsonb` columns that carry attribute names:
//! `key_schema`, `attribute_definitions` and index `projection`.
//!
//! DynamoDB accepts U+0000 in an attribute name, including a key attribute
//! name, and `DescribeTable` returns it byte-identical. PostgreSQL `jsonb`
//! rejects the `\u0000` escape (`unsupported Unicode escape sequence`), so a
//! `CreateTable` whose key schema carried the character failed at the catalog
//! insert. Every write of these columns goes through [`to_catalog_json`], which
//! maps the two affected characters to sequences `jsonb` accepts, and every
//! read goes through [`from_catalog_json`], which reverses the mapping:
//!
//! | input  | stored        |
//! |--------|---------------|
//! | U+0000 | U+0001 U+0001 |
//! | U+0001 | U+0001 U+0002 |
//!
//! Every other character is stored as itself, so a name without either
//! character is stored exactly as before. The mapping is the one the item
//! data path uses for `item_data` columns, so one decoder reads both once the
//! two paths share a helper.
//!
//! Decoding is lenient about a U+0001 not followed by U+0001 or U+0002: it is
//! returned as a literal U+0001, which is how a name written before this
//! module existed reads back unchanged. No catalog migration accompanies this
//! module: a row written before it cannot contain U+0000 (the write failed),
//! and a stored name containing U+0001 immediately followed by U+0001 or
//! U+0002 is the one legacy shape this decoder would misread.

use extenddb_storage::error::StorageError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

const ESC: char = '\u{1}';
const NUL_TAIL: char = '\u{1}';
const ESC_TAIL: char = '\u{2}';

/// Serialize `value` for a catalog `jsonb` column that may carry attribute
/// names.
pub(crate) fn to_catalog_json<T: Serialize + ?Sized>(value: &T) -> Result<Value, StorageError> {
    serde_json::to_value(value)
        .map(escape_json)
        .map_err(|e| StorageError::Internal(e.to_string()))
}

/// Deserialize a value written by [`to_catalog_json`].
pub(crate) fn from_catalog_json<T: DeserializeOwned>(v: Value) -> Result<T, StorageError> {
    serde_json::from_value(unescape_json(v)).map_err(|e| StorageError::Internal(e.to_string()))
}

/// Reverse the escape on a stored JSON tree without deserializing it, for a
/// caller that reads fields of the tree directly.
pub(crate) fn unescape_json(v: Value) -> Value {
    map_strings(v, &unescape_str)
}

/// Apply the escape to a JSON tree that is already serialized, for a caller
/// that builds the stored document itself.
pub(crate) fn escape_json(v: Value) -> Value {
    map_strings(v, &escape_str)
}

fn needs_escape(s: &str) -> bool {
    s.bytes().any(|b| b == 0 || b == 1)
}

fn escape_str(s: &str) -> Option<String> {
    if !needs_escape(s) {
        return None;
    }
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\u{0}' => {
                out.push(ESC);
                out.push(NUL_TAIL);
            }
            '\u{1}' => {
                out.push(ESC);
                out.push(ESC_TAIL);
            }
            other => out.push(other),
        }
    }
    Some(out)
}

fn unescape_str(s: &str) -> Option<String> {
    if !s.bytes().any(|b| b == 1) {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == ESC {
            match chars.peek() {
                Some(&NUL_TAIL) => {
                    chars.next();
                    out.push('\u{0}');
                }
                Some(&ESC_TAIL) => {
                    chars.next();
                    out.push('\u{1}');
                }
                _ => out.push('\u{1}'),
            }
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

/// Apply `f` to every object key and string value in the tree. `f` returns
/// `None` when the string is unchanged, so an ordinary tree is rebuilt without
/// copying any string.
fn map_strings(v: Value, f: &dyn Fn(&str) -> Option<String>) -> Value {
    match v {
        Value::String(s) => match f(&s) {
            Some(t) => Value::String(t),
            None => Value::String(s),
        },
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|item| map_strings(item, f)).collect())
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                let key = f(&k).unwrap_or(k);
                out.insert(key, map_strings(val, f));
            }
            Value::Object(out)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    use serde_json::json;

    fn key_schema(name: &str) -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    #[test]
    fn plain_names_are_stored_as_before() {
        let ks = key_schema("pk");
        let stored = to_catalog_json(&ks).unwrap();
        assert_eq!(stored, serde_json::to_value(&ks).unwrap());
        let back: Vec<KeySchemaElement> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ks);
    }

    #[test]
    fn nul_in_key_attribute_name_round_trips_and_is_stored_without_nul() {
        let ks = key_schema("p\u{0}k");
        let stored = to_catalog_json(&ks).unwrap();
        let text = stored.to_string();
        assert!(
            !text.contains("\\u0000"),
            "stored form carries no NUL: {text}"
        );
        assert_eq!(stored[0]["AttributeName"], json!("p\u{1}\u{1}k"));
        let back: Vec<KeySchemaElement> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ks);
    }

    #[test]
    fn u0001_in_attribute_definition_round_trips() {
        let ad = vec![AttributeDefinition {
            attribute_name: "a\u{1}b\u{1}\u{1}c\u{0}".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }];
        let stored = to_catalog_json(&ad).unwrap();
        assert_eq!(
            stored[0]["AttributeName"],
            json!("a\u{1}\u{2}b\u{1}\u{2}\u{1}\u{2}c\u{1}\u{1}")
        );
        let back: Vec<AttributeDefinition> = from_catalog_json(stored).unwrap();
        assert_eq!(back, ad);
    }

    #[test]
    fn legacy_u0001_without_tail_reads_back_unchanged() {
        // A name written before the escape existed: a lone U+0001 is a literal.
        let legacy = json!([{"AttributeName": "a\u{1}z", "KeyType": "HASH"}]);
        let back: Vec<KeySchemaElement> = from_catalog_json(legacy).unwrap();
        assert_eq!(back[0].attribute_name, "a\u{1}z");
    }

    #[test]
    fn object_keys_and_nested_values_are_escaped() {
        let v = json!({"k\u{0}": {"NonKeyAttributes": ["n\u{0}", "plain"]}, "n": 1});
        let stored = escape_json(v.clone());
        assert_eq!(
            stored,
            json!({"k\u{1}\u{1}": {"NonKeyAttributes": ["n\u{1}\u{1}", "plain"]}, "n": 1})
        );
        assert_eq!(unescape_json(stored), v);
    }

    #[test]
    fn exhaustive_round_trip_over_the_affected_alphabet() {
        let alphabet = ['\u{0}', '\u{1}', '\u{2}', 'a'];
        let mut frontier = vec![String::new()];
        for _ in 0..5 {
            let mut next = Vec::new();
            for s in &frontier {
                for ch in alphabet {
                    let mut t = s.clone();
                    t.push(ch);
                    next.push(t);
                }
            }
            for s in &next {
                let stored = escape_str(s).unwrap_or_else(|| s.clone());
                assert!(!stored.contains('\u{0}'), "{s:?}");
                let back = unescape_str(&stored).unwrap_or_else(|| stored.clone());
                assert_eq!(&back, s, "round trip of {s:?} via {stored:?}");
            }
            frontier = next;
        }
    }
}
