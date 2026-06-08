// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for transforming and parsing partition key (pk) and sort key (sk) values.

use crate::error::StorageError;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::borrow::Cow;

/// Parsed sort key value ready for SQL binding.
pub enum SortKeyValue {
    S(String),
    N(bigdecimal::BigDecimal),
    B(Vec<u8>),
}

/// Build a composite partition key TEXT value from multiple HASH attributes.
///
/// For single-attribute keys, returns the value directly (no encoding).
/// For multi-attribute keys, uses netstring encoding: each part is encoded as
/// `<decimal-length>:<value>,` and concatenated. This is provably collision-free
/// regardless of value content, and compatible with `PostgreSQL` TEXT columns
/// (no null bytes).
pub fn composite_pk_to_text(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<String, StorageError> {
    let hash_elements: Vec<_> = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .collect();
    if hash_elements.len() == 1 {
        let val = item
            .get(&hash_elements[0].attribute_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        return Ok(pk_to_text(val)?.into_owned());
    }
    let mut parts = Vec::with_capacity(hash_elements.len());
    for ks in &hash_elements {
        let val = item.get(&ks.attribute_name).ok_or_else(|| {
            StorageError::Internal(format!(
                "missing partition key attribute {}",
                ks.attribute_name
            ))
        })?;
        parts.push(pk_to_text(val)?.into_owned());
    }
    Ok(encode_netstring_composite(&parts))
}

/// Parse an `AttributeValue` into a typed sort key for SQL binding.
pub fn parse_sk(
    value: &AttributeValue,
    sk_type: ScalarAttributeType,
) -> Result<SortKeyValue, StorageError> {
    match (sk_type, value) {
        (ScalarAttributeType::S, AttributeValue::S(s)) => Ok(SortKeyValue::S(s.clone())),
        (ScalarAttributeType::N, AttributeValue::N(n)) => {
            let d = n
                .parse::<bigdecimal::BigDecimal>()
                .map_err(|e| StorageError::Internal(format!("invalid numeric sort key: {e}")))?;
            Ok(SortKeyValue::N(d))
        }
        (ScalarAttributeType::B, AttributeValue::B(b)) => Ok(SortKeyValue::B(b.clone())),
        _ => Err(StorageError::Internal("sort key type mismatch".to_owned())),
    }
}

/// Extract the partition key value as TEXT for storage.
///
/// Per design doc §5.1: partition keys are always stored as TEXT.
/// S → direct (borrowed), N → string representation (borrowed), B → base64 (owned).
pub fn pk_to_text(value: &AttributeValue) -> Result<Cow<'_, str>, StorageError> {
    match value {
        AttributeValue::S(s) => Ok(Cow::Borrowed(s)),
        AttributeValue::N(n) => Ok(Cow::Borrowed(n)),
        AttributeValue::B(b) => Ok(Cow::Owned(BASE64.encode(b))),
        _ => Err(StorageError::Internal(
            "partition key must be S, N, or B".to_owned(),
        )),
    }
}

/// Determine which sort key column to use based on the attribute type.
#[must_use]
pub fn sk_column(attr_type: ScalarAttributeType) -> &'static str {
    match attr_type {
        ScalarAttributeType::S => "sk_s",
        ScalarAttributeType::N => "sk_n",
        ScalarAttributeType::B => "sk_b",
    }
}

/// Column name for the Nth sort key based on attribute type.
///
/// Uses 1-indexed naming for the column suffix: index 0 → `sk_s` (no number,
/// backward compatible with single-SK tables), index 1 → `sk2_s`, index 2 →
/// `sk3_s`, etc. The offset-by-one is intentional to preserve backward
/// compatibility with existing single-SK data tables.
#[must_use]
pub fn sk_column_n(index: usize, attr_type: ScalarAttributeType) -> String {
    let suffix = match attr_type {
        ScalarAttributeType::S => "s",
        ScalarAttributeType::N => "n",
        ScalarAttributeType::B => "b",
    };
    if index == 0 {
        format!("sk_{suffix}")
    } else {
        format!("sk{}_{suffix}", index + 1)
    }
}

/// Look up the sort key attribute definition from the key schema.
#[must_use]
pub fn sk_info<'a>(
    key_schema: &'a [KeySchemaElement],
    attr_defs: &'a [AttributeDefinition],
) -> Option<(&'a str, ScalarAttributeType)> {
    let sk_element = key_schema.iter().find(|ks| ks.key_type == KeyType::Range)?;
    let attr_def = attr_defs
        .iter()
        .find(|ad| ad.attribute_name == sk_element.attribute_name)?;
    Some((&sk_element.attribute_name, attr_def.attribute_type))
}

/// Encode multiple string parts into a single netstring-encoded composite key.
///
/// Format: `<len>:<value>,<len>:<value>,...` — e.g., `"abc"` + `"de"` → `"3:abc,2:de,"`.
/// This encoding is unambiguous for arbitrary byte content and contains no null bytes.
#[must_use]
pub fn encode_netstring_composite(parts: &[String]) -> String {
    let mut out = String::new();
    for p in parts {
        out.push_str(&p.len().to_string());
        out.push(':');
        out.push_str(p);
        out.push(',');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_netstring_single() {
        let parts = vec!["abc".to_owned()];
        assert_eq!(encode_netstring_composite(&parts), "3:abc,");
    }

    #[test]
    fn encode_netstring_multiple() {
        let parts = vec!["abc".to_owned(), "de".to_owned()];
        assert_eq!(encode_netstring_composite(&parts), "3:abc,2:de,");
    }

    #[test]
    fn encode_netstring_empty_part() {
        let parts = vec!["".to_owned(), "x".to_owned()];
        assert_eq!(encode_netstring_composite(&parts), "0:,1:x,");
    }

    #[test]
    fn encode_netstring_collision_free() {
        // These two inputs must produce different encodings
        let a = vec!["ab".to_owned(), "cd".to_owned()];
        let b = vec!["abc".to_owned(), "d".to_owned()];
        assert_ne!(
            encode_netstring_composite(&a),
            encode_netstring_composite(&b)
        );
    }
}
