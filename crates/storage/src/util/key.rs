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

/// Merge the attribute definitions supplied by an `UpdateTable` request into the
/// table's persisted set, returning the union.
///
/// `UpdateTable` carries only the attribute definitions the request itself needs,
/// which for a GSI creation is that index's key attributes. The service unions
/// them into the stored set rather than replacing it.
///
/// Replacing rather than merging is issue #259: it drops the base table's own key
/// definitions, [`sk_info`] then finds no definition for the sort key and returns
/// `None`, and keyed reads silently degrade to a partition-only lookup that
/// returns another item under the same partition key.
///
/// Measured against real DynamoDB (us-east-1, 2026-08-13) on a table created with
/// `[pk S, sk S]`:
///
/// - `UpdateTable` supplying only `[f01, f02]` is accepted and the next
///   `DescribeTable` reports all four definitions, so the set is a union.
/// - Supplying the full set, base definitions included, is accepted and does not
///   duplicate them.
/// - Re-declaring `pk` as `N` while the table holds it as `S` is **accepted** and
///   the stored type stays `S`. The service neither applies the new type nor
///   rejects the request, so this keeps the existing definition and ignores the
///   conflicting one rather than inventing an error the service does not return.
///
/// Existing definitions keep their order and are never rewritten; new ones are
/// appended in request order.
#[must_use]
pub fn merge_attribute_definitions(
    existing: &[AttributeDefinition],
    incoming: &[AttributeDefinition],
) -> Vec<AttributeDefinition> {
    let mut merged = existing.to_vec();
    for new_def in incoming {
        let already_defined = merged
            .iter()
            .any(|ad| ad.attribute_name == new_def.attribute_name);
        if !already_defined {
            merged.push(new_def.clone());
        }
    }
    merged
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

    fn ad(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.into(),
            attribute_type: attr_type,
        }
    }

    /// The #259 case: `UpdateTable` supplies only the new index's key attributes,
    /// so the base table's own pk/sk definitions must survive the merge. Replacing
    /// instead is what made `sk_info` return `None` and keyed reads return the
    /// wrong item.
    #[test]
    fn merge_attr_defs_keeps_base_keys_when_request_carries_only_new_attrs() {
        let existing = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::S),
        ];
        let incoming = vec![
            ad("f01", ScalarAttributeType::S),
            ad("f02", ScalarAttributeType::S),
        ];

        let merged = merge_attribute_definitions(&existing, &incoming);

        let names: Vec<&str> = merged.iter().map(|a| a.attribute_name.as_str()).collect();
        assert_eq!(names, vec!["pk", "sk", "f01", "f02"]);
        // The sort key must still be resolvable, which is the property the read
        // path depends on.
        let schema = vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ];
        assert_eq!(
            sk_info(&schema, &merged),
            Some(("sk", ScalarAttributeType::S))
        );
    }

    /// A client may send the full set instead of only the new attributes; the
    /// service accepts that without duplicating anything (measured 2026-08-13).
    #[test]
    fn merge_attr_defs_is_idempotent_for_redeclared_identical_defs() {
        let existing = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::S),
        ];
        let incoming = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::S),
            ad("f01", ScalarAttributeType::S),
        ];

        let merged = merge_attribute_definitions(&existing, &incoming);

        let names: Vec<&str> = merged.iter().map(|a| a.attribute_name.as_str()).collect();
        assert_eq!(names, vec!["pk", "sk", "f01"]);
    }

    /// Real DynamoDB accepts a conflicting redeclaration and keeps the stored
    /// type (measured: `pk` held as `S`, re-declared as `N`, still `S` afterwards).
    /// Applying the new type would silently move the key to a different physical
    /// column.
    #[test]
    fn merge_attr_defs_keeps_the_stored_type_on_a_conflicting_redeclaration() {
        let existing = vec![ad("pk", ScalarAttributeType::S)];
        let incoming = vec![ad("pk", ScalarAttributeType::N)];

        let merged = merge_attribute_definitions(&existing, &incoming);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].attribute_type, ScalarAttributeType::S);
    }

    #[test]
    fn merge_attr_defs_with_no_incoming_defs_is_a_no_op() {
        let existing = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::N),
        ];

        let merged = merge_attribute_definitions(&existing, &[]);

        assert_eq!(merged, existing);
    }

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
