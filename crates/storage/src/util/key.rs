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
use std::collections::BTreeSet;

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

/// The attribute definitions a table should hold after an `UpdateTable`.
///
/// DynamoDB treats the request's `AttributeDefinitions` as neither a replacement
/// for the stored set nor a pure addition to it. The effective set is the stored
/// definitions merged with the request's (see [`merge_attribute_definitions`]),
/// then pruned to the attributes still referenced by the table key schema or by
/// an index that survives the update.
///
/// Measured against real DynamoDB on top of the merge behaviour documented above:
///
/// - An unused definition supplied alongside a GSI add is dropped, so the next
///   `DescribeTable` does not report it.
/// - Deleting a GSI drops the definitions only that index referenced, while every
///   definition another index or the table key still uses survives.
///
/// `surviving_index_key_schemas` must be the key schemas of the indexes that exist
/// *after* the update is applied: an index being created is included, an index
/// being deleted is not. Callers read this from their own catalog after applying
/// the index changes, which is what makes a deletion prune.
///
/// Pruning can never remove a definition the table key schema names, so a table
/// cannot lose its own pk/sk definitions through this path (issue #259). Merge
/// order is preserved: stored definitions keep their order, surviving new ones are
/// appended in request order.
#[must_use]
pub fn effective_attribute_definitions(
    stored: &[AttributeDefinition],
    requested: &[AttributeDefinition],
    table_key_schema: &[KeySchemaElement],
    surviving_index_key_schemas: &[Vec<KeySchemaElement>],
) -> Vec<AttributeDefinition> {
    let mut referenced: BTreeSet<&str> = table_key_schema
        .iter()
        .map(|ks| ks.attribute_name.as_str())
        .collect();
    for ks in surviving_index_key_schemas {
        referenced.extend(ks.iter().map(|k| k.attribute_name.as_str()));
    }
    merge_attribute_definitions(stored, requested)
        .into_iter()
        .filter(|def| referenced.contains(def.attribute_name.as_str()))
        .collect()
}

/// Recover sort key attribute definitions that were lost from a table's stored
/// metadata, using the data table's PRIMARY KEY columns as the source of truth.
///
/// A table whose base key schema names a RANGE attribute with no matching
/// attribute definition cannot be read correctly: [`sk_info`] returns `None` and
/// keyed reads degrade to a partition-only lookup. Issue #259 produced exactly
/// that state, by having `UpdateTable` replace the stored attribute definitions
/// with the request's subset. The write path is fixed by
/// [`merge_attribute_definitions`]; this recovers tables already damaged by it.
///
/// The lost information is the attribute's scalar type. `primary_key_columns`
/// must be the data table's PRIMARY KEY column names, because that, and only that,
/// records the type: every table is created with all three typed columns for each
/// sort key position (`sk_s`, `sk_n`, `sk_b`) and only the one matching the
/// declared type joins the PRIMARY KEY. Passing the table's full column list here
/// would match `sk_s` on every table and silently mistype every numeric and binary
/// sort key, which would put the table straight back into the wrong-answer state
/// this is meant to repair.
///
/// Returns the definitions to add, in key schema order. Attributes that already
/// have a definition are left untouched, and a RANGE attribute with no matching
/// PRIMARY KEY column is skipped rather than guessed: the caller reports it so the
/// table can be looked at by hand instead of being silently given a wrong type.
///
/// Only sort keys are recovered. A missing HASH definition is not recoverable this
/// way, because the partition key column is always `TEXT` regardless of the
/// declared type, and it is not needed for correctness: partition key values are
/// encoded by [`composite_pk_to_text`], which takes only the key schema.
#[must_use]
pub fn recover_sort_key_definitions(
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    primary_key_columns: &[String],
) -> Vec<AttributeDefinition> {
    let mut recovered = Vec::new();
    let range_elements: Vec<&KeySchemaElement> = base_key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .collect();

    for (position, element) in range_elements.iter().enumerate() {
        if attr_defs
            .iter()
            .any(|ad| ad.attribute_name == element.attribute_name)
        {
            continue;
        }
        let attr_type = [
            ScalarAttributeType::S,
            ScalarAttributeType::N,
            ScalarAttributeType::B,
        ]
        .into_iter()
        .find(|candidate| {
            let expected = sk_column_n(position, *candidate);
            primary_key_columns.iter().any(|c| c == &expected)
        });
        if let Some(attr_type) = attr_type {
            recovered.push(AttributeDefinition {
                attribute_name: element.attribute_name.clone(),
                attribute_type: attr_type,
            });
        }
    }
    recovered
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

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.into(),
            key_type,
        }
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    /// Every data table is created with all three typed columns for each sort key
    /// position, so column existence says nothing about the type; only PRIMARY KEY
    /// membership does. This is the test that discriminates: an implementation
    /// probing existence would answer `S` here, silently mistyping the key and
    /// putting the table back into the wrong-answer state of #259.
    #[test]
    fn recover_sort_key_defs_uses_primary_key_membership_not_column_existence() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];

        // The real table has sk_s, sk_n and sk_b; only sk_n is in the PRIMARY KEY.
        let recovered = recover_sort_key_definitions(&schema, &[], &cols(&["pk", "sk_n"]));

        assert_eq!(
            recovered,
            vec![ad("sk", ScalarAttributeType::N)],
            "the type must come from the PRIMARY KEY column, not from which typed \
             column happens to exist"
        );
    }

    #[test]
    fn recover_sort_key_defs_recovers_each_scalar_type_from_the_key_column() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];

        for (col, expected) in [
            ("sk_s", ScalarAttributeType::S),
            ("sk_n", ScalarAttributeType::N),
            ("sk_b", ScalarAttributeType::B),
        ] {
            assert_eq!(
                recover_sort_key_definitions(&schema, &[], &cols(&["pk", col])),
                vec![ad("sk", expected)],
                "PRIMARY KEY column {col} must recover {expected:?}"
            );
        }
    }

    /// Multiple sort keys use the 1-indexed column suffix, so each position must be
    /// matched against its own column rather than the first one found.
    #[test]
    fn recover_sort_key_defs_handles_multiple_sort_keys_by_position() {
        let schema = vec![
            ks("pk", KeyType::Hash),
            ks("sk", KeyType::Range),
            ks("sk2", KeyType::Range),
        ];

        let recovered = recover_sort_key_definitions(&schema, &[], &cols(&["pk", "sk_s", "sk2_n"]));

        assert_eq!(
            recovered,
            vec![
                ad("sk", ScalarAttributeType::S),
                ad("sk2", ScalarAttributeType::N)
            ]
        );
    }

    #[test]
    fn recover_sort_key_defs_leaves_intact_metadata_alone() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let defs = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::S),
        ];

        assert!(recover_sort_key_definitions(&schema, &defs, &cols(&["pk", "sk_s"])).is_empty());
    }

    #[test]
    fn recover_sort_key_defs_ignores_a_hash_only_table() {
        let schema = vec![ks("pk", KeyType::Hash)];

        assert!(recover_sort_key_definitions(&schema, &[], &cols(&["pk"])).is_empty());
    }

    /// No sort key column in the PRIMARY KEY means no evidence. Skipping keeps the
    /// loud failure in place instead of writing a guessed type that would resolve
    /// the key to the wrong physical column.
    #[test]
    fn recover_sort_key_defs_skips_a_range_key_with_no_primary_key_column() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];

        assert!(
            recover_sort_key_definitions(&schema, &[], &cols(&["pk"])).is_empty(),
            "a type must never be invented when no sort key column is in the PRIMARY KEY"
        );
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

    /// An unused definition supplied alongside a GSI add is dropped: DynamoDB
    /// stores only what a key or a live index references.
    #[test]
    fn effective_attr_defs_drops_a_definition_no_key_or_index_uses() {
        let stored = vec![ad("pk", ScalarAttributeType::S)];
        let requested = vec![
            ad("g1", ScalarAttributeType::S),
            ad("extraUnused", ScalarAttributeType::S),
        ];
        let table_ks = vec![ks("pk", KeyType::Hash)];
        let surviving = vec![vec![ks("g1", KeyType::Hash)]];

        let effective = effective_attribute_definitions(&stored, &requested, &table_ks, &surviving);

        let names: Vec<&str> = effective
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        assert_eq!(names, vec!["pk", "g1"]);
    }

    /// Removing an index drops the definitions only that index referenced, and
    /// keeps the ones another surviving index still uses.
    #[test]
    fn effective_attr_defs_prunes_only_the_removed_index_attributes() {
        let stored = vec![
            ad("pk", ScalarAttributeType::S),
            ad("goneAttr", ScalarAttributeType::S),
            ad("keepAttr", ScalarAttributeType::S),
        ];
        let table_ks = vec![ks("pk", KeyType::Hash)];
        // Only the index on keepAttr survives; the one on goneAttr was deleted.
        let surviving = vec![vec![ks("keepAttr", KeyType::Hash)]];

        let effective = effective_attribute_definitions(&stored, &[], &table_ks, &surviving);

        let names: Vec<&str> = effective
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        assert_eq!(names, vec!["pk", "keepAttr"]);
    }

    /// The table's own key definitions can never be pruned, whatever the indexes
    /// do. This is the #259 property: losing pk/sk makes keyed reads degrade to a
    /// partition-only lookup.
    #[test]
    fn effective_attr_defs_never_prunes_the_table_key() {
        let stored = vec![
            ad("pk", ScalarAttributeType::S),
            ad("sk", ScalarAttributeType::N),
        ];
        let table_ks = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];

        // No indexes at all: both table key definitions must still survive.
        let effective = effective_attribute_definitions(&stored, &[], &table_ks, &[]);

        let names: Vec<&str> = effective
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        assert_eq!(names, vec!["pk", "sk"]);
        assert!(sk_info(&table_ks, &effective).is_some());
    }

    /// A conflicting redeclaration keeps the STORED type, so a live index key
    /// cannot be silently retyped. Inherited from `merge_attribute_definitions`
    /// and asserted here because pruning must not disturb it.
    #[test]
    fn effective_attr_defs_keeps_the_stored_type_on_a_conflicting_redeclaration() {
        let stored = vec![ad("pk", ScalarAttributeType::S)];
        let requested = vec![ad("pk", ScalarAttributeType::N)];
        let table_ks = vec![ks("pk", KeyType::Hash)];

        let effective = effective_attribute_definitions(&stored, &requested, &table_ks, &[]);

        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].attribute_type, ScalarAttributeType::S);
    }

    /// Sequential adds accumulate: the stored set is the base of the merge, so an
    /// earlier index's attribute is not lost when a later index is added.
    #[test]
    fn effective_attr_defs_accumulate_across_sequential_adds() {
        let stored = vec![
            ad("pk", ScalarAttributeType::S),
            ad("g1", ScalarAttributeType::S),
        ];
        let requested = vec![ad("g2", ScalarAttributeType::S)];
        let table_ks = vec![ks("pk", KeyType::Hash)];
        let surviving = vec![vec![ks("g1", KeyType::Hash)], vec![ks("g2", KeyType::Hash)]];

        let effective = effective_attribute_definitions(&stored, &requested, &table_ks, &surviving);

        let names: Vec<&str> = effective
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        assert_eq!(names, vec!["pk", "g1", "g2"]);
    }

    /// An index sort key is referenced just like its hash key.
    #[test]
    fn effective_attr_defs_keeps_index_sort_key_attributes() {
        let stored = vec![
            ad("pk", ScalarAttributeType::S),
            ad("gh", ScalarAttributeType::S),
            ad("gr", ScalarAttributeType::N),
        ];
        let table_ks = vec![ks("pk", KeyType::Hash)];
        let surviving = vec![vec![ks("gh", KeyType::Hash), ks("gr", KeyType::Range)]];

        let effective = effective_attribute_definitions(&stored, &[], &table_ks, &surviving);

        let names: Vec<&str> = effective
            .iter()
            .map(|a| a.attribute_name.as_str())
            .collect();
        assert_eq!(names, vec!["pk", "gh", "gr"]);
    }
}
