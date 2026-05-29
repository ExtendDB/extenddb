// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for secondary index operations in the engine layer.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, IndexInfo, Item, KeySchemaElement, ProjectionType,
    ScalarAttributeType,
};

/// Build the combined key schema for `LastEvaluatedKey` extraction.
///
/// For index queries/scans, the LEK includes both the base table key attributes
/// and the index key attributes (deduplicated), matching real `DynamoDB` behavior.
pub fn combined_lek_key_schema(
    base_key_schema: &[KeySchemaElement],
    index_info: Option<&IndexInfo>,
) -> Vec<KeySchemaElement> {
    let Some(idx) = index_info else {
        return base_key_schema.to_vec();
    };
    let mut combined = base_key_schema.to_vec();
    for ks in &idx.key_schema {
        if !combined
            .iter()
            .any(|k| k.attribute_name == ks.attribute_name)
        {
            combined.push(ks.clone());
        }
    }
    combined
}

/// Filter an item to only the attributes projected into a secondary index.
///
/// For `ProjectionType::All`, returns the item unchanged.
/// For `KeysOnly`, retains only the base table and index key attributes.
/// For `Include`, retains keys plus the explicitly included `NonKeyAttributes`.
pub fn apply_index_projection(
    item: &Item,
    index_info: &IndexInfo,
    base_key_schema: &[KeySchemaElement],
) -> Item {
    match index_info.projection.projection_type {
        ProjectionType::All => item.clone(),
        ProjectionType::KeysOnly | ProjectionType::Include => {
            let mut allowed: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
            for ks in base_key_schema {
                allowed.insert(&ks.attribute_name);
            }
            for ks in &index_info.key_schema {
                allowed.insert(&ks.attribute_name);
            }
            if let Some(ref non_key) = index_info.projection.non_key_attributes {
                for attr in non_key {
                    allowed.insert(attr);
                }
            }
            item.iter()
                .filter(|(k, _)| allowed.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }
}

/// Validate `ExclusiveStartKey` against the (table or index) key schema for
/// `Scan`. Base-table scans use the long DynamoDB error message; index scans
/// use the short one.
///
/// # Errors
///
/// Returns `ValidationException` if the start key has missing keys, extras,
/// or scalar type mismatches.
pub fn validate_scan_exclusive_start_key(
    start_key: &Item,
    key_info: &extenddb_core::types::TableKeyInfo,
    index_info: Option<&IndexInfo>,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let required = combined_lek_key_schema(&key_info.key_schema, index_info);
    let message = scan_invalid_start_key_message(index_info);
    check_exclusive_start_key(
        start_key,
        &required,
        &key_info.attribute_definitions,
        message,
    )
}

/// Validate `ExclusiveStartKey` for `Query`. Same rules as Scan; uses the
/// short DynamoDB error message in all cases.
///
/// # Errors
///
/// Returns `ValidationException` if the start key has missing keys, extras,
/// or scalar type mismatches.
pub fn validate_query_exclusive_start_key(
    start_key: &Item,
    key_info: &extenddb_core::types::TableKeyInfo,
    index_info: Option<&IndexInfo>,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let required = combined_lek_key_schema(&key_info.key_schema, index_info);
    check_exclusive_start_key(
        start_key,
        &required,
        &key_info.attribute_definitions,
        QUERY_INVALID_START_KEY_MSG,
    )
}

const QUERY_INVALID_START_KEY_MSG: &str = "The provided starting key is invalid";
const SCAN_INVALID_START_KEY_MSG_BASE: &str = "The provided starting key is invalid: \
     The provided key element does not match the schema";
const SCAN_INVALID_START_KEY_MSG_INDEX: &str = "The provided starting key is invalid";

fn scan_invalid_start_key_message(index_info: Option<&IndexInfo>) -> &'static str {
    if index_info.is_some() {
        SCAN_INVALID_START_KEY_MSG_INDEX
    } else {
        SCAN_INVALID_START_KEY_MSG_BASE
    }
}

/// Three rules: required-keys-present, no-extras, scalar-type-match.
fn check_exclusive_start_key(
    start_key: &Item,
    required: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
    error_message: &str,
) -> Result<(), extenddb_core::error::DynamoDbError> {
    let invalid =
        || extenddb_core::error::DynamoDbError::ValidationException(error_message.to_owned());

    for ks in required {
        if !start_key.contains_key(&ks.attribute_name) {
            return Err(invalid());
        }
    }
    if start_key.len() != required.len() {
        return Err(invalid());
    }
    for ks in required {
        let declared = attribute_definitions
            .iter()
            .find(|ad| ad.attribute_name == ks.attribute_name)
            .ok_or_else(invalid)?;
        let supplied = start_key.get(&ks.attribute_name).ok_or_else(invalid)?;
        if !attr_value_matches_scalar(supplied, declared.attribute_type) {
            return Err(invalid());
        }
    }

    Ok(())
}

/// Whether an `AttributeValue` matches the declared scalar type (S / N / B).
fn attr_value_matches_scalar(value: &AttributeValue, scalar: ScalarAttributeType) -> bool {
    matches!(
        (value, scalar),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}
