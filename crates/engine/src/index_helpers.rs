// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for secondary index operations in the engine layer.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, DescribeTableInput, IndexInfo, IndexStatus, Item,
    KeySchemaElement, ProjectionType, ScalarAttributeType, TableKeyInfo, VectorIndexDescription,
};

use crate::OperationContext;

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
/// `Scan`. Base-table scans use the long `DynamoDB` error message; index scans
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
/// short `DynamoDB` error message in all cases.
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

/// How Query and Scan must refuse an index name that is not a row in the
/// `indexes` catalog. A vector index never is one, so both handlers land here
/// for vector indexes and for genuinely absent names alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexReadRefusal {
    /// No vector index carries the name either, or one does but the service
    /// reports it as absent (measured 2026-08-20: CREATING before the
    /// backfill starts is indistinguishable from a nonexistent index).
    NotFound,
    /// A vector index mid-backfill. Scan gets the backfilling wording while
    /// Query keeps the ordinary type refusal (measured 2026-08-20).
    Backfilling,
    /// A vector index past its backfill: the operation-specific type refusal.
    NotSupported,
}

/// Classify a vector index (or its absence) for a Query/Scan refusal.
///
/// Mirrors the measured lifecycle: ACTIVE and CREATING-while-backfilling are
/// recognized as vector indexes; every other state reads as index-not-found,
/// matching how `SearchVectors` treats a non-ACTIVE index as absent. The
/// backfilling arm requires CREATING so a stale `Backfilling` member on any
/// other status cannot resurrect the backfilling wording.
pub fn classify_vector_index_read(
    vector_index: Option<&VectorIndexDescription>,
) -> VectorIndexReadRefusal {
    match vector_index {
        Some(vi) if vi.index_status.is_active() => VectorIndexReadRefusal::NotSupported,
        Some(vi) if vi.index_status == IndexStatus::Creating && vi.backfilling == Some(true) => {
            VectorIndexReadRefusal::Backfilling
        }
        _ => VectorIndexReadRefusal::NotFound,
    }
}

/// Resolve an index name that `index_info_by_table_id` reported missing
/// against the table's vector index metadata.
///
/// The `key_info` name pre-filter keeps the dominant case (a mistyped GSI
/// name on a table with no vector indexes) free of the extra round trip; the
/// `describe_table` read runs only when the name matches a known vector
/// index, because the key-info cache carries no lifecycle status and backfill
/// transitions never invalidate it.
///
/// # Errors
/// Propagates storage failures from `describe_table`.
pub async fn classify_unresolved_index_read(
    ctx: &OperationContext,
    key_info: &TableKeyInfo,
    index_name: &str,
) -> Result<VectorIndexReadRefusal, DynamoDbError> {
    if !key_info
        .vector_indexes
        .iter()
        .any(|vi| vi.index_name == index_name)
    {
        return Ok(VectorIndexReadRefusal::NotFound);
    }
    let table = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: key_info.table_name.clone(),
            },
        )
        .await
        .map_err(crate::create_table::storage_err_to_dynamo)?;
    Ok(classify_vector_index_read(
        table
            .vector_indexes
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|vi| vi.index_name == index_name),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        DistanceFunction, IndexStatus, Projection, VectorAttribute, VectorIndexDescription,
    };

    fn description(index_status: IndexStatus, backfilling: Option<bool>) -> VectorIndexDescription {
        VectorIndexDescription {
            index_name: "vidx".to_owned(),
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            dimensions: 4,
            search_schema: None,
            distance_function: DistanceFunction::Cosine,
            index_status,
            backfilling,
            index_size_bytes: 0,
            item_count: 0,
            index_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/t/index/vidx".to_owned(),
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }
    }

    #[test]
    fn absent_index_is_not_found() {
        assert_eq!(
            classify_vector_index_read(None),
            VectorIndexReadRefusal::NotFound
        );
    }

    #[test]
    fn active_index_gets_the_type_refusal() {
        let vi = description(IndexStatus::Active, None);
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotSupported
        );
    }

    #[test]
    fn creating_before_backfill_reads_as_not_found() {
        // Measured 2026-08-20: CREATING with Backfilling=false answers with
        // the index-not-found message, exactly like a nonexistent index.
        let vi = description(IndexStatus::Creating, Some(false));
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotFound
        );
        let vi = description(IndexStatus::Creating, None);
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotFound
        );
    }

    #[test]
    fn creating_while_backfilling_is_backfilling() {
        let vi = description(IndexStatus::Creating, Some(true));
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::Backfilling
        );
    }

    #[test]
    fn stale_backfilling_on_a_non_creating_status_stays_not_found() {
        // The backfilling wording is tied to CREATING; a stale Backfilling
        // member on any other status must not resurrect it.
        let vi = description(IndexStatus::Deleting, Some(true));
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotFound
        );
        let vi = description(IndexStatus::Updating, Some(true));
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotFound
        );
    }

    #[test]
    fn deleting_reads_as_not_found() {
        let vi = description(IndexStatus::Deleting, None);
        assert_eq!(
            classify_vector_index_read(Some(&vi)),
            VectorIndexReadRefusal::NotFound
        );
    }
}
