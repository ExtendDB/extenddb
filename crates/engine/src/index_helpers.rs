// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for secondary index operations in the engine layer.

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, IndexInfo, IndexType, Item, KeySchemaElement,
    ProjectionType, ScalarAttributeType, Select, combined_lek_key_schema,
};

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

/// Decide whether a read should return the index's projected view.
///
/// DynamoDB defaults table reads to `ALL_ATTRIBUTES`, but index reads default
/// to `ALL_PROJECTED_ATTRIBUTES`. Explicit `ProjectionExpression` requests are
/// handled separately by the projection evaluator.
pub fn index_projection_for_read<'a>(
    index_info: Option<&'a IndexInfo>,
    select: Option<&Select>,
    has_projection_expression: bool,
) -> Option<&'a IndexInfo> {
    if has_projection_expression {
        return None;
    }
    let index_info = index_info?;
    match select {
        None | Some(Select::AllProjectedAttributes) => Some(index_info),
        Some(Select::AllAttributes | Select::SpecificAttributes | Select::Count) => None,
    }
}

/// Enforce DynamoDB's GSI projection boundary.
///
/// A local secondary index can fetch non-projected attributes from the base
/// table. A global secondary index cannot; it can only return attributes that
/// are actually projected into the index.
pub fn validate_gsi_projection_request(
    index_info: Option<&IndexInfo>,
    select: Option<&Select>,
    projection: &Option<Vec<Vec<PathElement>>>,
    maps: &ExpressionMaps,
    base_key_schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    let Some(index_info) = index_info else {
        return Ok(());
    };
    if index_info.index_type != IndexType::Gsi
        || index_info.projection.projection_type == ProjectionType::All
    {
        return Ok(());
    }

    if matches!(select, Some(Select::AllAttributes)) {
        return Err(non_projected_gsi_attribute_error(
            index_info,
            "ALL_ATTRIBUTES",
        ));
    }

    let Some(projection) = projection else {
        return Ok(());
    };
    for path in projection {
        let Some(attr_name) = projection_root_attribute(path, maps)? else {
            continue;
        };
        if !index_projects_attribute(index_info, base_key_schema, &attr_name) {
            return Err(non_projected_gsi_attribute_error(index_info, &attr_name));
        }
    }

    Ok(())
}

fn projection_root_attribute(
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<Option<String>, DynamoDbError> {
    let Some(PathElement::Attribute(name)) = path.first() else {
        return Ok(None);
    };
    if let Some(name_ref) = name.strip_prefix('#') {
        return maps
            .resolve_name(name_ref)
            .map(|name| Some(name.to_owned()));
    }
    Ok(Some(name.clone()))
}

fn index_projects_attribute(
    index_info: &IndexInfo,
    base_key_schema: &[KeySchemaElement],
    attr_name: &str,
) -> bool {
    base_key_schema
        .iter()
        .chain(index_info.key_schema.iter())
        .any(|key| key.attribute_name == attr_name)
        || index_info
            .projection
            .non_key_attributes
            .as_ref()
            .is_some_and(|attrs| attrs.iter().any(|attr| attr == attr_name))
}

fn non_projected_gsi_attribute_error(index_info: &IndexInfo, attr_name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: Global secondary index {} does not project attribute {attr_name}",
        index_info.index_name
    ))
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

fn attr_value_matches_scalar(value: &AttributeValue, scalar: ScalarAttributeType) -> bool {
    matches!(
        (value, scalar),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use extenddb_core::expression::{ExpressionMaps, PathElement};
    use extenddb_core::types::{
        IndexInfo, IndexType, KeySchemaElement, KeyType, Projection, ProjectionType, Select,
    };

    use super::{index_projection_for_read, validate_gsi_projection_request};

    fn key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }
    }

    fn index(index_type: IndexType, projection: Projection) -> IndexInfo {
        IndexInfo {
            index_name: "by_customer".to_owned(),
            index_id: "idx-1".to_owned(),
            index_type,
            key_schema: vec![key("gsi_pk")],
            projection,
        }
    }

    fn include_projection(attrs: &[&str]) -> Projection {
        Projection {
            projection_type: ProjectionType::Include,
            non_key_attributes: Some(attrs.iter().map(|attr| (*attr).to_owned()).collect()),
        }
    }

    fn keys_only_projection() -> Projection {
        Projection {
            projection_type: ProjectionType::KeysOnly,
            non_key_attributes: None,
        }
    }

    #[test]
    fn index_reads_default_to_projected_attributes() {
        let idx = index(IndexType::Gsi, keys_only_projection());

        assert!(index_projection_for_read(Some(&idx), None, false).is_some());
        assert!(
            index_projection_for_read(Some(&idx), Some(&Select::AllProjectedAttributes), false)
                .is_some()
        );
        assert!(
            index_projection_for_read(Some(&idx), Some(&Select::AllAttributes), false).is_none()
        );
        assert!(index_projection_for_read(Some(&idx), None, true).is_none());
    }

    #[test]
    fn gsi_projection_request_rejects_non_projected_attributes() {
        let idx = index(IndexType::Gsi, keys_only_projection());
        let projection = Some(vec![vec![PathElement::Attribute("secret".to_owned())]]);

        let err = validate_gsi_projection_request(
            Some(&idx),
            None,
            &projection,
            &ExpressionMaps::default(),
            &[key("pk")],
        )
        .expect_err("non-projected GSI attribute should fail");

        assert!(
            err.to_string()
                .contains("does not project attribute secret")
        );
    }

    #[test]
    fn gsi_projection_request_allows_projected_attributes_and_aliases() {
        let idx = index(IndexType::Gsi, include_projection(&["status"]));
        let projection = Some(vec![vec![PathElement::Attribute("#s".to_owned())]]);
        let maps = ExpressionMaps::new(
            HashMap::from([("s".to_owned(), "status".to_owned())]),
            HashMap::new(),
        );

        validate_gsi_projection_request(Some(&idx), None, &projection, &maps, &[key("pk")])
            .expect("included attribute is projected");
    }

    #[test]
    fn lsi_projection_request_can_fetch_non_projected_attributes() {
        let idx = index(IndexType::Lsi, keys_only_projection());
        let projection = Some(vec![vec![PathElement::Attribute("secret".to_owned())]]);

        validate_gsi_projection_request(
            Some(&idx),
            None,
            &projection,
            &ExpressionMaps::default(),
            &[key("pk")],
        )
        .expect("LSI can fetch non-projected attributes from the base table");
    }

    #[test]
    fn gsi_all_attributes_requires_all_projection() {
        let idx = index(IndexType::Gsi, keys_only_projection());

        let err = validate_gsi_projection_request(
            Some(&idx),
            Some(&Select::AllAttributes),
            &None,
            &ExpressionMaps::default(),
            &[key("pk")],
        )
        .expect_err("GSI ALL_ATTRIBUTES cannot fetch from the base table");

        assert!(err.to_string().contains("ALL_ATTRIBUTES"));
    }
}
