// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Scan` operation handler.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, parse_projection, tokenize_with_limit};
use extenddb_core::types::{
    IndexType, ScanInput, ScanOutput, Select, TableKeyInfo, extract_key, item_size_bytes,
};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::{build_expression_maps, parse_optional_filter};
use crate::index_helpers::{combined_lek_key_schema, validate_scan_exclusive_start_key};
use crate::legacy_filter::desugar_filter;
use crate::read_helpers::apply_post_read;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `Scan` request.
///
/// Reads all items from the table (or segment), applies `FilterExpression`
/// post-read, applies `ProjectionExpression`, and enforces the 1 MB limit.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
#[allow(clippy::cast_possible_wrap)] // item counts won't exceed i64::MAX
pub async fn handle_scan(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: ScanInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // P118: Fetch key_info first so we can use table_id for index lookup.
    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // GSI/LSI: resolve index metadata if scanning a secondary index.
    // Uses table_id from pre-fetched key_info to skip redundant table lookup (P118 #4).
    let index_info = if let Some(ref idx_name) = input.index_name {
        Some(
            ctx.storage
                .index_info_by_table_id(&key_info.table_id, idx_name)
                .await
                .map_err(storage_err_to_dynamo)?,
        )
    } else {
        None
    };

    // ConsistentRead is not supported on GSI scans (tenet 1: fidelity).
    if input.consistent_read == Some(true)
        && let Some(ref idx) = index_info
        && idx.index_type == IndexType::Gsi
    {
        return Err(DynamoDbError::ValidationException(
            "Consistent reads are not supported on global secondary indexes".to_owned(),
        ));
    }

    // Validate Segment/TotalSegments — DynamoDB returns different messages per direction
    match (input.segment, input.total_segments) {
        (Some(_), None) => {
            return Err(DynamoDbError::ValidationException(
                "The TotalSegments parameter is required but was not present in the request when Segment parameter is present"
                    .to_owned(),
            ));
        }
        (None, Some(_)) => {
            return Err(DynamoDbError::ValidationException(
                "The Segment parameter is required but was not present in the request when parameter TotalSegments is present"
                    .to_owned(),
            ));
        }
        (Some(seg), Some(total)) => {
            if total < 1 {
                return Err(DynamoDbError::ValidationException(
                    "The parameter TotalSegments should be greater than or equal to 1".to_owned(),
                ));
            }
            if seg < 0 {
                return Err(DynamoDbError::ValidationException(
                    "The parameter Segment should be greater than or equal to 0".to_owned(),
                ));
            }
            if seg >= total {
                return Err(DynamoDbError::ValidationException(format!(
                    "The Segment parameter is zero-based and must be less than parameter TotalSegments: \
                     Segment: {seg} is not less than TotalSegments: {total}"
                )));
            }
        }
        (None, None) => {}
    }

    // Validate Limit >= 1
    if let Some(limit) = input.limit
        && limit < 1
    {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )));
    }

    // For index scans, build a key_info that reflects the index's key schema.
    let scan_key_info = if let Some(ref idx) = index_info {
        TableKeyInfo {
            table_name: key_info.table_name.clone(),
            account_id: key_info.account_id.clone(),
            table_id: key_info.table_id.clone(),
            key_schema: idx.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            has_lsi: key_info.has_lsi,
            stream_specification: None, // Scans don't capture stream records
        }
    } else {
        key_info.clone()
    };

    // --- Legacy vs expression mutual exclusivity checks ---
    let has_fe = input.filter_expression.is_some();
    let has_sf = input.scan_filter.as_ref().is_some_and(|m| !m.is_empty());

    if has_fe && has_sf {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"
                .to_owned(),
        ));
    }

    let has_pe = input.projection_expression.is_some();
    let has_atg = input
        .attributes_to_get
        .as_ref()
        .is_some_and(|a| !a.is_empty());

    if has_pe && has_atg {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"
                .to_owned(),
        ));
    }

    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // Parse FilterExpression or desugar legacy ScanFilter
    let (filter, filter_maps) = if let Some(ref sf) = input.scan_filter {
        if !sf.is_empty() {
            let cond_op = input.conditional_operator.unwrap_or_default();
            let (expr, fmaps) = desugar_filter(sf, cond_op)?;
            (Some(expr), Some(fmaps))
        } else {
            (
                parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?,
                None,
            )
        }
    } else {
        (
            parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?,
            None,
        )
    };

    // Parse ProjectionExpression or desugar legacy AttributesToGet
    let (effective_projection_str, extra_proj_names) = if input.projection_expression.is_some() {
        (input.projection_expression.clone(), HashMap::new())
    } else if let Some(ref attrs) = input.attributes_to_get {
        let mut names_map = HashMap::new();
        let placeholders: Vec<String> = attrs
            .iter()
            .enumerate()
            .map(|(i, attr)| {
                let placeholder = format!("#_ag{i}");
                names_map.insert(placeholder.clone(), attr.clone());
                placeholder
            })
            .collect();
        (Some(placeholders.join(", ")), names_map)
    } else {
        (None, HashMap::new())
    };

    let projection = if let Some(ref proj_str) = effective_projection_str {
        let proj_tokens = tokenize_with_limit(proj_str, ctx.limits.max_expression_tokens)?;
        Some(parse_projection(&proj_tokens)?)
    } else {
        None
    };

    // Validate unused expression attributes
    {
        let exprs: Vec<&extenddb_core::expression::Expr> = filter.iter().collect();
        let mut extra_names = std::collections::HashSet::new();
        if let Some(ref proj) = projection {
            for path in proj {
                for el in path {
                    if let extenddb_core::expression::PathElement::Attribute(name) = el
                        && let Some(ref_name) = name.strip_prefix('#')
                    {
                        extra_names.insert(ref_name.to_owned());
                    }
                }
            }
        }
        extenddb_core::expression::validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &[],
            &extra_names,
            &std::collections::HashSet::new(),
        )?;
    }

    // Validate Select vs ProjectionExpression and index requirements
    if let Some(Select::AllProjectedAttributes) = input.select
        && index_info.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "ALL_PROJECTED_ATTRIBUTES can be used only when scanning an index".to_owned(),
        ));
    }
    if let Some(Select::Count) = input.select
        && effective_projection_str.is_some()
    {
        return Err(DynamoDbError::ValidationException(
            "Cannot specify the ProjectionExpression when Select is COUNT".to_owned(),
        ));
    }

    let index_proj = if matches!(input.select, Some(Select::AllProjectedAttributes)) {
        index_info.as_ref()
    } else {
        None
    };

    // Build the combined expression maps for post-read evaluation.
    let combined_maps = {
        let mut names = maps.names.clone();
        let mut values = maps.values.clone();

        if let Some(ref fm) = filter_maps {
            values.extend(fm.values.clone());
        }
        if !extra_proj_names.is_empty() {
            for (k, v) in &extra_proj_names {
                let stripped = k.strip_prefix('#').unwrap_or(k);
                names.insert(stripped.to_owned(), v.clone());
            }
        }
        ExpressionMaps::new(names, values)
    };

    // Validate ExclusiveStartKey matches the key schema
    if let Some(ref start_key) = input.exclusive_start_key {
        validate_scan_exclusive_start_key(start_key, &key_info, index_info.as_ref())?;
    }

    // Validate begins_with operand types upfront (before any rows are scanned).
    if let Some(ref f) = filter {
        extenddb_core::expression::validate_begins_with_operands(f, &combined_maps).map_err(
            |e| crate::expression_helpers::prefix_expression_error(e, "FilterExpression"),
        )?;
    }

    // Scan storage
    let (raw_items, storage_last_key) = ctx
        .storage
        .scan(
            &scan_key_info,
            input.limit,
            input.exclusive_start_key.as_ref(),
            input.segment,
            input.total_segments,
            input.index_name.as_deref(),
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: RCU based on total pre-projection size of all scanned items.
    let pre_projection_bytes: usize = raw_items.iter().map(item_size_bytes).sum();
    let strongly_consistent = input.consistent_read == Some(true);
    let rcu = capacity_helpers::read_capacity_units(pre_projection_bytes, strongly_consistent);

    // Determine which key schema to use for LastEvaluatedKey extraction.
    let lek_key_schema = combined_lek_key_schema(&key_info.key_schema, index_info.as_ref());

    // For index scans, the storage layer returns a LEK with only the index key
    // attributes. Enrich it with the base table key attributes from the last
    // raw item so the LEK matches DynamoDB's format (all combined keys).
    let enriched_storage_last_key = if storage_last_key.is_some() && index_info.is_some() {
        raw_items
            .last()
            .map(|item| extract_key(item, &lek_key_schema))
    } else {
        storage_last_key
    };

    // Apply FilterExpression, ProjectionExpression, and 1 MB limit
    let result = apply_post_read(
        &raw_items,
        enriched_storage_last_key,
        &filter,
        &projection,
        &combined_maps,
        &lek_key_schema,
        input.select.as_ref(),
        index_proj,
        &key_info.key_schema,
    )?;

    let output = ScanOutput {
        items: result.items,
        count: result.count,
        scanned_count: result.scanned_count,
        last_evaluated_key: result.last_evaluated_key,
        consumed_capacity: capacity_helpers::read_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            rcu,
        ),
    };

    let body = serialize_output(&output)?;
    #[allow(clippy::cast_sign_loss)] // count is non-negative
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: result.count as u64,
            returned_bytes: pre_projection_bytes as u64,
            index_name: input.index_name,
            ..Default::default()
        },
    })
}
