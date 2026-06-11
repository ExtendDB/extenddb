// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `GetItem` operation handler.
//!
//! REQ-DATA-002: `GetItem` with `ConsistentRead`.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::apply_projection;
use extenddb_core::types::GetItemInput;
use extenddb_core::types::GetItemOutput;
use extenddb_core::types::item_size_bytes;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `GetItem` request.
///
/// Validates the input, reads the item by primary key, and returns it.
/// A nonexistent item returns an empty response (HTTP 200), not an error.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_get_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: GetItemInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    extenddb_core::validation::validate_get_item(
        &input,
        &ctx.limits,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )?;

    // Reject ExpressionAttributeNames supplied with no ProjectionExpression
    // (legacy AttributesToGet does not count as an expression).
    extenddb_core::expression::validate_expression_param_usage(
        input.expression_attribute_names.as_ref(),
        input
            .projection_expression
            .as_ref()
            .is_some_and(|s| !s.is_empty()),
        None,
        true,
        &[],
    )?;

    let item = ctx
        .storage
        .get_item(&key_info, &input.key)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: full item size pre-projection, rounded up to 4 KB.
    let pre_projection_bytes = item.as_ref().map_or(0, item_size_bytes);
    let strongly_consistent = input.consistent_read == Some(true);
    let rcu = capacity_helpers::read_capacity_units(pre_projection_bytes, strongly_consistent);

    // Apply projection if requested.
    // M4: Mutual exclusivity — real DynamoDB rejects both at once.
    if input.projection_expression.is_some()
        && input
            .attributes_to_get
            .as_ref()
            .is_some_and(|a| !a.is_empty())
    {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"
                .to_owned(),
        ));
    }

    // M2: Use name placeholders to avoid reserved-word collisions when
    // desugaring legacy AttributesToGet into a ProjectionExpression.
    let (effective_projection, extra_proj_names) = if input.projection_expression.is_some() {
        (input.projection_expression.clone(), HashMap::new())
    } else if let Some(attrs) = &input.attributes_to_get {
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

    let effective_proj_names = if extra_proj_names.is_empty() {
        input.expression_attribute_names.clone()
    } else {
        let mut merged = input.expression_attribute_names.clone().unwrap_or_default();
        merged.extend(extra_proj_names);
        Some(merged)
    };

    // Parse and validate the projection once, before the item fetch result
    // matters, so the unused-name check runs whether or not the item exists.
    let projection = match effective_projection {
        Some(ref proj_str) => {
            let parsed = crate::expression_helpers::parse_projection_expr(proj_str, &ctx.limits)?;
            // Reject ExpressionAttributeNames entries the projection never
            // references. Scoped to a user-supplied ProjectionExpression;
            // desugared AttributesToGet uses synthetic placeholders.
            if input.projection_expression.is_some() {
                crate::read_helpers::validate_projection_unused_names(
                    input.expression_attribute_names.as_ref(),
                    &parsed,
                )?;
            }
            Some(parsed)
        }
        None => None,
    };

    let item = match (projection, item) {
        (Some(paths), Some(fetched)) => {
            let maps = build_expression_maps(effective_proj_names.as_ref(), None);
            Some(apply_projection(&fetched, &paths, &maps)?)
        }
        (_, item) => item,
    };

    let output = GetItemOutput {
        item,
        consumed_capacity: capacity_helpers::read_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            rcu,
        ),
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: u64::from(output.item.is_some()),
            returned_bytes: pre_projection_bytes as u64,
            ..Default::default()
        },
    })
}
