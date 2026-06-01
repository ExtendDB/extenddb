// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `GetItem` operation handler.
//!
//! REQ-DATA-002: `GetItem` with `ConsistentRead`.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    apply_projection, parse_projection, tokenize_for, validate_no_reserved_words,
};
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

    let item = ctx
        .storage
        .get_item(&key_info, &input.key, input.consistent_read == Some(true))
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

    // Validate projection expression upfront (before item fetch result matters)
    if let Some(ref proj_str) = effective_projection {
        let proj_tokens = tokenize_for(
            proj_str,
            ctx.limits.max_expression_tokens,
            "ProjectionExpression",
        )?;
        if ctx.limits.enforce_reserved_keywords {
            validate_no_reserved_words(&proj_tokens)?;
        }
    }

    let item = match (&effective_projection, item) {
        (Some(proj_str), Some(fetched)) => {
            let proj_tokens = tokenize_for(
                proj_str,
                ctx.limits.max_expression_tokens,
                "ProjectionExpression",
            )?;
            let projection = parse_projection(&proj_tokens)?;
            let maps = build_expression_maps(effective_proj_names.as_ref(), None);
            Some(apply_projection(&fetched, &projection, &maps)?)
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
