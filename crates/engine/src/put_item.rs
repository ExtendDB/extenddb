// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PutItem` operation handler.
//!
//! REQ-DATA-001: `PutItem` with `ReturnValues` (`NONE`, `ALL_OLD`).

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{PutItemInput, PutItemOutput, ReturnValues, item_size_bytes};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::{storage_err_to_dynamo, storage_err_to_dynamo_with_ccf};
use crate::expression_helpers::resolve_condition;
use crate::serialize_output;
use crate::stream_capture;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `PutItem` request.
///
/// Validates the input, writes the item, and returns the old item if requested.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_put_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    // Validate table name from raw body first (takes priority over enum errors)
    if let Some(table_name) = body.get("TableName").and_then(|v| v.as_str()) {
        extenddb_core::validation::validate_table_name(table_name, &ctx.limits)?;
    } else if body.get("TableName").is_some_and(|v| v.is_string()) {
        // empty string — caught by validate_table_name above
    } else if body.get("TableName").is_none() || body.get("TableName").is_some_and(|v| v.is_null())
    {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null".to_owned()
        ));
    }

    // Pre-validate enum fields (report all invalid enums together)
    crate::validate_enum_fields(
        &body,
        &[
            ("ReturnValues", "returnValues", &["NONE", "ALL_OLD"]),
            (
                "ReturnConsumedCapacity",
                "returnConsumedCapacity",
                &["INDEXES", "TOTAL", "NONE"],
            ),
            (
                "ReturnItemCollectionMetrics",
                "returnItemCollectionMetrics",
                &["SIZE", "NONE"],
            ),
        ],
    )?;

    let input: PutItemInput = serde_json::from_value(body).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("parameter values were invalid")
            || msg.contains("may not be empty")
            || msg.contains("contains duplicates")
            || msg.contains("Null attribute value")
            || msg.contains("validation error detected")
            || msg.contains("must not be empty")
            || msg.contains("Syntax error; key")
        {
            DynamoDbError::ValidationException(msg)
        } else {
            DynamoDbError::SerializationException(format!(
                "Start of structure or map found where not expected: {e}"
            ))
        }
    })?;

    // Reject EAV/EAN without an expression
    let has_expression = input
        .condition_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    if !has_expression
        && input
            .expression_attribute_values
            .as_ref()
            .is_some_and(|m| !m.is_empty())
    {
        return Err(DynamoDbError::ValidationException(
            "ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null".to_owned(),
        ));
    }
    if !has_expression
        && input
            .expression_attribute_names
            .as_ref()
            .is_some_and(|m| !m.is_empty())
    {
        return Err(DynamoDbError::ValidationException(
            "ExpressionAttributeNames can only be specified when using expressions: ConditionExpression is null".to_owned(),
        ));
    }

    extenddb_core::validation::validate_table_name(&input.table_name, &ctx.limits)?;

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    extenddb_core::validation::validate_put_item(
        &input,
        &ctx.limits,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )?;

    let (condition, maps) = resolve_condition(
        input.condition_expression.as_deref(),
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
        input.expected.as_ref(),
        input.conditional_operator,
        &ctx.limits,
    )?;

    if input.expected.is_none() || input.expected.as_ref().is_some_and(|m| m.is_empty()) {
        let exprs: Vec<&extenddb_core::expression::Expr> = condition.iter().collect();
        extenddb_core::expression::validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &[],
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
        )?;
    }

    let return_old = input.return_values == ReturnValues::AllOld;

    // Capacity metering: full item size, rounded up to 1 KB.
    let item_bytes = item_size_bytes(&input.item);
    let wcu = capacity_helpers::write_capacity_units(item_bytes);

    // Extract item collection metrics before item is moved into storage.
    let icm = capacity_helpers::item_metrics(
        input.return_item_collection_metrics,
        &key_info.key_schema,
        &input.item,
        key_info.has_lsi,
    );

    // Check if streams are enabled (need old item for stream record).
    let view_type = stream_capture::stream_view_type(&key_info);

    let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
        view_type: vt,
        user_identity: None,
        region: ctx.region.clone(),
    });
    // When streams are enabled, always request old item so the storage layer
    // can determine Insert vs Modify and build old images.
    let need_old_for_stream = stream.is_some();

    let old_item = ctx
        .storage
        .put_item(
            &key_info,
            input.item,
            return_old || need_old_for_stream,
            condition.as_ref(),
            &maps,
            stream.as_ref(),
        )
        .await
        .map_err(|e| {
            storage_err_to_dynamo_with_ccf(e, input.return_values_on_condition_check_failure)
        })?;

    // Stream records are now captured atomically within the storage transaction.

    let output = PutItemOutput {
        attributes: if return_old { old_item } else { None },
        consumed_capacity: capacity_helpers::write_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            wcu,
        ),
        item_collection_metrics: icm,
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            write_capacity_units: wcu,
            ..Default::default()
        },
    })
}
