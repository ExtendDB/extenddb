// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DeleteItem` operation handler.

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{DeleteItemInput, DeleteItemOutput, ReturnValues, item_size_bytes};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::{storage_err_to_dynamo, storage_err_to_dynamo_with_ccf};
use crate::expression_helpers::resolve_condition;
use crate::serialize_output;
use crate::stream_capture;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `DeleteItem` request.
///
/// Validates the input, deletes the item, and returns the old item if requested.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_delete_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    crate::validate_enum_fields(
        &body,
        &[
            ("ReturnValues", "returnValues", &["NONE", "ALL_OLD"]),
            (
                "ReturnConsumedCapacity",
                "returnConsumedCapacity",
                &["INDEXES", "TOTAL", "NONE"],
            ),
        ],
    )?;
    let input: DeleteItemInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    extenddb_core::validation::validate_table_name(&input.table_name, &ctx.limits)?;

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    extenddb_core::validation::validate_delete_item(
        &input,
        &ctx.limits,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )?;

    let has_condition = input
        .condition_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    extenddb_core::expression::validate_expression_param_usage(
        input.expression_attribute_names.as_ref(),
        has_condition,
        input.expression_attribute_values.as_ref(),
        has_condition,
        &["ConditionExpression"],
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

    let view_type = stream_capture::stream_view_type(&key_info);
    let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
        view_type: vt,
        user_identity: None,
        region: ctx.region.clone(),
    });
    let need_old_for_stream = stream.is_some();

    let old_item = ctx
        .storage
        .delete_item(
            &key_info,
            &input.key,
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

    // Capacity metering: WCU based on deleted item size (or key size if item didn't exist).
    let wcu = capacity_helpers::write_capacity_units(
        old_item
            .as_ref()
            .map_or_else(|| item_size_bytes(&input.key), item_size_bytes),
    );

    let output = DeleteItemOutput {
        attributes: if return_old { old_item } else { None },
        consumed_capacity: capacity_helpers::write_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            wcu,
        ),
        item_collection_metrics: capacity_helpers::item_metrics(
            input.return_item_collection_metrics,
            &key_info.key_schema,
            &input.key,
            key_info.has_lsi,
        ),
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
