// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The describe, list, and cancel handlers. The start handler and its
//! response builder live in the parent module; these three share the
//! operation ARN validation and the record renderers.

use super::*;

/// Handle a `DescribeBulkOperation` request.
///
/// # Errors
///
/// Returns the three-layer ARN errors, or `InternalServerError` for a
/// stored row that does not render.
pub async fn handle_describe_bulk_operation(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    require_bulk_operation_arn(&body)?;
    let input: DescribeBulkOperationInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;
    // The account and region comparisons run inside the read, ahead of the
    // operation id check: a foreign account is denied (measured; the
    // server's authorization layer answers it first with the full denial
    // format, so that branch serves dev mode) and a foreign region is not
    // found (measured with a real operation's ARN respelled to another
    // region). Both are decided without consulting storage, so neither
    // depends on whether the foreign operation exists.
    let parsed = arn::own_arn(&input.bulk_operation_arn, &ctx.region, &ctx.account_id)?;
    let record = ctx
        .storage
        .get_bulk_operation(&ctx.account_id, &parsed.operation_id)
        .await
        .map_err(bulk_storage_err)?
        .ok_or_else(|| does_not_exist(&input.bulk_operation_arn))?;
    serialize_output(&DescribeBulkOperationOutput {
        bulk_operation_description: describe(&record, &ctx.region)?,
    })
}

/// Handle a `ListBulkOperations` request.
///
/// # Errors
///
/// Returns `ValidationException` for an out-of-range `MaxResults` or a
/// `NextToken` that does not decode.
pub async fn handle_list_bulk_operations(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: ListBulkOperationsInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;
    let limit = input.max_results.unwrap_or(MAX_RESULTS_DEFAULT);
    if limit < 1 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{limit}' at 'maxResults' failed to satisfy \
             constraint: Member must have value greater than or equal to 1"
        )));
    }
    if limit > MAX_RESULTS_CEILING {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{limit}' at 'maxResults' failed to satisfy \
             constraint: Member must have value less than or equal to {MAX_RESULTS_CEILING}"
        )));
    }
    let after = input
        .next_token
        .as_deref()
        .map(decode_next_token)
        .transpose()?;
    // The store fetches one row past the page and returns the cursor of the
    // last emitted row when more exist; the token carries no trust because
    // the query is keyed by the caller's account.
    let (records, cursor) = ctx
        .storage
        .list_bulk_operations(
            &ctx.account_id,
            u32::try_from(limit).unwrap_or(u32::MAX),
            after,
        )
        .await
        .map_err(bulk_storage_err)?;
    let summaries = records
        .iter()
        .map(|record| summary(record, &ctx.region))
        .collect::<Result<Vec<_>, _>>()?;
    serialize_output(&ListBulkOperationsOutput {
        bulk_operations_summary_list: summaries,
        next_token: cursor
            .map(|cursor| encode_next_token(cursor.start_time_ms, &cursor.operation_id)),
    })
}

/// Handle a `CancelBulkOperation` request.
///
/// The transition is one conditional update in storage and the answer
/// comes from the row it returns, so two concurrent cancels and a cancel
/// racing the terminal transition never produce a wrong answer. The ARN
/// read is the same as Describe's: a foreign account is denied and a
/// foreign region is not found, both before the operation id is checked
/// and both without consulting storage.
///
/// # Errors
///
/// Returns the three-layer ARN errors, or `ValidationException` naming the
/// status for an operation already terminal.
pub async fn handle_cancel_bulk_operation(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    require_bulk_operation_arn(&body)?;
    let input: CancelBulkOperationInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;
    let parsed = arn::own_arn(&input.bulk_operation_arn, &ctx.region, &ctx.account_id)?;
    let outcome = ctx
        .storage
        .request_cancel(&ctx.account_id, &parsed.operation_id)
        .await
        .map_err(bulk_storage_err)?;
    match outcome {
        CancelOutcome::Cancelling => {
            // The transition log carries the fields of the terminal log
            // line. The conditional update returns only the outcome, so
            // one read after it fills in the table name and the counters;
            // the status is logged as the update wrote it, because the
            // runner can move the row on before the read answers. A read
            // that misses still logs the transition with the fields at
            // hand: the answer to the caller is already decided.
            if let Ok(Some(row)) = ctx
                .storage
                .get_bulk_operation(&ctx.account_id, &parsed.operation_id)
                .await
            {
                tracing::info!(
                    account_id = %row.account_id,
                    operation_id = %row.operation_id,
                    table_name = %row.table_name,
                    status = "CANCELLING",
                    processed_item_count = row.processed_item_count,
                    failed_item_count = row.failed_item_count,
                    "bulk operation cancel requested"
                );
            } else {
                tracing::info!(
                    account_id = %ctx.account_id,
                    operation_id = %parsed.operation_id,
                    status = "CANCELLING",
                    "bulk operation cancel requested"
                );
            }
            serialize_output(&CancelBulkOperationOutput {
                bulk_operation_arn: arn::format_arn(
                    &ctx.region,
                    &ctx.account_id,
                    &parsed.operation_id,
                ),
                bulk_operation_status: BulkOperationStatus::Cancelling,
            })
        }
        CancelOutcome::AlreadyTerminal(status) => Err(DynamoDbError::ValidationException(format!(
            "Invalid Request: Operation already in terminal state: {status}"
        ))),
        CancelOutcome::NotFound => Err(does_not_exist(&input.bulk_operation_arn)),
    }
}
