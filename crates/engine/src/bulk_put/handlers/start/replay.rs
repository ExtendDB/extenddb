// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `ClientToken` replay, answered from the operation the token started
//! before the Start asks the store or the policies anything.

use super::*;

/// The answer to a Start whose `ClientToken` an earlier Start of the account
/// used inside the replay window: that operation's current description when
/// the request fingerprint is equal, the measured mismatch error when it is
/// not (measured on the service: nothing is created and no other check is
/// consulted). `None` for a request with no token or an unseen one.
///
/// The lookup is the start transaction with an account limit of zero, which
/// answers a seen token from its row and admits no insert, as on the
/// saturated-account path below. The transaction at step 7 looks the token
/// up again under its locks, so concurrent Starts with one token still
/// create one operation.
///
/// # Errors
///
/// The storage error of the lookup.
pub(super) async fn replayed(
    ctx: &OperationContext,
    new: &NewBulkOperation,
) -> Result<Option<Value>, DynamoDbError> {
    if new.client_token_hash.is_none() {
        return Ok(None);
    }
    let lookup = NewBulkOperation {
        operation_id: new.operation_id.clone(),
        table_id: new.table_id.clone(),
        table_name: new.table_name.clone(),
        table_arn: new.table_arn.clone(),
        client_token: new.client_token.clone(),
        client_token_hash: new.client_token_hash.clone(),
        request_fingerprint: new.request_fingerprint.clone(),
        input_config: new.input_config.clone(),
        output_config: new.output_config.clone(),
        input_files: Vec::new(),
        max_write_request_units: new.max_write_request_units,
        performance_tier: new.performance_tier.clone(),
        start_time_ms: new.start_time_ms,
        total_input_bytes: None,
        initial_failure: None,
        run_context: None,
    };
    let outcome = ctx
        .storage
        .start_bulk_operation(&ctx.account_id, lookup, 0)
        .await
        .map_err(bulk_storage_err)?;
    match outcome {
        StartOutcome::IdempotentReplay(_) | StartOutcome::IdempotentMismatch => start_response(
            outcome,
            &ctx.region,
            ctx.limits.max_concurrent_bulk_operations,
        )
        .map(Some),
        _ => Ok(None),
    }
}
