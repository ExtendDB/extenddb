// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `ExecuteTransaction` executor.
//!
//! All-read or all-write, all-or-nothing. Reads behave like `TransactGetItems`
//! (`Responses[i] = {Item}`, no `TableName`); any write failure cancels with a
//! positional `CancellationReasons` list. `ClientRequestToken` gives
//! idempotency. 1..=100 statements, no two on the same item. Every behaviour is
//! justified by a measurement against the live service; see the project's
//! PartiQL behavioural spec.
//!
//! The executor composes the engine's atomic multi-item handlers:
//! `handle_transact_get_items` for the read form and `handle_transact_write_items`
//! for the write form (which already enforces atomicity, conflicting-key
//! rejection, and `ClientRequestToken` idempotency). It only translates each
//! PartiQL statement into the corresponding transact member and remaps two
//! error strings the PartiQL surface words differently.

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::{Statement, parse};
use extenddb_core::types::CancellationReason;

use super::common;
use super::{SingleRequest, write};
use crate::{
    DispatchResult, OperationContext, handle_transact_get_items, handle_transact_write_items,
};

/// Maximum number of statements in a single `ExecuteTransaction`.
const MAX_STATEMENTS: usize = 100;

/// Execute an `ExecuteTransaction` request.
///
/// # Errors
///
/// Returns the service's request-level errors (read/write mix, conflicting
/// keys, size limit, idempotency mismatch) or `TransactionCanceledException`
/// with positional cancellation reasons.
pub(super) async fn run(
    ctx: &OperationContext,
    body: &Value,
) -> Result<DispatchResult, DynamoDbError> {
    let statements = body
        .get("TransactStatements")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "TransactStatements must be a non-empty list".to_owned(),
            )
        })?;

    if statements.len() > MAX_STATEMENTS {
        return Err(too_many_statements(statements.len()));
    }

    // A transaction is atomic, so a malformed statement fails the whole request.
    let mut parsed: Vec<(SingleRequest, Statement)> = Vec::with_capacity(statements.len());
    for obj in statements {
        let req = SingleRequest::from_body(obj)?;
        let stmt =
            parse(&req.statement).map_err(|e| DynamoDbError::ValidationException(e.message))?;
        parsed.push((req, stmt));
    }

    let reads = parsed.iter().filter(|(_, s)| s.is_read()).count();
    let writes = parsed.len() - reads;
    if reads > 0 && writes > 0 {
        return Err(DynamoDbError::ValidationException(
            "ExecuteTransaction API does not support both read and write operations in the same request."
                .to_owned(),
        ));
    }

    let rcc = body.get("ReturnConsumedCapacity").and_then(Value::as_str);

    if writes == 0 {
        run_read(ctx, &parsed, rcc).await
    } else {
        run_write(ctx, body, &parsed, rcc).await
    }
}

/// Execute an all-read transaction via `TransactGetItems`.
async fn run_read(
    ctx: &OperationContext,
    parsed: &[(SingleRequest, Statement)],
    rcc: Option<&str>,
) -> Result<DispatchResult, DynamoDbError> {
    // Resolve every table first. On the service an unknown table in a read
    // transaction cancels the whole request with a positional ResourceNotFound
    // reason (`None` for the statements whose tables exist), and this happens
    // before any per-statement validation, including the index refusal.
    let mut key_infos = Vec::with_capacity(parsed.len());
    let mut reasons = Vec::with_capacity(parsed.len());
    let mut missing = false;
    for (_, stmt) in parsed {
        let Statement::Select(select) = stmt else {
            unreachable!("read transaction holds only SELECT statements");
        };
        match ctx.table_key_info(&select.from.table).await {
            Ok(info) => {
                key_infos.push(Some(info));
                reasons.push(CancellationReason::none());
            }
            Err(_) => {
                missing = true;
                key_infos.push(None);
                reasons.push(CancellationReason {
                    code: "ResourceNotFound".to_owned(),
                    message: Some("Requested resource not found".to_owned()),
                    item: None,
                });
            }
        }
    }
    if missing {
        let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
        return Err(DynamoDbError::TransactionCanceledException {
            message: format!(
                "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                codes.join(", ")
            ),
            cancellation_reasons: reasons,
        });
    }

    let mut transact_items: Vec<Value> = Vec::with_capacity(parsed.len());
    for (i, ((req, stmt), key_info)) in parsed.iter().zip(&key_infos).enumerate() {
        let Statement::Select(select) = stmt else {
            unreachable!("read transaction holds only SELECT statements");
        };
        let key_info = key_info
            .as_ref()
            .expect("every table resolved above or the request was cancelled");
        let table = select.from.table.clone();

        // Measured: any index segment, existing or not, GSI or vector, is
        // refused with this positional message before the key is examined.
        if select.from.index.is_some() {
            return Err(DynamoDbError::ValidationException(format!(
                "Validation failed in TransactStatements[{i}]: Reads on indices are not \
                 supported within transactions."
            )));
        }

        // A multi-part base table key (this engine's preview extension) is
        // refused with the service's multi-attribute message.
        common::ensure_single_attribute_keys(&key_info.key_schema)?;

        let (pk, sk) = common::key_names(key_info);
        let pk = pk.ok_or_else(|| {
            DynamoDbError::InternalServerError("table has no partition key".to_owned())
        })?;

        // A transaction read must be a full-primary-key point read; a partial
        // key fails the entire request, indexed by its position.
        let key = common::point_read_key(select, pk, sk, &req.parameters)?.ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "Validation failed in TransactStatements[{i}]: Select statements within \
                 ExecuteTransaction must specify the primary key in the where clause."
            ))
        })?;

        transact_items.push(json!({ "Get": { "TableName": table, "Key": Value::Object(key) } }));
    }

    let mut tbody = json!({ "TransactItems": transact_items });
    if let Some(rcc) = rcc {
        tbody["ReturnConsumedCapacity"] = json!(rcc);
    }

    // The handler's output is already the transaction read shape: Responses as a
    // positional list of `{Item}` / `{}`, plus a ConsumedCapacity list.
    handle_transact_get_items(tbody, ctx).await
}

/// Execute an all-write transaction via `TransactWriteItems`.
async fn run_write(
    ctx: &OperationContext,
    body: &Value,
    parsed: &[(SingleRequest, Statement)],
    rcc: Option<&str>,
) -> Result<DispatchResult, DynamoDbError> {
    let mut transact_items: Vec<Value> = Vec::with_capacity(parsed.len());
    // Positional record of which members are INSERTs, so a condition failure on
    // one can be remapped to the DuplicateItem cancellation reason the service
    // reports for a duplicate-key INSERT.
    let mut is_insert: Vec<bool> = Vec::with_capacity(parsed.len());

    for (req, stmt) in parsed {
        let (member, insert) = build_write_member(ctx, stmt, req).await?;
        transact_items.push(member);
        is_insert.push(insert);
    }

    let mut tbody = json!({ "TransactItems": transact_items });
    if let Some(token) = body.get("ClientRequestToken").and_then(Value::as_str) {
        tbody["ClientRequestToken"] = json!(token);
    }
    if let Some(rcc) = rcc {
        tbody["ReturnConsumedCapacity"] = json!(rcc);
    }

    match handle_transact_write_items(tbody, ctx).await {
        Ok(result) => Ok(reshape_write(result)),
        Err(e) => Err(remap_write_error(e, &is_insert)),
    }
}

/// Translate one PartiQL write statement into its `TransactWriteItems` member,
/// returning the member and whether it is an `INSERT`.
async fn build_write_member(
    ctx: &OperationContext,
    stmt: &Statement,
    req: &SingleRequest,
) -> Result<(Value, bool), DynamoDbError> {
    match stmt {
        Statement::Insert(insert) => {
            let parts = write::build_insert_parts(ctx, insert, &req.parameters).await?;
            let mut put = json!({
                "TableName": parts.table,
                "Item": parts.item,
                "ConditionExpression": parts.condition,
                "ExpressionAttributeNames": Value::Object(parts.names),
            });
            attach_rvoccf(&mut put, req);
            Ok((json!({ "Put": put }), true))
        }
        Statement::Update(update) => {
            let parts = write::build_update_parts(ctx, update, &req.parameters).await?;
            let mut upd = json!({
                "TableName": parts.table,
                "Key": Value::Object(parts.key),
                "UpdateExpression": parts.update_expression,
                "ConditionExpression": parts.condition,
            });
            if !parts.names.is_empty() {
                upd["ExpressionAttributeNames"] = Value::Object(parts.names);
            }
            if !parts.values.is_empty() {
                upd["ExpressionAttributeValues"] = Value::Object(parts.values);
            }
            attach_rvoccf(&mut upd, req);
            Ok((json!({ "Update": upd }), false))
        }
        Statement::Delete(delete) => {
            let parts = write::build_delete_parts(ctx, delete, &req.parameters).await?;
            let mut del = json!({
                "TableName": parts.table,
                "Key": Value::Object(parts.key),
            });
            if let Some(condition) = parts.condition {
                del["ConditionExpression"] = json!(condition);
            }
            if !parts.names.is_empty() {
                del["ExpressionAttributeNames"] = Value::Object(parts.names);
            }
            if !parts.values.is_empty() {
                del["ExpressionAttributeValues"] = Value::Object(parts.values);
            }
            attach_rvoccf(&mut del, req);
            Ok((json!({ "Delete": del }), false))
        }
        Statement::Select(_) => Err(DynamoDbError::InternalServerError(
            "SELECT routed to the transaction write path".to_owned(),
        )),
    }
}

/// Carry a statement's per-statement `ReturnValuesOnConditionCheckFailure` onto
/// its transact member, so a conditional cancel returns the pre-image item.
fn attach_rvoccf(member: &mut Value, req: &SingleRequest) {
    if let Some(rvoccf) = &req.return_values_on_condition_check_failure {
        member["ReturnValuesOnConditionCheckFailure"] = json!(rvoccf);
    }
}

/// Reshape a `TransactWriteItems` response into the PartiQL write shape: an
/// empty `Responses` list, carrying the `ConsumedCapacity` list when present.
fn reshape_write(result: DispatchResult) -> DispatchResult {
    let mut out = Map::new();
    out.insert("Responses".to_owned(), Value::Array(Vec::new()));
    if let Some(cc) = result.body.get("ConsumedCapacity") {
        out.insert("ConsumedCapacity".to_owned(), cc.clone());
    }
    DispatchResult {
        body: Value::Object(out),
        metrics: result.metrics,
    }
}

/// Remap the two write errors the PartiQL surface words differently from the
/// classic `TransactWriteItems` handler.
///
/// A duplicate-key `INSERT` fails the underlying `Put`'s `attribute_not_exists`
/// condition, which the handler reports as `ConditionalCheckFailed`; the PartiQL
/// surface reports `DuplicateItem` for that position. A reused idempotency token
/// with a different body uses the PartiQL message.
fn remap_write_error(err: DynamoDbError, is_insert: &[bool]) -> DynamoDbError {
    match err {
        DynamoDbError::TransactionCanceledException {
            cancellation_reasons,
            ..
        } => {
            let mut reasons = cancellation_reasons;
            for (i, reason) in reasons.iter_mut().enumerate() {
                if is_insert.get(i) == Some(&true) && reason.code == "ConditionalCheckFailed" {
                    reason.code = "DuplicateItem".to_owned();
                    reason.message = Some("Duplicate primary key exists in table".to_owned());
                    reason.item = None;
                }
            }
            let codes: Vec<String> = reasons.iter().map(|r| r.code.clone()).collect();
            DynamoDbError::TransactionCanceledException {
                message: format!(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                    codes.join(", ")
                ),
                cancellation_reasons: reasons,
            }
        }
        DynamoDbError::IdempotentParameterMismatchException(_) => {
            DynamoDbError::IdempotentParameterMismatchException(
                "Specified idempotent token was used with different request parameters within the idempotency window"
                    .to_owned(),
            )
        }
        other => other,
    }
}

/// The over-limit error, whose message ends with the service's constraint
/// phrase for `transactStatements` (the service echoes every request object;
/// the suffix is what tests assert).
fn too_many_statements(len: usize) -> DynamoDbError {
    let repr = std::iter::repeat_n("ParameterizedStatement", len)
        .collect::<Vec<_>>()
        .join(", ");
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '[{repr}]' at 'transactStatements' failed to satisfy \
         constraint: Member must have length less than or equal to 100"
    ))
}
