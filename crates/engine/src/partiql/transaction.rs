// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `ExecuteTransaction` executor.
//!
//! All-read or all-write, all-or-nothing. Reads behave like `TransactGetItems`
//! (`Responses[i] = {Item}`, no `TableName`); any write failure cancels with a
//! positional `CancellationReasons` list. `ClientRequestToken` gives
//! idempotency. 1..=100 statements, no two on the same item. Every behaviour is
//! justified by a measurement against the live service, cited where it is
//! implemented.
//!
//! The executor composes the engine's atomic multi-item handlers:
//! `handle_transact_get_items` for the read form and `handle_transact_write_items`
//! for the write form (which already enforces atomicity, conflicting-key
//! rejection, and `ClientRequestToken` idempotency). It translates each
//! PartiQL statement into the corresponding transact member, cancels the write
//! form with positional reasons when a member cannot be translated against its
//! table, and remaps two error strings the PartiQL surface words differently.
//!
//! The members are authorized after the checks of their form (see
//! `authorize_members`). In an all-read transaction, a member on a table
//! that exists and that reads an index, or does not name the full primary
//! key, fails the request before any member is authorized, as the form of
//! an EXISTS member, an EXISTS on an index and a `BatchExecuteStatement`
//! read without the full primary key do (measured 2026-11-03 for a caller
//! that may not run them). For these two refusals of a read transaction the
//! order against authorization is not measured.

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::Projection as ItemProjection;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{Projection, Returning, SelectStatement};
use extenddb_core::partiql::parser::parse_for_request;
use extenddb_core::types::{CancellationReason, Item};

use super::common;
use super::expr_bridge::ExprBridge;
use super::{SingleRequest, authorize, item_depth, write};
use crate::expression_helpers::{build_expression_maps, parse_projection_expr};
use crate::{
    DispatchResult, OperationContext, handle_transact_get_items, handle_transact_write_items,
};

use members::{build_write_member, check_exists_form, check_parameter_count, member_reason};

mod members;

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
    super::request_checks::check_execute_transaction(body)?;
    let statements = body
        .get("TransactStatements")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "TransactStatements must be a non-empty list".to_owned(),
            )
        })?;

    // The service checks each member in order and stops at the first that
    // fails, so a refusal of a member wins over any refusal of a later one
    // (measured 2026-11-05). The members are read and parsed up to the first
    // whose statement fails its checks; that refusal is reported once the
    // members before it have passed the checks they get against their tables.
    let mut parsed: Vec<(SingleRequest, Statement)> = Vec::with_capacity(statements.len());
    let mut refusal = None;
    for (i, obj) in statements.iter().enumerate() {
        match parse_member(i, obj) {
            Ok(member) => parsed.push(member),
            Err(error) => {
                refusal = Some(error);
                break;
            }
        }
    }

    // Each table the statements name is read from the catalog once, and
    // every member, and the TransactGetItems or TransactWriteItems request
    // built from them, runs against that read.
    let tables: Vec<&str> = parsed
        .iter()
        .map(|(_, stmt)| common::statement_table(stmt))
        .collect();
    let ctx = &super::snapshot::snapshot_context(ctx, &tables).await?;

    // A member's refusals against its table come ahead of the refusal of a
    // read and a write in one request, which a member that differs from the
    // members before it gets (measured 2026-11-05: an index read, a read
    // without the full key, a key-only EXISTS and a RETURNING clause after a
    // member of the other kind are refused for themselves).
    let mut reads = None;
    for (i, (req, stmt)) in parsed.iter().enumerate() {
        check_member(ctx, i, stmt, req).await?;
        if *reads.get_or_insert(stmt.is_read()) != stmt.is_read() {
            return Err(DynamoDbError::ValidationException(
                "ExecuteTransaction API does not support both read and write operations in the same request."
                    .to_owned(),
            ));
        }
    }
    if let Some(refusal) = refusal {
        return Err(refusal);
    }

    let rcc = body.get("ReturnConsumedCapacity").and_then(Value::as_str);

    if reads == Some(true) {
        run_read(ctx, &parsed, rcc).await
    } else {
        run_write(ctx, body, &parsed, rcc).await
    }
}

/// Read and parse the member at position `i`, and make the checks its
/// statement gets before its table is read.
///
/// A statement of only whitespace fails the whole request without the
/// statement's position, ahead of its parameters (measured 2026-11-05). A
/// malformed statement fails the whole request, its message prefixed
/// with the statement's position (measured 2026-11-02 on an empty WHERE). A
/// parameter that cannot be converted is reported without the statement's
/// position (measured 2026-11-02), and so is one the service refuses for its
/// content, such as an empty set (measured 2026-11-06); the checks on a
/// parsed statement name it.
/// A projected call (`COUNT(*)`) is among those checks, so it is refused
/// ahead of every table lookup (measured 2026-11-02), and so are a
/// projection list that names an identifier twice or two paths that
/// overlap, and a `?` without a parameter (measured 2026-11-05).
fn parse_member(i: usize, obj: &Value) -> Result<(SingleRequest, Statement), DynamoDbError> {
    let positional = |message: &str| {
        DynamoDbError::ValidationException(format!(
            "Validation failed in TransactStatements[{i}]: {message}"
        ))
    };
    let statement = SingleRequest::without_parameters(obj)?.statement;
    if super::request_checks::is_blank_statement(&statement) {
        return Err(DynamoDbError::ValidationException(
            super::request_checks::EMPTY_TRANSACTION_STATEMENT.to_owned(),
        ));
    }
    super::check_parameters(obj.get("Parameters")).map_err(DynamoDbError::ValidationException)?;
    let req = SingleRequest::from_body(obj)?;
    let stmt = parse_for_request(&req.statement, req.parameters.len())
        .map_err(|e| positional(&e.message))?;
    super::check_statement(&stmt, &req.parameters, &req.statement)
        .map_err(|message| positional(&message))?;
    if let Statement::Select(select) = &stmt {
        super::select::check_projection(select).map_err(|message| positional(&message))?;
    }
    check_parameter_count(&stmt, req.parameters.len(), i)?;
    Ok((req, stmt))
}

/// The refusals the member at position `i` gets against its table, before
/// any member is authorized. A member on a missing table gets none of them
/// but the refusal of an EXISTS through an index: the missing table cancels
/// the request once every member is checked.
///
/// A read is refused when it reads an index, then when its WHERE does not
/// give the full primary key; a key value of the wrong type cancels the
/// request later (measured 2026-11-05). An EXISTS is refused when it reads
/// an index, ahead of a missing table, then when it is not a read of one
/// item with a further condition (see `members::check_exists_form`). A
/// `RETURNING` clause is refused (see [`reject_returning`]).
///
/// # Errors
///
/// The member's refusal.
async fn check_member(
    ctx: &OperationContext,
    i: usize,
    stmt: &Statement,
    req: &SingleRequest,
) -> Result<(), DynamoDbError> {
    match stmt {
        Statement::Select(select) => check_read_form(ctx, i, select, req).await,
        Statement::Exists(_) => check_exists_form(ctx, i, stmt, req).await,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            if let Some(table) = returning_table(stmt)
                && ctx.table_key_info(table).await.is_ok()
            {
                reject_returning(stmt, i)?;
            }
            Ok(())
        }
    }
}

/// The refusals a read member at position `i` gets against its table: a
/// read of an index, existing or not, GSI or vector, then a read whose WHERE
/// is not of one item by its full primary key.
async fn check_read_form(
    ctx: &OperationContext,
    i: usize,
    select: &SelectStatement,
    req: &SingleRequest,
) -> Result<(), DynamoDbError> {
    let Ok(key_info) = ctx.table_key_info(&select.from.table).await else {
        return Ok(());
    };
    if select.from.index.is_some() {
        return Err(DynamoDbError::ValidationException(format!(
            "Validation failed in TransactStatements[{i}]: Reads on indices are not \
             supported within transactions."
        )));
    }
    // A multi-part base table key (this engine's preview extension) is
    // refused once the members are authorized (see `run_read`).
    if common::ensure_single_attribute_keys(&key_info.key_schema).is_err() {
        return Ok(());
    }
    let (pk, sk) = common::key_names(&key_info.key_schema);
    let pk = pk.ok_or_else(|| {
        DynamoDbError::InternalServerError("table has no partition key".to_owned())
    })?;
    match common::point_read_key(select, pk, sk, &req.parameters)? {
        Some(_) => Ok(()),
        None => Err(DynamoDbError::ValidationException(format!(
            "Validation failed in TransactStatements[{i}]: Select statements within \
             ExecuteTransaction must specify the primary key in the where clause."
        ))),
    }
}

/// Execute an all-read transaction via `TransactGetItems`.
///
/// The members are authorized once their form is checked: after the
/// refusals of a read of an index and of a read without the full primary
/// key, which fail the request (see [`check_member`]), and ahead of a
/// missing table and a key value of the wrong type, which cancel it (see
/// `authorize::gate`).
async fn run_read(
    ctx: &OperationContext,
    parsed: &[(SingleRequest, Statement)],
    rcc: Option<&str>,
) -> Result<DispatchResult, DynamoDbError> {
    // A member on a missing table reports ResourceNotFound and a member whose
    // key value has the wrong type reports that the key does not match the
    // schema, in one cancellation (measured 2026-11-05 with the two in either
    // order); every other member reports `None`.
    let mut transact_items: Vec<Value> = Vec::with_capacity(parsed.len());
    let mut projections: Vec<Option<ItemProjection>> = Vec::with_capacity(parsed.len());
    let mut reasons: Vec<CancellationReason> = Vec::with_capacity(parsed.len());
    let mut multi_attribute = None;
    for (req, stmt) in parsed {
        let Statement::Select(select) = stmt else {
            unreachable!("read transaction holds only SELECT statements");
        };
        let Ok(key_info) = ctx.table_key_info(&select.from.table).await else {
            reasons.push(not_found_reason());
            continue;
        };
        // A multi-part base table key (this engine's preview extension) is
        // refused with the service's multi-attribute message.
        if let Err(e) = common::ensure_single_attribute_keys(&key_info.key_schema) {
            multi_attribute.get_or_insert(e);
            reasons.push(CancellationReason::none());
            continue;
        }
        let (pk, sk) = common::key_names(&key_info.key_schema);
        let pk = pk.ok_or_else(|| {
            DynamoDbError::InternalServerError("table has no partition key".to_owned())
        })?;
        let key = common::point_read_key(select, pk, sk, &req.parameters)?.ok_or_else(|| {
            DynamoDbError::InternalServerError("a read member has its full key".to_owned())
        })?;
        reasons.push(match common::check_key_types(&key, &key_info) {
            Ok(()) => CancellationReason::none(),
            Err(_) => CancellationReason::validation_error(common::KEY_SCHEMA_MISMATCH),
        });
        projections.push(member_projection(ctx, select)?);
        transact_items.push(json!({
            "Get": { "TableName": select.from.table, "Key": Value::Object(key) }
        }));
    }
    authorize_members(ctx, parsed).await?;
    if reasons
        .iter()
        .any(|reason| reason.code == "ResourceNotFound")
    {
        return Err(cancellation(reasons));
    }
    if let Some(refusal) = multi_attribute {
        return Err(refusal);
    }
    if reasons.iter().any(|reason| reason.code != "None") {
        return Err(cancellation(reasons));
    }

    let mut tbody = json!({ "TransactItems": transact_items });
    if let Some(rcc) = rcc {
        tbody["ReturnConsumedCapacity"] = json!(rcc);
    }

    // The handler's output is the transaction read shape: Responses as a
    // positional list of `{Item}` / `{}`, plus a ConsumedCapacity list. The
    // items are read whole and projected here, since `TransactGetItems` drops
    // an item that its projection leaves empty.
    project_items(handle_transact_get_items(tbody, ctx).await?, &projections)
}

/// The projection a read member's projection list makes, or `None` for
/// `SELECT *`. The item keeps the shape of the paths, as a
/// `ProjectionExpression` gives it: `SELECT a, m.k` returns `{a, m: {k}}`
/// (measured 2026-11-05).
///
/// # Errors
///
/// A refusal of the projection, which the statement checks make first.
fn member_projection(
    ctx: &OperationContext,
    select: &SelectStatement,
) -> Result<Option<ItemProjection>, DynamoDbError> {
    let Projection::Paths(paths) = &select.projection else {
        return Ok(None);
    };
    let mut bridge = ExprBridge::new(&[]);
    let rendered: Vec<String> = paths.iter().map(|path| bridge.path(path)).collect();
    let maps = build_expression_maps(Some(&bridge.names_map()), None);
    let paths = parse_projection_expr(&rendered.join(", "), &ctx.limits)?;
    ItemProjection::compile(&paths, &maps, true).map(Some)
}

/// Give each item of a `TransactGetItems` answer its member's projection.
/// An item that has none of the projected paths is returned empty, and a
/// member whose item is missing returns no item (measured 2026-11-05).
fn project_items(
    mut result: DispatchResult,
    projections: &[Option<ItemProjection>],
) -> Result<DispatchResult, DynamoDbError> {
    let Some(Value::Array(responses)) = result.body.get_mut("Responses") else {
        return Ok(result);
    };
    for (entry, projection) in responses.iter_mut().zip(projections) {
        let (Some(projection), Some(item)) = (projection, entry.get("Item")) else {
            continue;
        };
        let projected = serde_json::from_value::<Item>(item.clone())
            .map(|item| projection.apply(&item))
            .and_then(serde_json::to_value)
            .map_err(|e| {
                tracing::error!(internal_error = %e, "failed to project a transaction read");
                DynamoDbError::InternalServerError("Internal server error".to_owned())
            })?;
        entry["Item"] = projected;
    }
    Ok(result)
}

/// Execute an all-write transaction via `TransactWriteItems`.
///
/// Every member is translated against its table first. When one cannot be,
/// because its table is missing or its key does not match the table's schema,
/// the request is cancelled with one reason per member and nothing runs: no
/// condition is evaluated, no duplicate `INSERT` is found and two members on
/// one item are not refused (measured 2026-11-02). When a member would store
/// a value nested past the item limit, two members on one item are refused
/// next, then every member reports its own reason (see `item_depth`).
async fn run_write(
    ctx: &OperationContext,
    body: &Value,
    parsed: &[(SingleRequest, Statement)],
    rcc: Option<&str>,
) -> Result<DispatchResult, DynamoDbError> {
    authorize_members(ctx, parsed).await?;

    let mut transact_items: Vec<Value> = Vec::with_capacity(parsed.len());
    // Positional record of which members are INSERTs, so a condition failure on
    // one can be remapped to the DuplicateItem cancellation reason the service
    // reports for a duplicate-key INSERT.
    let mut is_insert: Vec<bool> = Vec::with_capacity(parsed.len());
    let mut reasons: Vec<CancellationReason> = Vec::with_capacity(parsed.len());
    let mut cancelled = false;

    for (i, (req, stmt)) in parsed.iter().enumerate() {
        if let Some(reason) = member_reason(ctx, stmt, req).await? {
            reasons.push(reason);
            cancelled = true;
            continue;
        }
        reasons.push(CancellationReason::none());
        // Built even after another member's reason, so a refusal of this
        // statement itself fails the request rather than being hidden by it.
        let (member, insert) = build_write_member(ctx, i, stmt, req).await?;
        transact_items.push(member);
        is_insert.push(insert);
    }
    if cancelled {
        return Err(cancellation(reasons));
    }
    if item_depth::has_member_past_the_item_limit(parsed)
        && let Some(reasons) =
            item_depth::transaction_reasons(ctx, parsed, &transact_items, &is_insert).await?
    {
        return Err(cancellation(reasons));
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

/// Authorize every member, after the checks of the request and of the
/// members' form, and before a missing table, a key or a condition fails a
/// member. When the caller may not run one, the request is cancelled with
/// one reason per member, `AccessDenied` with the refusal for each refused
/// member and `None` for the others, and nothing is written (measured
/// 2026-11-03: a condition that fails beside a refused member has the
/// reason `None`).
async fn authorize_members(
    ctx: &OperationContext,
    parsed: &[(SingleRequest, Statement)],
) -> Result<(), DynamoDbError> {
    let mut reasons = Vec::with_capacity(parsed.len());
    let mut refused = false;
    for (req, stmt) in parsed {
        let key_info = ctx.table_key_info(common::statement_table(stmt)).await.ok();
        let surface = authorize::Surface::Transaction;
        match authorize::statement(ctx, stmt, key_info.as_ref(), req, surface).await {
            Ok(_) => reasons.push(CancellationReason::none()),
            Err(DynamoDbError::AccessDeniedException(message)) => {
                refused = true;
                reasons.push(CancellationReason {
                    code: "AccessDenied".to_owned(),
                    message: Some(message),
                    item: None,
                });
            }
            Err(e) => return Err(e),
        }
    }
    if refused {
        return Err(cancellation(reasons));
    }
    Ok(())
}

/// The reason a member on a missing table reports.
fn not_found_reason() -> CancellationReason {
    CancellationReason {
        code: "ResourceNotFound".to_owned(),
        message: Some("Requested resource not found".to_owned()),
        item: None,
    }
}

/// A cancellation carrying one reason per member, its message listing their
/// codes in order.
fn cancellation(reasons: Vec<CancellationReason>) -> DynamoDbError {
    let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
    DynamoDbError::TransactionCanceledException {
        message: format!(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
            codes.join(", ")
        ),
        cancellation_reasons: reasons,
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
                    *reason = common::duplicate_item_reason();
                }
            }
            cancellation(reasons)
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

/// The table of a member that carries a `RETURNING` clause, or `None`.
fn returning_table(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::Delete(delete) if delete.returning.is_some() => Some(&delete.table),
        Statement::Update(update) if update.returning.is_some() => Some(&update.table),
        _ => None,
    }
}

/// Refuse a member that carries a `RETURNING` clause, naming its position.
///
/// The service rejects the whole request with a `ValidationException`, so
/// nothing in the transaction applies, rather than running the member and
/// cancelling (measured 2026-11-02). A `DELETE` whose clause is not
/// `RETURNING ALL OLD *` reports that clause as invalid instead, the refusal a
/// single statement gives.
fn reject_returning(stmt: &Statement, index: usize) -> Result<(), DynamoDbError> {
    let reason = match stmt {
        Statement::Delete(delete) => match delete.returning {
            None => return Ok(()),
            Some(Returning::AllOld) => RETURNING_REFUSAL.to_owned(),
            Some(other) => write::invalid_delete_returning(other).message().to_owned(),
        },
        Statement::Update(update) if update.returning.is_some() => RETURNING_REFUSAL.to_owned(),
        _ => return Ok(()),
    };
    Err(DynamoDbError::ValidationException(format!(
        "Validation failed in TransactStatements[{index}]: {reason}"
    )))
}

/// The service's refusal of a `RETURNING` clause in a transaction member.
const RETURNING_REFUSAL: &str = "RETURNING clause is not supported in ExecuteTransaction.";

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::parse;

    fn refusal(sql: &str, index: usize) -> Option<String> {
        reject_returning(&parse(sql).unwrap(), index)
            .err()
            .map(|e| e.message().to_owned())
    }

    #[test]
    fn a_cancellation_lists_every_reason_code_in_order() {
        let err = cancellation(vec![
            CancellationReason::none(),
            not_found_reason(),
            CancellationReason::validation_error(common::KEY_SCHEMA_MISMATCH),
        ]);
        assert_eq!(
            err.message(),
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [None, ResourceNotFound, ValidationError]"
        );
    }

    #[test]
    fn every_update_returning_mode_is_refused_with_its_position() {
        for mode in ["ALL OLD", "MODIFIED OLD", "ALL NEW", "MODIFIED NEW"] {
            let sql = format!("UPDATE \"t\" SET a = 1 WHERE pk = 'k' RETURNING {mode} *");
            assert_eq!(
                refusal(&sql, 3).as_deref(),
                Some(
                    "Validation failed in TransactStatements[3]: RETURNING clause is not \
                     supported in ExecuteTransaction."
                ),
                "{mode}"
            );
        }
    }

    #[test]
    fn delete_returning_all_old_is_refused_like_an_update() {
        assert_eq!(
            refusal("DELETE FROM \"t\" WHERE pk = 'k' RETURNING ALL OLD *", 0).as_deref(),
            Some(
                "Validation failed in TransactStatements[0]: RETURNING clause is not \
                 supported in ExecuteTransaction."
            )
        );
    }

    #[test]
    fn delete_with_another_returning_clause_reports_the_invalid_clause() {
        assert_eq!(
            refusal(
                "DELETE FROM \"t\" WHERE pk = 'k' RETURNING MODIFIED OLD *",
                1
            )
            .as_deref(),
            Some(
                "Validation failed in TransactStatements[1]: Invalid returning clause: \
                 RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE \
                 statements."
            )
        );
    }

    #[test]
    fn members_without_returning_pass() {
        for sql in [
            "UPDATE \"t\" SET a = 1 WHERE pk = 'k'",
            "DELETE FROM \"t\" WHERE pk = 'k'",
            "INSERT INTO \"t\" VALUE {'pk': 'k'}",
            "SELECT * FROM \"t\" WHERE pk = 'k'",
        ] {
            assert_eq!(refusal(sql, 0), None, "{sql}");
        }
    }
}
