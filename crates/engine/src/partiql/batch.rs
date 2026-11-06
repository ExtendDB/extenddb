// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `BatchExecuteStatement` executor.
//!
//! All-read or all-write (mixing is a request-level `ValidationException`).
//! Reads behave like `BatchGetItem` (`Responses[i] = {TableName, Item}`), writes
//! carry per-statement `Error` entries for partial success. 1..=25 statements,
//! no two on the same key. Every behaviour is justified by a measurement
//! against the live service, cited where it is implemented.
//!
//! The executor composes the single-statement building blocks: a read is the
//! point read `handle_get_item` already serves, a write is `write::run` (each
//! statement committing independently, which is what makes partial success
//! real). Nothing here re-implements statement translation.
//!
//! Only the statements that translate count as reads or writes, and only
//! those whose request passes its checks count in the check for two
//! statements on one item (see `translation`).
//!
//! Each statement is authorized at its place among its checks (see
//! `authorize::gate`). Two statements that name one item fail the request
//! before any statement is authorized, as a read without the full primary
//! key is refused before it is authorized (measured 2026-11-03 for that
//! refusal, with a caller that may not run the statement). For two
//! statements on one item the order against authorization is not measured.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{FromSource, Projection, SelectStatement};
use extenddb_core::partiql::parser::parse_for_request;
use extenddb_core::types::{Item, TableKeyInfo, item_size_bytes};
use extenddb_storage::error::StorageError;

use super::authorize::{Gate, Surface};
use super::common::{self, StatementError};
use super::{SingleRequest, write};
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::{DispatchMetrics, DispatchResult, OperationContext, handle_get_item};

use translation::Translation;

mod translation;

/// Per-statement request state after parsing.
struct Parsed {
    req: SingleRequest,
    stmt: Option<Statement>,
    parse_err: Option<String>,
}

/// Execute a `BatchExecuteStatement` request.
///
/// # Errors
///
/// Returns the service's request-level errors (read/write mix, duplicate key,
/// size limit); per-statement failures are carried inside `Responses`.
// The steps follow the service's order; helpers would split that order across functions.
#[allow(clippy::too_many_lines)]
pub(super) async fn run(
    ctx: &OperationContext,
    body: &Value,
) -> Result<DispatchResult, DynamoDbError> {
    // A parameter of the wrong JSON type in any statement fails the request
    // before anything else is checked, and no statement runs (see
    // `check_request_parameter_types`).
    super::check_request_parameter_types(super::member_parameters(body.get("Statements")))?;
    super::request_checks::check_batch_execute_statement(body)?;
    let statements = body
        .get("Statements")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DynamoDbError::ValidationException("Statements must be a non-empty list".to_owned())
        })?;

    let rcc = body.get("ReturnConsumedCapacity").and_then(Value::as_str);

    // Check and parse each statement; a failure is deferred to per-statement
    // execution (the batch stays well-formed as long as size/mix/keys are OK).
    // A parameter that cannot be converted fails only its own statement
    // (measured 2026-11-02), and so does a parameter the service refuses for
    // its content, after a statement of only whitespace and ahead of a
    // syntax error (measured 2026-11-06: an empty set, a set with a member
    // twice or a null member, a NULL that is not true, an attribute value
    // with no type or two, a null member of a map or a list). The JSON types
    // were checked above, so these are the only parameter errors left, and
    // `from_body` reads the parameters that pass them.
    let mut checked_all = Vec::with_capacity(statements.len());
    for obj in statements {
        let parameters = super::check_parameters(obj.get("Parameters"));
        let req = match &parameters {
            Ok(()) => SingleRequest::from_body(obj)?,
            Err(_) => SingleRequest::without_parameters(obj)?,
        };
        let statement = if super::request_checks::is_blank_statement(&req.statement) {
            Err(super::request_checks::EMPTY_BATCH_STATEMENT.to_owned())
        } else {
            parameters.and_then(|()| {
                parse_for_request(&req.statement, req.parameters.len()).map_err(|e| e.message)
            })
        };
        let projected_call = match &statement {
            Ok(Statement::Select(select)) => select
                .projection_error
                .clone()
                .map(|message| (select.from.clone(), message)),
            _ => None,
        };
        // A projection list is checked with the statement: its refusal is the
        // statement's own, and the statement counts as neither a read nor a
        // write (measured 2026-11-05).
        let checked = statement.and_then(|stmt| {
            super::check_statement(&stmt, &req.parameters, &req.statement)?;
            if let Statement::Select(select) = &stmt {
                super::select::check_projection(select)?;
            }
            Ok(stmt)
        });
        let projected = match (&checked, projected_call) {
            (Err(message), Some((from, call))) if *message == call => Some(from),
            _ => None,
        };
        checked_all.push((req, checked, projected));
    }

    // Each table the statements read or write is read from the catalog once,
    // and every statement runs against that read.
    let tables: Vec<&str> = checked_all
        .iter()
        .filter_map(|(_, checked, projected)| match (checked, projected) {
            (Ok(Statement::Exists(_)), _) | (Err(_), None) => None,
            (Ok(stmt), _) => Some(common::statement_table(stmt)),
            (Err(_), Some(from)) => Some(from.table.as_str()),
        })
        .collect();
    let ctx = &super::snapshot::snapshot_context(ctx, &tables).await?;

    let mut parsed: Vec<Parsed> = Vec::with_capacity(checked_all.len());
    for (req, checked, projected) in checked_all {
        let checked = match (checked, projected) {
            (Err(message), Some(from)) => Err(projected_call_error(ctx, &from, message).await?),
            (checked, _) => checked,
        };
        match checked {
            // A condition check belongs to a transaction: in a batch it is a
            // per-statement refusal and counts as neither a read nor a write.
            Ok(Statement::Exists(_)) => parsed.push(Parsed {
                req,
                stmt: None,
                parse_err: Some(write::exists_outside_transaction().message().to_owned()),
            }),
            Ok(stmt) => parsed.push(Parsed {
                req,
                stmt: Some(stmt),
                parse_err: None,
            }),
            Err(message) => parsed.push(Parsed {
                req,
                stmt: None,
                parse_err: Some(message),
            }),
        }
    }

    // Each statement's table key schema, from the read above (None when the
    // table is unknown; that statement's ResourceNotFound surfaces
    // per-statement).
    let mut key_infos: Vec<Option<TableKeyInfo>> = Vec::with_capacity(parsed.len());
    for p in &parsed {
        match &p.stmt {
            Some(stmt) => {
                let table = common::statement_table(stmt);
                key_infos.push(ctx.table_key_info(table).await.ok());
            }
            None => key_infos.push(None),
        }
    }

    // Each statement that parsed is translated into the read or the write it
    // runs as (see `translation`).
    let mut translations: Vec<Option<Translation>> = Vec::with_capacity(parsed.len());
    for (p, key_info) in parsed.iter().zip(&key_infos) {
        translations.push(match &p.stmt {
            Some(Statement::Select(select)) => {
                Some(check_read(ctx, select, &p.req, key_info.as_ref()).await?)
            }
            Some(stmt) => {
                Some(translation::translate_write(ctx, stmt, &p.req, key_info.as_ref()).await?)
            }
            None => None,
        });
    }

    // Classify over the statements that translate. A batch is all reads or
    // all writes; mixing fails the whole request.
    let counted = || {
        parsed
            .iter()
            .zip(&translations)
            .filter(|(_, translation)| translation.as_ref().is_some_and(Translation::counts))
            .filter_map(|(p, _)| p.stmt.as_ref())
    };
    let reads = counted().filter(|s| s.is_read()).count();
    let writes = counted().filter(|s| !s.is_read()).count();
    if reads > 0 && writes > 0 {
        return Err(DynamoDbError::ValidationException(
            "Read and write requests together in the same batch is not supported.".to_owned(),
        ));
    }

    // Two statements naming the same item fail the whole request.
    detect_duplicate_keys(&parsed, &translations)?;

    // Execute positionally.
    let mut responses: Vec<Value> = Vec::with_capacity(parsed.len());
    let mut capacity = CapacityList::default();
    for ((p, translation), key_info) in parsed.into_iter().zip(translations).zip(&key_infos) {
        if let Some(msg) = p.parse_err {
            responses.push(StatementError::parse(msg).into_entry(None));
            continue;
        }
        let stmt = p.stmt.expect("non-parse-error statement is Some");
        let translation = translation.expect("a statement that parsed is translated");
        let table = common::statement_table(&stmt).to_owned();
        let mut req = p.req;
        req.return_consumed_capacity = rcc.map(str::to_owned);

        // Each statement is authorized at its place among its checks (see
        // `authorize::gate`). A refused statement's entry carries the
        // refusal and its table, and the others run (measured 2026-11-03).
        let mut gate = Gate::new(ctx, &stmt, key_info.as_ref(), &req, Surface::Batch);
        let key = match translation {
            Translation::Runs { key } => key,
            Translation::Untranslated(error) => {
                let answer = gate.settle(Err::<Value, _>(error)).await?;
                responses.push(refusal_entry(answer, false, &table));
                continue;
            }
            Translation::Refused { error, names_table } => {
                let answer = gate.settle(Err::<Value, _>(error)).await?;
                responses.push(refusal_entry(answer, names_table, &table));
                continue;
            }
        };
        let result = match &stmt {
            Statement::Select(_) => {
                let key = key.clone().expect("a read that runs names its item");
                execute_read(ctx, &req, &table, key, &mut gate).await
            }
            _ => write::run(ctx, &stmt, &req, &mut gate).await,
        };
        // Every error of a statement that reached its item names the table:
        // a failed condition, a duplicate key, a key value past its size
        // limit, an update the item cannot take (measured 2026-11-02 for the
        // key size, 2026-11-05 for the others).
        let reached_item = result.is_err() && gate.decided();
        let entry = match gate.settle(result).await? {
            Ok(result) => {
                capacity.add(&result.body);
                match &stmt {
                    Statement::Select(select) => read_entry(select, &table, &result.body),
                    _ => write_entry(&table, &result.body),
                }
            }
            Err(error) if reached_item => {
                if rcc.is_some()
                    && !stmt.is_read()
                    && is_charged_failure(&error)
                    && let (Some(key_info), Some(key)) = (key_info, &key)
                {
                    capacity.add_failed(&table, failed_write_units(ctx, key_info, key).await?);
                }
                StatementError::from_item_error(&error).into_entry(Some(&table))
            }
            Err(error) => StatementError::from_error(&error).into_entry(Some(&table)),
        };
        responses.push(entry);
    }

    let mut out = Map::new();
    out.insert("Responses".to_owned(), Value::Array(responses));
    if let Some(cc) = capacity.into_value() {
        out.insert("ConsumedCapacity".to_owned(), cc);
    }
    Ok(DispatchResult {
        body: Value::Object(out),
        metrics: DispatchMetrics::default(),
    })
}

/// The error a read whose projection is a call (`COUNT(*)`) reports: the
/// call's refusal, on an existing table or a missing one, unless the target's
/// key has more than one HASH or RANGE attribute, whose refusal comes first
/// (measured 2026-11-02 on an index with two HASH attributes).
async fn projected_call_error(
    ctx: &OperationContext,
    from: &FromSource,
    message: String,
) -> Result<String, DynamoDbError> {
    let key_info = match ctx.table_key_info(&from.table).await {
        Ok(info) => info,
        Err(StorageError::TableNotFound(_)) => return Ok(message),
        Err(e) => return Err(storage_err_to_dynamo(e)),
    };
    let schema = match &from.index {
        None => key_info.key_schema,
        Some(index) => match ctx
            .storage
            .index_info_by_table_id(&key_info.table_id, index)
            .await
        {
            Ok(info) => info.key_schema,
            Err(StorageError::IndexNotFound(_)) => return Ok(message),
            Err(e) => return Err(storage_err_to_dynamo(e)),
        },
    };
    Ok(match common::ensure_single_attribute_keys(&schema) {
        Ok(()) => message,
        Err(refusal) => refusal.message().to_owned(),
    })
}

/// The refusal of a `BatchExecuteStatement` read that is not a read of one
/// item by its full primary key. It comes before the statement is
/// authorized (measured 2026-11-03).
pub(super) const PRIMARY_KEY_REQUIRED: &str = "Select statements within BatchExecuteStatement must specify the primary key in the where \
     clause.";

/// How far the read statement `select` gets (see [`Translation`]), from the
/// checks it gets before its item is read, in the service's order. A read
/// that runs names its item. The outer error is a fault that fails the
/// request.
async fn check_read(
    ctx: &OperationContext,
    select: &SelectStatement,
    req: &SingleRequest,
    key_info: Option<&TableKeyInfo>,
) -> Result<Translation, DynamoDbError> {
    let untranslated = |error| Ok(Translation::Untranslated(error));
    // A read that is not of one item by its full primary key translates, and
    // counts as a read (measured 2026-11-05).
    let primary_key_required = || {
        Ok(Translation::Refused {
            error: DynamoDbError::ValidationException(PRIMARY_KEY_REQUIRED.to_owned()),
            names_table: false,
        })
    };
    // An unknown table is a per-statement ResourceNotFound (the call stays 200).
    let Some(key_info) = key_info else {
        return untranslated(DynamoDbError::ResourceNotFoundException(String::new()));
    };

    // A named index is resolved before the key check. Measured on the service:
    // a missing index is a per-statement ValidationError with the bare index
    // message; a multi-attribute index key is the multi-attribute refusal; an
    // index that exists (GSI, LSI or vector) can never satisfy the primary-key
    // rule and gets the key message whatever the WHERE names.
    if let Some(index_name) = &select.from.index {
        let is_vector = key_info
            .vector_indexes
            .iter()
            .any(|v| &v.index_name == index_name);
        if !is_vector {
            match ctx
                .storage
                .index_info_by_table_id(&key_info.table_id, index_name)
                .await
            {
                Ok(info) => {
                    if let Err(e) = common::ensure_single_attribute_keys(&info.key_schema) {
                        return untranslated(e);
                    }
                }
                Err(StorageError::IndexNotFound(_)) => {
                    return untranslated(DynamoDbError::ValidationException(
                        "The table does not have the specified index".to_owned(),
                    ));
                }
                Err(e) => return Err(storage_err_to_dynamo(e)),
            }
        }
        return primary_key_required();
    }

    // A multi-part base table key (this engine's preview extension) is refused
    // per statement with the service's multi-attribute message.
    if let Err(e) = common::ensure_single_attribute_keys(&key_info.key_schema) {
        return untranslated(e);
    }

    let (pk, sk) = common::key_names(&key_info.key_schema);
    let Some(pk) = pk else {
        return Err(DynamoDbError::InternalServerError(
            "table has no partition key".to_owned(),
        ));
    };

    // A batch SELECT must be a full-primary-key point read; a partial key is a
    // per-statement ValidationError.
    let Some(key) = common::point_read_key(select, pk, sk, &req.parameters)? else {
        return primary_key_required();
    };
    // A key value of the wrong type gets the message a single SELECT gets,
    // not the GetItem one (measured 2026-11-02).
    if let Err(e) = common::check_key_types(&key, key_info) {
        return untranslated(e);
    }
    Ok(Translation::Runs { key: Some(key) })
}

/// Read the item `key` names in `table` for a read statement of `req`,
/// passing `gate` first: the `GetItem` answer, or the statement's refusal.
async fn execute_read(
    ctx: &OperationContext,
    req: &SingleRequest,
    table: &str,
    key: Map<String, Value>,
    gate: &mut Gate<'_>,
) -> Result<DispatchResult, DynamoDbError> {
    gate.pass().await?;
    let mut get_body = json!({ "TableName": table, "Key": Value::Object(key) });
    // ConsistentRead is carried per member: a consistent member is charged a
    // full read unit where an eventual one is charged half (measured
    // 2026-11-02).
    if req.consistent_read {
        get_body["ConsistentRead"] = json!(true);
    }
    if let Some(rcc) = &req.return_consumed_capacity {
        get_body["ReturnConsumedCapacity"] = json!(rcc);
    }
    handle_get_item(get_body, ctx).await
}

/// A read member's `Responses[i]` entry: its table, plus the item `body`
/// holds. A batch SELECT with a projection list projects the item, as the
/// service does; SELECT * returns it whole.
fn read_entry(select: &SelectStatement, table: &str, body: &Value) -> Value {
    let mut entry = Map::new();
    entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
    if let Some(item) = body.get("Item") {
        let shaped = match &select.projection {
            Projection::All => item.clone(),
            Projection::Paths(paths) => super::select::project_item(item, paths),
        };
        entry.insert("Item".to_owned(), shaped);
    }
    Value::Object(entry)
}

/// The `Responses[i]` entry of a statement refused before it ran: `answer`
/// is the refusal, or the statement's `AccessDenied`. The refusal carries
/// the statement's table when `names_table`.
fn refusal_entry(answer: Result<Value, DynamoDbError>, names_table: bool, table: &str) -> Value {
    let error = match answer {
        Err(error) => error,
        Ok(_) => unreachable!("a refused statement is answered with an error"),
    };
    let entry = if names_table {
        StatementError::from_item_error(&error)
    } else {
        StatementError::from_error(&error)
    };
    entry.into_entry(Some(table))
}

/// Fail the request when two statements that run address the same item.
fn detect_duplicate_keys(
    parsed: &[Parsed],
    translations: &[Option<Translation>],
) -> Result<(), DynamoDbError> {
    let mut seen: HashSet<String> = HashSet::with_capacity(parsed.len());
    for (p, translation) in parsed.iter().zip(translations) {
        let (Some(stmt), Some(key)) = (&p.stmt, translation.as_ref().and_then(Translation::key))
        else {
            continue;
        };
        let id = common::canonical_identity(common::statement_table(stmt), key);
        if !seen.insert(id) {
            return Err(DynamoDbError::ValidationException(
                "Provided list of item keys contains duplicates".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Whether a write that failed at its item is charged the write units of
/// that item: a failed condition, a duplicate INSERT and an update that the
/// item cannot take are; a refusal of the request is not (measured
/// 2026-11-05).
fn is_charged_failure(error: &DynamoDbError) -> bool {
    match error {
        DynamoDbError::ConditionalCheckFailedException(..)
        | DynamoDbError::DuplicateItemException(_) => true,
        DynamoDbError::ValidationException(message) => UPDATE_FAILURES.contains(&message.as_str()),
        _ => false,
    }
}

/// The refusals of an update that the stored item cannot take.
const UPDATE_FAILURES: [&str; 2] = [
    "An operand in the update expression has an incorrect data type",
    "The document path provided in the update expression is invalid for update",
];

/// The write units a failed write is charged: those of the item stored under
/// `key`, or one when there is none (measured 2026-11-05: a failed condition
/// on a 3 KB item is charged 3 units, on a missing item 1, and a duplicate
/// INSERT of a small item over a 3 KB item 3).
async fn failed_write_units(
    ctx: &OperationContext,
    key_info: &TableKeyInfo,
    key: &Map<String, Value>,
) -> Result<f64, DynamoDbError> {
    let key: Item = serde_json::from_value(Value::Object(key.clone())).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to read a translated PartiQL key");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })?;
    let stored = ctx
        .storage
        .get_item(key_info, &key)
        .await
        .map_err(storage_err_to_dynamo)?;
    Ok(capacity_helpers::write_capacity_units(
        stored.as_ref().map_or(0, item_size_bytes),
    ))
}

/// A write member's `Responses[i]` entry: its table, plus the `RETURNING` row
/// as `Item` when the statement returned one. A batch member honours
/// `RETURNING` with the same projection `ExecuteStatement` puts in `Items`;
/// the singular `Item` cannot hold an empty row, so an empty projection leaves
/// the field out (measured 2026-11-02).
fn write_entry(table: &str, body: &Value) -> Value {
    let mut entry = Map::new();
    entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
    if let Some(item) = body
        .get("Items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
    {
        entry.insert("Item".to_owned(), item.clone());
    }
    Value::Object(entry)
}

/// Accumulates per-table `ConsumedCapacity` objects from each sub-operation into
/// the single list a batch returns, summing `CapacityUnits`, and at INDEXES
/// granularity the nested `Table` units and the units of each global and
/// local secondary index, per table in first-seen order (measured
/// 2026-11-05).
///
/// The units of a failed write are added to its table's `CapacityUnits`
/// alone, and only when a statement on that table succeeded: a table on
/// which every write failed has no entry (measured 2026-11-05).
#[derive(Default)]
struct CapacityList {
    order: Vec<String>,
    totals: HashMap<String, TableCapacity>,
    failed: HashMap<String, f64>,
}

/// The units summed for one table of a batch.
#[derive(Default)]
struct TableCapacity {
    units: f64,
    table: Option<f64>,
    global: BTreeMap<String, f64>,
    local: BTreeMap<String, f64>,
}

impl CapacityList {
    /// Fold one sub-operation response's `ConsumedCapacity` object in.
    fn add(&mut self, body: &Value) {
        let Some(cc) = body.get("ConsumedCapacity") else {
            return;
        };
        let Some(table) = cc.get("TableName").and_then(Value::as_str) else {
            return;
        };
        if !self.totals.contains_key(table) {
            self.order.push(table.to_owned());
            self.totals
                .insert(table.to_owned(), TableCapacity::default());
        }
        let entry = self.totals.get_mut(table).expect("inserted above");
        entry.units += units_of(cc).unwrap_or(0.0);
        if let Some(units) = cc.get("Table").and_then(units_of) {
            entry.table = Some(entry.table.unwrap_or(0.0) + units);
        }
        add_index_units(&mut entry.global, cc.get("GlobalSecondaryIndexes"));
        add_index_units(&mut entry.local, cc.get("LocalSecondaryIndexes"));
    }

    /// Count `units` for a write on `table` that failed at its item.
    fn add_failed(&mut self, table: &str, units: f64) {
        *self.failed.entry(table.to_owned()).or_default() += units;
    }

    /// The `ConsumedCapacity` list, or `None` when nothing was accumulated.
    fn into_value(self) -> Option<Value> {
        if self.order.is_empty() {
            return None;
        }
        let list: Vec<Value> = self
            .order
            .iter()
            .map(|table| {
                let entry = &self.totals[table];
                let failed = self.failed.get(table).copied().unwrap_or(0.0);
                let mut obj = Map::new();
                obj.insert("TableName".to_owned(), Value::String(table.clone()));
                obj.insert("CapacityUnits".to_owned(), json!(entry.units + failed));
                if let Some(units) = entry.table {
                    obj.insert("Table".to_owned(), json!({ "CapacityUnits": units }));
                }
                for (field, indexes) in [
                    ("GlobalSecondaryIndexes", &entry.global),
                    ("LocalSecondaryIndexes", &entry.local),
                ] {
                    if !indexes.is_empty() {
                        let arms: Map<String, Value> = indexes
                            .iter()
                            .map(|(name, units)| (name.clone(), json!({ "CapacityUnits": units })))
                            .collect();
                        obj.insert(field.to_owned(), Value::Object(arms));
                    }
                }
                Value::Object(obj)
            })
            .collect();
        Some(Value::Array(list))
    }
}

/// The `CapacityUnits` of one arm of a `ConsumedCapacity` object.
fn units_of(arm: &Value) -> Option<f64> {
    arm.get("CapacityUnits").and_then(Value::as_f64)
}

/// Add the units of each index of an `INDEXES` breakdown to `totals`.
fn add_index_units(totals: &mut BTreeMap<String, f64>, indexes: Option<&Value>) {
    let Some(Value::Object(indexes)) = indexes else {
        return;
    };
    for (name, arm) in indexes {
        *totals.entry(name.clone()).or_default() += units_of(arm).unwrap_or(0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_returned_row_becomes_the_member_item() {
        let body = json!({ "Items": [{ "pk": {"S": "k"}, "data": {"S": "new"} }] });
        assert_eq!(
            write_entry("t", &body),
            json!({ "TableName": "t", "Item": { "pk": {"S": "k"}, "data": {"S": "new"} } })
        );
    }

    #[test]
    fn no_returned_row_leaves_the_item_out() {
        assert_eq!(
            write_entry("t", &json!({ "Items": [] })),
            json!({ "TableName": "t" })
        );
    }

    fn capacity(table: &str, units: f64, gsi: f64, lsi: f64) -> Value {
        json!({ "ConsumedCapacity": {
            "TableName": table,
            "CapacityUnits": units,
            "Table": { "CapacityUnits": 1.0 },
            "GlobalSecondaryIndexes": { "gsi": { "CapacityUnits": gsi } },
            "LocalSecondaryIndexes": { "lsi": { "CapacityUnits": lsi } },
        }})
    }

    #[test]
    fn the_index_arms_are_summed_per_table() {
        let mut list = CapacityList::default();
        list.add(&capacity("t", 4.0, 2.0, 1.0));
        list.add(&capacity("u", 2.0, 0.5, 0.5));
        list.add(&capacity("t", 3.0, 1.0, 1.0));
        assert_eq!(
            list.into_value(),
            Some(json!([
                {
                    "TableName": "t",
                    "CapacityUnits": 7.0,
                    "Table": { "CapacityUnits": 2.0 },
                    "GlobalSecondaryIndexes": { "gsi": { "CapacityUnits": 3.0 } },
                    "LocalSecondaryIndexes": { "lsi": { "CapacityUnits": 2.0 } },
                },
                {
                    "TableName": "u",
                    "CapacityUnits": 2.0,
                    "Table": { "CapacityUnits": 1.0 },
                    "GlobalSecondaryIndexes": { "gsi": { "CapacityUnits": 0.5 } },
                    "LocalSecondaryIndexes": { "lsi": { "CapacityUnits": 0.5 } },
                },
            ]))
        );
    }

    #[test]
    fn a_failed_write_counts_in_the_total_of_a_table_with_an_entry() {
        let mut list = CapacityList::default();
        list.add_failed("t", 3.0);
        list.add(&json!({ "ConsumedCapacity": { "TableName": "t", "CapacityUnits": 2.0 } }));
        list.add_failed("u", 1.0);
        assert_eq!(
            list.into_value(),
            Some(json!([{ "TableName": "t", "CapacityUnits": 5.0 }]))
        );
        let mut failed_only = CapacityList::default();
        failed_only.add_failed("t", 1.0);
        assert_eq!(failed_only.into_value(), None);
    }

    #[test]
    fn failures_at_the_item_are_charged() {
        for error in [
            DynamoDbError::ConditionalCheckFailedException(
                "The conditional request failed".to_owned(),
                None,
            ),
            DynamoDbError::DuplicateItemException(
                "Duplicate primary key exists in table".to_owned(),
            ),
            DynamoDbError::ValidationException(UPDATE_FAILURES[0].to_owned()),
            DynamoDbError::ValidationException(UPDATE_FAILURES[1].to_owned()),
        ] {
            assert!(is_charged_failure(&error), "{error:?}");
        }
        let key_update = DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Cannot update attribute pk. This \
             attribute is part of the key"
                .to_owned(),
        );
        assert!(!is_charged_failure(&key_update));
        assert!(!is_charged_failure(&DynamoDbError::AccessDeniedException(
            "refused".to_owned()
        )));
    }
}
