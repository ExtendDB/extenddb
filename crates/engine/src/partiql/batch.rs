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
//! Each statement is authorized at its place among its checks (see
//! `authorize::gate`). Two statements that name one item fail the request
//! before any statement is authorized, as a read without the full primary
//! key is refused before it is authorized (measured 2026-11-03 for that
//! refusal, with a caller that may not run the statement). For two
//! statements on one item the order against authorization is not measured.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{FromSource, SelectStatement};
use extenddb_core::partiql::parser::parse_for_request;
use extenddb_core::types::TableKeyInfo;
use extenddb_storage::error::StorageError;

use super::authorize::{Gate, Surface};
use super::common::{self, StatementError};
use super::{SingleRequest, write};
use crate::create_table::storage_err_to_dynamo;
use crate::{DispatchMetrics, DispatchResult, OperationContext, handle_get_item};

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
pub(super) async fn run(
    ctx: &OperationContext,
    body: &Value,
) -> Result<DispatchResult, DynamoDbError> {
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
    // (measured 2026-11-02).
    let mut checked_all = Vec::with_capacity(statements.len());
    for obj in statements {
        let req = SingleRequest::from_body(obj)?;
        let statement = super::check_parameters(obj.get("Parameters")).and_then(|()| {
            parse_for_request(&req.statement, req.parameters.len()).map_err(|e| e.message)
        });
        let projected_call = match &statement {
            Ok(Statement::Select(select)) => select
                .projection_error
                .clone()
                .map(|message| (select.from.clone(), message)),
            _ => None,
        };
        let checked = statement.and_then(|stmt| {
            super::check_statement(&stmt, &req.parameters)?;
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

    // Classify over the statements that parsed. A batch is all reads or all
    // writes; mixing fails the whole request.
    let reads = parsed
        .iter()
        .filter_map(|p| p.stmt.as_ref())
        .filter(|s| s.is_read())
        .count();
    let writes = parsed
        .iter()
        .filter_map(|p| p.stmt.as_ref())
        .filter(|s| !s.is_read())
        .count();
    if reads > 0 && writes > 0 {
        return Err(DynamoDbError::ValidationException(
            "Read and write requests together in the same batch is not supported.".to_owned(),
        ));
    }
    let is_write_batch = writes > 0;

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

    // Two statements naming the same item fail the whole request.
    detect_duplicate_keys(&parsed, &key_infos, is_write_batch)?;

    // Execute positionally.
    let mut responses: Vec<Value> = Vec::with_capacity(parsed.len());
    let mut capacity = CapacityList::default();
    for (i, p) in parsed.into_iter().enumerate() {
        if let Some(msg) = p.parse_err {
            responses.push(StatementError::parse(msg).into_entry(None));
            continue;
        }
        let stmt = p.stmt.expect("non-parse-error statement is Some");
        let table = common::statement_table(&stmt).to_owned();
        let mut req = p.req;
        req.return_consumed_capacity = rcc.map(str::to_owned);

        // Each statement is authorized at its place among its checks (see
        // `authorize::gate`). A refused statement's entry carries the
        // refusal and its table, and the others run (measured 2026-11-03).
        let mut gate = Gate::new(ctx, &stmt, key_infos[i].as_ref(), &req, Surface::Batch);
        let answer = if is_write_batch {
            let result = write::run(ctx, &stmt, &req, &mut gate).await;
            gate.settle(result).await?.map(|result| {
                capacity.add(&result.body);
                write_entry(&table, &result.body)
            })
        } else {
            let Statement::Select(select) = &stmt else {
                unreachable!("read batch holds only SELECT statements");
            };
            let read = ReadTarget {
                select,
                req: &req,
                table: &table,
                key_info: key_infos[i].as_ref(),
            };
            let result = execute_read(ctx, &read, &mut capacity, &mut gate).await?;
            gate.settle(result).await?
        };
        // A key value past its size limit names the table, on every write
        // and on a read (measured 2026-11-02 for SELECT, INSERT, UPDATE and
        // DELETE).
        responses.push(
            answer.unwrap_or_else(|e| StatementError::from_key_error(&e).into_entry(Some(&table))),
        );
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

/// One read statement of a batch and what it reads.
struct ReadTarget<'r> {
    select: &'r SelectStatement,
    req: &'r SingleRequest,
    table: &'r str,
    key_info: Option<&'r TableKeyInfo>,
}

/// Execute one read statement, passing `gate` before the item is read: its
/// `Responses[i]` entry, or the refusal its entry carries. The outer error
/// is a fault that fails the request.
async fn execute_read(
    ctx: &OperationContext,
    read: &ReadTarget<'_>,
    capacity: &mut CapacityList,
    gate: &mut Gate<'_>,
) -> Result<Result<Value, DynamoDbError>, DynamoDbError> {
    let ReadTarget {
        select,
        req,
        table,
        key_info,
    } = *read;
    // An unknown table is a per-statement ResourceNotFound (the call stays 200).
    let Some(key_info) = key_info else {
        return Ok(Err(DynamoDbError::ResourceNotFoundException(String::new())));
    };
    let primary_key_required =
        || DynamoDbError::ValidationException(PRIMARY_KEY_REQUIRED.to_owned());

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
                        return Ok(Err(e));
                    }
                }
                Err(StorageError::IndexNotFound(_)) => {
                    return Ok(Err(DynamoDbError::ValidationException(
                        "The table does not have the specified index".to_owned(),
                    )));
                }
                Err(e) => return Err(storage_err_to_dynamo(e)),
            }
        }
        return Ok(Err(primary_key_required()));
    }

    // A multi-part base table key (this engine's preview extension) is refused
    // per statement with the service's multi-attribute message.
    if let Err(e) = common::ensure_single_attribute_keys(&key_info.key_schema) {
        return Ok(Err(e));
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
        return Ok(Err(primary_key_required()));
    };
    // A key value of the wrong type gets the message a single SELECT gets,
    // not the GetItem one (measured 2026-11-02).
    if let Err(e) = common::check_key_types(&key, key_info) {
        return Ok(Err(e));
    }

    if let Err(e) = gate.pass().await {
        return Ok(Err(e));
    }
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
    let result = match handle_get_item(get_body, ctx).await {
        Ok(result) => result,
        Err(e) => return Ok(Err(e)),
    };
    capacity.add(&result.body);
    let mut entry = Map::new();
    entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
    if let Some(item) = result.body.get("Item") {
        // A batch SELECT with a projection list projects the item, as the
        // service does; SELECT * returns it whole.
        let shaped = match &select.projection {
            extenddb_core::partiql::ast::Projection::All => item.clone(),
            extenddb_core::partiql::ast::Projection::Paths(paths) => {
                super::select::project_item(item, paths)
            }
        };
        entry.insert("Item".to_owned(), shaped);
    }
    Ok(Ok(Value::Object(entry)))
}

/// Fail the request when two statements address the same item.
fn detect_duplicate_keys(
    parsed: &[Parsed],
    key_infos: &[Option<TableKeyInfo>],
    is_write_batch: bool,
) -> Result<(), DynamoDbError> {
    let mut seen: HashMap<String, ()> = HashMap::with_capacity(parsed.len());
    for (i, p) in parsed.iter().enumerate() {
        let (Some(stmt), Some(key_info)) = (&p.stmt, &key_infos[i]) else {
            continue;
        };
        let (pk, sk) = common::key_names(&key_info.key_schema);
        let Some(pk) = pk else {
            continue;
        };
        let key = if is_write_batch {
            common::write_key(stmt, pk, sk, &p.req.parameters)?
        } else if let Statement::Select(select) = stmt {
            common::point_read_key(select, pk, sk, &p.req.parameters)?
        } else {
            None
        };
        if let Some(key) = key {
            let id = common::canonical_identity(common::statement_table(stmt), &key);
            if seen.insert(id, ()).is_some() {
                return Err(DynamoDbError::ValidationException(
                    "Provided list of item keys contains duplicates".to_owned(),
                ));
            }
        }
    }
    Ok(())
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
/// the single list a batch returns, summing `CapacityUnits` (and the nested
/// `Table` breakdown at INDEXES granularity) per table in first-seen order.
#[derive(Default)]
struct CapacityList {
    order: Vec<String>,
    totals: HashMap<String, (f64, Option<f64>)>,
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
        let units = cc
            .get("CapacityUnits")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let table_units = cc
            .get("Table")
            .and_then(|t| t.get("CapacityUnits"))
            .and_then(Value::as_f64);
        if !self.totals.contains_key(table) {
            self.order.push(table.to_owned());
            self.totals.insert(table.to_owned(), (0.0, None));
        }
        let entry = self.totals.get_mut(table).expect("inserted above");
        entry.0 += units;
        if let Some(tu) = table_units {
            entry.1 = Some(entry.1.unwrap_or(0.0) + tu);
        }
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
                let (units, table_units) = self.totals[table];
                let mut obj = Map::new();
                obj.insert("TableName".to_owned(), Value::String(table.clone()));
                obj.insert("CapacityUnits".to_owned(), json!(units));
                if let Some(tu) = table_units {
                    obj.insert("Table".to_owned(), json!({ "CapacityUnits": tu }));
                }
                Value::Object(obj)
            })
            .collect();
        Some(Value::Array(list))
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
}
