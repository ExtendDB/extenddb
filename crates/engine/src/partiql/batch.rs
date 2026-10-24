// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `BatchExecuteStatement` executor.
//!
//! All-read or all-write (mixing is a request-level `ValidationException`).
//! Reads behave like `BatchGetItem` (`Responses[i] = {TableName, Item}`), writes
//! carry per-statement `Error` entries for partial success. 1..=25 statements,
//! no two on the same key. Every behaviour is justified by a measurement
//! against the live service; see the project's PartiQL behavioural spec.
//!
//! The executor composes the single-statement building blocks: a read is the
//! point read `handle_get_item` already serves, a write is `write::run` (each
//! statement committing independently, which is what makes partial success
//! real). Nothing here re-implements statement translation.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::SelectStatement;
use extenddb_core::partiql::{Statement, parse};
use extenddb_core::types::TableKeyInfo;
use extenddb_storage::error::StorageError;

use super::common::{self, StatementError};
use super::{SingleRequest, write};
use crate::create_table::storage_err_to_dynamo;
use crate::{DispatchMetrics, DispatchResult, OperationContext, handle_get_item};

/// Maximum number of statements in a single `BatchExecuteStatement`.
const MAX_STATEMENTS: usize = 25;

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
    let statements = body
        .get("Statements")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DynamoDbError::ValidationException("Statements must be a non-empty list".to_owned())
        })?;

    if statements.len() > MAX_STATEMENTS {
        return Err(too_many_statements(statements.len()));
    }

    let rcc = body.get("ReturnConsumedCapacity").and_then(Value::as_str);

    // Parse each statement; a parse failure is deferred to per-statement
    // execution (the batch stays well-formed as long as size/mix/keys are OK).
    let mut parsed: Vec<Parsed> = Vec::with_capacity(statements.len());
    for obj in statements {
        let req = SingleRequest::from_body(obj)?;
        match parse(&req.statement) {
            Ok(stmt) => parsed.push(Parsed {
                req,
                stmt: Some(stmt),
                parse_err: None,
            }),
            Err(e) => parsed.push(Parsed {
                req,
                stmt: None,
                parse_err: Some(e.message),
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

    // Resolve each statement's table key schema (None when the table is
    // unknown; that statement's ResourceNotFound surfaces per-statement).
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

        if is_write_batch {
            let mut req = p.req;
            req.return_consumed_capacity = rcc.map(str::to_owned);
            match write::run(ctx, stmt, &req).await {
                Ok(result) => {
                    capacity.add(&result.body);
                    responses.push(json!({ "TableName": table }));
                }
                Err(e) => responses.push(StatementError::from_error(&e).into_entry(Some(&table))),
            }
        } else {
            let Statement::Select(select) = &stmt else {
                unreachable!("read batch holds only SELECT statements");
            };
            responses.push(
                execute_read(
                    ctx,
                    select,
                    &p.req,
                    &table,
                    key_infos[i].as_ref(),
                    rcc,
                    &mut capacity,
                )
                .await?,
            );
        }
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

/// Execute one read statement, returning its `Responses[i]` entry.
async fn execute_read(
    ctx: &OperationContext,
    select: &SelectStatement,
    req: &SingleRequest,
    table: &str,
    key_info: Option<&TableKeyInfo>,
    rcc: Option<&str>,
    capacity: &mut CapacityList,
) -> Result<Value, DynamoDbError> {
    // An unknown table is a per-statement ResourceNotFound (the call stays 200).
    let Some(key_info) = key_info else {
        return Ok(
            StatementError::from_error(&DynamoDbError::ResourceNotFoundException(String::new()))
                .into_entry(None),
        );
    };

    let batch_key_error = || {
        json!({
            "Error": {
                "Code": "ValidationError",
                "Message": "Select statements within BatchExecuteStatement must specify the primary key in the where clause."
            }
        })
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
                        return Ok(StatementError::from_error(&e).into_entry(None));
                    }
                }
                Err(StorageError::IndexNotFound(_)) => {
                    return Ok(
                        StatementError::from_error(&DynamoDbError::ValidationException(
                            "The table does not have the specified index".to_owned(),
                        ))
                        .into_entry(None),
                    );
                }
                Err(e) => return Err(storage_err_to_dynamo(e)),
            }
        }
        return Ok(batch_key_error());
    }

    // A multi-part base table key (this engine's preview extension) is refused
    // per statement with the service's multi-attribute message.
    if let Err(e) = common::ensure_single_attribute_keys(&key_info.key_schema) {
        return Ok(StatementError::from_error(&e).into_entry(None));
    }

    let (pk, sk) = common::key_names(key_info);
    let Some(pk) = pk else {
        return Err(DynamoDbError::InternalServerError(
            "table has no partition key".to_owned(),
        ));
    };

    // A batch SELECT must be a full-primary-key point read; a partial key is a
    // per-statement ValidationError.
    let Some(key) = common::point_read_key(select, pk, sk, &req.parameters)? else {
        return Ok(batch_key_error());
    };

    let mut get_body = json!({ "TableName": table, "Key": Value::Object(key) });
    if let Some(rcc) = rcc {
        get_body["ReturnConsumedCapacity"] = json!(rcc);
    }
    match handle_get_item(get_body, ctx).await {
        Ok(result) => {
            capacity.add(&result.body);
            let mut entry = Map::new();
            entry.insert("TableName".to_owned(), Value::String(table.to_owned()));
            if let Some(item) = result.body.get("Item") {
                // A batch SELECT with a projection list projects the item, as
                // the service does; SELECT * returns it whole.
                let shaped = match &select.projection {
                    extenddb_core::partiql::ast::Projection::All => item.clone(),
                    extenddb_core::partiql::ast::Projection::Paths(paths) => {
                        super::select::project_item(item, paths)
                    }
                };
                entry.insert("Item".to_owned(), shaped);
            }
            Ok(Value::Object(entry))
        }
        Err(e) => Ok(StatementError::from_error(&e).into_entry(Some(table))),
    }
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
        let (pk, sk) = common::key_names(key_info);
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

/// The over-limit error, whose message ends with the service's constraint
/// phrase (the service echoes every request object; the suffix is what tests
/// assert).
fn too_many_statements(len: usize) -> DynamoDbError {
    let repr = std::iter::repeat_n("BatchStatementRequest", len)
        .collect::<Vec<_>>()
        .join(", ");
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value '[{repr}]' at 'statements' failed to satisfy \
         constraint: Member must have length less than or equal to 25"
    ))
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
