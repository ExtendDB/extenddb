// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL write executor: `INSERT`, `UPDATE`, `DELETE`.
//!
//! Drives the engine's put/update/delete handlers with the PartiQL-specific
//! semantics on top: INSERT is create-only (`DuplicateItemException`), UPDATE
//! carries an implicit existence condition (not an upsert), both require the
//! full key, extra WHERE terms become a condition expression, and `RETURNING`
//! shapes the `Items` payload. Every behaviour is justified by a measurement
//! against the live service; see the project's PartiQL behavioural spec.
//!
//! The statement is translated into the classic request shapes
//! (`PutItem`/`UpdateItem`/`DeleteItem` bodies) and handed to the existing
//! handlers, so the measured classic semantics (condition evaluation, capacity,
//! item sizing, error mapping) are reused rather than re-implemented.
//!
//! WHERE-to-expression translation lives locally here (`ExprBuilder` and the
//! key/condition split). The SELECT worktree owns the shared
//! `partiql/expr_bridge.rs`; running in parallel, this module keeps a local
//! copy and the two should be unified once both land (see report-2b-write.md).

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{
    ArithOp, CompareOp, DeleteStatement, InsertStatement, Operand, Path, PathStep, Returning,
    UpdateStatement, WhereExpr,
};
use extenddb_core::types::{AttributeValue, KeyType, ScalarAttributeType, TableKeyInfo};

use super::{SingleRequest, bind_parameter};
use crate::create_table::storage_err_to_dynamo;
use crate::{
    DispatchResult, OperationContext, handle_delete_item, handle_put_item, handle_update_item,
};

/// Execute a parsed write statement (`INSERT`/`UPDATE`/`DELETE`).
///
/// # Errors
///
/// Returns the service's error for the failure (duplicate key, failed
/// condition, missing full key, invalid returning clause), per the spec.
pub(super) async fn run(
    ctx: &OperationContext,
    stmt: Statement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    match stmt {
        Statement::Insert(insert) => run_insert(ctx, insert, req).await,
        Statement::Update(update) => run_update(ctx, update, req).await,
        Statement::Delete(delete) => run_delete(ctx, delete, req).await,
        // SELECT is dispatched to the read executor by the caller; reaching
        // here is a routing bug, not a client error.
        Statement::Select(_) => Err(DynamoDbError::InternalServerError(
            "SELECT routed to the write executor".to_owned(),
        )),
    }
}

// --- INSERT ---------------------------------------------------------------

async fn run_insert(
    ctx: &OperationContext,
    insert: InsertStatement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    let parts = build_insert_parts(ctx, &insert, &req.parameters).await?;
    let mut body = json!({
        "TableName": parts.table,
        "Item": parts.item,
        "ConditionExpression": parts.condition,
        "ExpressionAttributeNames": Value::Object(parts.names),
    });
    if let Some(rcc) = &req.return_consumed_capacity {
        body["ReturnConsumedCapacity"] = json!(rcc);
    }

    let result = handle_put_item(body, ctx).await.map_err(map_insert_error);
    reshape(result?)
}

/// The translated classic `PutItem` pieces for an `INSERT`: the item, the
/// create-only condition, and the names map it references. Shared by the
/// single-statement executor and the transaction executor (which wraps them in
/// a `TransactWriteItems` `Put` member).
pub(super) struct PutParts {
    pub table: String,
    pub item: Value,
    pub condition: String,
    pub names: Map<String, Value>,
}

/// Translate an `INSERT` into its `PutItem` pieces (create-only condition,
/// item, names), resolving `?` parameters. Does not dispatch.
///
/// # Errors
///
/// Returns the service's key-validation message when a key attribute is missing
/// or mistyped, or `ResourceNotFoundException` for a missing table.
pub(super) async fn build_insert_parts(
    ctx: &OperationContext,
    insert: &InsertStatement,
    params: &[AttributeValue],
) -> Result<PutParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&insert.table)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Build the item from the VALUE map, resolving `?` parameters to their typed
    // AttributeValues.
    let mut item: BTreeMap<String, AttributeValue> = BTreeMap::new();
    for (name, operand) in &insert.value {
        item.insert(name.clone(), resolve_av(operand, params)?);
    }

    // PartiQL names the missing or mistyped key attribute with its own message,
    // which differs from the classic PutItem key-validation message, so the
    // check runs here before the put.
    validate_insert_keys(&item, &key_info)?;

    // Create-only: a duplicate full key must fail. `attribute_not_exists` on the
    // partition key is the create-only condition; the target slot is addressed
    // by its full key, so an item with the same partition key but a different
    // sort key does not trip it.
    let (pk_name, _sk_name) = key_names(&key_info)?;
    let mut builder = ExprBuilder::default();
    let pk_ref = builder.name(pk_name);
    let condition = format!("attribute_not_exists({pk_ref})");

    Ok(PutParts {
        table: insert.table.clone(),
        item: to_json(&item)?,
        condition,
        names: builder.names,
    })
}

/// Map a create-only condition failure to the PartiQL duplicate-key error.
///
/// The only condition the INSERT path sets is `attribute_not_exists(pk)`, so a
/// `ConditionalCheckFailedException` from it means the full key already exists.
fn map_insert_error(e: DynamoDbError) -> DynamoDbError {
    match e {
        DynamoDbError::ConditionalCheckFailedException(..) => {
            DynamoDbError::DuplicateItemException(
                "Duplicate primary key exists in table".to_owned(),
            )
        }
        other => other,
    }
}

// --- UPDATE ---------------------------------------------------------------

async fn run_update(
    ctx: &OperationContext,
    update: UpdateStatement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    let parts = build_update_parts(ctx, &update, &req.parameters).await?;

    let mut body = json!({
        "TableName": parts.table,
        "Key": Value::Object(parts.key),
        "UpdateExpression": parts.update_expression,
        "ConditionExpression": parts.condition,
    });
    if !parts.names.is_empty() {
        body["ExpressionAttributeNames"] = Value::Object(parts.names);
    }
    if !parts.values.is_empty() {
        body["ExpressionAttributeValues"] = Value::Object(parts.values);
    }
    if let Some(rv) = returning_to_return_values(update.returning) {
        body["ReturnValues"] = json!(rv);
    }
    if let Some(rcc) = &req.return_consumed_capacity {
        body["ReturnConsumedCapacity"] = json!(rcc);
    }
    if let Some(rvoccf) = &req.return_values_on_condition_check_failure {
        body["ReturnValuesOnConditionCheckFailure"] = json!(rvoccf);
    }

    reshape(handle_update_item(body, ctx).await?)
}

/// The translated classic `UpdateItem` pieces for an `UPDATE`: the key, the
/// update expression, the implicit-existence-plus-residual condition, and the
/// names/values maps. Shared by the single-statement and transaction executors.
pub(super) struct UpdateParts {
    pub table: String,
    pub key: Map<String, Value>,
    pub update_expression: String,
    pub condition: String,
    pub names: Map<String, Value>,
    pub values: Map<String, Value>,
}

/// Translate an `UPDATE` into its `UpdateItem` pieces. Does not dispatch.
///
/// # Errors
///
/// Returns the service's full-key message when the WHERE does not pin the whole
/// primary key, or `ResourceNotFoundException` for a missing table.
pub(super) async fn build_update_parts(
    ctx: &OperationContext,
    update: &UpdateStatement,
    params: &[AttributeValue],
) -> Result<UpdateParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&update.table)
        .await
        .map_err(storage_err_to_dynamo)?;
    let (pk_name, sk_name) = key_names(&key_info)?;

    let (key, residual) = split_key_where(&update.where_clause, pk_name, sk_name, params)?;

    let mut builder = ExprBuilder::default();

    // SET first, then REMOVE, matching the statement order the service accepts.
    let mut set_parts = Vec::new();
    for action in &update.sets {
        let path = builder.path(&action.path);
        let value = builder.operand(&action.value, params)?;
        set_parts.push(format!("{path} = {value}"));
    }
    let mut remove_parts = Vec::new();
    for path in &update.removes {
        remove_parts.push(builder.path(path));
    }
    let mut update_expression = String::new();
    if !set_parts.is_empty() {
        update_expression.push_str("SET ");
        update_expression.push_str(&set_parts.join(", "));
    }
    if !remove_parts.is_empty() {
        if !update_expression.is_empty() {
            update_expression.push(' ');
        }
        update_expression.push_str("REMOVE ");
        update_expression.push_str(&remove_parts.join(", "));
    }

    // UPDATE is not an upsert: an implicit existence condition on the partition
    // key makes a missing item fail with ConditionalCheckFailedException. Any
    // residual WHERE term is ANDed on as the caller's condition.
    let pk_ref = builder.name(pk_name);
    let condition = match build_condition(&mut builder, &residual, params)? {
        Some(residual_expr) => format!("attribute_exists({pk_ref}) AND ({residual_expr})"),
        None => format!("attribute_exists({pk_ref})"),
    };

    Ok(UpdateParts {
        table: update.table.clone(),
        key,
        update_expression,
        condition,
        names: builder.names,
        values: builder.values,
    })
}

// --- DELETE ---------------------------------------------------------------

async fn run_delete(
    ctx: &OperationContext,
    delete: DeleteStatement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    // DELETE allows only `RETURNING ALL OLD *`; any other form is rejected with
    // the clause named verbatim.
    if let Some(returning) = delete.returning
        && returning != Returning::AllOld
    {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid returning clause: RETURNING {}. \
             Only RETURNING ALL OLD * is allowed in DELETE statements.",
            returning_clause_text(returning)
        )));
    }

    let parts = build_delete_parts(ctx, &delete, &req.parameters).await?;

    let mut body = json!({
        "TableName": parts.table,
        "Key": Value::Object(parts.key),
    });
    if let Some(condition) = parts.condition {
        body["ConditionExpression"] = json!(condition);
    }
    if !parts.names.is_empty() {
        body["ExpressionAttributeNames"] = Value::Object(parts.names);
    }
    if !parts.values.is_empty() {
        body["ExpressionAttributeValues"] = Value::Object(parts.values);
    }
    if matches!(delete.returning, Some(Returning::AllOld)) {
        body["ReturnValues"] = json!("ALL_OLD");
    }
    if let Some(rcc) = &req.return_consumed_capacity {
        body["ReturnConsumedCapacity"] = json!(rcc);
    }
    if let Some(rvoccf) = &req.return_values_on_condition_check_failure {
        body["ReturnValuesOnConditionCheckFailure"] = json!(rvoccf);
    }

    reshape(handle_delete_item(body, ctx).await?)
}

/// The translated classic `DeleteItem` pieces for a `DELETE`: the key, an
/// optional residual condition, and the names/values maps. Shared by the
/// single-statement and transaction executors.
pub(super) struct DeleteParts {
    pub table: String,
    pub key: Map<String, Value>,
    pub condition: Option<String>,
    pub names: Map<String, Value>,
    pub values: Map<String, Value>,
}

/// Translate a `DELETE` into its `DeleteItem` pieces. Does not validate the
/// `RETURNING` clause (the single-statement path does that) and does not
/// dispatch.
///
/// # Errors
///
/// Returns the service's full-key message when the WHERE does not pin the whole
/// primary key, or `ResourceNotFoundException` for a missing table.
pub(super) async fn build_delete_parts(
    ctx: &OperationContext,
    delete: &DeleteStatement,
    params: &[AttributeValue],
) -> Result<DeleteParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&delete.table)
        .await
        .map_err(storage_err_to_dynamo)?;
    let (pk_name, sk_name) = key_names(&key_info)?;

    let (key, residual) = split_key_where(&delete.where_clause, pk_name, sk_name, params)?;

    // DELETE has no implicit existence condition: deleting a missing item is a
    // silent success. Only a residual WHERE term becomes a condition.
    let mut builder = ExprBuilder::default();
    let condition = build_condition(&mut builder, &residual, params)?;

    Ok(DeleteParts {
        table: delete.table.clone(),
        key,
        condition,
        names: builder.names,
        values: builder.values,
    })
}

// --- response shaping -----------------------------------------------------

/// Reshape a classic put/update/delete response into the PartiQL write shape:
/// a `RETURNING` item becomes the single element of `Items`, an absent one
/// leaves `Items` empty, and `ConsumedCapacity` passes through when present.
fn reshape(result: DispatchResult) -> Result<DispatchResult, DynamoDbError> {
    let mut items: Vec<Value> = Vec::new();
    if let Some(attributes) = result.body.get("Attributes") {
        items.push(attributes.clone());
    }
    let mut body = json!({ "Items": items });
    if let Some(cc) = result.body.get("ConsumedCapacity") {
        body["ConsumedCapacity"] = cc.clone();
    }
    Ok(DispatchResult {
        body,
        metrics: result.metrics,
    })
}

// --- key / condition translation -----------------------------------------

/// The partition and (optional) sort key attribute names of the target table.
///
/// A multi-part base table key (this engine's preview extension) is refused
/// with the service's multi-attribute message before any write is attempted.
fn key_names(key_info: &TableKeyInfo) -> Result<(&str, Option<&str>), DynamoDbError> {
    super::common::ensure_single_attribute_keys(&key_info.key_schema)?;
    let mut partition = None;
    let mut sort = None;
    for element in &key_info.key_schema {
        match element.key_type {
            KeyType::Hash => partition = Some(element.attribute_name.as_str()),
            KeyType::Range => sort = Some(element.attribute_name.as_str()),
        }
    }
    let partition = partition.ok_or_else(|| {
        DynamoDbError::InternalServerError("table has no partition key".to_owned())
    })?;
    Ok((partition, sort))
}

/// Split a write's WHERE into the full primary key (returned in wire form) and
/// the residual condition terms. Fails with the service's full-key message when
/// the key is not pinned by equality.
fn split_key_where<'a>(
    where_clause: &'a WhereExpr,
    pk_name: &str,
    sk_name: Option<&str>,
    params: &[AttributeValue],
) -> Result<(Map<String, Value>, Vec<&'a WhereExpr>), DynamoDbError> {
    let mut terms = Vec::new();
    flatten_and(where_clause, &mut terms);

    let mut pk_value: Option<&Operand> = None;
    let mut sk_value: Option<&Operand> = None;
    let mut residual = Vec::new();
    for term in terms {
        if pk_value.is_none()
            && let Some(rhs) = key_equality(term, pk_name)
        {
            pk_value = Some(rhs);
            continue;
        }
        if let Some(sort) = sk_name
            && sk_value.is_none()
            && let Some(rhs) = key_equality(term, sort)
        {
            sk_value = Some(rhs);
            continue;
        }
        residual.push(term);
    }

    let full_key = pk_value.is_some() && (sk_name.is_none() || sk_value.is_some());
    if !full_key {
        return Err(DynamoDbError::ValidationException(
            "Where clause does not contain a mandatory equality on all key attributes".to_owned(),
        ));
    }

    let mut key = Map::new();
    key.insert(
        pk_name.to_owned(),
        to_json_value(&resolve_av(pk_value.unwrap(), params)?)?,
    );
    if let (Some(sort), Some(value)) = (sk_name, sk_value) {
        key.insert(sort.to_owned(), to_json_value(&resolve_av(value, params)?)?);
    }
    Ok((key, residual))
}

/// If `term` is `<keyname> = <literal-or-parameter>`, return the right operand.
fn key_equality<'a>(term: &'a WhereExpr, key_name: &str) -> Option<&'a Operand> {
    if let WhereExpr::Compare(Operand::Path(path), CompareOp::Eq, rhs) = term
        && path.steps.is_empty()
        && path.root == key_name
        && matches!(rhs, Operand::Literal(_) | Operand::Parameter(_))
    {
        return Some(rhs);
    }
    None
}

/// Flatten the top-level `AND` spine into its maximal non-`AND` terms. An
/// `OR`/`NOT` at the top becomes a single term (and so is never a key equality,
/// which is what forces the full-key error for those shapes).
fn flatten_and<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) {
    match expr {
        WhereExpr::And(a, b) => {
            flatten_and(a, out);
            flatten_and(b, out);
        }
        other => out.push(other),
    }
}

/// Build a condition expression from residual WHERE terms, or `None` when there
/// are none. Each term is a self-contained predicate, so multiple terms are
/// ANDed with each wrapped in parentheses.
fn build_condition(
    builder: &mut ExprBuilder,
    residual: &[&WhereExpr],
    params: &[AttributeValue],
) -> Result<Option<String>, DynamoDbError> {
    if residual.is_empty() {
        return Ok(None);
    }
    if residual.len() == 1 {
        return Ok(Some(builder.condition(residual[0], params)?));
    }
    let mut parts = Vec::new();
    for term in residual {
        parts.push(format!("({})", builder.condition(term, params)?));
    }
    Ok(Some(parts.join(" AND ")))
}

// --- expression builder ---------------------------------------------------

/// Allocates `#name` / `:value` placeholders and the maps the classic handlers
/// expect, shared across a statement's update and condition expressions.
#[derive(Default)]
struct ExprBuilder {
    names: Map<String, Value>,
    values: Map<String, Value>,
    next_name: usize,
    next_value: usize,
}

impl ExprBuilder {
    /// Allocate a name placeholder for a real attribute name.
    fn name(&mut self, real: &str) -> String {
        let placeholder = format!("#n{}", self.next_name);
        self.next_name += 1;
        self.names
            .insert(placeholder.clone(), Value::String(real.to_owned()));
        placeholder
    }

    /// Allocate a value placeholder for a typed AttributeValue.
    fn value(&mut self, av: &AttributeValue) -> Result<String, DynamoDbError> {
        let placeholder = format!(":v{}", self.next_value);
        self.next_value += 1;
        self.values.insert(placeholder.clone(), to_json_value(av)?);
        Ok(placeholder)
    }

    /// Translate a document path to placeholder form (`#n0.#n1[2]`).
    fn path(&mut self, path: &Path) -> String {
        let mut rendered = self.name(&path.root);
        for step in &path.steps {
            match step {
                PathStep::Attribute(attr) => {
                    rendered.push('.');
                    rendered.push_str(&self.name(attr));
                }
                PathStep::Index(index) => {
                    rendered.push_str(&format!("[{index}]"));
                }
            }
        }
        rendered
    }

    /// Translate an operand to expression text.
    fn operand(
        &mut self,
        operand: &Operand,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        Ok(match operand {
            Operand::Literal(av) => self.value(av)?,
            Operand::Parameter(index) => {
                let av = bind_parameter(params, *index)?;
                self.value(&av)?
            }
            Operand::Path(path) => self.path(path),
            Operand::Arith(left, op, right) => {
                let symbol = match op {
                    ArithOp::Add => "+",
                    ArithOp::Sub => "-",
                };
                // The service type-checks an inline literal operand of an
                // arithmetic expression up front: a non-number literal is an
                // operand-type error naming the operator and the type, distinct
                // from the generic runtime message a string ATTRIBUTE operand
                // produces in the classic evaluator.
                for side in [left.as_ref(), right.as_ref()] {
                    if let Operand::Literal(av) = side
                        && !matches!(av, AttributeValue::N(_))
                    {
                        return Err(DynamoDbError::ValidationException(format!(
                            "Incorrect operand type for operator or function; operator or function: {symbol}, operand type: {}",
                            av_type_label(av)
                        )));
                    }
                }
                let left = self.operand(left, params)?;
                let right = self.operand(right, params)?;
                format!("{left} {symbol} {right}")
            }
            Operand::Function(name, args) => {
                let mut rendered = Vec::new();
                for arg in args {
                    rendered.push(self.operand(arg, params)?);
                }
                format!("{}({})", name, rendered.join(", "))
            }
        })
    }

    /// Translate a boolean WHERE/condition expression to expression text.
    fn condition(
        &mut self,
        expr: &WhereExpr,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        Ok(match expr {
            WhereExpr::Compare(left, op, right) => {
                let symbol = match op {
                    CompareOp::Eq => "=",
                    CompareOp::Ne => "<>",
                    CompareOp::Lt => "<",
                    CompareOp::Le => "<=",
                    CompareOp::Gt => ">",
                    CompareOp::Ge => ">=",
                };
                let left = self.operand(left, params)?;
                let right = self.operand(right, params)?;
                format!("{left} {symbol} {right}")
            }
            WhereExpr::Between(value, low, high) => {
                let value = self.operand(value, params)?;
                let low = self.operand(low, params)?;
                let high = self.operand(high, params)?;
                format!("{value} BETWEEN {low} AND {high}")
            }
            WhereExpr::In(value, items) => {
                let value = self.operand(value, params)?;
                let mut rendered = Vec::new();
                for item in items {
                    rendered.push(self.operand(item, params)?);
                }
                format!("{value} IN ({})", rendered.join(", "))
            }
            WhereExpr::BeginsWith(value, prefix) => {
                let value = self.operand(value, params)?;
                let prefix = self.operand(prefix, params)?;
                format!("begins_with({value}, {prefix})")
            }
            WhereExpr::Contains(value, operand) => {
                let value = self.operand(value, params)?;
                let operand = self.operand(operand, params)?;
                format!("contains({value}, {operand})")
            }
            WhereExpr::AttributeType(value, kind) => {
                let value = self.operand(value, params)?;
                let kind = self.operand(kind, params)?;
                format!("attribute_type({value}, {kind})")
            }
            WhereExpr::Function(name, args) => {
                let mut rendered = Vec::new();
                for arg in args {
                    rendered.push(self.operand(arg, params)?);
                }
                format!("{}({})", name, rendered.join(", "))
            }
            WhereExpr::IsMissing(path, missing) => {
                let rendered = self.path(path);
                if *missing {
                    format!("attribute_not_exists({rendered})")
                } else {
                    format!("attribute_exists({rendered})")
                }
            }
            WhereExpr::And(a, b) => {
                let a = self.condition(a, params)?;
                let b = self.condition(b, params)?;
                format!("({a}) AND ({b})")
            }
            WhereExpr::Or(a, b) => {
                let a = self.condition(a, params)?;
                let b = self.condition(b, params)?;
                format!("({a}) OR ({b})")
            }
            WhereExpr::Not(inner) => {
                let inner = self.condition(inner, params)?;
                format!("NOT ({inner})")
            }
        })
    }
}

// --- helpers --------------------------------------------------------------

/// Resolve an operand that must be a literal or a positional parameter to its
/// AttributeValue (used for INSERT item values and WHERE key equalities).
pub(super) fn resolve_av(
    operand: &Operand,
    params: &[AttributeValue],
) -> Result<AttributeValue, DynamoDbError> {
    match operand {
        Operand::Literal(av) => Ok(av.clone()),
        Operand::Parameter(index) => bind_parameter(params, *index),
        _ => Err(DynamoDbError::ValidationException(
            "Unsupported value expression".to_owned(),
        )),
    }
}

/// Validate that every key attribute is present in an INSERT item and typed to
/// match the table schema, with the PartiQL-specific messages.
fn validate_insert_keys(
    item: &BTreeMap<String, AttributeValue>,
    key_info: &TableKeyInfo,
) -> Result<(), DynamoDbError> {
    for element in &key_info.key_schema {
        let name = &element.attribute_name;
        let Some(value) = item.get(name) else {
            return Err(DynamoDbError::ValidationException(format!(
                "Key attribute should be present in the item: Key {name}"
            )));
        };
        if let Some(definition) = key_info
            .attribute_definitions
            .iter()
            .find(|d| &d.attribute_name == name)
        {
            let matches = match definition.attribute_type {
                ScalarAttributeType::S => matches!(value, AttributeValue::S(_)),
                ScalarAttributeType::N => matches!(value, AttributeValue::N(_)),
                ScalarAttributeType::B => matches!(value, AttributeValue::B(_)),
            };
            if !matches {
                return Err(DynamoDbError::ValidationException(format!(
                    "Key attribute's data type should match its data type in table's schema: Key {name}"
                )));
            }
        }
    }
    Ok(())
}

/// Map a `RETURNING` mode to the classic `ReturnValues` string, or `None` when
/// the statement has no `RETURNING` clause.
fn returning_to_return_values(returning: Option<Returning>) -> Option<&'static str> {
    returning.map(|mode| match mode {
        Returning::AllOld => "ALL_OLD",
        Returning::ModifiedOld => "UPDATED_OLD",
        Returning::AllNew => "ALL_NEW",
        Returning::ModifiedNew => "UPDATED_NEW",
    })
}

/// The clause text as it appears in the DELETE invalid-returning message.
fn returning_clause_text(returning: Returning) -> &'static str {
    match returning {
        Returning::AllOld => "ALL OLD *",
        Returning::ModifiedOld => "MODIFIED OLD *",
        Returning::AllNew => "ALL NEW *",
        Returning::ModifiedNew => "MODIFIED NEW *",
    }
}

/// Serialize an item map to the DynamoDB JSON wire form.
fn to_json(item: &BTreeMap<String, AttributeValue>) -> Result<Value, DynamoDbError> {
    serde_json::to_value(item).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to serialize PartiQL item");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })
}

/// Serialize a single AttributeValue to the DynamoDB JSON wire form.
pub(super) fn to_json_value(av: &AttributeValue) -> Result<Value, DynamoDbError> {
    serde_json::to_value(av).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to serialize PartiQL value");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })
}

/// The DynamoDB type label of an attribute value, for the arithmetic
/// operand-type error ("operand type: S").
fn av_type_label(av: &AttributeValue) -> &'static str {
    match av {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}
