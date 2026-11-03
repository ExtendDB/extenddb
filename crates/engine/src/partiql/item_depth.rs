// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The item nesting check on a PartiQL `INSERT` or `UPDATE`.
//!
//! A parameter may hold 32 maps or lists around its leaf; 33 is refused before
//! the statement is read (see `core::partiql::params`). A stored attribute may
//! hold 31, so a 32-level value passes the parameter check and meets the item
//! check, which refuses it with a shorter message than `PutItem` and
//! `UpdateItem` give the same item (measured 2026-11-02). Those operations
//! refuse a 32-level value as part of the request, before the item is read; a
//! PartiQL `UPDATE` reaches the item check only once its condition holds and
//! its update applies, so a missing item fails its condition and a `SET`
//! through a path the item does not have is the update's own error. A `SET`
//! or `REMOVE` of a key attribute is refused ahead of all of these, as it is
//! at any depth (measured 2026-11-02). In `ExecuteTransaction` the item check
//! runs with the members' conditions: two members on one item are refused
//! first, then every member reports its own reason (measured 2026-11-02).

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{Expr, ExpressionMaps, apply_update_validated, evaluate_condition};
use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{Operand, UpdateStatement};
use extenddb_core::partiql::params::NESTING_MESSAGE;
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
    extract_key,
};
use extenddb_core::validation::{
    validate_attribute_values_nesting_depth, validate_item_nesting_depth, validate_key_only,
    validate_key_sizes,
};
use extenddb_storage::error::StorageError;

use super::SingleRequest;
use super::common;
use super::write::{UpdateParts, build_update_parts};
use crate::OperationContext;
use crate::create_table::{storage_err_to_dynamo, storage_err_to_dynamo_with_ccf};
use crate::expression_helpers::{build_expression_maps, parse_condition_expr, parse_update_expr};
use crate::transact_write_helpers::validate_no_key_updates;

/// The item check's message on a PartiQL write (measured 2026-11-02).
pub(super) const ITEM_NESTING_MESSAGE: &str = "Nesting Levels have exceeded supported limits";

/// Give the engine's item nesting refusal the PartiQL wording. The engine words
/// it as `PutItem` does, and an `INSERT` is put through `PutItem`.
pub(super) fn item_nesting_error(err: DynamoDbError) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(message) if message.ends_with(NESTING_MESSAGE) => {
            DynamoDbError::ValidationException(ITEM_NESTING_MESSAGE.to_owned())
        }
        other => other,
    }
}

/// Whether a `SET` value nests as deep as an item attribute may not, so the
/// update can never be written.
pub(super) fn sets_a_value_past_the_item_limit(
    update: &UpdateStatement,
    params: &[AttributeValue],
) -> bool {
    update
        .sets
        .iter()
        .any(|action| holds_a_value_past_the_item_limit(&action.value, params))
}

/// Whether a write statement stores a value nested past the item limit: an
/// `INSERT` value, or an `UPDATE` `SET` value.
fn stores_a_value_past_the_item_limit(stmt: &Statement, params: &[AttributeValue]) -> bool {
    match stmt {
        Statement::Insert(insert) => insert
            .value
            .iter()
            .any(|(_, value)| holds_a_value_past_the_item_limit(value, params)),
        Statement::Update(update) => sets_a_value_past_the_item_limit(update, params),
        _ => false,
    }
}

fn holds_a_value_past_the_item_limit(operand: &Operand, params: &[AttributeValue]) -> bool {
    let past = |value: &AttributeValue| {
        validate_attribute_values_nesting_depth(std::iter::once(value)).is_err()
    };
    match operand {
        Operand::Literal(value) => past(value),
        Operand::Parameter(index) => params.get(*index).is_some_and(past),
        Operand::Arith(left, _, right) => {
            holds_a_value_past_the_item_limit(left, params)
                || holds_a_value_past_the_item_limit(right, params)
        }
        Operand::Function(_, args) => args
            .iter()
            .any(|arg| holds_a_value_past_the_item_limit(arg, params)),
        Operand::Path(_) => false,
    }
}

/// The answer to an `UPDATE` that [`sets_a_value_past_the_item_limit`], which
/// `UpdateItem` would refuse before reading the item. A `SET` or `REMOVE` of a
/// key attribute is refused first. The update is then evaluated as the
/// storage layer evaluates any update, against the stored item, and nothing
/// is written: a condition that fails or an update that does not apply is the
/// answer, then the item check on the updated item.
///
/// `None` when the updated item passes the item check, which happens only
/// when the deep value is not stored, such as `if_not_exists` on an attribute
/// the item has. The caller then sends the update to `UpdateItem`, whose
/// nesting check of every `ExpressionAttributeValue` refuses it, where the
/// service applies the update (measured 2026-11-02).
pub(super) async fn unwritable_update_error(
    ctx: &OperationContext,
    parts: &UpdateParts,
    req: &SingleRequest,
) -> Option<DynamoDbError> {
    match evaluate_without_writing(ctx, parts, req).await {
        Ok(item) => validate_item_nesting_depth(&item)
            .is_err()
            .then(|| DynamoDbError::ValidationException(ITEM_NESTING_MESSAGE.to_owned())),
        Err(err) => Some(err),
    }
}

/// The item an update would store, or the error it meets on the way.
async fn evaluate_without_writing(
    ctx: &OperationContext,
    parts: &UpdateParts,
    req: &SingleRequest,
) -> Result<Item, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&parts.table)
        .await
        .map_err(storage_err_to_dynamo)?;
    let key: Item = from_wire(&parts.key)?;
    validate_key_only(&key, &key_info.key_schema, &key_info.attribute_definitions)?;
    validate_key_sizes(&key, &key_info.key_schema, &ctx.limits)?;

    let names: HashMap<String, String> = from_wire(&parts.names)?;
    let values: HashMap<String, AttributeValue> = from_wire(&parts.values)?;
    let maps = build_expression_maps(Some(&names), Some(&values));
    let actions = parse_update_expr(&parts.update_expression, &ctx.limits)?;
    validate_no_key_updates(&actions, &key_info, &maps)?;
    let condition = parse_condition_expr(&parts.condition, &ctx.limits)?;

    // The condition is evaluated against the stored item, or an empty one, and
    // the update applied to the stored item, as `update_item` does.
    let on_failure =
        return_values_on_failure(req.return_values_on_condition_check_failure.as_deref());
    let stored =
        stored_item_if_condition_holds(ctx, &key_info, &key, &condition, &maps, on_failure).await?;
    let mut item = stored.unwrap_or(key);
    apply_update_validated(
        &actions,
        &mut item,
        &maps,
        &key_info.vector_indexes,
        &key_info.attribute_definitions,
    )?;
    Ok(item)
}

/// The stored item, or `None` when there is none, once `condition` holds
/// against it (an absent item is evaluated as an empty one).
///
/// # Errors
///
/// Returns the `ConditionalCheckFailedException` a write gets when the
/// condition does not hold, carrying the stored item for `ALL_OLD`.
async fn stored_item_if_condition_holds(
    ctx: &OperationContext,
    key_info: &TableKeyInfo,
    key: &Item,
    condition: &Expr,
    maps: &ExpressionMaps,
    on_failure: ReturnValuesOnConditionCheckFailure,
) -> Result<Option<Item>, DynamoDbError> {
    let stored = ctx
        .storage
        .get_item(key_info, key)
        .await
        .map_err(storage_err_to_dynamo)?;
    if evaluate_condition(condition, stored.as_ref().unwrap_or(&Item::new()), maps)? {
        Ok(stored)
    } else {
        Err(storage_err_to_dynamo_with_ccf(
            StorageError::ConditionFailed(stored),
            on_failure,
        ))
    }
}

fn return_values_on_failure(value: Option<&str>) -> ReturnValuesOnConditionCheckFailure {
    match value {
        Some("ALL_OLD") => ReturnValuesOnConditionCheckFailure::AllOld,
        _ => ReturnValuesOnConditionCheckFailure::None,
    }
}

/// Whether a member of a write transaction stores a value nested past the
/// item limit, so the transaction takes [`transaction_reasons`].
pub(super) fn has_member_past_the_item_limit(members: &[(SingleRequest, Statement)]) -> bool {
    members
        .iter()
        .any(|(req, stmt)| stores_a_value_past_the_item_limit(stmt, &req.parameters))
}

/// The reasons a write `ExecuteTransaction` is cancelled with when a member
/// would store a value nested past the item limit, or `None` when no member
/// has a reason. `items` are the members' `TransactWriteItems` translations,
/// and `is_insert` marks the `INSERT`s among them.
///
/// `TransactWriteItems` refuses such a value as part of the request. The
/// service refuses two members on one item first, then runs every member's
/// condition with the item check: a member whose condition fails reports
/// that (a duplicate key for an `INSERT`), a member whose condition holds and
/// whose value is past the limit reports the item check's refusal, and every
/// other member reports `None` (measured 2026-11-02).
///
/// # Errors
///
/// Returns the refusal of two members on one item, or an error the
/// evaluation meets that is not a member's reason.
pub(super) async fn transaction_reasons(
    ctx: &OperationContext,
    members: &[(SingleRequest, Statement)],
    items: &[Value],
    is_insert: &[bool],
) -> Result<Option<Vec<CancellationReason>>, DynamoDbError> {
    let mut targets = Vec::with_capacity(items.len());
    let mut seen = HashSet::with_capacity(items.len());
    for item in items {
        let target = MemberTarget::read(ctx, item).await?;
        if !seen.insert(target.identity()) {
            return Err(DynamoDbError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_owned(),
            ));
        }
        targets.push(target);
    }
    let mut reasons = Vec::with_capacity(members.len());
    let mut cancelled = false;
    for (((req, stmt), target), insert) in members.iter().zip(&targets).zip(is_insert) {
        let reason = member_reason(ctx, stmt, req, target, *insert).await?;
        cancelled |= reason.is_some();
        reasons.push(reason.unwrap_or_else(CancellationReason::none));
    }
    Ok(cancelled.then_some(reasons))
}

/// The reason one transaction member reports: its failed condition, or the
/// item check's refusal of a value it would store, or `None`.
async fn member_reason(
    ctx: &OperationContext,
    stmt: &Statement,
    req: &SingleRequest,
    target: &MemberTarget,
    insert: bool,
) -> Result<Option<CancellationReason>, DynamoDbError> {
    let err = match target.failed_condition(ctx).await? {
        Some(_) if insert => return Ok(Some(common::duplicate_item_reason())),
        Some(failed) => failed,
        None => match stmt {
            Statement::Insert(_) if stores_a_value_past_the_item_limit(stmt, &req.parameters) => {
                DynamoDbError::ValidationException(ITEM_NESTING_MESSAGE.to_owned())
            }
            Statement::Update(update)
                if sets_a_value_past_the_item_limit(update, &req.parameters) =>
            {
                let parts = build_update_parts(ctx, update, &req.parameters).await?;
                match unwritable_update_error(ctx, &parts, req).await {
                    Some(err) => err,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        },
    };
    Ok(Some(match super::bare_expression_error(err) {
        DynamoDbError::ConditionalCheckFailedException(_, item) => {
            CancellationReason::condition_check_failed_with_item(item)
        }
        DynamoDbError::ValidationException(message) => {
            CancellationReason::validation_error(message)
        }
        other => return Err(other),
    }))
}

/// The item a transaction member writes or checks and the condition it
/// carries, read back from its `TransactWriteItems` translation.
struct MemberTarget {
    key_info: TableKeyInfo,
    key: Item,
    condition: Option<String>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
    on_failure: ReturnValuesOnConditionCheckFailure,
}

impl MemberTarget {
    async fn read(ctx: &OperationContext, member: &Value) -> Result<Self, DynamoDbError> {
        let Some((kind, body)) = member.as_object().and_then(|m| m.iter().next()) else {
            return Err(internal("a transaction member without an operation"));
        };
        let table = body
            .get("TableName")
            .and_then(Value::as_str)
            .ok_or_else(|| internal("a transaction member without a table"))?;
        let key_info = ctx
            .table_key_info(table)
            .await
            .map_err(storage_err_to_dynamo)?;
        let key = if kind == "Put" {
            let item: Item = from_wire_field(body, "Item")?;
            extract_key(&item, &key_info.key_schema)
        } else {
            from_wire_field(body, "Key")?
        };
        Ok(Self {
            condition: body
                .get("ConditionExpression")
                .and_then(Value::as_str)
                .map(str::to_owned),
            names: from_wire_field(body, "ExpressionAttributeNames")?,
            values: from_wire_field(body, "ExpressionAttributeValues")?,
            on_failure: return_values_on_failure(
                body.get("ReturnValuesOnConditionCheckFailure")
                    .and_then(Value::as_str),
            ),
            key_info,
            key,
        })
    }

    /// The item's identity, as `TransactWriteItems` compares two members.
    fn identity(&self) -> String {
        format!(
            "{}:{}",
            self.key_info.table_name,
            serde_json::to_string(&self.key).unwrap_or_default()
        )
    }

    /// The refusal of this member's condition when it has one that does not
    /// hold against the stored item, else `None`.
    async fn failed_condition(
        &self,
        ctx: &OperationContext,
    ) -> Result<Option<DynamoDbError>, DynamoDbError> {
        let Some(text) = &self.condition else {
            return Ok(None);
        };
        let maps = build_expression_maps(Some(&self.names), Some(&self.values));
        let condition = parse_condition_expr(text, &ctx.limits)?;
        match stored_item_if_condition_holds(
            ctx,
            &self.key_info,
            &self.key,
            &condition,
            &maps,
            self.on_failure,
        )
        .await
        {
            Ok(_) => Ok(None),
            Err(err @ DynamoDbError::ConditionalCheckFailedException(..)) => Ok(Some(err)),
            Err(other) => Err(other),
        }
    }
}

/// Read back a wire-form map the translation built.
fn from_wire<T: serde::de::DeserializeOwned>(
    map: &serde_json::Map<String, Value>,
) -> Result<T, DynamoDbError> {
    serde_json::from_value(Value::Object(map.clone())).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to read a translated PartiQL map");
        internal("a translated PartiQL map")
    })
}

/// Read back one map field of a translated member, empty when it is absent.
fn from_wire_field<T: serde::de::DeserializeOwned + Default>(
    body: &Value,
    field: &str,
) -> Result<T, DynamoDbError> {
    match body.get(field) {
        Some(Value::Object(map)) => from_wire(map),
        _ => Ok(T::default()),
    }
}

fn internal(what: &str) -> DynamoDbError {
    tracing::error!(what, "failed to read back a PartiQL translation");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

#[cfg(test)]
mod tests;
