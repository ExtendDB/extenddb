// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The UPDATE statement: an UpdateItem of an item that exists, and the checks of its paths.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, UpdateAction};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::partiql::ast::{
    Path, PathStep, SetMembersAction, SetMembersOp, UpdateStatement,
};
use extenddb_core::types::{AttributeValue, TableKeyInfo};
use extenddb_core::validation::validate_index_key_literal_operands;

use super::expr_builder::ExprBuilder;
use super::{
    build_condition, from_wire, full_key_error, index_keys, key_names, reshape, resolve_av,
    returning_to_return_values, split_key_where,
};
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::{build_expression_maps, parse_update_expr};
use crate::partiql::authorize::Gate;
use crate::partiql::{SingleRequest, common, item_depth};
use crate::transact_write_helpers::validate_no_key_updates;
use crate::update_item::handle_update_item_with;
use crate::{DispatchResult, OperationContext};

// --- UPDATE ---------------------------------------------------------------

pub(super) async fn run_update(
    ctx: &OperationContext,
    update: &UpdateStatement,
    req: &SingleRequest,
    gate: &mut Gate<'_>,
) -> Result<DispatchResult, DynamoDbError> {
    let parts = build_update_parts(ctx, update, &req.parameters).await?;
    // The item check below reads the item.
    gate.pass().await?;
    // A path into a key attribute among the WHERE's other conditions is
    // refused after the refusals of the `UpdateItem` request, which the
    // update below gives: a SET or REMOVE of a key attribute, then a
    // secondary index key given a value of the wrong type (measured
    // 2026-11-05).
    if let Some(refusal) = &parts.condition_key_refusal {
        let key_info = ctx
            .table_key_info(&update.table)
            .await
            .map_err(storage_err_to_dynamo)?;
        if update_refusal(&ctx.limits, &parts, &key_info)?.is_none() {
            return Err(DynamoDbError::ValidationException(refusal.clone()));
        }
    }
    if item_depth::sets_a_value_past_the_item_limit(update, &req.parameters)
        && let Some(err) = item_depth::unwritable_update_error(ctx, &parts, req).await
    {
        return Err(crate::partiql::bare_expression_error(err));
    }

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

    // The service sizes an UPDATE by its finished item alone, not by the
    // statement as it sizes UpdateItem (measured 2026-11-04).
    let finished_item_sizing = true;
    let mut result = handle_update_item_with(body, ctx, finished_item_sizing)
        .await
        .map_err(crate::partiql::bare_expression_error)?;
    crate::partiql::returning::project_modified(&mut result.body, update)?;
    Ok(reshape(result))
}

/// The translated classic `UpdateItem` pieces for an `UPDATE`: the key, the
/// update expression, the implicit-existence-plus-residual condition, and the
/// names/values maps. Shared by the single-statement and transaction executors.
pub(crate) struct UpdateParts {
    pub table: String,
    pub key: Map<String, Value>,
    pub update_expression: String,
    pub condition: String,
    pub names: Map<String, Value>,
    pub values: Map<String, Value>,
    /// The refusal of a path into a key attribute among the WHERE's other
    /// conditions (see `common::condition_key_refusal`), which the
    /// statement gets once it is authorized.
    pub condition_key_refusal: Option<String>,
}

/// The refusal `UpdateItem` gives the request of `parts` on the table
/// `key_info` before it reads the item, in its words and in its order: a
/// SET or REMOVE of a key attribute, then a secondary index key given a
/// value of the wrong type. A statement is refused for a path into a key
/// attribute among its other conditions only after these (measured
/// 2026-11-05 on `ExecuteStatement` and `BatchExecuteStatement`).
///
/// # Errors
///
/// The error a classic request gets for a value of the request that does
/// not read back (see `from_wire`), or for an update expression its parser
/// refuses. The statement checks refuse both kinds of statement first.
pub(crate) fn update_refusal(
    limits: &LimitsConfig,
    parts: &UpdateParts,
    key_info: &TableKeyInfo,
) -> Result<Option<DynamoDbError>, DynamoDbError> {
    let (actions, maps) = request_actions(limits, parts)?;
    Ok(validate_no_key_updates(&actions, key_info, &maps)
        .and_then(|()| check_index_keys(&actions, key_info, &maps))
        .err())
}

/// The second refusal of [`update_refusal`]: the request of `parts` gives a
/// secondary index key a value of the wrong type.
///
/// # Errors
///
/// The errors of [`update_refusal`].
pub(crate) fn update_index_key_refusal(
    limits: &LimitsConfig,
    parts: &UpdateParts,
    key_info: &TableKeyInfo,
) -> Result<Option<DynamoDbError>, DynamoDbError> {
    let (actions, maps) = request_actions(limits, parts)?;
    Ok(check_index_keys(&actions, key_info, &maps).err())
}

/// The actions and the expression maps of the request of `parts`, read as
/// `UpdateItem` reads them.
fn request_actions(
    limits: &LimitsConfig,
    parts: &UpdateParts,
) -> Result<(Vec<UpdateAction>, ExpressionMaps), DynamoDbError> {
    let names: HashMap<String, String> = from_wire(&parts.names)?;
    let values: HashMap<String, AttributeValue> = from_wire(&parts.values)?;
    let maps = build_expression_maps(Some(&names), Some(&values));
    Ok((parse_update_expr(&parts.update_expression, limits)?, maps))
}

/// Refuse an action that gives a secondary index key of the table a value
/// of the wrong type as a literal operand, as `UpdateItem` does.
fn check_index_keys(
    actions: &[UpdateAction],
    key_info: &TableKeyInfo,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    validate_index_key_literal_operands(
        actions,
        &index_keys(key_info),
        &key_info.attribute_definitions,
        maps,
    )
}

/// Translate an `UPDATE` into its `UpdateItem` pieces. Does not dispatch.
///
/// # Errors
///
/// Returns the service's full-key message when the WHERE does not pin the whole
/// primary key, or `ResourceNotFoundException` for a missing table.
pub(crate) async fn build_update_parts(
    ctx: &OperationContext,
    update: &UpdateStatement,
    params: &[AttributeValue],
) -> Result<UpdateParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&update.table)
        .await
        .map_err(storage_err_to_dynamo)?;
    // Only the FROM-first form can omit WHERE. It is refused like an UPDATE
    // without the full key, after a missing table (measured 2026-11-02).
    let where_clause = update.where_clause.as_ref().ok_or_else(full_key_error)?;
    let (pk_name, sk_name) = key_names(&key_info)?;

    let (key, residual) = split_key_where(where_clause, pk_name, sk_name, params)?;
    common::check_key_types(&key, &key_info)?;
    let condition_key_refusal =
        common::condition_key_refusal(where_clause, pk_name, sk_name, params);

    let mut builder = ExprBuilder::default();

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
    let (add_parts, delete_parts) = set_member_parts(&mut builder, &update.set_members, params)?;
    let update_expression = [
        ("SET", set_parts),
        ("REMOVE", remove_parts),
        ("ADD", add_parts),
        ("DELETE", delete_parts),
    ]
    .into_iter()
    .filter(|(_, parts)| !parts.is_empty())
    .map(|(clause, parts)| format!("{clause} {}", parts.join(", ")))
    .collect::<Vec<_>>()
    .join(" ");

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
        condition_key_refusal,
    })
}

/// Refuse an `UPDATE` whose targets name one document path twice, a path and
/// a path inside it, or one element both as a map member and as a list
/// element. The targets are taken in this order: the `SET` targets, the
/// attributes `set_add` and `set_delete` assign, then the `REMOVE` paths, and
/// each is compared with the ones before it. Nothing is read or written
/// (measured 2026-11-02: `SET q = 1, q = 2`, `SET q = 1 REMOVE q`, `set_add`
/// twice on one set, a `SET` beside a `set_add` of the same attribute, and
/// `SET m = {...}, m.a = 2`, with and without `RETURNING`; measured
/// 2026-11-03: `SET a.b = 1, a[0] = 2` on an item where a is a map, a list or
/// absent, and on a missing item).
///
/// The message names the later of the two paths first, which gives the
/// measured `path one: [m, a], path two: [m]` for `SET m = {...}, m.a = 2`
/// and `path one: [a, [0]], path two: [a, b]` for `SET a.b = 1, a[0] = 2`;
/// the order with the child path first was not measured.
///
/// # Errors
///
/// Returns the service's message for the first such pair.
pub(crate) fn check_overlapping_paths(update: &UpdateStatement) -> Result<(), String> {
    let paths: Vec<&Path> = update
        .sets
        .iter()
        .map(|action| &action.path)
        .chain(update.set_members.iter().map(|action| &action.path))
        .chain(&update.removes)
        .collect();
    first_overlap(&paths)
}

/// Refuse the first of `paths` that names one document path with a path
/// before it, a path inside it or around it, or one element both as a map
/// member and as a list element, naming the later path first (see
/// [`check_overlapping_paths`]). A SELECT's projected paths get this refusal
/// too (measured 2026-11-05: `m.k, m` in `ExecuteStatement`,
/// `BatchExecuteStatement` and `ExecuteTransaction`, `m, m.k` and `l[1], l`
/// in `ExecuteTransaction`).
///
/// # Errors
///
/// Returns the service's message for the first such pair.
pub(crate) fn first_overlap(paths: &[&Path]) -> Result<(), String> {
    for (i, later) in paths.iter().enumerate() {
        if let Some((earlier, meeting)) = paths[..i]
            .iter()
            .find_map(|earlier| meeting(earlier, later).map(|meeting| (earlier, meeting)))
        {
            return Err(format!(
                "Two document paths {meeting} with each other; must remove or rewrite one of \
                 these paths; path one: {}, path two: {}",
                path_list(later),
                path_list(earlier)
            ));
        }
    }
    Ok(())
}

/// How two paths meet, as the refusal words it: `overlap` when one is the
/// other or leads into it, `conflict` when they part at one element that one
/// names as a map member and the other as a list element.
fn meeting(a: &Path, b: &Path) -> Option<&'static str> {
    if a.root != b.root {
        return None;
    }
    for (x, y) in a.steps.iter().zip(&b.steps) {
        match (x, y) {
            _ if x == y => {}
            (PathStep::Attribute(_), PathStep::Index(_))
            | (PathStep::Index(_), PathStep::Attribute(_)) => return Some("conflict"),
            _ => return None,
        }
    }
    Some("overlap")
}

/// A path as the overlap message lists it: `[m, a]`, with an index as `[0]`.
fn path_list(path: &Path) -> String {
    let mut parts = vec![path.root.clone()];
    for step in &path.steps {
        parts.push(match step {
            PathStep::Attribute(name) => name.clone(),
            PathStep::Index(index) => format!("[{index}]"),
        });
    }
    format!("[{}]", parts.join(", "))
}

/// The classic `ADD` and `DELETE` actions for `set_add` and `set_delete`
/// assignments: the service adds the members to the set, or removes them, the
/// way those actions do (measured 2026-11-02). A `?` parameter has to bind to
/// a set; any other value gets the refusal a literal that is not a set gets.
fn set_member_parts(
    builder: &mut ExprBuilder,
    actions: &[SetMembersAction],
    params: &[AttributeValue],
) -> Result<(Vec<String>, Vec<String>), DynamoDbError> {
    let mut add_parts = Vec::new();
    let mut delete_parts = Vec::new();
    for action in actions {
        let members = resolve_av(&action.members, params)?;
        if !matches!(
            members,
            AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
        ) {
            return Err(DynamoDbError::ValidationException(
                action.op.not_a_set_message(&action.first_argument_at),
            ));
        }
        let path = builder.path(&action.path);
        let value = builder.value(&members)?;
        match action.op {
            SetMembersOp::Add => add_parts.push(format!("{path} {value}")),
            SetMembersOp::Delete => delete_parts.push(format!("{path} {value}")),
        }
    }
    Ok((add_parts, delete_parts))
}
