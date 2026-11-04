// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The UPDATE statement: an UpdateItem of an item that exists, and the checks of its paths.

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{
    Path, PathStep, SetMembersAction, SetMembersOp, UpdateStatement,
};
use extenddb_core::types::AttributeValue;

use super::expr_builder::ExprBuilder;
use super::{
    build_condition, full_key_error, key_names, reshape, resolve_av, returning_to_return_values,
    split_key_where,
};
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::authorize::Gate;
use crate::partiql::{SingleRequest, common, item_depth};
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
