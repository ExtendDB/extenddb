// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The INSERT statement: a create-only PutItem.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::InsertStatement;
use extenddb_core::types::AttributeValue;

use super::expr_builder::ExprBuilder;
use super::{key_names, reshape, resolve_av, to_json, validate_insert_keys};
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::authorize::Gate;
use crate::partiql::{SingleRequest, common, item_depth};
use crate::{DispatchResult, OperationContext, handle_put_item};

// --- INSERT ---------------------------------------------------------------

pub(super) async fn run_insert(
    ctx: &OperationContext,
    insert: &InsertStatement,
    req: &SingleRequest,
    gate: &mut Gate<'_>,
) -> Result<DispatchResult, DynamoDbError> {
    let parts = build_insert_parts(ctx, insert, &req.parameters).await?;
    gate.pass().await?;
    let mut body = json!({
        "TableName": parts.table,
        "Item": parts.item,
        "ConditionExpression": parts.condition,
        "ExpressionAttributeNames": Value::Object(parts.names),
    });
    if let Some(rcc) = &req.return_consumed_capacity {
        body["ReturnConsumedCapacity"] = json!(rcc);
    }

    let result = handle_put_item(body, ctx).await.map_err(|e| {
        crate::partiql::bare_expression_error(item_depth::item_nesting_error(map_insert_error(e)))
    });
    reshape(result?)
}

/// The translated classic `PutItem` pieces for an `INSERT`: the item, the
/// create-only condition, and the names map it references. Shared by the
/// single-statement executor and the transaction executor (which wraps them in
/// a `TransactWriteItems` `Put` member).
pub(crate) struct PutParts {
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
pub(crate) async fn build_insert_parts(
    ctx: &OperationContext,
    insert: &InsertStatement,
    params: &[AttributeValue],
) -> Result<PutParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&insert.table)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Build the item from the VALUE map, resolving `?` parameters to their typed
    // AttributeValues. The values are the ones authorization read the key
    // from (see `common::insert_values`).
    let mut item: BTreeMap<String, AttributeValue> = BTreeMap::new();
    for (name, operand) in common::insert_values(insert) {
        item.insert(name.to_owned(), resolve_av(operand, params)?);
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
