// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The DELETE statement: a DeleteItem of one item.

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{DeleteStatement, Returning, WhereExpr};
use extenddb_core::types::AttributeValue;

use super::expr_builder::ExprBuilder;
use super::{build_condition, invalid_delete_returning, key_names, reshape, split_key_where};
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::{SingleRequest, common};
use crate::{DispatchResult, OperationContext, handle_delete_item};

// --- DELETE ---------------------------------------------------------------

pub(super) async fn run_delete(
    ctx: &OperationContext,
    delete: DeleteStatement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    // DELETE allows only `RETURNING ALL OLD *`; any other form is rejected with
    // the clause named verbatim.
    if let Some(returning) = delete.returning
        && returning != Returning::AllOld
    {
        return Err(invalid_delete_returning(returning));
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

    reshape(
        handle_delete_item(body, ctx)
            .await
            .map_err(crate::partiql::bare_expression_error)?,
    )
}

/// The translated classic `DeleteItem` pieces for a `DELETE`: the key, an
/// optional residual condition, and the names/values maps. Shared by the
/// single-statement and transaction executors.
pub(crate) struct DeleteParts {
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
pub(crate) async fn build_delete_parts(
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
    common::check_key_types(&key, &key_info)?;

    let mut builder = ExprBuilder::default();
    let condition = delete_condition(&mut builder, pk_name, &residual, params)?;

    Ok(DeleteParts {
        table: delete.table.clone(),
        key,
        condition,
        names: builder.names,
        values: builder.values,
    })
}

/// The condition a `DELETE` carries, or `None` when its WHERE is the key alone.
///
/// DELETE has no implicit existence condition: deleting a missing item is a
/// silent success. A residual WHERE term is checked only against an item that
/// exists, so a missing key with a non-key predicate is a silent no-op too,
/// not a failed condition (measured 2026-11-02). `attribute_not_exists` on the
/// partition key is true exactly when the key is missing, which lets the
/// classic handler express that in one condition.
fn delete_condition(
    builder: &mut ExprBuilder,
    pk_name: &str,
    residual: &[&WhereExpr],
    params: &[AttributeValue],
) -> Result<Option<String>, DynamoDbError> {
    let Some(residual_expr) = build_condition(builder, residual, params)? else {
        return Ok(None);
    };
    let pk_ref = builder.name(pk_name);
    Ok(Some(format!(
        "attribute_not_exists({pk_ref}) OR ({residual_expr})"
    )))
}
