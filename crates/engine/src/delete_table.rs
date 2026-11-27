// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{DeleteTableInput, DeleteTableOutput, DescribeTableInput, TableStatus};
use extenddb_core::validation::validate_table_name;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;

pub async fn handle_delete_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DeleteTableInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;
    // A table that CreateTable is still creating is refused with the
    // service's text, before the backend checks deletion protection (measured
    // on DynamoDB on 2026-11-21, with and without deletion protection). A
    // CREATING table with a restore summary is a restore target, which the
    // backend refuses as a table being restored. When the describe fails, the
    // backend's delete runs as before: it answers a missing table itself, and
    // a table whose description cannot be built stays deletable.
    let probe = DescribeTableInput {
        table_name: input.table_name.clone(),
    };
    if let Ok(desc) = ctx.storage.describe_table(&ctx.account_id, probe).await
        && desc.table_status == TableStatus::Creating
        && desc.restore_summary.is_none()
    {
        return Err(DynamoDbError::ResourceInUseException(format!(
            "Attempt to change a resource which is still in use: Table is being created: {}",
            input.table_name
        )));
    }

    let table_name = input.table_name.clone();
    let mut table_desc = ctx
        .storage
        .delete_table(&ctx.account_id, input)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Drop the cached TableKeyInfo so subsequent requests see the deletion
    // (or get a fresh negative-cache entry) immediately.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;

    // The deleted table's tags rows are gone; if a table with the same name
    // is recreated, ABAC must not see the prior tag map.
    let arn = format!(
        "arn:aws:dynamodb:{}:{}:table/{}",
        ctx.region, ctx.account_id, table_name
    );
    ctx.auth_cache.invalidate_resource_tags(&arn).await;

    table_desc.prepare_for_response();
    let output = DeleteTableOutput {
        table_description: table_desc,
    };
    serialize_output(&output)
}
