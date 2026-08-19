// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{DescribeTableInput, DescribeTableOutput};
use extenddb_core::validation::validate_table_name;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;

pub async fn handle_describe_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DescribeTableInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;

    let table_desc = ctx
        .storage
        .describe_table(&ctx.account_id, input)
        .await
        .map_err(storage_err_to_dynamo)?;

    // A backend must not tell a client an index is ready while it is still being
    // populated; the first search would silently undercount.
    table_desc.validate_vector_index_readiness()?;

    let output = DescribeTableOutput { table: table_desc };
    serialize_output(&output)
}
