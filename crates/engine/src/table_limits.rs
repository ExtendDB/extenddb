// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The account limits `CreateTable` and `UpdateTable` enforce before the
//! backend is called: the table quota and the provisioned-capacity maxima.
//! The checks and their sentences are in
//! [`extenddb_core::validation::provisioned_limits`]; this module reads what
//! the account holds.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    CreateTableInput, DescribeTableInput, ListTablesInput, TableDescription, UpdateTableInput,
};
use extenddb_core::validation::provisioned_limits::{self as limits, CapacityUnits};
use extenddb_storage::error::StorageError;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;

/// The page size the account's tables are listed in.
const LIST_PAGE: i32 = 100;

/// Refuse a `CreateTable` the account's limits do not admit.
///
/// The service reports a taken name ahead of any limit (measured), so a name
/// the account already holds is left for the backend to refuse. After that,
/// the capacity a provisioned table asks for, then the table quota. Two
/// concurrent requests can both pass the quota with one slot left, so an
/// account can end one table over it.
///
/// # Errors
///
/// `LimitExceededException` when a limit refuses the table, or the storage
/// error that stopped the account's tables from being read.
pub(crate) async fn enforce_create_table(
    ctx: &OperationContext,
    input: &CreateTableInput,
) -> Result<(), DynamoDbError> {
    let names = table_names(ctx).await?;
    if names.contains(&input.table_name) {
        return Ok(());
    }
    if let Some(requested) = limits::create_table_capacity(input) {
        let reserved = reserved_capacity(ctx, &names).await?;
        limits::check_create_table_capacity(requested, reserved, &ctx.limits, &ctx.region)?;
    }
    limits::check_table_count(names.len(), &ctx.limits)
}

/// Refuse an `UpdateTable` that would raise `existing`, or the account, above
/// a provisioned-capacity maximum.
///
/// # Errors
///
/// `LimitExceededException` when a maximum refuses the change, or the storage
/// error that stopped the account's tables from being read.
pub(crate) async fn enforce_update_table(
    ctx: &OperationContext,
    input: &UpdateTableInput,
    existing: &TableDescription,
) -> Result<(), DynamoDbError> {
    let Some(after) = limits::update_table_capacity(existing, input) else {
        return Ok(());
    };
    let names = table_names(ctx).await?;
    let reserved = reserved_capacity(ctx, &names).await?;
    limits::check_update_table_capacity(
        input.provisioned_throughput.as_ref(),
        existing,
        reserved,
        after,
        &ctx.limits,
        &ctx.region,
    )
}

/// Every table name in the caller's account.
async fn table_names(ctx: &OperationContext) -> Result<Vec<String>, DynamoDbError> {
    let mut names = Vec::new();
    let mut start = None;
    loop {
        let page = ctx
            .storage
            .list_tables(
                &ctx.account_id,
                ListTablesInput {
                    limit: Some(LIST_PAGE),
                    exclusive_start_table_name: start,
                },
            )
            .await
            .map_err(storage_err_to_dynamo)?;
        let empty = page.table_names.is_empty();
        names.extend(page.table_names);
        match page.last_evaluated_table_name {
            Some(last) if !empty => start = Some(last),
            _ => return Ok(names),
        }
    }
}

/// What the tables `names` and their global secondary indexes hold.
async fn reserved_capacity(
    ctx: &OperationContext,
    names: &[String],
) -> Result<CapacityUnits, DynamoDbError> {
    let mut reserved = CapacityUnits::default();
    for name in names {
        let described = ctx
            .storage
            .describe_table(
                &ctx.account_id,
                DescribeTableInput {
                    table_name: name.clone(),
                },
            )
            .await;
        match described {
            Ok(desc) => reserved = reserved.saturating_add(limits::table_capacity(&desc)),
            // Deleted since it was listed, so it holds nothing.
            Err(StorageError::TableNotFound(_)) => {}
            Err(e) => return Err(storage_err_to_dynamo(e)),
        }
    }
    Ok(reserved)
}
