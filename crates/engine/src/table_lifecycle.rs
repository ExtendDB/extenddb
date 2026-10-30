// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog's lifecycle status for a table, for handlers that decide on it.
//!
//! `TableKeyInfo` carries no status, and the key-info read reports a `DELETING`
//! row as not found on MongoDB and Cassandra, so a handler that has to tell a
//! `DELETING` table from a missing one cannot get the answer from it. The
//! description carries the status on every backend. The callers (the two TTL
//! handlers and the tag path) are control-plane calls, so the read is not on a
//! data-plane path.
//!
//! The read is `describe_table`: four catalog queries on PostgreSQL and SQLite
//! (`tables`, `indexes`, `vector_indexes`, `table_restores`) and two on MongoDB
//! and Cassandra (`tables`, `indexes`). `DescribeTimeToLive`,
//! `UpdateTimeToLive`, `TagResource`, `UntagResource` and `ListTagsOfResource`
//! each make it once per call. The tag path keeps the key-info read after it
//! for every status but `DELETING`, so a tag call on an `ACTIVE` table sends
//! seven queries before its tag statements on PostgreSQL and SQLite, where it
//! sent three, and four on MongoDB and Cassandra, where it sent two.
//! `DescribeTimeToLive` on an `ACTIVE` table sends five and three, where it
//! sent one. `UpdateTimeToLive` sends the four or two more, and on a
//! `DELETING` table it skips the TTL read after the write and the index
//! statements.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{DescribeTableInput, TableStatus};
use extenddb_storage::error::StorageError;

use crate::OperationContext;

/// Read `table_name`'s current status from the catalog.
///
/// # Errors
///
/// Returns [`StorageError::TableNotFound`] when no catalog row exists, and
/// whatever the backend raised otherwise. The caller maps it, so each operation
/// keeps the message it already answers for a missing table.
pub(crate) async fn read_table_status(
    table_name: &str,
    ctx: &OperationContext,
) -> Result<TableStatus, StorageError> {
    let desc = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await?;
    Ok(desc.table_status)
}

/// The service's refusal of a change to a table that is being deleted, worded
/// once in [`extenddb_core::error::ErrorMessageKey::TableBeingDeleted`] and
/// classified the way the control-plane mappers classify
/// [`StorageError::TableBeingDeleted`].
pub(crate) fn being_deleted(table_name: &str) -> DynamoDbError {
    DynamoDbError::ResourceInUseException(extenddb_core::error::error_message(
        extenddb_core::error::ErrorMessageKey::TableBeingDeleted,
        &[table_name],
    ))
}
