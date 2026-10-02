// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table-level index propagation holds.
//!
//! A hold prevents GSI workers from applying queued writes to any index on a
//! table while a backfill is in progress. Workers check for a hold before
//! processing each `gsi_pending` row and skip rows whose table is held.

use std::sync::Arc;

use extenddb_storage::error::StorageError;

use crate::cassandra_util::CassandraSession;

/// Insert a propagation hold for `(table_id, index_id)`.
///
/// Must be called before the index data table is created so that no worker
/// can apply a queued write to the index before the backfill completes.
pub(crate) async fn take_propagation_hold(
    session: &Arc<CassandraSession>,
    account_keyspace: &str,
    table_id: &str,
    index_id: &str,
) -> Result<(), StorageError> {
    let cql = format!(
        "INSERT INTO {account_keyspace}.index_propagation_holds (table_id, index_id) VALUES (?, ?)"
    );
    crate::cassandra_util::execute(
        session,
        &cql,
        cdrs_tokio::query_values!(table_id, index_id),
        "take_propagation_hold",
    )
    .await
}

/// Remove the propagation hold for `(table_id, index_id)`.
///
/// Must be called after the backfill completes (or is abandoned) so that
/// workers resume applying queued writes.
pub(crate) async fn release_propagation_hold(
    session: &Arc<CassandraSession>,
    account_keyspace: &str,
    table_id: &str,
    index_id: &str,
) -> Result<(), StorageError> {
    let cql = format!(
        "DELETE FROM {account_keyspace}.index_propagation_holds WHERE table_id = ? AND index_id = ?"
    );
    crate::cassandra_util::execute(
        session,
        &cql,
        cdrs_tokio::query_values!(table_id, index_id),
        "release_propagation_hold",
    )
    .await
}

/// Returns `true` if any propagation hold exists for `table_id`.
pub(crate) async fn is_held(
    session: &Arc<CassandraSession>,
    account_keyspace: &str,
    table_id: &str,
) -> Result<bool, StorageError> {
    let cql = format!(
        "SELECT index_id FROM {account_keyspace}.index_propagation_holds WHERE table_id = ? LIMIT 1"
    );
    let rows = crate::cassandra_util::query_rows::<StorageError>(
        session,
        &cql,
        cdrs_tokio::query_values!(table_id),
        "is_held",
    )
    .await?;
    Ok(!rows.is_empty())
}
