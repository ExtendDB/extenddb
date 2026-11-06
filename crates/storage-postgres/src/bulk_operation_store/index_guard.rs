// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Keeps a bulk operation and an index creation on one table apart.
//!
//! Start refuses a table with an index that is not `ACTIVE`, and an index
//! creation is refused while the table has an operation `IN_PROGRESS` or
//! `CANCELLING`. Each side reads the other's row inside the transaction
//! that writes its own, under the `tables` row lock: `UpdateTable` takes it
//! `FOR UPDATE` before its index rows are written, and the start
//! transaction takes it `FOR SHARE` before it reads the indexes. Whichever
//! commits second waits for the first and then reads its committed row, so
//! the two are never both accepted. The `indexes`, `vector_indexes`,
//! `tables` and `bulk_operations` rows all live in the catalog database.
//!
//! A start whose call began before or during a build that has finished by
//! the time its transaction reads the indexes would read them `ACTIVE`.
//! Builds in this process are therefore also counted per table in memory: a
//! start takes a [`BuildMark`] when it is called, before any await, and is
//! refused when a build was running then or began before its transaction
//! read the indexes. A build is counted from just before its catalog commit,
//! so an `UpdateTable` that fails before it moves no count. Builds in another
//! process are seen through the stored status.
//!
//! The start transaction waits for the account's advisory lock under a
//! `lock_timeout`, then takes the table row `FOR SHARE NOWAIT`. An
//! `UpdateTable` that holds the row `FOR UPDATE` takes a second pool
//! connection for the new index's data table; starts that each held a
//! connection while they waited could take every connection it needs. A
//! wait past the timeout, or a row another transaction holds, rolls the
//! attempt back, which returns its connection to the pool, and the start
//! tries again. The account's lock is therefore never held while a start
//! waits for a row, so the account's starts on other tables are not held
//! up; and only the start holding the account's lock holds a table's row,
//! for the rest of its transaction, so a stream of starts on a table does
//! not keep `UpdateTable` and `DeleteTable` on it waiting.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, PoisonError};

use extenddb_core::types::GlobalSecondaryIndexUpdate;
use extenddb_storage::bulk_operation_store::index_creation_refused_message;

use super::*;

/// Per table id: how many builds have begun in this process, and how many
/// are running. Entries are never removed, so a count never repeats for a
/// table.
static BUILDS: LazyLock<Mutex<HashMap<String, (u64, u32)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One table's builds in this process as one read saw them.
#[derive(Clone, Copy)]
pub(super) struct BuildMark {
    begun: u64,
    running: u32,
}

impl BuildMark {
    pub(super) fn now(table_id: &str) -> Self {
        let builds = BUILDS.lock().unwrap_or_else(PoisonError::into_inner);
        let (begun, running) = builds.get(table_id).copied().unwrap_or((0, 0));
        Self { begun, running }
    }

    /// Whether a build was running when this mark was taken.
    pub(super) fn building(self) -> bool {
        self.running > 0
    }

    /// Whether a build began between `earlier` and this mark.
    pub(super) fn began_since(self, earlier: Self) -> bool {
        self.begun != earlier.begun
    }
}

/// Held by `UpdateTable` that creates a global secondary index from just
/// before its catalog commit, while it still holds the table row
/// `FOR UPDATE`, until it returns, the build included. Dropping it ends the
/// build in the count.
pub(crate) struct BuildGuard {
    table_id: String,
}

impl BuildGuard {
    /// Counts one build on the table as begun and running. `UpdateTable`
    /// calls it just before its catalog commit, after every step that can
    /// refuse or fail the request, so such a request never moves the count
    /// a start compares. A commit that fails after the call stays counted:
    /// it can fail after it took effect.
    pub(crate) fn begin(table_id: &str) -> Self {
        let mut builds = BUILDS.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = builds.entry(table_id.to_owned()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += 1;
        Self {
            table_id: table_id.to_owned(),
        }
    }
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        let mut builds = BUILDS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = builds.get_mut(&self.table_id) {
            entry.1 = entry.1.saturating_sub(1);
        }
    }
}

/// What the start transaction read of its table's row.
pub(super) enum RowRead {
    /// The table's stored status is `DELETING`, or its row is gone:
    /// `DeleteTable` removed it after the start handler's lookup.
    Deleting,
    /// The row is locked `FOR SHARE` and the table is not being deleted.
    Ready,
    /// Another transaction holds the row in a mode that conflicts with
    /// `FOR SHARE`; nothing was read.
    Locked,
}

/// Whether `error` is the `lock_not_available` (55P03) that a wait past
/// `lock_timeout`, or a `NOWAIT` lock that is held, raises.
pub(super) fn lock_timed_out(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "55P03")
}

/// Takes the table row lock the start transaction shares with an index
/// creation and with `DeleteTable`, without waiting, and reads the table's
/// status with it. A table row that is absent takes no lock and reads as
/// [`RowRead::Deleting`]: the start handler found the table at its lookup,
/// so `DeleteTable` removed the row since.
pub(super) async fn lock_table_row(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    table_id: &str,
) -> Result<RowRead, StorageError> {
    let status: Option<String> = match sqlx::query_scalar(
        "SELECT table_status FROM tables WHERE account_id = $1 AND table_id = $2 \
         FOR SHARE NOWAIT",
    )
    .bind(account_id)
    .bind(table_id)
    .fetch_optional(conn)
    .await
    {
        Ok(status) => status,
        Err(error) if lock_timed_out(&error) => return Ok(RowRead::Locked),
        Err(error) => return Err(internal(error)),
    };
    Ok(
        if status.as_deref().is_none_or(|status| status == "DELETING") {
            RowRead::Deleting
        } else {
            RowRead::Ready
        },
    )
}

/// Whether the table has a secondary or vector index that is not `ACTIVE`.
/// Read under the table row lock, which an index creation holds until its
/// index rows commit.
pub(super) async fn index_busy(
    conn: &mut sqlx::PgConnection,
    table_id: &str,
) -> Result<bool, StorageError> {
    let (busy,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM ( \
             SELECT index_status FROM indexes WHERE table_id = $1 \
             UNION ALL \
             SELECT index_status FROM vector_indexes WHERE table_id = $1 \
         ) AS states WHERE index_status <> 'ACTIVE'",
    )
    .bind(table_id)
    .fetch_one(conn)
    .await
    .map_err(internal)?;
    Ok(busy > 0)
}

/// Refuses an `UpdateTable` that creates a global secondary index while the
/// table has an operation `IN_PROGRESS` or `CANCELLING`, and otherwise
/// answers whether the request creates one. Called inside the
/// `UpdateTable` transaction after it took the table row `FOR UPDATE`, so
/// a start that holds the row `FOR SHARE` has committed before this reads.
/// The caller holds the [`BuildGuard`] it begins just before its commit,
/// until it returns.
pub(crate) async fn refuse_index_creation_during_run(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    table_id: &str,
    table_name: &str,
    updates: &[GlobalSecondaryIndexUpdate],
) -> Result<bool, StorageError> {
    if !updates.iter().any(|update| update.create.is_some()) {
        return Ok(false);
    }
    let (running,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM bulk_operations \
         WHERE account_id = $1 AND table_id = $2 \
         AND status IN ('IN_PROGRESS', 'CANCELLING')",
    )
    .bind(account_id)
    .bind(table_id)
    .fetch_one(conn)
    .await
    .map_err(internal)?;
    if running > 0 {
        return Err(StorageError::IndexesInUse(index_creation_refused_message(
            table_name,
        )));
    }
    Ok(true)
}
