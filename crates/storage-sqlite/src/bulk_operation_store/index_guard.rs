// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Keeps a bulk operation and an index creation on one table apart.
//!
//! Start refuses a table with an index that is not `ACTIVE`, and an index
//! creation is refused while the table has an operation `IN_PROGRESS` or
//! `CANCELLING`. Each side reads the other's row inside the
//! `BEGIN IMMEDIATE` transaction that writes its own, so the two are
//! serialized by the database's write lock, across processes on one file
//! as well as within one.
//!
//! A global secondary index build in this process keeps the engine write
//! lock until the index is `ACTIVE`, so a start that waited for the lock
//! behind a build would read the index already `ACTIVE`. Builds in this
//! process are therefore also counted per table in memory: a start takes a
//! [`BuildMark`] when it is called, before any await, and is refused when a
//! build was running then or began before its transaction read the
//! indexes. Builds in another process are seen through the stored status.

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

/// Held by `UpdateTable` that creates a global secondary index, from the
/// end of its catalog transaction, after every check that can refuse the
/// request and before the commit, until it returns, the build included.
/// Dropping it ends the build in the count.
pub(crate) struct BuildGuard {
    table_id: String,
}

impl BuildGuard {
    /// Counts one build on the table as begun and running. `UpdateTable`
    /// calls it after the last check that can refuse the request, so a
    /// refused request never moves the count a start compares.
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

/// Whether the table's stored status is `DELETING`, or its row is gone. The
/// start handler found the table at its lookup, so an absent row means
/// `DeleteTable` removed it since.
pub(super) async fn table_deleting(
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    table_id: &str,
) -> Result<bool, StorageError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = ? AND table_id = ?")
            .bind(account_id)
            .bind(table_id)
            .fetch_optional(conn)
            .await
            .map_err(internal)?;
    Ok(status.as_deref().is_none_or(|status| status == "DELETING"))
}

/// Whether the table has a secondary or vector index that is not `ACTIVE`.
pub(super) async fn index_busy(
    conn: &mut sqlx::SqliteConnection,
    table_id: &str,
) -> Result<bool, StorageError> {
    let busy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM ( \
             SELECT index_status FROM indexes WHERE table_id = ?1 \
             UNION ALL \
             SELECT index_status FROM vector_indexes WHERE table_id = ?1 \
         ) WHERE index_status <> 'ACTIVE'",
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
/// `UpdateTable` transaction, which holds the write lock a start also takes. The caller holds the
/// [`BuildGuard`] it begins once its last refusal has passed, until it
/// returns.
pub(crate) async fn refuse_index_creation_during_run(
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    table_id: &str,
    table_name: &str,
    updates: &[GlobalSecondaryIndexUpdate],
) -> Result<bool, StorageError> {
    if !updates.iter().any(|update| update.create.is_some()) {
        return Ok(false);
    }
    let running: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM bulk_operations \
         WHERE account_id = ? AND table_id = ? \
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
