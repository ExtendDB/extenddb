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
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use extenddb_core::types::GlobalSecondaryIndexUpdate;
use extenddb_storage::bulk_operation_store::index_creation_refused_message;

use super::*;

/// Per table id, while a build runs on the table or a start holds a mark of
/// it: the number of the last build that began on the table, the builds
/// running, and the marks held. The entry is removed when the last of them
/// ends, and a mark holds it, so the number cannot repeat between two
/// marks of one start.
static BUILDS: LazyLock<Mutex<HashMap<String, Builds>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Default)]
struct Builds {
    last_begun: u64,
    running: u32,
    marks: u32,
}

fn builds() -> MutexGuard<'static, HashMap<String, Builds>> {
    BUILDS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Applies `end` to the entry of `table_id`, and removes the entry when no
/// build runs and no mark is held.
fn release(table_id: &str, end: impl FnOnce(&mut Builds)) {
    let mut builds = builds();
    if let Some(entry) = builds.get_mut(table_id) {
        end(entry);
        if entry.running == 0 && entry.marks == 0 {
            builds.remove(table_id);
        }
    }
}

/// One table's builds in this process as one read saw them. The mark keeps
/// the table's entry until it drops, so a later mark compares with it.
pub(super) struct BuildMark {
    table_id: String,
    last_begun: u64,
    running: u32,
}

impl BuildMark {
    pub(super) fn now(table_id: &str) -> Self {
        let mut builds = builds();
        let entry = builds.entry(table_id.to_owned()).or_default();
        entry.marks += 1;
        Self {
            table_id: table_id.to_owned(),
            last_begun: entry.last_begun,
            running: entry.running,
        }
    }

    /// Whether a build was running when this mark was taken.
    pub(super) fn building(&self) -> bool {
        self.running > 0
    }

    /// Whether a build began between `earlier` and this mark.
    pub(super) fn began_since(&self, earlier: &Self) -> bool {
        self.last_begun != earlier.last_begun
    }
}

impl Drop for BuildMark {
    fn drop(&mut self) {
        release(&self.table_id, |entry| entry.marks -= 1);
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
        let mut builds = builds();
        let entry = builds.entry(table_id.to_owned()).or_default();
        entry.last_begun += 1;
        entry.running += 1;
        Self {
            table_id: table_id.to_owned(),
        }
    }
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        release(&self.table_id, |entry| entry.running -= 1);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A build that begins and ends between two marks of its table is seen
    /// by the later mark, and the table's entry goes when the last build and
    /// the last mark on it end.
    #[test]
    fn a_table_entry_goes_with_its_last_build_and_mark() {
        let table = format!("index-guard-{}", uuid::Uuid::new_v4());
        drop(BuildGuard::begin(&table));
        assert!(
            !builds().contains_key(&table),
            "an ended build keeps no entry"
        );

        let before = BuildMark::now(&table);
        drop(BuildGuard::begin(&table));
        let after = BuildMark::now(&table);
        assert!(!before.building() && !after.building());
        assert!(after.began_since(&before), "the build between the marks");
        assert!(!BuildMark::now(&table).began_since(&after));
        drop((before, after));
        assert!(
            !builds().contains_key(&table),
            "the last mark removes the entry"
        );

        // A Start that holds a mark keeps the entry, so a later Start's
        // marks still see the build that begins between them.
        let hold = BuildMark::now(&table);
        drop(BuildGuard::begin(&table));
        let before = BuildMark::now(&table);
        drop(BuildGuard::begin(&table));
        assert!(
            BuildMark::now(&table).began_since(&before),
            "a build while another mark holds the entry"
        );
        drop((hold, before));
    }
}
