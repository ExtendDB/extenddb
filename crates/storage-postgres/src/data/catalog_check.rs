// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data relations checked against the catalog, for `extenddb catalog-check`.
//!
//! Every data relation this backend creates for a table is named from a
//! catalog id by [`table_storage_relations`], the list `DeleteTable` drops,
//! and every such name starts with [`STORAGE_RELATION_PREFIX`]. The check
//! lists the relations with that prefix in the data database and compares
//! them with the names the catalog's ids give.
//!
//! # Why a relation unnamed after the index writers finish stays unnamed
//!
//! Every id that names a relation is a new UUID generated just before its
//! catalog row is inserted (`create_table.rs` for the table, its GSIs, LSIs
//! and vector indexes, which restore uses too; `update_table.rs` for a GSI
//! or vector index added later). No path inserts a row for an id it did not
//! just generate, so a row inserted later cannot name a relation that exists
//! already. The recovery paths (`gsi_build.rs`, `vector_index.rs`) only
//! create a relation for a row that has committed.
//!
//! Every path creates the row first. All but one also commit it before they
//! create the relation: `CreateTable` and a vector index add. The exception
//! is a GSI added by `UpdateTable`, which inserts its row, creates the
//! relation and commits the relation, and only then commits the row. Its
//! transaction holds `ROW EXCLUSIVE` on `indexes` from the insert to its end,
//! so a `SHARE` lock on `indexes` is granted only once every such add whose
//! relation exists has committed or rolled back, and a catalog read taken
//! after the grant sees the row of each one that committed. A relation that
//! this read does not name is therefore named by no row, now or later.
//!
//! # The reads, and what they leave out while a server is running
//!
//! [`check`] lists the relations, waits for the `indexes` writers in flight
//! and reads the catalog ([`read_after_index_writers`]), lists the relations
//! again and reads the catalog again.
//!
//! - Orphaned: in the first listing, unnamed by the read after the wait, and
//!   still in the second listing. A relation whose catalog row a server has
//!   just deleted and whose drop has not yet committed when the second
//!   listing runs is reported: a synchronous `DeleteTable` between its
//!   catalog commit and its data commit, an `UpdateTable` that deletes a GSI
//!   or vector index before its drop runs, and a GSI whose build failed, for
//!   the second (`GRACE_PUBLISH`) between the removal of its row and its
//!   drop.
//! - Missing: named by a row that is expected in both catalog reads, and
//!   absent from the second listing, which runs between them. A row is not
//!   expected while it or its table is `CREATING` or `DELETING`, which
//!   covers a catalog-first path between its row's commit and its relations,
//!   and a drop that runs before the row goes. A synchronous `DeleteTable`
//!   commits the row's removal before its drop, so the second read no longer
//!   expects the relations a listing misses. What is still reported is a
//!   table row that is `ACTIVE` before its relations commit: a `CreateTable`
//!   when `control_plane_delay_seconds` is 0, which inserts the row
//!   `ACTIVE`, and one whose relations take longer to commit than that
//!   delay (0.25 s by default).
//!
//! # The drop
//!
//! [`drop_orphaned`] waits for the `indexes` writers the same way, reads the
//! catalog under the lock and releases it. Each reported relation that the
//! read does not name is then dropped in a transaction of its own with a
//! lock timeout of [`DROP_LOCK_TIMEOUT`], so a session holding the relation
//! costs one failed line, and nothing a server does waits on the command
//! while it drops.
//!
//! `migrate`'s advisory lock would not help: servers do not hold it, so it
//! does not exclude a running `CreateTable` or `UpdateTable`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use extenddb_storage::diagnostics::{CatalogRelation, OrphanDrop, StorageRelationCheck};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use super::{
    STORAGE_RELATION_PREFIX, data_table_name, index_table_name, lock_not_available,
    table_storage_relations, vector_table_name,
};

/// How long `catalog-check` waits, in all, for the index changes in flight
/// to finish, before the check or the drop gives up.
pub(crate) const INDEX_LOCK_BUDGET: Duration = Duration::from_secs(30);

/// The lock timeout of one attempt at the `SHARE` lock on `indexes`.
///
/// While the request waits, every new write to `indexes` (a `CreateTable`
/// with indexes, a GSI add, a status change, the cascade of a
/// `DeleteTable`) queues behind it, so this is the longest a server write
/// waits on the command. A fifth of PostgreSQL's default `deadlock_timeout`
/// (1 s); the command holds no other lock while it waits, so it cannot be
/// part of a deadlock.
const INDEX_LOCK_ATTEMPT: Duration = Duration::from_millis(200);

/// The pause between two attempts, in which the writers that queued behind
/// the last one run.
const INDEX_LOCK_PAUSE: Duration = Duration::from_millis(50);

/// The lock timeout of each drop: the bound the server's own synchronous
/// drops use ([`crate::delete_table::SYNC_DROP_LOCK_TIMEOUT`], 1 s).
pub(crate) const DROP_LOCK_TIMEOUT: Duration = crate::delete_table::SYNC_DROP_LOCK_TIMEOUT;

/// SQLSTATE `undefined_table`: the relation to drop no longer exists.
const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";

/// Every catalog row that names data relations, with its table's row. One
/// statement, so the three tables are read from one snapshot.
const CATALOG_ROWS: &str = "\
    SELECT 'table' AS kind, t.table_id, NULL::text AS index_id, NULL::text AS index_name, \
           NULL::text AS index_status, t.account_id, t.table_name, t.table_status \
      FROM tables t \
    UNION ALL \
    SELECT 'index', i.table_id, i.index_id, i.index_name, i.index_status, \
           t.account_id, t.table_name, t.table_status \
      FROM indexes i LEFT JOIN tables t ON t.table_id = i.table_id \
    UNION ALL \
    SELECT 'vector', v.table_id, v.index_id, v.index_name, v.index_status, \
           t.account_id, t.table_name, t.table_status \
      FROM vector_indexes v LEFT JOIN tables t ON t.table_id = v.table_id";

type CatalogRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// A relation name the catalog gives, and the row that gives it.
struct Named {
    owner: String,
    /// Whether the relation should exist: the row and its table are present
    /// and neither is being created or deleted.
    expected: bool,
}

/// One table's catalog rows, keyed by the relation each one names.
#[derive(Default)]
struct TableRows {
    index_ids: Vec<String>,
    vector_index_ids: Vec<String>,
    owners: BTreeMap<String, Named>,
}

/// A relation name as `pg_tables` lists it, without the quotes the naming
/// functions put around it.
fn bare(quoted: &str) -> String {
    quoted.trim_matches('"').to_owned()
}

/// Whether a row in `status` has its relations: it exists and is neither
/// being created (a catalog-first path commits the row before the relations)
/// nor deleted (a drop may already have run).
fn settled(status: Option<&str>) -> bool {
    status.is_some_and(|s| s != "CREATING" && s != "DELETING")
}

/// Every relation name the catalog gives, unquoted, for rows in any status.
async fn named_relations(
    catalog: &mut PgConnection,
) -> Result<BTreeMap<String, Named>, sqlx::Error> {
    let rows: Vec<CatalogRow> = sqlx::query_as(CATALOG_ROWS).fetch_all(catalog).await?;
    let mut tables: BTreeMap<String, TableRows> = BTreeMap::new();
    for (kind, table_id, index_id, index_name, index_status, account, name, status) in rows {
        let table = match (account, name) {
            (Some(account), Some(name)) => format!("table {account}/{name}"),
            _ => format!("table id {table_id}"),
        };
        let table_settled = settled(status.as_deref());
        let entry = tables.entry(table_id.clone()).or_default();
        match (kind.as_str(), index_id) {
            ("index" | "vector", Some(index_id)) => {
                let vector = kind == "vector";
                let relation = if vector {
                    entry.vector_index_ids.push(index_id.clone());
                    vector_table_name(&index_id)
                } else {
                    entry.index_ids.push(index_id.clone());
                    index_table_name(&index_id)
                };
                let index = index_name.unwrap_or(index_id);
                let noun = if vector { "vector index" } else { "index" };
                entry.owners.insert(
                    bare(&relation),
                    Named {
                        owner: format!("{noun} {index} of {table}"),
                        expected: table_settled && settled(index_status.as_deref()),
                    },
                );
            }
            ("table", _) => {
                entry.owners.insert(
                    bare(&data_table_name(&table_id)),
                    Named {
                        owner: table,
                        expected: table_settled,
                    },
                );
            }
            // `index_id` is NOT NULL in both index tables.
            _ => {}
        }
    }

    // The names come from the list DeleteTable drops, so a relation family
    // added there is covered here too. The owners above only label them.
    let mut named = BTreeMap::new();
    for (table_id, mut entry) in tables {
        for relation in
            table_storage_relations(&table_id, &entry.index_ids, &entry.vector_index_ids)
        {
            let relation = bare(&relation);
            let owner = entry.owners.remove(&relation).unwrap_or_else(|| Named {
                owner: format!("table id {table_id}"),
                expected: false,
            });
            named.insert(relation, owner);
        }
    }
    Ok(named)
}

/// A catalog transaction at READ COMMITTED whose lock waits are bounded by
/// `lock_timeout`.
///
/// Both settings are made before any statement that takes a snapshot, so
/// every later statement reads the data committed when it starts, whatever
/// `default_transaction_isolation` the database, the role or the connection
/// sets. Under REPEATABLE READ a snapshot taken before the lock wait would
/// hide the rows committed during it.
async fn begin_read_committed(
    catalog: &PgPool,
    lock_timeout: Duration,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut tx = catalog.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await?;
    // A number of milliseconds, at least 1 (0 would disable the timeout).
    let ms = lock_timeout.as_millis().max(1);
    sqlx::query(&format!("SET LOCAL lock_timeout = '{ms}ms'"))
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Wait until every transaction writing `indexes` when this is called has
/// ended, then read the catalog. See the module documentation for why a
/// relation this read does not name is named by no row later.
///
/// The `SHARE` lock is requested in attempts of [`INDEX_LOCK_ATTEMPT`],
/// [`INDEX_LOCK_PAUSE`] apart, until `deadline`; the last attempt's error
/// (SQLSTATE 55P03) is returned once it has passed. The lock is released as
/// soon as the read returns.
async fn read_after_index_writers(
    catalog: &PgPool,
    deadline: Instant,
) -> Result<BTreeMap<String, Named>, sqlx::Error> {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let mut hold = begin_read_committed(catalog, left.min(INDEX_LOCK_ATTEMPT)).await?;
        let read = async {
            sqlx::query("LOCK TABLE indexes IN SHARE MODE")
                .execute(&mut *hold)
                .await?;
            named_relations(&mut hold).await
        }
        .await;
        // The transaction only locked and read; ending it releases the lock.
        hold.rollback().await?;
        match read {
            Err(e) if lock_not_available(&e) && Instant::now() < deadline => {
                let left = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(left.min(INDEX_LOCK_PAUSE)).await;
            }
            read => return read,
        }
    }
}

/// Read the catalog with lock waits bounded by what is left until `deadline`.
async fn read_catalog(
    catalog: &PgPool,
    deadline: Instant,
) -> Result<BTreeMap<String, Named>, sqlx::Error> {
    let left = deadline.saturating_duration_since(Instant::now());
    let mut tx = begin_read_committed(catalog, left).await?;
    let named = named_relations(&mut tx).await;
    tx.rollback().await?;
    named
}

/// The data relations this backend created that the data database holds now.
async fn storage_relations(data: &PgPool) -> Result<BTreeSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename::text FROM pg_tables \
         WHERE schemaname = current_schema() AND starts_with(tablename::text, $1)",
    )
    .bind(STORAGE_RELATION_PREFIX)
    .fetch_all(data)
    .await?;
    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// Compare the data database's relations with the catalog, waiting at most
/// `budget` for the index changes in flight.
pub(crate) async fn check(
    catalog: &PgPool,
    data: &PgPool,
    budget: Duration,
) -> Result<StorageRelationCheck, sqlx::Error> {
    let deadline = Instant::now() + budget;
    let listed = storage_relations(data).await?;
    let named = read_after_index_writers(catalog, deadline).await?;
    #[cfg(feature = "test-hooks")]
    test_pause::reached(test_pause::CatalogCheckPoint::AfterCatalogRead).await;
    let listed_again = storage_relations(data).await?;
    let named_again = read_catalog(catalog, deadline).await?;

    let orphaned = listed
        .into_iter()
        .filter(|relation| listed_again.contains(relation) && !named.contains_key(relation))
        .collect();
    let missing = named
        .into_iter()
        .filter(|(relation, row)| {
            row.expected
                && !listed_again.contains(relation)
                && named_again
                    .get(relation)
                    .is_some_and(|again| again.expected)
        })
        .map(|(relation, row)| CatalogRelation {
            relation,
            owner: row.owner,
        })
        .collect();
    Ok(StorageRelationCheck { orphaned, missing })
}

/// Drop the relations in `orphaned` that no catalog row names once the index
/// changes in flight have finished, waiting at most `budget` for those.
///
/// Fails only when the catalog cannot be locked or read; nothing is dropped
/// then. A relation that cannot be dropped gets [`OrphanDrop::Failed`] and
/// the rest still run.
pub(crate) async fn drop_orphaned(
    catalog: &PgPool,
    data: &PgPool,
    orphaned: Vec<String>,
    budget: Duration,
) -> Result<Vec<(String, OrphanDrop)>, sqlx::Error> {
    if orphaned.is_empty() {
        return Ok(Vec::new());
    }
    let named = read_after_index_writers(catalog, Instant::now() + budget).await?;

    let mut outcomes = Vec::with_capacity(orphaned.len());
    for relation in orphaned {
        let outcome = if !relation.starts_with(STORAGE_RELATION_PREFIX) {
            OrphanDrop::Kept("not a data relation of this backend".to_owned())
        } else if let Some(row) = named.get(&relation) {
            OrphanDrop::Kept(format!("the catalog names it: {}", row.owner))
        } else {
            drop_relation(data, &relation).await
        };
        outcomes.push((relation, outcome));
    }
    Ok(outcomes)
}

/// Drop one relation in a transaction of its own, waiting at most
/// [`DROP_LOCK_TIMEOUT`] for its lock.
///
/// Without `IF EXISTS`, so a relation another session dropped first is told
/// apart from one dropped here, and PostgreSQL sends no notice to print.
async fn drop_relation(data: &PgPool, relation: &str) -> OrphanDrop {
    let dropped = async {
        let mut tx = data.begin().await?;
        let ms = DROP_LOCK_TIMEOUT.as_millis();
        sqlx::query(&format!("SET LOCAL lock_timeout = '{ms}ms'"))
            .execute(&mut *tx)
            .await?;
        let quoted = format!("\"{}\"", relation.replace('"', "\"\""));
        sqlx::query(&format!("DROP TABLE {quoted}"))
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
    .await;
    match dropped {
        Ok(()) => OrphanDrop::Dropped,
        Err(e) if sqlstate(&e).as_deref() == Some(SQLSTATE_UNDEFINED_TABLE) => {
            OrphanDrop::AlreadyGone
        }
        Err(e) if lock_not_available(&e) => OrphanDrop::Failed(format!(
            "another session held a lock on it for more than {} ms, so it was kept; run the \
             command again: {e}",
            DROP_LOCK_TIMEOUT.as_millis()
        )),
        Err(e) => OrphanDrop::Failed(e.to_string()),
    }
}

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    e.as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}

/// Test hooks: hold the next check at a chosen point, so a test can change
/// the catalog or the data database in the window the point opens.
/// Compiled only with `test-hooks`.
#[cfg(feature = "test-hooks")]
pub(crate) mod test_pause {
    use extenddb_storage::test_pause::{Pause, PausePoint};

    /// Where a check can be held.
    #[doc(hidden)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CatalogCheckPoint {
        /// After the catalog read taken once the index writers have finished,
        /// before the second listing and the second catalog read.
        AfterCatalogRead,
    }

    static PAUSE: PausePoint<CatalogCheckPoint> = PausePoint::new();

    /// A pause armed by [`arm_catalog_check_pause`]. Dropping it disarms the
    /// pause and releases a check waiting at it.
    #[doc(hidden)]
    pub type CatalogCheckPause = Pause<CatalogCheckPoint>;

    /// Arm a pause for the next check in this process that reaches `point`
    /// (one check, once).
    ///
    /// # Panics
    ///
    /// In debug builds, when a pause is already armed ([`PausePoint::arm`]).
    #[doc(hidden)]
    #[must_use]
    pub fn arm_catalog_check_pause(point: CatalogCheckPoint) -> CatalogCheckPause {
        PAUSE.arm(point)
    }

    pub(crate) async fn reached(point: CatalogCheckPoint) {
        PAUSE.reached(point).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_storage_relation_name_carries_the_listing_prefix() {
        let names = table_storage_relations(
            "8f2b5c1e-0000-4000-8000-000000000001",
            &["8f2b5c1e-0000-4000-8000-000000000002".to_owned()],
            &["8f2b5c1e-0000-4000-8000-000000000003".to_owned()],
        );
        assert_eq!(names.len(), 3);
        for name in names {
            assert!(
                bare(&name).starts_with(STORAGE_RELATION_PREFIX),
                "{name} would be invisible to catalog-check"
            );
        }
    }

    #[test]
    fn bare_removes_the_identifier_quotes() {
        assert_eq!(bare(&data_table_name("abc")), "_ddb_abc");
        assert_eq!(bare(&vector_table_name("abc")), "_ddb_vec_abc");
    }

    #[test]
    fn rows_being_created_or_deleted_are_not_settled() {
        for status in ["ACTIVE", "UPDATING"] {
            assert!(settled(Some(status)), "{status}");
        }
        for status in [None, Some("CREATING"), Some("DELETING")] {
            assert!(!settled(status), "{status:?}");
        }
    }
}
