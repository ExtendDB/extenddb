// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for `PostgreSQL`.
//!
//! The definition is in the catalog and the items in the data tables. How the
//! open fixes one instant for both depends on the layout.
//!
//! One shared database (no `data_database_connection_string`): catalog and
//! data are one database, so one transaction reads both. The open begins
//! `REPEATABLE READ READ ONLY`, and its first statement reads the `tables`
//! row, which pins the transaction's snapshot; the description, the GSI ids
//! and the items are then all read from that one MVCC snapshot, so no row
//! lock or barrier is needed and `UpdateTable` and `DeleteTable` never wait
//! on an open's row lock. The open then takes `LOCK TABLE ... IN ACCESS SHARE MODE` on
//! the data table. A `DeleteTable` that committed and dropped the data table
//! between the snapshot and the `LOCK` makes the lock fail with
//! `undefined_table` (42P01), which the open reports as `TableNotFound`. A
//! `DeleteTable` whose in-use probe runs in that same window (after the
//! snapshot, before the `LOCK` is granted) does not see the open; on this
//! layout its drops run in its own catalog transaction within a 1 s budget
//! before the catalog removal commits, so if the `LOCK` is granted first the
//! drop times out, is rolled back with the removal, and the `DeleteTable`
//! takes its asynchronous path (`delete_table.rs`). The row and the data
//! tables are removed together or not at all.
//!
//! Separate data database: no one transaction covers both, and the fence is
//! the one the in-catalog backup uses: the `tables` row is held `FOR SHARE`
//! on a catalog transaction from the definition read until the data
//! transaction (`REPEATABLE READ READ ONLY`) holds `ACCESS SHARE` on the data
//! table and has pinned its snapshot. `UpdateTable` and `DeleteTable` take
//! that row `FOR UPDATE`, so neither can commit in the gap, and a
//! `DeleteTable` waiting on the row finds the `ACCESS SHARE` when it probes.
//! The GSI data-table probe also runs before the barrier is released.
//!
//! On either layout, background workers that flip an index status (a vector
//! build finishing, `mark_active`) do not take the row lock, so on the
//! separate layout an index can be reported `CREATING` while the items are a
//! few milliseconds newer; the description keeps it either way (only an index
//! being deleted is dropped), and a restore builds it from the items.
//!
//! The in-catalog backup (`backup_engine.rs`) opens its data snapshot the
//! same way as the separate layout (`REPEATABLE READ READ ONLY`, `ACCESS
//! SHARE` on the relation) on both layouts, reads the definition on its
//! barrier connection, and drops a GSI whose data table is missing, and the
//! attribute definitions only that GSI used, by the same rule (the probe is
//! one function, [`index_data_table_exists`]); this module differs in
//! returning the definition and the stream together, in ordering by the
//! primary key, and in using one transaction on the shared layout.
//!
//! Connections: on one shared database an open and its stream hold one
//! connection, from the open until the stream's transaction ends. With a
//! separate data database the open holds one catalog connection (the
//! barrier, which also reads the definition) until the data snapshot is
//! pinned, and the stream holds one data connection until it ends; every
//! path takes catalog then data, so the two pools cannot deadlock each
//! other. The engine's `snapshot_open_slots` bounds open snapshots (one
//! permit per snapshot, from the open until the stream's transaction ends)
//! at half the pool (`snapshot_slot_count`) on both layouts, so open
//! snapshots leave at least half of the pool their items come from to
//! requests (with the bound at the whole pool, ten open snapshots on a
//! ten-connection data pool made `GetItem` and `DeleteTable` wait out the
//! pool's 30 s acquire timeout). The `snapshot_open_slots` field doc says
//! what the permits do not prevent. An open over the bound is refused with
//! `LimitExceeded` rather than queued. The backup design's
//! `max_concurrent_jobs` is the caller-side bound on jobs; a restore holds no
//! snapshot slot.
//!
//! `ACCESS SHARE` on the data table is held until the stream's last fetch
//! (the transaction commits as soon as a short page shows the end), not
//! until the stream is dropped. While it is held a `DROP TABLE` waits;
//! `DeleteTable` therefore takes its asynchronous path for a table another
//! session holds `ACCESS SHARE` on (any relation of its storage,
//! `data::table_storage_relations`), so the catalog row is not removed while
//! its data tables are still in use. The poller stays active while a
//! `DELETING` row's drop is deferred (`workers.rs`): each pass probes the
//! table for holders and skips it while one remains, then the poller pauses
//! for a second, so the drop comes at the next pass after the release. Every
//! path that ends this stream's data transaction after it may have taken
//! `ACCESS SHARE` also wakes the poller, and the active poller waits on that
//! wake as well as on its timer, so the drop is retried at once: the
//! transaction lives in a guard (`HeldSnapshot`) from
//! `BEGIN` on, whose drop wakes the poller (an open that fails or is
//! cancelled at any await, a stream dropped before its end, a stream that
//! hit an error), and the last page wakes it before it awaits the commit.
//!
//! `LOCK TABLE ... IN ACCESS SHARE MODE` keeps a `DROP TABLE` from removing
//! the relation while it is read (ordinary DML is unaffected). `LOCK` is a
//! utility statement and does not pin the snapshot; on the separate layout
//! the `SELECT pg_current_snapshot()` that follows does, and on the shared
//! layout the first catalog read already did. Its text is the manifest
//! marker: the `xmin:xmax:xip` of the snapshot, recorded so a later
//! change-log archive can tell which commits the snapshot includes; nothing
//! reopens a snapshot from it. Items are then read in keyset pages on the
//! same transaction: each page is one query that seeks past the last row's
//! primary key (`WHERE (pk, sk) > ($1, $2) ORDER BY pk, sk LIMIT n`, an
//! index scan on the primary key) and cuts itself at the byte budget
//! ([`PageQuery`]).
//!
//! Memory: a page holds at most `paging::fetch_batch()` rows (1000) and
//! stops after the row that takes its item text to or past
//! `paging::SNAPSHOT_FETCH_BYTES` (64 MiB), so the stream holds at most the
//! budget plus one row of text in decoded form, plus the one row's text
//! being parsed: the rows are pulled from the connection one at a time and
//! parsed as they arrive (`fetch_page`), so the page's text is never held
//! whole on this side. The cut is made in the page query, so the server
//! returns no row past it; the rows it reads past the cut before `LIMIT n`
//! stops it are at most `n - 1`, and each of them costs the server one
//! serialization of its item text and two spills of it (the two window
//! functions buffer the `n` rows before the outer `WHERE` drops them), so a
//! page costs the server `n` rows of text, not the rows it returns. `n` is
//! [`MIN_PAGE_ROWS`] for the first page and is then sized from the largest
//! row of the last pages, those holding at least a row bound's worth of rows
//! ([`RowBoundMemory`], [`next_page_rows`]: the budget divided by that row,
//! never below [`MIN_NEXT_PAGE_ROWS`]), so a table of large rows pages at
//! the rows the budget holds plus one, and the over-read that remains is the
//! second page, and then the page that follows a row bound's worth of small
//! rows: each asks for the rows the small rows before it justified (the
//! first page's 16, then a row bound's worth) and may meet large ones, so it
//! reads up to its `n` rows of up to the line cap (32 MiB) each; `n` then
//! stays at the floor while large rows keep arriving within a row bound's
//! worth of rows and returns to the row bound a row bound's worth of small
//! rows after the last large one, so such a page recurs at most once per row
//! bound's worth of small rows, on a table whose large rows are rarer than
//! that. The bound is the rows the page reads, `n` of up to the line cap
//! each (measured on a 20,000-row table with 30 such rows every 33rd key
//! among the first 1,000: `docs/design/15-backup-restore-bench-pr8.md`);
//! nothing in this form stops the server at the budget, since a window
//! function runs over the `n` rows before the outer `WHERE` drops them. The
//! floor has a cost of its own: after a page holding a row at the line cap
//! the bound stays at the floor for a row bound's worth of rows, 250 pages of
//! four, whatever those rows hold, so a table whose large rows are rarer
//! than one per row bound pays 250 round trips per large row where one page
//! of memory would page it at the row bound (measured on a 20,000-row table
//! with a 27 MB row at every 1,100th key: 2,585 page queries against 38 with
//! one page of memory, 25.6 against 25.1 s, 1.06 against 1.09 GB of temp;
//! the record).
//!
//! The plan is the planner's choice, not the query's promise. With the
//! planner's estimate of the rows after the seek above `n`, the page is
//! `Limit` over an `Index Scan` on the primary key index with no sort (the
//! 100k-row test table's first page, a page in its middle and a page ten
//! rows from its end, `the_page_query_is_an_index_scan` and
//! `the_last_page_sorts_the_rows_after_the_seek`). On a table of about a
//! thousand rows, a page near its end whose `n` is above the rows left
//! (1,060 rows, 57 left, `n` = 810) gets a `Bitmap Heap Scan` driven by a
//! `Bitmap Index Scan` on the same index and a `Sort` of those rows under
//! the `Limit` (PostgreSQL 16, the same test), so such a page sorts the rows
//! after the seek, at most the rows left, with their text; the seek is
//! served by the primary key index either way.
//!
//! Order: the page orders by the primary key in the key columns' own
//! collation, which is the order the primary key index has, so every table
//! pages by its index. The stream is reported `key_ordered` only when the
//! key shape has a format-defined order (`key_schema_is_orderable`) and the
//! text key columns (`pk`, `sk_s`) have a collation named "C" or "POSIX",
//! read from `pg_attribute` inside the snapshot transaction; the rule is the
//! name, so "default" on a database whose own collation is C streams
//! unordered, which is complete (`open_stream` says why the table is not
//! sorted bytewise instead).

use std::sync::Arc;

use extenddb_core::types::ScalarAttributeType;
use extenddb_core::types::{Item, TableDescription};
use extenddb_storage::backup_catalog::{JobBound, limit_exceeded_message};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, key_schema_is_orderable, paging};
use extenddb_storage::util::sk_column_n;
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Postgres, Transaction};
use tokio::sync::{Notify, OwnedSemaphorePermit};

use crate::PostgresEngine;
use crate::data::{all_sort_key_info, data_table_name, index_table_name};

/// Rows the first page asks for, before anything is known about the row
/// size. The server serializes every row the inner `LIMIT` reads, so this
/// bounds the first page's cost at 16 rows of up to the line cap (32 MiB)
/// each; a table of small rows pays one short round trip for it, since the
/// second page is sized from the first ([`next_page_rows`]).
#[doc(hidden)]
pub const MIN_PAGE_ROWS: usize = 16;

/// The fewest rows a page after the first asks for. The budget divided by
/// the largest row is two for 27 MB rows, while such a page keeps three (the
/// row that reaches the budget is kept), so without a floor every page of
/// such a table would be two rows and no page would reach its cut; at four,
/// a page of such rows reads one row past its cut.
#[doc(hidden)]
pub const MIN_NEXT_PAGE_ROWS: usize = 4;

/// The largest rows of the last pages, which size the next page's row bound
/// ([`next_page_rows`]): the memory keeps the pages that together hold at
/// least [`RowBoundMemory::rows`] rows (the row bound, 1000 in production),
/// so a large row keeps the bound low for at least a row bound's worth of
/// rows after it rather than for one page. With one page of memory a large
/// row sets the next bound to 4, four small rows set the one after back to
/// 1000, and on a table whose large rows are scattered among small ones the
/// 1000-row page meets large rows again and the cycle repeats every other
/// page (5,555 rows read for 1,000 kept on the measured shape); with the
/// memory the bound stays at 4 while large rows keep arriving within a row
/// bound's worth of rows, so that table pages at 4 after its first 1000-row
/// page (1,900 read for 1,000 kept).
#[derive(Debug, Clone)]
struct RowBoundMemory {
    /// How many rows the kept pages must hold at least.
    rows: usize,
    /// The largest row and the row count of each kept page, oldest first.
    pages: std::collections::VecDeque<(usize, usize)>,
}

impl RowBoundMemory {
    fn new(rows: usize) -> Self {
        Self {
            rows,
            pages: std::collections::VecDeque::new(),
        }
    }

    /// Record a page's largest row (`max_row`, 0 for an empty page) and its
    /// row count, and forget the oldest pages the memory no longer needs:
    /// the oldest page goes while the pages after it hold at least `rows`.
    fn record(&mut self, max_row: usize, rows: usize) {
        self.pages.push_back((max_row, rows));
        while self.pages.len() > 1 {
            let after_oldest: usize = self.pages.iter().skip(1).map(|&(_, n)| n).sum();
            if after_oldest < self.rows {
                break;
            }
            self.pages.pop_front();
        }
    }

    /// The largest row among the kept pages; `None` when none held a row.
    fn largest_row(&self) -> Option<usize> {
        self.pages
            .iter()
            .map(|&(max_row, _)| max_row)
            .max()
            .filter(|&b| b > 0)
    }
}

/// The name the snapshot-slot refusal gives its bound: the setting the slot
/// count is derived from (half of `pool_size`).
pub const SNAPSHOT_SLOT_BOUND: &str = "[storage.postgres] pool_size snapshot-slot";

const BEGIN_SNAPSHOT: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

fn db_err(e: &sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

impl SnapshotEngine for PostgresEngine {
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            self.open_table_snapshot_impl(&account_id, &table_name)
                .await
        })
    }
}

/// The transaction that holds the snapshot's `ACCESS SHARE`, from `BEGIN` to
/// its end. Every way it ends wakes the control-plane poller: a
/// `DeleteTable` that found the table in use left the row `DELETING`, and
/// the active poller, which waits on the wake as well as on its one-second
/// timer, then retries the drop at once rather than at its next poll (the
/// poller stays active while a drop is deferred, so the wake saves up to a
/// second, not the difference between a second and its 60 s idle pass).
/// Dropping the guard with the transaction still set rolls it back (sqlx
/// queues the rollback and returns the connection through the runtime) and
/// wakes the poller; the poller's drop waits on the lock under a lock
/// timeout, so waking it before the rollback has run is harmless. Waking
/// when no lock was taken yet is harmless too. The in-catalog backup
/// (`backup_engine.rs`) holds its data snapshot in the same guard.
pub(crate) struct HeldSnapshot {
    tx: Option<Transaction<'static, Postgres>>,
    wake: Arc<Notify>,
}

impl HeldSnapshot {
    pub(crate) fn new(tx: Transaction<'static, Postgres>, wake: Arc<Notify>) -> Self {
        Self { tx: Some(tx), wake }
    }

    /// The open transaction's connection; `None` once it has ended.
    fn conn(&mut self) -> Option<&mut PgConnection> {
        self.tx.as_deref_mut()
    }

    /// The connection during the open, where the transaction cannot have
    /// ended yet.
    pub(crate) fn open_conn(&mut self) -> Result<&mut PgConnection, StorageError> {
        self.conn().ok_or_else(|| {
            StorageError::Internal("table snapshot transaction ended during the open".to_owned())
        })
    }

    /// End the transaction by rolling it back, and wake the poller.
    fn release(&mut self) {
        if self.tx.take().is_some() {
            self.wake.notify_one();
        }
    }

    /// End the transaction by committing it after the last row. The poller
    /// is woken before the commit is awaited, so a stream dropped during
    /// that await has already woken it. The commit's own error is returned
    /// for a caller that reports it (the in-catalog backup); the stream
    /// ignores it, since a `READ ONLY` transaction's commit has nothing to
    /// fail on that the caller could act on.
    pub(crate) async fn finish(&mut self) -> Result<(), sqlx::Error> {
        if let Some(tx) = self.tx.take() {
            self.wake.notify_one();
            tx.commit().await?;
        }
        Ok(())
    }
}

impl Drop for HeldSnapshot {
    fn drop(&mut self) {
        self.release();
    }
}

/// What the layout-specific half of the open hands to the common half: the
/// held transaction with `ACCESS SHARE` taken and the snapshot pinned, the
/// description as of that snapshot, and the marker.
struct Pinned {
    held: HeldSnapshot,
    description: TableDescription,
    table_id: String,
    marker: String,
}

impl PostgresEngine {
    async fn open_table_snapshot_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableSnapshot, StorageError> {
        // Held for the snapshot's life (module doc, Connections); refused at
        // once rather than queued, as the backup design refuses a job over
        // its limit.
        let slot = Arc::clone(&self.snapshot_open_slots)
            .try_acquire_owned()
            .map_err(|_| {
                StorageError::LimitExceeded(limit_exceeded_message(JobBound::Counted {
                    name: SNAPSHOT_SLOT_BOUND,
                    limit: self.snapshot_slot_limit,
                }))
            })?;
        let pinned = if self.shared_data_pool {
            self.pin_on_one_transaction(account_id, table_name).await?
        } else {
            self.pin_behind_barrier(account_id, table_name).await?
        };
        open_stream(pinned, slot, table_name).await
    }

    /// Shared layout (module doc): one `REPEATABLE READ READ ONLY`
    /// transaction whose first statement, the `tables` row read, pins the
    /// snapshot for the definition and the items alike.
    async fn pin_on_one_transaction(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Pinned, StorageError> {
        let mut held = HeldSnapshot::new(
            self.pool
                .begin_with(BEGIN_SNAPSHOT)
                .await
                .map_err(|e| db_err(&e))?,
            Arc::clone(&self.control_plane_notify),
        );
        let table_id = active_table_id(held.open_conn()?, account_id, table_name, "").await?;
        let (mut description, gsi_ids) = self
            .definition_on(held.open_conn()?, account_id, table_name, &table_id)
            .await?;
        // Between the snapshot and the LOCK: the window in which a
        // DeleteTable can drop the data table (module doc).
        paging::open_pause(paging::OpenStage::AfterDefinition).await;
        lock_data_table(held.open_conn()?, &table_id, table_name).await?;
        let marker = current_snapshot(held.open_conn()?).await?;
        leave_out_gsis_without_data_table(held.open_conn()?, &mut description, &gsi_ids).await?;
        Ok(Pinned {
            held,
            description,
            table_id,
            marker,
        })
    }

    /// Separate layout (module doc): the `tables` row is held `FOR SHARE` on
    /// a catalog transaction until the data transaction holds `ACCESS SHARE`
    /// and has pinned its snapshot, and until the GSI data-table probe has
    /// run.
    async fn pin_behind_barrier(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Pinned, StorageError> {
        let mut meta = self.pool.begin().await.map_err(|e| db_err(&e))?;
        let table_id = active_table_id(&mut meta, account_id, table_name, " FOR SHARE").await?;
        // The description is read on the connection that holds the row, so
        // an open needs exactly one catalog connection (the in-catalog backup
        // reads its definition on its barrier transaction the same way). A
        // concurrent UpdateTable blocks on the FOR UPDATE it needs before it
        // can commit, so what is read here is what is in force.
        let (mut description, gsi_ids) = self
            .definition_on(&mut meta, account_id, table_name, &table_id)
            .await?;
        let mut held = HeldSnapshot::new(
            self.data_pool
                .begin_with(BEGIN_SNAPSHOT)
                .await
                .map_err(|e| db_err(&e))?,
            Arc::clone(&self.control_plane_notify),
        );
        lock_data_table(held.open_conn()?, &table_id, table_name).await?;
        // Directly before the pin, with the barrier held: a barrier released
        // any earlier would fall inside this pause, where the conformance
        // check's writes would then be visible to the pin.
        paging::open_pause(paging::OpenStage::AfterDefinition).await;
        let marker = current_snapshot(held.open_conn()?).await?;
        // Before the barrier is released: an UpdateTable dropping a GSI's
        // data table cannot commit until then, so a GSI that existed at the
        // instant is not left out.
        leave_out_gsis_without_data_table(held.open_conn()?, &mut description, &gsi_ids).await?;
        meta.commit().await.map_err(|e| db_err(&e))?;
        Ok(Pinned {
            held,
            description,
            table_id,
            marker,
        })
    }

    /// The description (minus indexes being deleted) and the GSI ids, read
    /// on `conn`.
    async fn definition_on(
        &self,
        conn: &mut PgConnection,
        account_id: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<(TableDescription, Vec<(String, String)>), StorageError> {
        let mut description = self
            .build_table_description_on(conn, account_id, table_name)
            .await?;
        drop_deleting_indexes(&mut description);
        let gsi_ids: Vec<(String, String)> = sqlx::query_as(
            "SELECT index_name, index_id FROM indexes \
             WHERE table_id = $1 AND index_type = 'GSI' ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| db_err(&e))?;
        Ok((description, gsi_ids))
    }
}

/// Read the `tables` row (with `lock`, `""` or `" FOR SHARE"`) and return the
/// table id of an `ACTIVE` table.
async fn active_table_id(
    conn: &mut PgConnection,
    account_id: &str,
    table_name: &str,
    lock: &str,
) -> Result<String, StorageError> {
    let row: Option<(String, String)> = sqlx::query_as(&format!(
        "SELECT table_id, table_status FROM tables \
         WHERE account_id = $1 AND table_name = $2{lock}"
    ))
    .bind(account_id)
    .bind(table_name)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| db_err(&e))?;
    let (table_id, status) =
        row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
    if status != "ACTIVE" {
        return Err(StorageError::TableNotActive(table_name.to_owned()));
    }
    Ok(table_id)
}

/// `ACCESS SHARE` on the data table. A table dropped since the snapshot saw
/// its row (`undefined_table`, 42P01) is `TableNotFound`.
async fn lock_data_table(
    conn: &mut PgConnection,
    table_id: &str,
    table_name: &str,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(table_id);
    match sqlx::query(&format!("LOCK TABLE {ddb_table} IN ACCESS SHARE MODE"))
        .execute(&mut *conn)
        .await
    {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42P01") => {
            Err(StorageError::TableNotFound(table_name.to_owned()))
        }
        Err(e) => Err(db_err(&e)),
    }
}

/// `pg_current_snapshot()` text: on the separate layout the first
/// non-utility statement of the data transaction, which pins its snapshot.
async fn current_snapshot(conn: &mut PgConnection) -> Result<String, StorageError> {
    sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| db_err(&e))
}

/// Whether a GSI's data table exists. Reads `pg_class`, not the pinned
/// snapshot. Shared with the in-catalog backup, which leaves out a GSI
/// without its data table by the same rule.
pub(crate) async fn index_data_table_exists(
    conn: &mut PgConnection,
    index_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(index_table_name(index_id))
        .fetch_one(&mut *conn)
        .await
}

/// Since #401 `UpdateTable` creates a new GSI's data table before it commits
/// the catalog row, so a GSI row without a data table is not expected; it is
/// what an index whose table was dropped out from under the catalog, or a
/// crash in a pre-#401 build, leaves. Such an index is not part of the table
/// as of this snapshot: leave it out, and the attribute definitions only it
/// used, as the in-catalog backup does, rather than describe an index a
/// restore cannot rebuild or a definition `CreateTable` would refuse. A GSI
/// in the description with no id row is left out the same way. A restore
/// rebuilds index rows from the items.
async fn leave_out_gsis_without_data_table(
    conn: &mut PgConnection,
    description: &mut TableDescription,
    gsi_ids: &[(String, String)],
) -> Result<(), StorageError> {
    let mut omitted = false;
    if let Some(gsis) = description.global_secondary_indexes.as_mut() {
        let mut present = Vec::with_capacity(gsis.len());
        for gsi in std::mem::take(gsis) {
            let Some((_, index_id)) = gsi_ids.iter().find(|(name, _)| *name == gsi.index_name)
            else {
                omitted = true;
                continue;
            };
            if index_data_table_exists(conn, index_id)
                .await
                .map_err(|e| db_err(&e))?
            {
                present.push(gsi);
            } else {
                omitted = true;
            }
        }
        *gsis = present;
    }
    if omitted {
        prune_unused_attribute_definitions(description);
    }
    Ok(())
}

/// The common half of the open: decide the order and the collation, then
/// hand the held transaction to the keyset stream.
async fn open_stream(
    mut pinned: Pinned,
    slot: OwnedSemaphorePermit,
    table_name: &str,
) -> Result<TableSnapshot, StorageError> {
    let ddb_table = data_table_name(&pinned.table_id);
    let columns = key_columns(&pinned.description);
    // Whether the text key columns already sort bytewise: a collation named
    // "C" or "POSIX" in `pg_attribute`, read inside the snapshot transaction.
    // The page orders by the primary key in the columns' own collation either
    // way, which is the order the primary key index has, so a table created
    // before the key columns were declared `TEXT COLLATE "C"` (it keeps the
    // database collation, `en_US.utf8` in the default setups, where `a < B`)
    // pages by its index and is reported unordered rather than sorted
    // bytewise (a Seq Scan and an external sort of the whole table before the
    // first row, with the lock, the connection and the permit held
    // throughout). `COLLATE "C"` is written into the page query only when the
    // columns have it, where it changes nothing and states the order the
    // stream relies on (`pk` and `sk_s` bytewise, `sk_n` and `sk_b` by value:
    // the order `extenddb_core::backup_format::key_order` defines), and the
    // stream is reported ordered only then.
    let text_columns: Vec<String> = columns
        .iter()
        .filter(|c| c.kind == KeyKind::Text)
        .map(|c| c.name.clone())
        .collect();
    let not_bytewise: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname::text FROM pg_attribute a \
         LEFT JOIN pg_collation c ON c.oid = a.attcollation \
         WHERE a.attrelid = to_regclass($1) AND a.attname = ANY($2) \
         AND NOT a.attisdropped \
         AND COALESCE(c.collname, '') NOT IN ('C', 'POSIX')",
    )
    .bind(&ddb_table)
    .bind(&text_columns)
    .fetch_all(pinned.held.open_conn()?)
    .await
    .map_err(|e| db_err(&e))?;
    let collate_c = not_bytewise.is_empty();
    let orderable = key_schema_is_orderable(&pinned.description);
    if orderable && !collate_c {
        tracing::warn!(
            table = table_name,
            table_id = %pinned.table_id,
            columns = ?not_bytewise,
            "table snapshot: key columns do not have collation \"C\"; streaming the \
             table unordered (key_ordered = false) rather than sorting it"
        );
    }
    let key_ordered = orderable && collate_c;
    let query = PageQuery::new(&ddb_table, columns, collate_c);
    paging::open_pause(paging::OpenStage::BeforeItems).await;

    Ok(TableSnapshot {
        description: pinned.description,
        info: SnapshotInfo {
            kind: SnapshotInfo::KIND_POSTGRES_REPEATABLE_READ.to_owned(),
            marker: pinned.marker,
        },
        key_ordered,
        items: keyset_stream(pinned.held, slot, query),
    })
}

/// Drop attribute definitions no key schema (table, GSI, LSI) names, after
/// an index has been left out of a description. `CreateTable` refuses a
/// definition no key uses.
fn prune_unused_attribute_definitions(description: &mut TableDescription) {
    let mut used: std::collections::HashSet<&str> = description
        .key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();
    for gsi in description.global_secondary_indexes.iter().flatten() {
        used.extend(gsi.key_schema.iter().map(|k| k.attribute_name.as_str()));
    }
    for lsi in description.local_secondary_indexes.iter().flatten() {
        used.extend(lsi.key_schema.iter().map(|k| k.attribute_name.as_str()));
    }
    let used: std::collections::HashSet<String> = used.into_iter().map(str::to_owned).collect();
    description
        .attribute_definitions
        .retain(|d| used.contains(&d.attribute_name));
}

/// The type a primary-key column of the data table has, which decides how
/// the seek binds its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// `pk`, `sk_s`, `sk<n>_s`: `TEXT`.
    Text,
    /// `sk_n`, `sk<n>_n`: `NUMERIC`, carried as its text.
    Numeric,
    /// `sk_b`, `sk<n>_b`: `BYTEA`.
    Bytes,
}

/// A primary-key column of the data table.
#[derive(Debug, Clone)]
struct KeyColumn {
    name: String,
    kind: KeyKind,
}

/// The primary-key columns of a table's data table, in the index's order:
/// `pk`, then one typed column per range key (`sk_column_n`).
fn key_columns(description: &TableDescription) -> Vec<KeyColumn> {
    let sks = all_sort_key_info(&description.key_schema, &description.attribute_definitions);
    let mut columns = vec![KeyColumn {
        name: "pk".to_owned(),
        kind: KeyKind::Text,
    }];
    for (i, &(_, sk_type)) in sks.iter().enumerate() {
        columns.push(KeyColumn {
            name: sk_column_n(i, sk_type),
            kind: match sk_type {
                ScalarAttributeType::S => KeyKind::Text,
                ScalarAttributeType::N => KeyKind::Numeric,
                ScalarAttributeType::B => KeyKind::Bytes,
            },
        });
    }
    columns
}

/// One key column's value from the last row of a page: where the next page
/// starts.
#[derive(Debug, Clone)]
enum KeyValue {
    Text(String),
    Numeric(String),
    Bytes(Vec<u8>),
}

/// The page query of one table: the first page's text and a following
/// page's, built once per open.
///
/// Three layers. The innermost reads the rows after the seek in primary-key
/// order and stops at `LIMIT n`; the middle computes, over those `n` rows
/// only, the running total of item text in the same order and the count of
/// rows the limit returned (`count(*) OVER ()`, the end-of-stream signal);
/// the outer keeps every row whose predecessors' total is under the byte
/// budget and orders the result, since a window function's output order is
/// not promised to the client. The layering matters: a window function runs
/// before `LIMIT`, so a running total written beside the `LIMIT` would be
/// computed over the whole rest of the table. The innermost `ORDER BY ...
/// LIMIT` behind a row comparison on the primary-key columns is an index
/// scan on the primary key; `item_data::text` is computed once, in the
/// innermost layer, and the other layers read the alias.
#[derive(Debug, Clone)]
struct PageQuery {
    first: String,
    after: String,
    columns: Vec<KeyColumn>,
}

impl PageQuery {
    fn new(ddb_table: &str, columns: Vec<KeyColumn>, collate_c: bool) -> Self {
        Self {
            first: page_sql(ddb_table, &columns, collate_c, false),
            after: page_sql(ddb_table, &columns, collate_c, true),
            columns,
        }
    }
}

/// The text of a page query (see [`PageQuery`]). With `after_key` the query
/// seeks past a key bound as `$1..$k` (text, numeric as text, bytes) and
/// takes the row bound and the byte budget as `$k+1` and `$k+2`; without, as
/// `$1` and `$2`.
fn page_sql(ddb_table: &str, columns: &[KeyColumn], collate_c: bool, after_key: bool) -> String {
    let order = |prefix: &str| -> String {
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let name = if prefix.is_empty() {
                    format!("t.{}", c.name)
                } else {
                    format!("{prefix}.k{i}")
                };
                if collate_c && c.kind == KeyKind::Text {
                    format!("{name} COLLATE \"C\"")
                } else {
                    name
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let seek = if after_key {
        let left = columns
            .iter()
            .map(|c| format!("t.{}", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let right = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let p = i + 1;
                match c.kind {
                    KeyKind::Text => format!("${p}::text"),
                    KeyKind::Numeric => format!("${p}::text::numeric"),
                    KeyKind::Bytes => format!("${p}::bytea"),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("WHERE ({left}) > ({right}) ")
    } else {
        String::new()
    };
    let k = if after_key { columns.len() } else { 0 };
    let n_param = k + 1;
    let budget_param = k + 2;
    let inner_select = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("t.{} AS k{i}", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let keys = (0..columns.len())
        .map(|i| format!("k{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let shown = columns
        .iter()
        .enumerate()
        .map(|(i, c)| match c.kind {
            KeyKind::Numeric => format!("page.k{i}::text AS c{i}"),
            KeyKind::Text | KeyKind::Bytes => format!("page.k{i} AS c{i}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {shown}, page.item, page.n_total FROM (\
           SELECT {keys}, item, \
             sum(octet_length(item)) OVER (ORDER BY {lim_order} \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running, \
             count(*) OVER () AS n_total \
           FROM (\
             SELECT {inner_select}, t.item_data::text AS item FROM {ddb_table} t \
             {seek}ORDER BY {t_order} LIMIT ${n_param}\
           ) lim\
         ) page \
         WHERE page.running - octet_length(page.item) < ${budget_param} \
         ORDER BY {page_order}",
        lim_order = order("lim"),
        t_order = order(""),
        page_order = order("page"),
    )
}

/// The page query of `description`'s table, for the crate's tests: the
/// text [`PageQuery`] runs for a page after a key when `after_key`, else the
/// first page's, with `COLLATE "C"` on the text key columns when
/// `collate_c` (what `open_stream` decides from `pg_attribute`).
#[doc(hidden)]
#[must_use]
pub fn page_sql_for(
    description: &TableDescription,
    table_id: &str,
    collate_c: bool,
    after_key: bool,
) -> String {
    page_sql(
        &data_table_name(table_id),
        &key_columns(description),
        collate_c,
        after_key,
    )
}

/// One page as the database returned it, decoded.
struct Page {
    /// The items the byte cut kept, in key order.
    items: Vec<Item>,
    /// The last kept row's key: where the next page starts. `None` for an
    /// empty page.
    last_key: Option<Vec<KeyValue>>,
    /// Each kept row's text, in bytes, in key order: what the next page's
    /// row bound is sized from.
    row_bytes: Vec<usize>,
    /// Rows the innermost `LIMIT` returned, cut or not; fewer than the row
    /// bound means the table is exhausted.
    n_total: usize,
}

/// Run the page query and decode its rows as they arrive: the rows are
/// pulled from the connection one at a time (`fetch`), each row's text is
/// parsed into its `Item` and dropped, so the page's text is never held
/// whole; what the page holds is its items. A row whose text does not parse
/// ends the page with `Internal`; a database error is mapped by
/// [`map_fetch_error`].
async fn fetch_page(
    conn: &mut PgConnection,
    query: &PageQuery,
    after: Option<&[KeyValue]>,
    n: usize,
    budget: usize,
) -> Result<Page, StorageError> {
    use futures::TryStreamExt as _;
    let sql = if after.is_some() {
        &query.after
    } else {
        &query.first
    };
    let mut q = sqlx::query(sql);
    for value in after.into_iter().flatten() {
        q = match value {
            KeyValue::Text(s) | KeyValue::Numeric(s) => q.bind(s.as_str()),
            KeyValue::Bytes(b) => q.bind(b.as_slice()),
        };
    }
    let mut rows = q
        .bind(i64::try_from(n).unwrap_or(i64::MAX))
        .bind(i64::try_from(budget).unwrap_or(i64::MAX))
        .fetch(conn);
    let mut page = Page {
        items: Vec::new(),
        last_key: None,
        row_bytes: Vec::new(),
        n_total: 0,
    };
    while let Some(row) = rows.try_next().await.map_err(|e| map_fetch_error(&e))? {
        let row: PgRow = row;
        let n_total: i64 = row.try_get("n_total").map_err(|e| db_err(&e))?;
        page.n_total = usize::try_from(n_total).unwrap_or(0);
        let mut key = Vec::with_capacity(query.columns.len());
        for (i, column) in query.columns.iter().enumerate() {
            let name = format!("c{i}");
            key.push(match column.kind {
                KeyKind::Text => {
                    KeyValue::Text(row.try_get(name.as_str()).map_err(|e| db_err(&e))?)
                }
                KeyKind::Numeric => {
                    KeyValue::Numeric(row.try_get(name.as_str()).map_err(|e| db_err(&e))?)
                }
                KeyKind::Bytes => {
                    KeyValue::Bytes(row.try_get(name.as_str()).map_err(|e| db_err(&e))?)
                }
            });
        }
        let data: String = row.try_get("item").map_err(|e| db_err(&e))?;
        drop(row);
        let item = serde_json::from_str::<Item>(&data)
            .map_err(|e| StorageError::Internal(format!("Parse item: {e}")))?;
        page.row_bytes.push(data.len());
        drop(data);
        page.items.push(item);
        page.last_key = Some(key);
    }
    Ok(page)
}

/// Rows the next page asks for, from the largest row of the last pages
/// (`memory`, [`RowBoundMemory`]): the rows the budget holds at that row
/// (`budget / largest`), between [`MIN_NEXT_PAGE_ROWS`] and the row bound.
/// The largest row and not the average, because the server serializes every
/// row the inner `LIMIT` reads: after a page of small rows with a few large
/// ones among them the average is small and a bound from it asks the next
/// page for hundreds of rows of a table that may now be large rows only (one
/// such page measured at 5.9 GB of server temp files for the three rows it
/// returned), where the largest row asks for the rows the budget holds of
/// it. A table of small rows pages at the row bound; a table of 27 MB rows
/// at [`MIN_NEXT_PAGE_ROWS`], of which one row per page is read past the cut
/// and dropped by the outer `WHERE`; a table whose large rows are scattered
/// among small ones pages at the row bound until the first page that meets
/// a large row, which reads up to its `n` rows of up to the line cap, and at
/// [`MIN_NEXT_PAGE_ROWS`] from there while large rows keep arriving within a
/// row bound's worth of rows, the bound returning to the row bound a row
/// bound's worth of small rows after the last large one; so every page that
/// follows a row bound's worth of small rows can read `n` rows of up to the
/// line cap, and no page can read more. The floor's cost: after a page
/// holding a row at the line cap the bound stays at the floor for a row
/// bound's worth of rows, 250 pages of four, whatever those rows hold, so a
/// table whose large rows are rarer than one per row bound pays 250 round
/// trips per large row (2,585 page queries against 38 with one page of
/// memory on a 20,000-row table with a 27 MB row at every 1,100th key, 25.6
/// against 25.1 s, the record). A memory that held no row (an empty page)
/// gives the row bound.
fn next_page_rows(memory: &RowBoundMemory, budget: usize, batch: usize) -> usize {
    let Some(max_row) = memory.largest_row() else {
        return batch;
    };
    (budget / max_row).max(MIN_NEXT_PAGE_ROWS).min(batch)
}

/// Stream items by keyset pages on the held transaction. The held
/// transaction lives in the stream's state: the last page commits it (waking
/// the poller first), an error rolls it back, and dropping the stream before
/// its end rolls it back through `HeldSnapshot`'s drop; each wakes the
/// control-plane poller. The row bound and the byte budget are read once
/// here, when the stream is built.
fn keyset_stream(
    held: HeldSnapshot,
    slot: OwnedSemaphorePermit,
    query: PageQuery,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        held: HeldSnapshot,
        /// Released with the transaction: the slot counts the connection.
        slot: Option<OwnedSemaphorePermit>,
        query: PageQuery,
        /// The last row's key; `None` before the first page.
        after: Option<Vec<KeyValue>>,
        /// Rows the next page asks for.
        n: usize,
        batch: usize,
        budget: usize,
        /// The largest rows of the last pages, which size `n`.
        memory: RowBoundMemory,
        buffer: std::vec::IntoIter<Item>,
        done: bool,
    }
    impl State {
        /// End the stream after an error: roll back, wake the poller, free
        /// the slot.
        fn abandon(&mut self) {
            self.done = true;
            self.held.release();
            self.slot = None;
        }
    }
    let batch = paging::fetch_batch();
    let state = State {
        held,
        slot: Some(slot),
        query,
        after: None,
        n: MIN_PAGE_ROWS.min(batch),
        batch,
        budget: paging::fetch_bytes(),
        memory: RowBoundMemory::new(batch),
        buffer: Vec::new().into_iter(),
        done: false,
    };
    Box::pin(futures::stream::unfold(
        state,
        move |mut state| async move {
            loop {
                if let Some(item) = state.buffer.next() {
                    return Some((Ok(item), state));
                }
                if state.done {
                    return None;
                }
                let conn = state.held.conn()?;
                paging::record_fetch();
                let page = fetch_page(
                    conn,
                    &state.query,
                    state.after.as_deref(),
                    state.n,
                    state.budget,
                )
                .await;
                let page = match page {
                    Ok(page) => page,
                    Err(e) => {
                        state.abandon();
                        return Some((Err(e), state));
                    }
                };
                let returned = page.items.len();
                paging::record_page_bytes(page.row_bytes.iter().sum());
                paging::record_rows_past_cut(
                    u64::try_from(page.n_total.saturating_sub(returned)).unwrap_or(u64::MAX),
                );
                // The table is exhausted when the limit returned fewer rows
                // than asked and the cut dropped none of them; a short page
                // the cut shortened still has rows past its last one.
                if page.n_total < state.n && returned == page.n_total {
                    state.done = true;
                    // Close the transaction now rather than at drop so vacuum is
                    // released as soon as the last row is out.
                    let _ = state.held.finish().await;
                    state.slot = None;
                } else {
                    state.after = page.last_key;
                    state
                        .memory
                        .record(page.row_bytes.iter().copied().max().unwrap_or(0), returned);
                    state.n = next_page_rows(&state.memory, state.budget, state.batch);
                }
                state.buffer = page.items.into_iter();
            }
        },
    ))
}

/// Two SQLSTATEs mean the server discarded the snapshot's rows before the
/// stream drained them: 40001 (`serialization_failure`), which on a
/// `REPEATABLE READ READ ONLY` transaction can only come from a hot standby
/// cancelling the query because its snapshot conflicts with recovery, and
/// 72000 (`snapshot_too_old`), raised on a primary when
/// `old_snapshot_threshold` is set and the snapshot outlives it, on
/// PostgreSQL 16 and earlier (17 removed the setting, so there only 40001
/// reaches this). Everything
/// else is an ordinary database error.
#[doc(hidden)]
#[must_use]
pub fn map_fetch_error(e: &sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(db) = e
        && matches!(db.code().as_deref(), Some("40001" | "72000"))
    {
        return StorageError::SnapshotExpired(db.message().to_owned());
    }
    db_err(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(kinds: &[(&str, KeyKind)]) -> Vec<KeyColumn> {
        kinds
            .iter()
            .map(|(name, kind)| KeyColumn {
                name: (*name).to_owned(),
                kind: *kind,
            })
            .collect()
    }

    fn memory_of(pages: &[(usize, usize)], rows: usize) -> RowBoundMemory {
        let mut memory = RowBoundMemory::new(rows);
        for &(max_row, n) in pages {
            memory.record(max_row, n);
        }
        memory
    }

    /// The row bound follows the largest row of the remembered pages within
    /// its clamps: small rows page at the row bound, 27 MB rows at the floor
    /// (the budget holds two, the floor is four), a memory that held no row
    /// (an empty page) at the row bound, a row bound under the floor (a
    /// test's small page) is the bound; and a page of 984 small rows and 3
    /// large ones is sized from the large row, not from the average (which
    /// is 82 KB and would ask the next page for about 800 rows of a table
    /// that is now large rows only).
    #[test]
    fn the_row_bound_adapts_within_its_clamps() {
        let budget = 64 * 1024 * 1024;
        assert_eq!(
            next_page_rows(&memory_of(&[(300, 16)], 1000), budget, 1000),
            1000
        );
        // The floor as its value: the budget holds two such rows, a page
        // keeps three, and four reads one past the cut.
        assert_eq!(
            next_page_rows(&memory_of(&[(27_000_000, 3)], 1000), budget, 1000),
            4
        );
        assert_eq!(
            next_page_rows(&memory_of(&[(1024 * 1024, 8)], 1000), budget, 1000),
            64
        );
        assert_eq!(
            next_page_rows(&memory_of(&[(0, 0)], 1000), budget, 1000),
            1000
        );
        assert_eq!(next_page_rows(&memory_of(&[(300, 3)], 3), budget, 3), 3);
        // 984 rows of 300 bytes and 3 of 27 MB: the page's largest row sizes
        // the next page.
        let next = next_page_rows(&memory_of(&[(27_000_000, 987)], 1000), budget, 1000);
        assert!(
            next < 8,
            "the next page asks for {next} rows after a page of 984 small rows and 3 large ones"
        );
        assert!(next >= MIN_NEXT_PAGE_ROWS);
    }

    /// The memory keeps the pages that together hold at least the row bound:
    /// after a page with a 27 MB row, pages of four small rows keep the bound
    /// at the floor until a row bound's worth of them have passed (250 pages
    /// of four at a row bound of 1000), and the page after that is sized from
    /// the small rows alone; a large row met within that span starts the
    /// count again; the memory holds at most the pages it needs (the oldest
    /// goes once the pages after it hold the row bound), so it does not grow
    /// with the table.
    #[test]
    fn the_memory_covers_a_row_bound_of_rows() {
        let budget = 64 * 1024 * 1024;
        let mut memory = RowBoundMemory::new(1000);
        memory.record(27_000_000, 83);
        for _ in 0..249 {
            memory.record(300, 4);
            assert_eq!(next_page_rows(&memory, budget, 1000), MIN_NEXT_PAGE_ROWS);
        }
        // 249 pages of four hold 996 rows after the large page: still kept.
        assert_eq!(memory.pages.len(), 250);
        memory.record(300, 4);
        // 1000 rows after it: the large page is forgotten.
        assert_eq!(next_page_rows(&memory, budget, 1000), 1000);
        assert_eq!(memory.pages.len(), 250);
        // A large row within the span starts the count again.
        memory.record(27_000_000, 4);
        for _ in 0..249 {
            memory.record(300, 4);
        }
        assert_eq!(next_page_rows(&memory, budget, 1000), MIN_NEXT_PAGE_ROWS);
        memory.record(300, 4);
        assert_eq!(next_page_rows(&memory, budget, 1000), 1000);
        // Pages of the row bound: one page of memory is enough.
        let mut memory = RowBoundMemory::new(1000);
        memory.record(27_000_000, 3);
        memory.record(300, 1000);
        assert_eq!(memory.pages.len(), 1);
        assert_eq!(next_page_rows(&memory, budget, 1000), 1000);
    }

    /// The seek compares the key columns as their own types (numeric through
    /// text, bytes as bytea), the first page has no seek, the text columns
    /// carry `COLLATE "C"` only when asked, the row bound and the budget
    /// take the parameters after the key, and `item_data::text` is computed
    /// once in the innermost layer.
    #[test]
    fn the_page_query_binds_the_key_by_type() {
        let cols = columns(&[("pk", KeyKind::Text), ("sk_n", KeyKind::Numeric)]);
        let after = page_sql("\"_ddb_t\"", &cols, true, true);
        assert!(
            after.contains("WHERE (t.pk, t.sk_n) > ($1::text, $2::text::numeric) ORDER BY t.pk COLLATE \"C\", t.sk_n LIMIT $3"),
            "{after}"
        );
        assert!(
            after.contains("< $4 ORDER BY page.k0 COLLATE \"C\", page.k1"),
            "{after}"
        );
        assert!(after.contains("page.k1::text AS c1"), "{after}");
        assert_eq!(after.matches("item_data::text").count(), 1, "{after}");
        let first = page_sql("\"_ddb_t\"", &cols, false, false);
        assert!(!first.contains("WHERE (t.pk"), "{first}");
        assert!(first.contains("ORDER BY t.pk, t.sk_n LIMIT $1"), "{first}");
        assert!(first.contains("< $2 ORDER BY page.k0, page.k1"), "{first}");
        let bytes = columns(&[
            ("pk", KeyKind::Text),
            ("sk_b", KeyKind::Bytes),
            ("sk2_s", KeyKind::Text),
        ]);
        let after = page_sql("\"_ddb_t\"", &bytes, true, true);
        assert!(
            after.contains("> ($1::text, $2::bytea, $3::text) ORDER BY t.pk COLLATE \"C\", t.sk_b, t.sk2_s COLLATE \"C\" LIMIT $4"),
            "{after}"
        );
    }
}
