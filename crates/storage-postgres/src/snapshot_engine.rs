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
//! `max_concurrent_jobs` is the caller-side bound; the restore path's slots
//! (`restore_server_slot_count`) do not count snapshots.
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
//! reopens a snapshot from it. Items are then read through a server-side
//! cursor in batches.
//!
//! Memory: one `FETCH` is `paging::DEFAULT_FETCH_BATCH` (1000) rows, all of
//! them held as item text at once. At the default 400 KB item limit one
//! row's text can reach 27.7 MB (the measured worst case in #394's line-cap
//! table), so a fetch is bounded by rows, not bytes. A byte budget (keyset
//! paging with a byte cutoff) is required before the stream gets a caller;
//! the backup design assigns it to the backup job PR.
//!
//! Order: the cursor orders by the primary key (`key_order_clause`) only when
//! the text key columns (`pk`, and `sk_s` for an `S` range key) have a
//! collation named "C" or "POSIX", read from `pg_attribute` inside the
//! snapshot transaction. The rule is the name: any other collation,
//! including "default" on a database whose own collation is C, streams
//! unordered, which is complete. A table created before the key columns were
//! declared `COLLATE "C"` keeps the database collation, and ordering it
//! bytewise is a Seq Scan and an external sort of the whole table before the
//! first FETCH (7 MB on disk for 100k rows at `work_mem` 4 MB), with the
//! lock, the connection and the permit held throughout. Such a table is
//! streamed with a plain `SELECT`, reported `key_ordered = false`, and a
//! warning names it.

use std::sync::Arc;

use extenddb_core::types::ScalarAttributeType;
use extenddb_core::types::{Item, TableDescription};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, key_schema_is_orderable, paging};
use extenddb_storage::util::sk_column;
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{PgConnection, Postgres, Transaction};
use tokio::sync::{Notify, OwnedSemaphorePermit};

use crate::PostgresEngine;
use crate::data::{all_sort_key_info, data_table_name, index_table_name};

const CURSOR: &str = "extenddb_snapshot";

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
                StorageError::LimitExceeded(
                    "too many table snapshots are open on this server; retry when one ends"
                        .to_owned(),
                )
            })?;
        let pinned = if self.shared_data_pool {
            self.pin_on_one_transaction(account_id, table_name).await?
        } else {
            self.pin_behind_barrier(account_id, table_name).await?
        };
        open_cursor(pinned, slot, table_name).await
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

/// The common half of the open: decide the order, then `DECLARE` the cursor
/// on the held transaction.
async fn open_cursor(
    mut pinned: Pinned,
    slot: OwnedSemaphorePermit,
    table_name: &str,
) -> Result<TableSnapshot, StorageError> {
    let ddb_table = data_table_name(&pinned.table_id);
    // Ordered only when the text key columns already sort bytewise: on a
    // table whose key columns keep the database collation the ORDER BY would
    // be a Seq Scan and a full sort before the first FETCH, with the lock,
    // the connection and the permit held throughout (module doc).
    let mut order_by = key_order_clause(&pinned.description);
    if order_by.is_some() {
        let not_bytewise: Vec<String> = sqlx::query_scalar(
            "SELECT a.attname::text FROM pg_attribute a \
             LEFT JOIN pg_collation c ON c.oid = a.attcollation \
             WHERE a.attrelid = to_regclass($1) AND a.attname = ANY($2) \
             AND NOT a.attisdropped \
             AND COALESCE(c.collname, '') NOT IN ('C', 'POSIX')",
        )
        .bind(&ddb_table)
        .bind(key_text_columns(&pinned.description))
        .fetch_all(pinned.held.open_conn()?)
        .await
        .map_err(|e| db_err(&e))?;
        if !not_bytewise.is_empty() {
            tracing::warn!(
                table = table_name,
                table_id = %pinned.table_id,
                columns = ?not_bytewise,
                "table snapshot: key columns do not have collation \"C\"; streaming the \
                 table unordered (key_ordered = false) rather than sorting it"
            );
            order_by = None;
        }
    }
    let key_ordered = order_by.is_some();
    let select = match &order_by {
        Some(cols) => format!("SELECT item_data::text FROM {ddb_table} ORDER BY {cols}"),
        None => format!("SELECT item_data::text FROM {ddb_table}"),
    };
    paging::open_pause(paging::OpenStage::BeforeItems).await;
    sqlx::query(&format!("DECLARE {CURSOR} NO SCROLL CURSOR FOR {select}"))
        .execute(pinned.held.open_conn()?)
        .await
        .map_err(|e| db_err(&e))?;

    Ok(TableSnapshot {
        description: pinned.description,
        info: SnapshotInfo {
            kind: SnapshotInfo::KIND_POSTGRES_REPEATABLE_READ.to_owned(),
            marker: pinned.marker,
        },
        key_ordered,
        items: cursor_stream(pinned.held, slot),
    })
}

/// The text key columns whose collation decides the order: `pk`, and `sk_s`
/// for an `S` range key (`sk_n` is `NUMERIC` and `sk_b` is `bytea`).
fn key_text_columns(description: &TableDescription) -> Vec<String> {
    let sks = all_sort_key_info(&description.key_schema, &description.attribute_definitions);
    let mut columns = vec!["pk".to_owned()];
    if matches!(sks.as_slice(), [(_, ScalarAttributeType::S)]) {
        columns.push("sk_s".to_owned());
    }
    columns
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

/// `ORDER BY` columns for the format's key order, or `None` when the key
/// shape has none (`key_schema_is_orderable`). The open uses the clause only
/// after checking that the text key columns (`pk`, and `sk_s` for an `S`
/// range key) have collation "C" or "POSIX" (`pg_attribute.attcollation`);
/// a table created before the key columns were declared `TEXT COLLATE "C"`
/// keeps the database collation (`en_US.utf8` in the default setups, where
/// `a < B`) and is streamed unordered instead, because `ORDER BY ... COLLATE
/// "C"` on it is a full sort before the first row. On the tables that reach
/// the clause the explicit `COLLATE "C"` therefore does not change the order;
/// it is kept as defence in depth (no test can tell it from its absence), so
/// the query states the order it relies on. `pk` and `sk_s` sort bytewise and
/// `sk_n` (`NUMERIC`) and `sk_b` (`BYTEA`) sort by value: the order
/// `extenddb_core::backup_format::key_order` defines.
fn key_order_clause(description: &TableDescription) -> Option<String> {
    if !key_schema_is_orderable(description) {
        return None;
    }
    let sks = all_sort_key_info(&description.key_schema, &description.attribute_definitions);
    match sks.as_slice() {
        [] => Some("pk COLLATE \"C\"".to_owned()),
        [(_, ScalarAttributeType::S)] => Some("pk COLLATE \"C\", sk_s COLLATE \"C\"".to_owned()),
        [(_, sk_type)] => Some(format!("pk COLLATE \"C\", {}", sk_column(*sk_type))),
        _ => None,
    }
}

/// Stream items from the open cursor, one `FETCH` per batch. The held
/// transaction lives in the stream's state: the last page commits it (waking
/// the poller first), an error rolls it back, and dropping the stream before
/// its end rolls it back through `HeldSnapshot`'s drop; each wakes the
/// control-plane poller.
fn cursor_stream(
    held: HeldSnapshot,
    slot: OwnedSemaphorePermit,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        held: HeldSnapshot,
        /// Released with the transaction: the slot counts the connection.
        slot: Option<OwnedSemaphorePermit>,
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
    let state = State {
        held,
        slot: Some(slot),
        buffer: Vec::new().into_iter(),
        done: false,
    };
    let batch = paging::fetch_batch();
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
                let rows: Result<Vec<(String,)>, sqlx::Error> =
                    sqlx::query_as(&format!("FETCH {batch} FROM {CURSOR}"))
                        .fetch_all(conn)
                        .await;
                let rows = match rows {
                    Ok(rows) => rows,
                    Err(e) => {
                        state.abandon();
                        return Some((Err(map_fetch_error(&e)), state));
                    }
                };
                if rows.len() < batch {
                    state.done = true;
                    // Close the transaction now rather than at drop so vacuum is
                    // released as soon as the last row is out.
                    let _ = state.held.finish().await;
                    state.slot = None;
                }
                let mut items = Vec::with_capacity(rows.len());
                for (data,) in rows {
                    match serde_json::from_str::<Item>(&data) {
                        Ok(item) => items.push(item),
                        Err(e) => {
                            state.abandon();
                            return Some((
                                Err(StorageError::Internal(format!("Parse item: {e}"))),
                                state,
                            ));
                        }
                    }
                }
                state.buffer = items.into_iter();
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
