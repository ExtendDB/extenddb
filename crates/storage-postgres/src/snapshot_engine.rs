// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for PostgreSQL.
//!
//! The definition is in the catalog database and the items in the data
//! database, so no one transaction covers both. The fence is the same one the
//! in-catalog backup uses: the `tables` row is held `FOR SHARE` from the
//! definition read until the data transaction's snapshot is pinned.
//! `UpdateTable` and `DeleteTable` take that row `FOR UPDATE`, so neither can
//! commit in the gap. Background workers that flip an index status (a vector
//! build finishing, `mark_active`) do not take the row lock, so an index can
//! be reported `CREATING` while the items are a few milliseconds newer; the
//! description keeps it either way (only an index being deleted is dropped),
//! and a restore builds it from the items.
//!
//! The data transaction is `REPEATABLE READ READ ONLY`. `LOCK TABLE ... IN
//! ACCESS SHARE MODE` keeps a `DROP TABLE` from removing the relation while
//! it is read (ordinary DML is unaffected). `LOCK` is a utility statement and
//! does not pin the snapshot; the `SELECT pg_current_snapshot()` that follows
//! does, and its text is the manifest marker. Items are then read through a
//! server-side cursor in batches, so the process never holds more than one
//! batch.

use extenddb_core::types::{Item, TableDescription};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, key_schema_is_orderable};
use extenddb_storage::util::sk_column;
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{Postgres, Transaction};

use crate::PostgresEngine;
use crate::data::{all_sort_key_info, data_table_name};

/// Rows fetched per cursor round trip. Measured on localhost PostgreSQL 16
/// with 318-byte items: 1000 and 10000 read a million rows in the same time;
/// 1000 holds less memory. Tests lower it through
/// [`set_snapshot_fetch_batch_for_tests`] so a small table spans several fetches.
const DEFAULT_FETCH_BATCH: usize = 1000;

/// Override for tests; 0 means the default. Compiled only with the
/// `test-hooks` feature, which the crate's own tests enable through a
/// dev-dependency on itself.
#[cfg(feature = "test-hooks")]
static FETCH_BATCH_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "test-hooks")]
fn fetch_batch() -> usize {
    match FETCH_BATCH_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => DEFAULT_FETCH_BATCH,
        n => n,
    }
}

#[cfg(not(feature = "test-hooks"))]
fn fetch_batch() -> usize {
    DEFAULT_FETCH_BATCH
}

/// Set the rows per fetch for every snapshot opened afterwards. Lets a
/// consistency test make a small table span many fetches so the stream is
/// still open against the backend while a writer runs.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn set_snapshot_fetch_batch_for_tests(rows: usize) {
    FETCH_BATCH_OVERRIDE.store(rows, std::sync::atomic::Ordering::Relaxed);
}

const CURSOR: &str = "extenddb_snapshot";

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

impl PostgresEngine {
    async fn open_table_snapshot_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableSnapshot, StorageError> {
        // Barrier: hold the table row until the data snapshot is pinned.
        let mut meta = self.pool.begin().await.map_err(|e| db_err(&e))?;
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_status FROM tables \
             WHERE account_id = $1 AND table_name = $2 FOR SHARE",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *meta)
        .await
        .map_err(|e| db_err(&e))?;
        let (table_id, status) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }

        // The description reads the catalog through the pool while the row
        // is held; a concurrent UpdateTable blocks on the FOR UPDATE it needs
        // before it can commit, so what is read here is what is in force.
        let mut description = self.build_table_description(account_id, table_name).await?;
        drop_deleting_indexes(&mut description);

        let ddb_table = data_table_name(&table_id);
        let mut snapshot: Transaction<'static, Postgres> = self
            .data_pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|e| db_err(&e))?;
        sqlx::query(&format!("LOCK TABLE {ddb_table} IN ACCESS SHARE MODE"))
            .execute(&mut *snapshot)
            .await
            .map_err(|e| db_err(&e))?;
        // First non-utility statement: pins the snapshot and reports it.
        let marker: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
            .fetch_one(&mut *snapshot)
            .await
            .map_err(|e| db_err(&e))?;
        meta.commit().await.map_err(|e| db_err(&e))?;

        let order_by = key_order_clause(&description);
        let key_ordered = order_by.is_some();
        let select = match &order_by {
            Some(cols) => format!("SELECT item_data::text FROM {ddb_table} ORDER BY {cols}"),
            None => format!("SELECT item_data::text FROM {ddb_table}"),
        };
        sqlx::query(&format!("DECLARE {CURSOR} NO SCROLL CURSOR FOR {select}"))
            .execute(&mut *snapshot)
            .await
            .map_err(|e| db_err(&e))?;

        Ok(TableSnapshot {
            description,
            info: SnapshotInfo {
                kind: "postgres_repeatable_read".to_owned(),
                marker,
            },
            key_ordered,
            items: cursor_stream(snapshot),
        })
    }
}

/// `ORDER BY` columns for the format's key order, or `None` when the key
/// shape has none (multi-part range keys). `pk` is `TEXT COLLATE "C"` and the
/// sort column is typed (`TEXT COLLATE "C"`, `NUMERIC`, `BYTEA`), so this is
/// bytewise on the hash key's text encoding and typed on the range key: the
/// order `extenddb_core::backup_format::key_order` defines.
fn key_order_clause(description: &TableDescription) -> Option<String> {
    if !key_schema_is_orderable(description) {
        return None;
    }
    let sks = all_sort_key_info(&description.key_schema, &description.attribute_definitions);
    match sks.as_slice() {
        [] => Some("pk".to_owned()),
        [(_, sk_type)] => Some(format!("pk, {}", sk_column(*sk_type))),
        _ => None,
    }
}

/// Stream items from the open cursor, one `FETCH` per batch. The transaction
/// lives in the stream's state; dropping the stream rolls it back.
fn cursor_stream(
    snapshot: Transaction<'static, Postgres>,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        tx: Option<Transaction<'static, Postgres>>,
        buffer: std::vec::IntoIter<Item>,
        done: bool,
    }
    let state = State {
        tx: Some(snapshot),
        buffer: Vec::new().into_iter(),
        done: false,
    };
    let batch = fetch_batch();
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
                let tx = state.tx.as_mut()?;
                let rows: Result<Vec<(String,)>, sqlx::Error> =
                    sqlx::query_as(&format!("FETCH {batch} FROM {CURSOR}"))
                        .fetch_all(&mut **tx)
                        .await;
                let rows = match rows {
                    Ok(rows) => rows,
                    Err(e) => {
                        state.done = true;
                        state.tx = None;
                        return Some((Err(map_fetch_error(&e)), state));
                    }
                };
                if rows.len() < batch {
                    state.done = true;
                    // Close the transaction now rather than at drop so vacuum is
                    // released as soon as the last row is out.
                    if let Some(tx) = state.tx.take() {
                        let _ = tx.commit().await;
                    }
                }
                let mut items = Vec::with_capacity(rows.len());
                for (data,) in rows {
                    match serde_json::from_str::<Item>(&data) {
                        Ok(item) => items.push(item),
                        Err(e) => {
                            state.done = true;
                            state.tx = None;
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
/// `old_snapshot_threshold` is set and the snapshot outlives it. Everything
/// else is an ordinary database error.
fn map_fetch_error(e: &sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(db) = e
        && matches!(db.code().as_deref(), Some("40001" | "72000"))
    {
        return StorageError::SnapshotExpired(db.message().to_owned());
    }
    db_err(e)
}
