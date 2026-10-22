// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for SQLite.
//!
//! Catalog and data share one file, so one read transaction covers both. In
//! WAL mode a read transaction sees the database as of its first read for
//! its whole life and never blocks writers; writers continue and the WAL
//! grows (it cannot checkpoint past the reader) until the transaction ends.
//!
//! No lock is needed: one read transaction reads the `tables`, `indexes`,
//! `vector_indexes` and data rows, and its first read pins the WAL snapshot
//! for all of them. The engine's writer lock is not taken, so writers never
//! wait on a snapshot being opened, and the snapshot never waits on a pool
//! connection while holding the lock.
//!
//! In-memory databases are pinned to a single connection (a second connection
//! would open a different database), so a snapshot held across the stream
//! would exclude every writer for its duration and the backup could not be
//! observed to be consistent against concurrent writes. They are refused with
//! `Unsupported`.
//!
//! Items are read in primary-key order through row-value keyset pagination
//! on the primary key columns (`WHERE (pk, sk_x) > (?, ?) ORDER BY pk, sk_x
//! LIMIT n`), which SQLite serves from the primary key index. Multi-part
//! range keys fall back to rowid order and report `key_ordered = false`.

use extenddb_core::types::{Item, ScalarAttributeType, TableDescription};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, key_schema_is_orderable};
use extenddb_storage::util::sk_column;
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{Row, Sqlite, Transaction};

use crate::data::{all_sort_key_info, data_table_name};
use crate::store::SqliteEngine;

/// Rows per keyset page. Tests lower it through
/// `set_snapshot_fetch_batch_for_tests` so a small table spans several pages.
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

fn db_err(e: &sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

impl SnapshotEngine for SqliteEngine {
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

/// How the stream pages through the table.
enum Paging {
    /// Hash-only key: `pk > ?`.
    Pk,
    /// One range key stored in `sk_col`: `(pk, sk_col) > (?, ?)`.
    PkSk {
        sk_col: &'static str,
        sk_type: ScalarAttributeType,
    },
    /// Multi-part range key: rowid order.
    Rowid,
}

impl SqliteEngine {
    async fn open_table_snapshot_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableSnapshot, StorageError> {
        if self.in_memory {
            return Err(StorageError::Unsupported(
                "table snapshots require a file-backed SQLite database; an in-memory \
                 database has one connection and cannot hold a snapshot open alongside writers"
                    .to_owned(),
            ));
        }

        let mut tx: Transaction<'static, Sqlite> =
            self.pool.begin().await.map_err(|e| db_err(&e))?;
        // This first read pins the WAL snapshot; everything below, catalog
        // and data, is read from it.
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_status FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_err(&e))?;
        let (table_id, status) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }
        let mut description = self
            .build_table_description_on(&mut tx, account_id, table_name)
            .await?;
        drop_deleting_indexes(&mut description);
        let ddb_table = data_table_name(&table_id);

        let paging = paging_for(&description);
        let key_ordered = !matches!(paging, Paging::Rowid);
        Ok(TableSnapshot {
            description,
            info: SnapshotInfo {
                kind: "sqlite_wal_read_transaction".to_owned(),
                marker: String::new(),
            },
            key_ordered,
            items: keyset_stream(tx, ddb_table, paging),
        })
    }
}

/// `pk` is TEXT under BINARY collation and the sort column is typed (text,
/// order-preserving number encoding, blob), so paging on the primary key
/// yields the order `extenddb_core::backup_format::key_order` defines.
fn paging_for(description: &TableDescription) -> Paging {
    if !key_schema_is_orderable(description) {
        return Paging::Rowid;
    }
    let sks = all_sort_key_info(&description.key_schema, &description.attribute_definitions);
    match sks.as_slice() {
        [] => Paging::Pk,
        [(_, sk_type)] => Paging::PkSk {
            sk_col: sk_column(*sk_type),
            sk_type: *sk_type,
        },
        _ => Paging::Rowid,
    }
}

/// Last-seen key for keyset pagination.
enum Cursor {
    Start,
    Pk(String),
    PkSkText(String, String),
    PkSkBlob(String, Vec<u8>),
    Rowid(i64),
}

fn keyset_stream(
    tx: Transaction<'static, Sqlite>,
    table: String,
    paging: Paging,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        tx: Option<Transaction<'static, Sqlite>>,
        table: String,
        paging: Paging,
        cursor: Cursor,
        buffer: std::vec::IntoIter<Item>,
        done: bool,
    }
    let state = State {
        tx: Some(tx),
        table,
        paging,
        cursor: Cursor::Start,
        buffer: Vec::new().into_iter(),
        done: false,
    };
    // Read the page size once so a change to the test knob during a stream
    // cannot end it early or late.
    let batch = fetch_batch();
    let limit = i64::try_from(batch).unwrap_or(i64::MAX);
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
                let page = fetch_page(tx, &state.table, &state.paging, &state.cursor, limit).await;
                let (items, next_cursor) = match page {
                    Ok(page) => page,
                    Err(e) => {
                        state.done = true;
                        state.tx = None;
                        return Some((Err(e), state));
                    }
                };
                if let Some(next) = next_cursor {
                    state.cursor = next;
                }
                if items.len() < batch {
                    state.done = true;
                    if let Some(tx) = state.tx.take() {
                        let _ = tx.commit().await;
                    }
                }
                state.buffer = items.into_iter();
            }
        },
    ))
}

/// Query one page of up to `limit` rows for the paging mode and cursor.
async fn query_page(
    tx: &mut Transaction<'static, Sqlite>,
    table: &str,
    paging: &Paging,
    cursor: &Cursor,
    limit: i64,
) -> Result<Vec<sqlx::sqlite::SqliteRow>, StorageError> {
    match (paging, cursor) {
        (Paging::Pk, Cursor::Start) => {
            sqlx::query(&format!(
                "SELECT pk, item_data FROM {table} ORDER BY pk LIMIT ?"
            ))
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::Pk, Cursor::Pk(last)) => {
            sqlx::query(&format!(
                "SELECT pk, item_data FROM {table} WHERE pk > ? ORDER BY pk LIMIT ?"
            ))
            .bind(last)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::PkSk { sk_col, .. }, Cursor::Start) => {
            sqlx::query(&format!(
                "SELECT pk, {sk_col}, item_data FROM {table} ORDER BY pk, {sk_col} LIMIT ?"
            ))
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::PkSk { sk_col, .. }, Cursor::PkSkText(pk, sk)) => {
            sqlx::query(&format!(
                "SELECT pk, {sk_col}, item_data FROM {table} \
                 WHERE (pk, {sk_col}) > (?, ?) ORDER BY pk, {sk_col} LIMIT ?"
            ))
            .bind(pk)
            .bind(sk)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::PkSk { sk_col, .. }, Cursor::PkSkBlob(pk, sk)) => {
            sqlx::query(&format!(
                "SELECT pk, {sk_col}, item_data FROM {table} \
                 WHERE (pk, {sk_col}) > (?, ?) ORDER BY pk, {sk_col} LIMIT ?"
            ))
            .bind(pk)
            .bind(sk)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::Rowid, Cursor::Start) => {
            sqlx::query(&format!(
                "SELECT rowid, item_data FROM {table} ORDER BY rowid LIMIT ?"
            ))
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        (Paging::Rowid, Cursor::Rowid(last)) => {
            sqlx::query(&format!(
                "SELECT rowid, item_data FROM {table} WHERE rowid > ? ORDER BY rowid LIMIT ?"
            ))
            .bind(last)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
        }
        _ => {
            return Err(StorageError::Internal(
                "snapshot cursor does not match its paging mode".to_owned(),
            ));
        }
    }
    .map_err(|e| db_err(&e))
}

/// One page: items and the cursor for the next page. Fewer items than
/// `limit` means the table is exhausted.
async fn fetch_page(
    tx: &mut Transaction<'static, Sqlite>,
    table: &str,
    paging: &Paging,
    cursor: &Cursor,
    limit: i64,
) -> Result<(Vec<Item>, Option<Cursor>), StorageError> {
    let rows = query_page(tx, table, paging, cursor, limit).await?;
    let mut items = Vec::with_capacity(rows.len());
    let mut next = None;
    for row in &rows {
        let data: String = row.try_get("item_data").map_err(|e| db_err(&e))?;
        let item: Item = serde_json::from_str(&data)
            .map_err(|e| StorageError::Internal(format!("Parse item: {e}")))?;
        items.push(item);
    }
    if let Some(last) = rows.last() {
        next = Some(match paging {
            Paging::Pk => Cursor::Pk(last.try_get("pk").map_err(|e| db_err(&e))?),
            Paging::PkSk { sk_col, sk_type } => {
                let pk: String = last.try_get("pk").map_err(|e| db_err(&e))?;
                match sk_type {
                    ScalarAttributeType::B => Cursor::PkSkBlob(
                        pk,
                        last.try_get::<Vec<u8>, _>(*sk_col)
                            .map_err(|e| db_err(&e))?,
                    ),
                    ScalarAttributeType::S | ScalarAttributeType::N => Cursor::PkSkText(
                        pk,
                        last.try_get::<String, _>(*sk_col).map_err(|e| db_err(&e))?,
                    ),
                }
            }
            Paging::Rowid => Cursor::Rowid(last.try_get("rowid").map_err(|e| db_err(&e))?),
        });
    }
    Ok((items, next))
}

#[cfg(test)]
mod tests {
    use extenddb_storage::SnapshotEngine;
    use extenddb_storage::snapshot_conformance;

    use super::*;

    const ACCOUNT: &str = "123456789012";

    async fn prepare(engine: &SqliteEngine) {
        crate::schema::apply(&engine.pool).await.expect("schema");
        for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
            sqlx::query("UPDATE settings SET value = '0' WHERE key = ?")
                .bind(key)
                .execute(&engine.pool)
                .await
                .expect("setting");
        }
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
    }

    /// A file-backed engine on a scratch database under the cargo target
    /// directory. The files are removed when the value is dropped, on success
    /// or on a failed assertion.
    struct FileEngine {
        engine: Option<SqliteEngine>,
        path: std::path::PathBuf,
    }

    impl std::ops::Deref for FileEngine {
        type Target = SqliteEngine;
        fn deref(&self) -> &SqliteEngine {
            self.engine.as_ref().expect("engine present until drop")
        }
    }

    impl Drop for FileEngine {
        fn drop(&mut self) {
            drop(self.engine.take());
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
            }
        }
    }

    async fn file_engine() -> FileEngine {
        let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
            || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            std::path::PathBuf::from,
        );
        assert!(target.is_dir(), "cargo target directory must exist");
        let path = target.join(format!("snapshot-test-{}.sqlite", uuid::Uuid::new_v4()));
        let engine = SqliteEngine::new(path.to_str().expect("UTF-8 path"), 4, "us-east-1", 409_600)
            .await
            .expect("file engine");
        prepare(&engine).await;
        FileEngine {
            engine: Some(engine),
            path,
        }
    }

    #[tokio::test]
    async fn snapshot_is_consistent_under_concurrent_writes() {
        let engine = file_engine().await;
        snapshot_conformance::run(
            &*engine,
            ACCOUNT,
            "snap_src",
            50,
            set_snapshot_fetch_batch_for_tests,
            || async {
                engine
                    .process_control_plane_transitions()
                    .await
                    .expect("pump");
            },
        )
        .await;
        snapshot_conformance::run_refusals(&*engine, ACCOUNT).await;
    }

    #[tokio::test]
    async fn snapshot_reports_key_order_for_single_range_key() {
        let engine = file_engine().await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("ordered"), false)
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "ordered", || async {
            engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        })
        .await;
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "ordered")
            .await
            .expect("open");
        assert!(snapshot.key_ordered);
        assert_eq!(snapshot.info.kind, "sqlite_wal_read_transaction");
    }

    #[tokio::test]
    async fn in_memory_database_refuses_snapshots() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("memory engine");
        prepare(&engine).await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("mem"), false)
            .await
            .expect("create");
        let err = engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .expect_err("refused");
        assert!(matches!(err, StorageError::Unsupported(_)), "{err}");
    }
}
