// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` for `SQLite`.
//!
//! Catalog and data share one file, so one read transaction covers both. In
//! WAL mode a read transaction sees the database as of its first read for
//! its whole life and never blocks writers; writers continue and the WAL
//! grows (it cannot checkpoint past the reader) until the transaction ends.
//! A checkpoint does not fail while a snapshot is held: `PRAGMA
//! wal_checkpoint(TRUNCATE)` waits out the busy timeout (5 s), then returns
//! a row with `busy = 1` and no frame written after the open checkpointed,
//! and the WAL file keeps every frame written since the open (19.9 MB after
//! 900 puts of small items in `wal_checkpoint_while_a_snapshot_is_held`).
//! Once the stream ends the next checkpoint truncates it.
//!
//! No lock is needed: one read transaction reads the `tables`, `indexes`,
//! `vector_indexes` and data rows, and its first read pins the WAL snapshot
//! for all of them. The engine's writer lock is not taken, so writers never
//! wait on a snapshot being opened, and the snapshot never waits on a pool
//! connection while holding the lock. The in-catalog backup
//! (`backup.rs`, `create_backup_from_wal_snapshot`) does hold the lock while
//! it pins its snapshot, because it also inserts the CREATING catalog row in
//! that step and pages by rowid; this path writes nothing and pages by the
//! primary key, so the lock buys it nothing.
//!
//! The snapshot holds one pool connection and one backup slot
//! (`backup_server_slots`, shared with the in-catalog backup) until the
//! stream ends or is dropped, so the slot arithmetic that keeps half the
//! pool free for requests counts snapshots too; when no slot is free the
//! open returns `LimitExceeded` at once rather than queueing. The slot count
//! is stated once, in `SqliteConfig`'s doc (`config.rs`). The other side of
//! sharing: a `CreateBackup` waits 30 s for a slot, so snapshots holding
//! every slot make every `CreateBackup` wait and then fail with the shared
//! refusal (`backup::slots_exhausted`), which names table snapshots for that
//! reason.
//!
//! In-memory databases are pinned to a single connection (a second connection
//! would open a different database), so a stream that held its transaction
//! would hold the only connection and every other request would wait for it.
//! On such a database the open reads every page in its one read transaction,
//! keeps the items in memory, and returns the stream over that copy: the
//! open holds the connection for the length of the read (other requests wait
//! for it, as they wait for `create_backup_single_connection`, and fail after
//! the pool's acquire timeout if the read takes longer), and no connection is
//! held once it returns. The copy's stream keeps the backup slot until it
//! ends or is dropped, so the slot bounds how many copies exist at once: the
//! copy is the table again in decoded form, so with `k` copies open the
//! process holds the table `k + 1` times, and `k` is at most the slot count.
//! The snapshot kind and the consistency are those of the file-backed path:
//! one read transaction covers catalog and data.
//!
//! Items are read in primary-key order through row-value keyset pagination
//! on the primary key columns (`WHERE (pk, sk_x) > (?, ?) ORDER BY pk, sk_x
//! LIMIT n`), which `SQLite` serves from the primary key index. Multi-part
//! range keys fall back to rowid order and report `key_ordered = false`.
//!
//! Memory: one page is `LIMIT paging::DEFAULT_FETCH_BATCH` (1000) rows, all
//! of them held as item text at once. At the default 400 KB item limit one
//! row's text can reach 27.7 MB (the measured worst case in #394's line-cap
//! table), so a page is bounded by rows, not bytes. A byte budget (keyset
//! paging with a byte cutoff) is required before the stream gets a caller;
//! the backup design assigns it to the backup job PR.

use extenddb_core::types::{Item, ScalarAttributeType, TableDescription};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::{drop_deleting_indexes, key_schema_is_orderable, paging};
use extenddb_storage::util::sk_column;
use extenddb_storage::{SnapshotEngine, SnapshotInfo, TableSnapshot};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{Row, Sqlite, Transaction};

use crate::data::{all_sort_key_info, data_table_name};
use crate::store::SqliteEngine;

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
        // A snapshot holds one pool connection for the stream's life, the
        // same resource an in-catalog backup's reader holds, so it takes a
        // backup slot: the slot arithmetic in `backup.rs` keeps at least half
        // the pool free for requests, and without a slot enough open snapshots
        // would hold every connection and every writer would wait out the
        // pool's acquire timeout under the engine write lock. Refuse rather
        // than queue; the caller bounds concurrency.
        let permit = std::sync::Arc::clone(&self.backup_server_slots)
            .try_acquire_owned()
            .map_err(|_| crate::backup::slots_exhausted(self.backup_server_slot_count))?;

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
        // Both stages fall here: the first read above pinned the snapshot for
        // catalog and data alike, and no page is read until the stream is
        // polled.
        paging::open_pause(paging::OpenStage::AfterDefinition).await;
        let ddb_table = data_table_name(&table_id);

        let mode = paging_for(&description);
        let key_ordered = !matches!(mode, Paging::Rowid);
        paging::open_pause(paging::OpenStage::BeforeItems).await;
        let items = if self.in_memory {
            // The only connection must not stay with the stream (module
            // doc): read every page now, in the one transaction, and serve
            // the stream from the copy. Draining the inner stream ends the
            // transaction; the slot moves to the copy's stream, so the slot
            // bounds how many copies exist at once.
            let mut inner = keyset_stream(tx, None, ddb_table, mode);
            let mut copy = Vec::new();
            while let Some(item) = inner.next().await {
                copy.push(item?);
            }
            drop(inner);
            copy_stream(copy, permit)
        } else {
            keyset_stream(tx, Some(permit), ddb_table, mode)
        };
        Ok(TableSnapshot {
            description,
            info: SnapshotInfo {
                kind: SnapshotInfo::KIND_SQLITE_WAL_READ_TRANSACTION.to_owned(),
                // A WAL read transaction has no addressable position, so
                // there is nothing for a later change-log archive to anchor
                // on; the marker is for that and for nothing else (no
                // backend reopens a snapshot from it).
                marker: String::new(),
            },
            key_ordered,
            items,
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

/// The in-memory copy as a stream that owns the backup slot until it has
/// yielded its last item (or is dropped), so a consumer that stops at the
/// item count and keeps the stream does not keep the slot.
fn copy_stream(
    copy: Vec<Item>,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> BoxStream<'static, Result<Item, StorageError>> {
    let permit = (!copy.is_empty()).then_some(permit);
    Box::pin(futures::stream::unfold(
        (copy.into_iter(), permit),
        |(mut items, mut permit)| async move {
            let item = items.next()?;
            if items.len() == 0 {
                permit = None;
            }
            Some((Ok(item), (items, permit)))
        },
    ))
}

/// Page through `table` in one read transaction. `permit` is the backup slot
/// the stream holds until its transaction ends (`None` for the in-memory
/// copy's inner read, whose slot the copy's stream holds): at the short page
/// that shows the end, at the first error, or when the stream is dropped,
/// whichever comes first. A consumer that stops at the last item or at the
/// first `Err` and keeps the stream therefore holds no slot a `CreateBackup`
/// is waiting for.
fn keyset_stream(
    tx: Transaction<'static, Sqlite>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    table: String,
    mode: Paging,
) -> BoxStream<'static, Result<Item, StorageError>> {
    struct State {
        tx: Option<Transaction<'static, Sqlite>>,
        /// The backup slot, released with the transaction.
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        table: String,
        paging: Paging,
        cursor: Cursor,
        buffer: std::vec::IntoIter<Item>,
        done: bool,
    }
    let state = State {
        tx: Some(tx),
        permit,
        table,
        paging: mode,
        cursor: Cursor::Start,
        buffer: Vec::new().into_iter(),
        done: false,
    };
    // Read the page size once so a change to the test knob during a stream
    // cannot end it early or late.
    let batch = paging::fetch_batch();
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
                paging::record_fetch();
                let page = fetch_page(tx, &state.table, &state.paging, &state.cursor, limit).await;
                let (items, next_cursor) = match page {
                    Ok(page) => page,
                    Err(e) => {
                        state.done = true;
                        state.tx = None;
                        state.permit = None;
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
                    state.permit = None;
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
    use extenddb_storage::snapshot_conformance::{self, PageSize};
    use extenddb_storage::{SnapshotEngine, TableEngine};
    use futures::StreamExt;

    use super::*;

    const ACCOUNT: &str = "123456789012";
    /// SQL backend: ordered keys, numbers to the service's 38 digits; the
    /// open's read transaction makes no `UpdateTable` wait.
    const EXPECT: snapshot_conformance::Expectations = snapshot_conformance::Expectations {
        key_ordered: true,
        numeric_key_digits: 38,
        update_table_waits_at: &[],
    };

    async fn prepare(engine: &SqliteEngine) {
        crate::migrations::apply_for_tests(&engine.pool).await;
        // Migration 1 seeds no setting, so the rows are written.
        for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
            sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, '0')")
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

    /// A file-backed engine on a scratch database under the cargo target
    /// directory, found from this test binary's own path
    /// (`<target>/<profile>/deps/<binary>`), so a relative `CARGO_TARGET_DIR`
    /// or a `--target-dir` on the command line, neither of which the test
    /// process can resolve from its environment, is honoured too. The files
    /// go in a `snapshot-scratch` directory beside the profile directories,
    /// which the test creates.
    async fn file_engine() -> FileEngine {
        let exe = std::env::current_exe().expect("the test binary's path");
        let target = exe
            .ancestors()
            .nth(3)
            .unwrap_or_else(|| panic!("{} is not <target>/<profile>/deps/<binary>", exe.display()));
        let scratch = target.join("snapshot-scratch");
        // Owner-only, like the database files the engine creates in it (0600),
        // so another local user cannot list the scratch databases either.
        let mut dir = std::fs::DirBuilder::new();
        dir.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut dir, 0o700);
        dir.create(&scratch)
            .unwrap_or_else(|e| panic!("create {}: {e}", scratch.display()));
        let path = scratch.join(format!("snapshot-test-{}.sqlite", uuid::Uuid::new_v4()));
        let engine = SqliteEngine::new(path.to_str().expect("UTF-8 path"), 4, "us-east-1", 409_600)
            .await
            .expect("file engine");
        prepare(&engine).await;
        FileEngine {
            engine: Some(engine),
            path,
        }
    }

    /// Every test here opens a snapshot or sets the shared page size, so each
    /// holds `snapshot_conformance::hooks_lock` for its whole body: the knob,
    /// the round-trip counter and the open pause are process-global, and a
    /// test that opened a snapshot outside the lock could take a pause the
    /// conformance check armed.
    async fn hooks() -> snapshot_conformance::HooksGuard {
        snapshot_conformance::hooks_lock().await
    }

    #[tokio::test]
    async fn snapshot_is_consistent_under_concurrent_writes() {
        let hooks = hooks().await;
        let engine = file_engine().await;
        snapshot_conformance::run(
            &hooks,
            &*engine,
            ACCOUNT,
            "snap_src",
            50,
            EXPECT,
            || async {
                engine
                    .process_control_plane_transitions()
                    .await
                    .expect("pump");
            },
        )
        .await;
        let pool = engine.pool.clone();
        snapshot_conformance::run_refusals(
            &hooks,
            &*engine,
            ACCOUNT,
            "snap_refused",
            || async {
                engine
                    .process_control_plane_transitions()
                    .await
                    .expect("pump");
            },
            |table| async move {
                sqlx::query("UPDATE tables SET table_status = 'UPDATING' WHERE table_name = ?")
                    .bind(table)
                    .execute(&pool)
                    .await
                    .expect("flip status");
            },
        )
        .await;
    }

    /// A range key whose attribute definition is missing (the shape issue
    /// #259 left behind) streams unordered and complete, not hash-only and
    /// short. It pages by rowid, at a page of seven so the rowid keyset
    /// query runs (40 items: six fetches), and no page repeats the previous
    /// page's last row.
    #[tokio::test]
    async fn missing_sort_key_definition_streams_unordered_and_complete() {
        use extenddb_core::expression::ExpressionMaps;
        use extenddb_core::types::{AttributeValue, DescribeTableInput, TableKeyInfo};
        use extenddb_storage::DataEngine;
        let _hooks = hooks().await;
        let engine = file_engine().await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("damaged"), false)
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "damaged", || async {
            engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        })
        .await;
        let desc = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "damaged".to_owned(),
                },
            )
            .await
            .expect("describe");
        let ki = TableKeyInfo {
            table_name: "damaged".to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id: desc.table_id.clone(),
            key_schema: desc.key_schema.clone(),
            base_key_schema: desc.key_schema.clone(),
            attribute_definitions: desc.attribute_definitions.clone(),
            ..Default::default()
        };
        // Four partitions, ten items each: hash-only paging would lose rows.
        for i in 0..40u32 {
            let item: Item = [
                ("pk".to_owned(), AttributeValue::S(format!("p{}", i % 4))),
                ("sk".to_owned(), AttributeValue::N(i.to_string())),
            ]
            .into_iter()
            .collect();
            engine
                .put_item(&ki, item, false, None, &ExpressionMaps::default(), None)
                .await
                .expect("put");
        }
        // Damage the catalog the way #259 did: drop the sort key's definition.
        let defs: Vec<extenddb_core::types::AttributeDefinition> = desc
            .attribute_definitions
            .iter()
            .filter(|d| d.attribute_name != "sk")
            .cloned()
            .collect();
        sqlx::query("UPDATE tables SET attribute_definitions = ? WHERE table_name = ?")
            .bind(serde_json::to_string(&defs).unwrap())
            .bind("damaged")
            .execute(&engine.pool)
            .await
            .expect("damage");
        let page = PageSize::set(7);
        let fetches_before = paging::fetches();
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "damaged")
            .await
            .expect("open");
        assert!(
            !snapshot.key_ordered,
            "a key without a definition has no format order"
        );
        let items: Vec<Item> = snapshot.items.map(|r| r.expect("item")).collect().await;
        let fetches = paging::fetches() - fetches_before;
        drop(page);
        let mut keys = std::collections::BTreeSet::new();
        for item in &items {
            let key = format!("{:?} {:?}", item.get("pk"), item.get("sk"));
            assert!(keys.insert(key.clone()), "duplicate item {key}");
        }
        assert!(
            fetches >= 6,
            "{fetches} fetches: the rowid keyset query did not page at 7"
        );
        assert_eq!(items.len(), 40, "every item streams");
    }

    /// Every open takes a backup slot; with none free the open is refused at
    /// once, and dropping a stream frees its slot.
    #[tokio::test]
    async fn snapshots_take_backup_slots() {
        let _hooks = hooks().await;
        let engine = file_engine().await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("slots"), false)
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "slots", || async {
            engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        })
        .await;
        let slots = engine.backup_server_slot_count;
        let mut held = Vec::new();
        for _ in 0..slots {
            held.push(
                engine
                    .open_table_snapshot(ACCOUNT, "slots")
                    .await
                    .expect("open"),
            );
        }
        let err = engine
            .open_table_snapshot(ACCOUNT, "slots")
            .await
            .expect_err("no slot left");
        assert!(matches!(err, StorageError::LimitExceeded(_)), "{err}");
        // CreateBackup's admission on the same slots, with its 30 s wait
        // shortened: with snapshots holding every slot and no backup running,
        // the refusal must name snapshots.
        match crate::backup::acquire_backup_permit(
            std::sync::Arc::clone(&engine.backup_server_slots),
            engine.backup_server_slot_count,
            std::time::Duration::from_millis(50),
        )
        .await
        {
            Err(StorageError::LimitExceeded(m)) => assert!(
                m.contains("backups or table snapshots") && m.contains(&format!("at most {slots}")),
                "{m}"
            ),
            other => panic!(
                "CreateBackup is refused while snapshots hold every slot: {:?}",
                other.err()
            ),
        }
        drop(held.pop());
        engine
            .open_table_snapshot(ACCOUNT, "slots")
            .await
            .expect("a freed slot is reusable");
    }

    /// What a held snapshot does to checkpoints: `PRAGMA
    /// wal_checkpoint(TRUNCATE)` while a stream is open, after writes that
    /// followed the open. Pins the reply the module doc describes.
    #[tokio::test]
    async fn wal_checkpoint_while_a_snapshot_is_held() {
        use futures::StreamExt as _;
        let _hooks = hooks().await;
        let engine = file_engine().await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("wal"), false)
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, "wal", || async {
            engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        })
        .await;
        snapshot_conformance::populate(&*engine, ACCOUNT, "wal", 20).await;
        let wal = format!("{}-wal", engine.path.display());
        let checkpoint = |label: &'static str| {
            let pool = engine.pool.clone();
            async move {
                let started = std::time::Instant::now();
                let (busy, log, done): (i64, i64, i64) =
                    sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
                        .fetch_one(&pool)
                        .await
                        .expect("checkpoint");
                eprintln!(
                    "{label}: busy={busy} log={log} checkpointed={done} in {:?}",
                    started.elapsed()
                );
                (busy, log, done)
            }
        };
        checkpoint("before the open").await;
        let page = PageSize::set(5);
        let mut snapshot = engine
            .open_table_snapshot(ACCOUNT, "wal")
            .await
            .expect("open");
        snapshot.items.next().await.expect("an item").expect("ok");
        drop(page);
        snapshot_conformance::populate(&*engine, ACCOUNT, "wal", 900).await;
        let held = checkpoint("snapshot held").await;
        let held_size = std::fs::metadata(&wal).expect("WAL file").len();
        eprintln!("WAL while held: {held_size} bytes");
        drop(snapshot);
        // The pooled connection's rollback and return run on the runtime.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let released = checkpoint("snapshot dropped").await;
        let released_size = std::fs::metadata(&wal).expect("WAL file").len();
        eprintln!("WAL after release: {released_size} bytes");
        // The statement does not fail while the snapshot is held: it waits
        // out the busy timeout and reports busy with nothing checkpointed,
        // so the WAL keeps every frame written since the open.
        assert_eq!(held.0, 1, "busy while the snapshot is held");
        assert!(held.1 > 0, "the writes after the open are in the WAL");
        assert_eq!(held.2, 0, "no frame after the snapshot can be checkpointed");
        assert!(held_size > 0, "the WAL holds them on disk");
        assert_eq!(released.0, 0, "the checkpoint completes once it is dropped");
        assert_eq!(released_size, 0, "and truncates the WAL");
    }

    #[tokio::test]
    async fn snapshot_reports_key_order_for_single_range_key() {
        let _hooks = hooks().await;
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
        assert_eq!(
            snapshot.info.kind,
            SnapshotInfo::KIND_SQLITE_WAL_READ_TRANSACTION
        );
    }

    /// A restored table's snapshot description carries the same
    /// `restore_summary` as `describe_table`'s, on a file-backed engine and
    /// on an in-memory one (whose stream is a copy). The restore record is
    /// written directly, as `RestoreTableFromBackup` leaves it.
    #[tokio::test]
    async fn a_restored_table_describes_its_restore() {
        let hooks = hooks().await;
        let file = file_engine().await;
        let memory = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("memory engine");
        prepare(&memory).await;
        for engine in [&*file, &memory] {
            engine
                .create_table_impl(
                    ACCOUNT,
                    snapshot_conformance::table_input("restored"),
                    false,
                )
                .await
                .expect("create");
            snapshot_conformance::populate(engine, ACCOUNT, "restored", 5).await;
            sqlx::query(
                "INSERT INTO table_restores \
                 (table_id, source_backup_arn, source_table_arn, restore_date_time) \
                 SELECT table_id, ?, ?, '2026-10-01T12:00:00.250Z' FROM tables \
                 WHERE table_name = 'restored'",
            )
            .bind(
                "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01759320000250-0a1b2c3d",
            )
            .bind("arn:aws:dynamodb:us-east-1:123456789012:table/src")
            .execute(&engine.pool)
            .await
            .expect("restore record");
            snapshot_conformance::assert_snapshot_describes_the_table(
                &hooks, engine, ACCOUNT, "restored", true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn in_memory_snapshot_copies_the_table_and_releases_the_connection() {
        let _hooks = hooks().await;
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("memory engine");
        prepare(&engine).await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("mem"), false)
            .await
            .expect("create");
        snapshot_conformance::populate(&engine, ACCOUNT, "mem", 40).await;
        let snapshot = engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .expect("open");
        assert!(snapshot.key_ordered);
        // The stream holds nothing: on the one connection these writes would
        // otherwise wait for it until the pool's acquire timeout.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            snapshot_conformance::populate(&engine, ACCOUNT, "mem", 60),
        )
        .await
        .expect("writes proceed while the stream is held");
        let items: Vec<Item> = snapshot.items.map(|r| r.expect("item")).collect().await;
        assert_eq!(
            items.len(),
            40,
            "the items as of the open, not the later 20"
        );
        let order = extenddb_core::backup_format::KeyOrder::resolve(
            &snapshot.description.key_schema,
            &snapshot.description.attribute_definitions,
        )
        .expect("key order");
        assert!(
            items
                .windows(2)
                .all(|w| order.compare(&w[0], &w[1]).expect("compare").is_lt()),
            "key order"
        );
    }

    /// The in-memory copy's stream keeps its backup slot until it has yielded
    /// its last item or is dropped, so the slot bounds how many decoded copies
    /// of a table exist: with every slot held by a copy a further open is
    /// refused, and a drained copy frees its slot.
    #[tokio::test]
    async fn in_memory_copies_hold_their_backup_slot() {
        let _hooks = hooks().await;
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("memory engine");
        prepare(&engine).await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("mem"), false)
            .await
            .expect("create");
        snapshot_conformance::populate(&engine, ACCOUNT, "mem", 10).await;
        let slots = engine.backup_server_slot_count;
        let mut held = Vec::new();
        for i in 0..slots {
            held.push(
                engine
                    .open_table_snapshot(ACCOUNT, "mem")
                    .await
                    .unwrap_or_else(|e| panic!("open {i}: {e}")),
            );
        }
        let err = engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .expect_err("every slot is held by a copy");
        assert!(matches!(err, StorageError::LimitExceeded(_)), "{err}");
        let drained = held.pop().expect("a held copy");
        let items: Vec<Item> = drained.items.map(|r| r.expect("item")).collect().await;
        assert_eq!(items.len(), 10);
        engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .expect("a drained copy frees its slot");
    }

    /// A file-backed engine with one backup slot (pool 4) and a populated
    /// check table, for the tests that hold a finished stream.
    async fn one_slot_engine(table: &str, items: usize) -> FileEngine {
        let engine = file_engine().await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input(table), false)
            .await
            .expect("create");
        snapshot_conformance::wait_active(&*engine, ACCOUNT, table, || async {
            engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        })
        .await;
        snapshot_conformance::populate(&*engine, ACCOUNT, table, items).await;
        assert_eq!(
            engine.backup_server_slot_count, 1,
            "precondition: one backup slot, so a second open needs the first one's"
        );
        engine
    }

    /// A stream ends its read transaction and gives up its backup slot at
    /// the fetch that shows the end (a short page), not when it next yields
    /// `None` or is dropped: with every item read and the stream still held,
    /// a checkpoint is not held back by it and the one slot is free for
    /// another open. Twenty-two items at a page of five: the fifth fetch
    /// returns two rows, so the end is known before the last item is yielded
    /// (after `None` the stream's state is dropped, which would release
    /// everything whether or not the last page did).
    #[tokio::test]
    async fn a_drained_stream_ends_its_transaction() {
        let _hooks = hooks().await;
        let engine = one_slot_engine("drained", 22).await;
        let page = PageSize::set(5);
        let mut snapshot = engine
            .open_table_snapshot(ACCOUNT, "drained")
            .await
            .expect("open");
        for _ in 0..22 {
            snapshot
                .items
                .next()
                .await
                .expect("an item")
                .expect("stream item");
        }
        drop(page);
        let (busy, _, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(&engine.pool)
            .await
            .expect("checkpoint");
        assert_eq!(busy, 0, "the drained stream's read transaction has ended");
        let second = engine
            .open_table_snapshot(ACCOUNT, "drained")
            .await
            .unwrap_or_else(|e| panic!("open with the drained stream held: {e}"));
        drop(second);
        assert!(
            snapshot.items.next().await.is_none(),
            "twenty-two items, then the end"
        );
    }

    /// A page that fails (a row whose item does not parse) ends the stream at
    /// the error and frees its backup slot at once: with the errored stream
    /// still held, the one slot is free for another open. The unparseable row
    /// is the seventh in key order, on the second (full) page of five, so the
    /// error is not also the end of the table.
    #[tokio::test]
    async fn a_fetch_error_ends_the_stream_and_frees_its_permit() {
        let _hooks = hooks().await;
        let engine = one_slot_engine("ended", 20).await;
        let desc = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "ended".to_owned(),
                },
            )
            .await
            .expect("describe");
        sqlx::query(&format!(
            "UPDATE {} SET item_data = '{{\"pk\": 5}}' WHERE pk = 'p006'",
            data_table_name(&desc.table_id)
        ))
        .execute(&engine.pool)
        .await
        .expect("make one row unparseable");
        let page = PageSize::set(5);
        let mut snapshot = engine
            .open_table_snapshot(ACCOUNT, "ended")
            .await
            .expect("open");
        let mut items = 0;
        loop {
            match snapshot.items.next().await {
                Some(Ok(_)) => items += 1,
                Some(Err(e)) => {
                    eprintln!("page error after {items} items: {e}");
                    break;
                }
                None => panic!("the stream ended without reporting the unparseable row"),
            }
        }
        drop(page);
        assert_eq!(items, 5, "the first page, then the error");
        let second = engine
            .open_table_snapshot(ACCOUNT, "ended")
            .await
            .unwrap_or_else(|e| panic!("open with the errored stream held: {e}"));
        drop(second);
        assert!(
            snapshot.items.next().await.is_none(),
            "nothing follows the first error"
        );
    }

    /// The in-memory copy's stream gives up its backup slot with its last
    /// item, not when it next yields `None` or is dropped: with every item
    /// read and the copy still held, a further open gets the slot.
    #[tokio::test]
    async fn an_in_memory_copy_read_to_its_last_item_frees_its_slot() {
        let _hooks = hooks().await;
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("memory engine");
        prepare(&engine).await;
        engine
            .create_table_impl(ACCOUNT, snapshot_conformance::table_input("mem"), false)
            .await
            .expect("create");
        snapshot_conformance::populate(&engine, ACCOUNT, "mem", 10).await;
        assert_eq!(
            engine.backup_server_slot_count, 1,
            "precondition: one backup slot"
        );
        let mut copy = engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .expect("open");
        for _ in 0..10 {
            copy.items.next().await.expect("an item").expect("item");
        }
        let second = engine
            .open_table_snapshot(ACCOUNT, "mem")
            .await
            .unwrap_or_else(|e| panic!("open with the read copy held: {e}"));
        drop(second);
        assert!(copy.items.next().await.is_none(), "ten items, then the end");
    }
}
