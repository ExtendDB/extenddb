// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` for the SQLite backend.
//!
//! Writes acquire the engine write lock (D1) and run in a transaction, so the
//! condition check, the write, index sync, and stream capture are one atomic
//! unit with no competing writer — no `INSERT ... ON CONFLICT DO NOTHING` race
//! dance is needed.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo, item_size_bytes};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{pk_to_text, sk_column, sk_info};
use extenddb_storage::{RowOutcome, StreamCapture};

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::{fetch_item_in_tx, upsert_item_in_tx, write_stream_record_in_tx};
use super::{bind_sk_fetch_optional, data_table_name, json_to_item};
use crate::store::SqliteEngine;

/// One marked row's ledger entry: its outcome, the size of `old`, the item
/// the row replaced or was evaluated against, and that item when the marker
/// asks to keep it or the condition failed. A batch builds one as each
/// marked row is written and inserts them together before its commit.
struct LedgerRow<'m> {
    marker: &'m extenddb_storage::BulkRowMarker,
    outcome: RowOutcome,
    old_size: Option<i64>,
    old_image: Option<String>,
}

impl<'m> LedgerRow<'m> {
    fn new(
        marker: &'m extenddb_storage::BulkRowMarker,
        outcome: RowOutcome,
        old: Option<&Item>,
    ) -> Result<Self, StorageError> {
        let old_size = old.map(|item| i64::try_from(item_size_bytes(item)).unwrap_or(i64::MAX));
        let keep = marker.keep_old || outcome == RowOutcome::ConditionFailed;
        let old_image = match old {
            Some(item) if keep => Some(
                serde_json::to_string(item).map_err(|e| StorageError::Internal(e.to_string()))?,
            ),
            _ => None,
        };
        Ok(Self {
            marker,
            outcome,
            old_size,
            old_image,
        })
    }
}

/// Values one ledger row binds: the seven columns of the insert.
const LEDGER_COLUMNS: usize = 7;

/// Ledger rows per `INSERT`. SQLite binds at most `SQLITE_MAX_VARIABLE_NUMBER`
/// values to a statement, 32,766 by default since SQLite 3.32.0; the bundled
/// SQLite (3.46.0, through `libsqlite3-sys`) is built with that default. At
/// seven values a row that is 4,680 rows. The bulk runner's batches hold at
/// most 100 rows (`bulk_put_write_batch_rows`), so each is one statement.
const LEDGER_ROWS_PER_STATEMENT: usize = 32_766 / LEDGER_COLUMNS;

/// Insert `bulk_row_outcomes` ledger rows, one multi-row `INSERT` per
/// `LEDGER_ROWS_PER_STATEMENT` rows. The caller chooses the connection: the
/// write transaction's for a batch and for an applied single row, a pool
/// connection (a single-statement transaction) for a failed condition on
/// the single-row path.
///
/// A plain insert: the ledger key is the row's position, and a second
/// outcome for one position, in these rows or already recorded, means the
/// row was submitted after it had been applied, which the batch refuses by
/// failing.
async fn insert_row_outcomes(
    conn: &mut sqlx::SqliteConnection,
    rows: &[LedgerRow<'_>],
) -> Result<(), StorageError> {
    for chunk in rows.chunks(LEDGER_ROWS_PER_STATEMENT) {
        let values = vec!["(?, ?, ?, ?, ?, ?, ?)"; chunk.len()].join(", ");
        let sql = format!(
            "INSERT INTO bulk_row_outcomes \
             (account_id, operation_id, file_index, row_index, outcome, old_size, old_image) \
             VALUES {values}"
        );
        let mut query = sqlx::query(&sql);
        for row in chunk {
            query = query
                .bind(&row.marker.account_id)
                .bind(&row.marker.operation_id)
                .bind(row.marker.file_index)
                .bind(row.marker.row_index)
                .bind(row.outcome.as_str())
                .bind(row.old_size)
                .bind(&row.old_image);
        }
        query
            .execute(&mut *conn)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }
    Ok(())
}

/// The batch-level ledger work, before any row is written: refuse the batch
/// unless `marker.owner` is the owner the last takeover recorded, then
/// delete the operation's ledger rows the last checkpoint covers.
async fn fence_and_prune(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    marker: &extenddb_storage::BulkRowMarker,
) -> Result<(), StorageError> {
    let owner: Option<(String,)> = sqlx::query_as(
        "SELECT owner FROM bulk_write_owners WHERE account_id = ? AND operation_id = ?",
    )
    .bind(&marker.account_id)
    .bind(&marker.operation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(crate::sqlite_util::map_sqlx_err)?;
    if owner.as_ref().map(|(owner,)| owner.as_str()) != Some(marker.owner.as_str()) {
        return Err(StorageError::Transient(format!(
            "bulk write batch refused: operation {} is owned by another claim",
            marker.operation_id
        )));
    }
    let (file, row) = marker.settled;
    sqlx::query(
        "DELETE FROM bulk_row_outcomes WHERE account_id = ? AND operation_id = ? \
         AND (file_index < ? OR (file_index = ? AND row_index < ?))",
    )
    .bind(&marker.account_id)
    .bind(&marker.operation_id)
    .bind(file)
    .bind(file)
    .bind(row)
    .execute(&mut **tx)
    .await
    .map_err(crate::sqlite_util::map_sqlx_err)?;
    Ok(())
}

/// What one row's write inside a transaction produced.
struct RowWrite {
    old: Option<Item>,
    /// Whether async index work was enqueued, so the caller wakes the
    /// workers after commit.
    enqueued: bool,
}

/// Index key attributes present in the item must match their declared
/// scalar type and be non-empty, matching real DynamoDB. This is up-front
/// input validation (a top-level ValidationException), so it runs before
/// any write work. Mirrors the PostgreSQL backend, including the definitions
/// it checks against: the ones read with the indexes, not the cached set.
fn validate_index_keys(
    indexes: &[super::index::IndexMeta],
    key_info: &TableKeyInfo,
    item: &Item,
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let index_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = indexes
        .iter()
        .map(|idx| extenddb_core::validation::IndexKeyRef {
            index_name: &idx.index_name,
            key_schema: &idx.key_schema,
        })
        .collect();
    extenddb_core::validation::validate_index_keys(
        item,
        &index_refs,
        super::index::fresh_attr_defs(indexes, &key_info.attribute_definitions),
    )
    .map_err(|e| StorageError::Validation(e.to_string()))
}

/// One row's write inside an open `BEGIN IMMEDIATE` transaction: the read
/// of the current image when anything needs it (a marked row needs it for
/// its ledger entry), the condition against it, the upsert, index and
/// stream maintenance. The caller records a marked row's outcome from the
/// returned image: a single write right after this returns, a batch with
/// its other rows before its commit.
///
/// A failed condition returns `Err(ConditionFailed(old))` after the read
/// and before any statement that changes a row, so the transaction stays
/// usable and a batch can continue with its next row.
#[allow(clippy::too_many_arguments)]
async fn put_row_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key_info: &TableKeyInfo,
    indexes: &[super::index::IndexMeta],
    system_delay: u64,
    item: &Item,
    return_old: bool,
    condition: Option<&Expr>,
    maps: &ExpressionMaps,
    stream: Option<&StreamCapture>,
    marker: Option<&extenddb_storage::BulkRowMarker>,
) -> Result<RowWrite, StorageError> {
    let need_old = condition.is_some()
        || return_old
        || !indexes.is_empty()
        || stream.is_some()
        || marker.is_some();

    let old = if need_old {
        fetch_item_in_tx(tx, key_info, item).await?
    } else {
        None
    };

    if condition.is_some() {
        let empty = Item::new();
        let target = old.as_ref().unwrap_or(&empty);
        if let Err(e) = check_condition(condition, target, maps) {
            return Err(match e {
                StorageError::ConditionFailed(_) => StorageError::ConditionFailed(old),
                other => other,
            });
        }
    }

    upsert_item_in_tx(tx, key_info, item).await?;

    // Synchronous indexes (LSIs + zero-delay GSIs) are applied in-txn;
    // async GSIs are enqueued into gsi_pending within the same txn.
    if !indexes.is_empty() {
        sync_indexes(
            tx,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            indexes,
            old.as_ref(),
            Some(item),
            system_delay,
        )
        .await?;
    }
    let enqueued_gsi = enqueue_async_indexes(
        tx,
        key_info,
        indexes,
        old.as_ref(),
        Some(item),
        system_delay,
    )
    .await?
        > 0;
    // Vector indexes: applied in this transaction when the propagation delay is
    // 0, otherwise enqueued alongside the async GSI work. Gated on the cached
    // key info so a table without them costs no extra query, and deliberately
    // outside the `indexes` guard above: a table may have a vector index and no
    // GSI or LSI at all.
    let enqueued_vector = if key_info.vector_indexes.is_empty() {
        false
    } else {
        crate::data::vector_index::maintain_vector_indexes(
            tx,
            &key_info.table_id,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            old.as_ref(),
            Some(item),
            system_delay,
        )
        .await?
            > 0
    };

    if let Some(capture) = stream {
        write_stream_record_in_tx(tx, key_info, capture, old.as_ref(), Some(item)).await?;
    }

    Ok(RowWrite {
        old,
        enqueued: enqueued_gsi || enqueued_vector,
    })
}

impl SqliteEngine {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
        marker: Option<&extenddb_storage::BulkRowMarker>,
    ) -> Result<Option<Item>, StorageError> {
        // Read the index set only after acquiring the write lock, so a GSI added
        // by a concurrent UpdateTable (which holds the same lock) cannot be missed
        // and left unmaintained by this write.
        // Read the propagation delay BEFORE taking the write lock. It is a
        // runtime setting, not an invariant of this write, so it does not need
        // to be read under the lock, and the lock serialises every write in the
        // process: work done inside it is the backend's throughput bottleneck.
        let system_delay = self.index_propagation_delay().await;
        let _writer = self.write_lock.lock().await;
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        validate_index_keys(&indexes, key_info, &item)?;

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let written = match put_row_in_tx(
            &mut tx,
            key_info,
            &indexes,
            system_delay,
            &item,
            return_old,
            condition,
            maps,
            stream,
            marker,
        )
        .await
        {
            Ok(written) => written,
            Err(StorageError::ConditionFailed(old)) => {
                // Nothing else is written for a failed condition, so the
                // write transaction is discarded and the outcome is recorded
                // as a single-statement transaction of its own, still under
                // the write lock (D1). The ledger row lets a resumed run tell
                // this failure from a fresh one instead of re-evaluating the
                // condition against the operation's own earlier writes.
                drop(tx);
                if let Some(m) = marker {
                    let mut conn = self
                        .pool
                        .acquire()
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let row = LedgerRow::new(m, RowOutcome::ConditionFailed, old.as_ref())?;
                    insert_row_outcomes(&mut conn, &[row]).await?;
                }
                return Err(StorageError::ConditionFailed(old));
            }
            Err(other) => return Err(other),
        };

        // The applied outcome rides the write transaction: both commit or
        // neither does, following the gsi_pending precedent for bookkeeping
        // rows inside the write transaction.
        if let Some(m) = marker {
            let row = LedgerRow::new(m, RowOutcome::Applied, written.old.as_ref())?;
            insert_row_outcomes(&mut tx, &[row]).await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if written.enqueued {
            self.gsi_notify.notify_waiters();
        }

        Ok(if return_old { written.old } else { None })
    }

    /// Implementation of `DataEngine::put_items_batch`: every row of the
    /// batch in one `BEGIN IMMEDIATE` transaction under one acquisition of
    /// the process write lock.
    ///
    /// The first marked row's owner is checked against the operation's
    /// recorded owner before anything is written, and the ledger rows the
    /// last checkpoint covers are deleted (`fence_and_prune`). Each row then
    /// goes through the same path a single put uses, and its outcome, a
    /// failed condition included, is kept with what the row replaced. After
    /// the last row the outcomes go into the ledger in this transaction, one
    /// multi-row insert for up to 4,680 rows, so a batch of the runner's
    /// size costs one ledger statement instead of one a row. Any other error
    /// returns before commit, so nothing from the batch is visible.
    pub(crate) async fn put_items_batch_impl(
        &self,
        key_info: &TableKeyInfo,
        rows: Vec<extenddb_storage::BatchRow>,
        stream: Option<&StreamCapture>,
    ) -> Result<Vec<extenddb_storage::BatchRowResult>, StorageError> {
        use extenddb_storage::BatchRowResult;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let system_delay = self.index_propagation_delay().await;
        let _writer = self.write_lock.lock().await;
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        for row in &rows {
            validate_index_keys(&indexes, key_info, &row.item)?;
        }

        // BUSY and LOCKED past the busy timeout (another writer holding the
        // file, a backup for example) and a pool timeout arrive as
        // `Transient` through `map_sqlx_err`, which the bulk runner resumes
        // after instead of failing the operation.
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
        if let Some(marker) = rows.iter().find_map(|row| row.marker.as_ref()) {
            fence_and_prune(&mut tx, marker).await?;
        }
        let mut results = Vec::with_capacity(rows.len());
        let mut ledger: Vec<LedgerRow<'_>> = Vec::with_capacity(rows.len());
        let mut enqueued = false;
        let default_maps = ExpressionMaps::default();

        for row in &rows {
            let (condition, maps) = match row.condition.as_ref() {
                Some((expr, maps)) => (Some(expr), maps),
                None => (None, &default_maps),
            };
            match put_row_in_tx(
                &mut tx,
                key_info,
                &indexes,
                system_delay,
                &row.item,
                row.return_old,
                condition,
                maps,
                stream,
                row.marker.as_ref(),
            )
            .await
            {
                Ok(written) => {
                    if let Some(m) = row.marker.as_ref() {
                        ledger.push(LedgerRow::new(
                            m,
                            RowOutcome::Applied,
                            written.old.as_ref(),
                        )?);
                    }
                    enqueued |= written.enqueued;
                    results.push(BatchRowResult::Written {
                        old: if row.return_old { written.old } else { None },
                    });
                }
                Err(StorageError::ConditionFailed(old)) => {
                    if let Some(m) = row.marker.as_ref() {
                        ledger.push(LedgerRow::new(
                            m,
                            RowOutcome::ConditionFailed,
                            old.as_ref(),
                        )?);
                    }
                    results.push(BatchRowResult::ConditionFailed { old });
                }
                Err(error) => return Err(error),
            }
        }

        insert_row_outcomes(&mut tx, &ledger).await?;
        tx.commit()
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
        if enqueued {
            self.gsi_notify.notify_waiters();
        }
        Ok(results)
    }

    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let json_opt = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = extenddb_storage::util::parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &self.pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk_text.as_ref())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}
