// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane side of a store-backed restore on `PostgreSQL`
//! ([`RestoreEngine`]): the items of one verified data file written into the
//! CREATING target, the progress record, the ACTIVE flip and the counts.
//!
//! One data transaction per file: the base rows in multi-row INSERTs of
//! `COPY_BATCH_ITEMS` rows or `COPY_BATCH_BYTES` of item text, each
//! secondary index's rows beside them (the `insert_restore_rows` the legacy
//! copy shares), each vector index's rows through the write path's
//! `VectorInsertPlan`, and the file's `restore_progress` row last, so a file
//! commits whole with its record or not at all. The data database has no
//! `tables` row and no job record, so nothing in `bulk_load` reads or
//! conditions on the claim's generation: the fence is the base table's
//! primary key, which refuses a file applied twice, and the claim's
//! liveness, which the engine confirms before each file. Plain INSERTs, so
//! a key the backup holds twice, within a file or across files, fails the
//! file (SQLSTATE 23505) instead of collapsing two items. The progress row
//! is a plain INSERT for a file applied from its start and an upsert
//! (`ON CONFLICT (table_id, file_index) DO UPDATE SET items_applied`) for a
//! file resumed past it, so a row left short of the file's count by a
//! decoder that died after the commit is completed by the resume.
//!
//! `finish_restore` is `activate_restore_target` (the catalog transaction
//! the legacy copy ends with: the record DONE, the table and every index
//! ACTIVE, conditioned on the generation and on CREATING), then the
//! progress rows deleted on the data pool. The delete runs after the commit
//! and cannot fail the restore: its error is logged at WARN and
//! `Finished::Active` returned, since the restore is complete once the flip
//! committed and the rows are the reconciler's `sweep_restore_progress` to
//! remove, as they are after a crash between the two.

use std::collections::HashSet;

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement};
use extenddb_storage::backup_catalog::{
    JobClaim, RESTORE_POSTGRES_BEFORE_PROGRESS_DELETE, RestoreTargetKey, restore_pause_point,
};
use extenddb_storage::backup_definition::{COPY_BATCH_BYTES, COPY_BATCH_ITEMS};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::PostgresEngine;
use crate::backup_engine::{RestoreTarget, RowInsertError, insert_restore_rows};
use crate::catalog_json::from_catalog_json;
use crate::data::data_table_name;
use crate::data::vector_index::{VectorInsertPlan, fetch_vector_indexes_for_table};

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

/// The key shape of a CREATING restore target, read from its catalog row
/// through the escape of `catalog_json`.
struct TargetShape {
    key_schema: Vec<KeySchemaElement>,
    attribute_definitions: Vec<AttributeDefinition>,
}

impl PostgresEngine {
    /// The target's key schema and attribute definitions, when `key` names
    /// a CREATING restore target with this table id.
    async fn restore_target_shape(
        &self,
        key: &RestoreTargetKey,
    ) -> Result<TargetShape, StorageError> {
        let row: Option<(serde_json::Value, serde_json::Value)> = sqlx::query_as(
            "SELECT t.key_schema, t.attribute_definitions FROM tables t \
             JOIN table_restores r ON r.table_id = t.table_id \
             WHERE t.table_id = $1 AND t.account_id = $2 AND t.table_name = $3 \
             AND t.table_status = 'CREATING'",
        )
        .bind(&key.table_id)
        .bind(&key.account_id)
        .bind(&key.table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        let (key_schema, attribute_definitions) = row.ok_or_else(|| {
            StorageError::TableNotFound(format!(
                "{} is not a CREATING restore target with table id {}",
                key.table_name, key.table_id
            ))
        })?;
        Ok(TargetShape {
            key_schema: from_catalog_json(key_schema)
                .map_err(|e| StorageError::Internal(format!("key schema: {e}")))?,
            attribute_definitions: from_catalog_json(attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("attribute definitions: {e}")))?,
        })
    }
}

/// The error for a key the target already holds: the backup carries it
/// twice, within the file or in a file applied before it. The engine
/// prefixes the file's key, so the file is not named here.
fn duplicate_key_error(e: &sqlx::Error) -> StorageError {
    StorageError::Validation(format!(
        "holds a primary key the target already has (the backup holds a key twice): {e}"
    ))
}

/// Write `batch` into the base table, every secondary index and every vector
/// index inside the file's transaction, and clear it.
async fn flush_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &RestoreTarget<'_>,
    vector_plans: &[VectorInsertPlan<'_>],
    batch: &mut Vec<(Item, String)>,
) -> Result<u64, StorageError> {
    insert_restore_rows(tx, target, batch)
        .await
        .map_err(|e| match e {
            RowInsertError::DuplicateKey(e) => duplicate_key_error(&e),
            RowInsertError::Storage(e) => e,
        })?;
    for plan in vector_plans {
        for (item, _) in batch.iter() {
            plan.insert(tx, item).await?;
        }
    }
    let written = u64::try_from(batch.len()).unwrap_or(u64::MAX);
    batch.clear();
    Ok(written)
}

impl RestoreEngine for PostgresEngine {
    fn bulk_load<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        _claim: &'a dyn JobClaim,
        file: BulkLoadFile,
        mut items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            use futures::StreamExt;

            let shape = self.restore_target_shape(target).await?;
            let source = format!("data file {}", file.file_index);
            let rows = RestoreTarget::load(
                &self.pool,
                &target.table_id,
                &shape.key_schema,
                &shape.attribute_definitions,
                source.clone(),
            )
            .await?;
            let vector_metas = fetch_vector_indexes_for_table(&self.pool, &target.table_id).await?;
            let vector_plans: Vec<VectorInsertPlan<'_>> = vector_metas
                .iter()
                .map(|(meta, _)| {
                    VectorInsertPlan::new(meta, &shape.key_schema, &shape.attribute_definitions)
                })
                .collect();

            let mut tx = self.data_pool.begin().await.map_err(db_err)?;
            let mut batch: Vec<(Item, String)> = Vec::with_capacity(COPY_BATCH_ITEMS);
            let mut batch_bytes = 0usize;
            let mut applied: u64 = 0;
            while let Some(item) = items.next().await {
                let item = item?;
                let item_json = serde_json::to_string(&item)
                    .map_err(|e| StorageError::Internal(format!("item: {e}")))?;
                batch_bytes += item_json.len();
                batch.push((item, item_json));
                if batch.len() == COPY_BATCH_ITEMS || batch_bytes >= COPY_BATCH_BYTES {
                    applied += flush_batch(&mut tx, &rows, &vector_plans, &mut batch).await?;
                    batch_bytes = 0;
                }
            }
            if !batch.is_empty() {
                applied += flush_batch(&mut tx, &rows, &vector_plans, &mut batch).await?;
            }
            // The file's record, in the file's transaction. A file applied from
            // its start writes a new row: a duplicate `(table_id, file_index)`
            // is a file applied twice, which the base table's primary key has
            // refused above for any file with items. A file resumed past its
            // start (`start_at > 0`) updates the row the earlier run left
            // (one with `items_applied` short of the file's count: the
            // decoder died after the file's commit and the job's FAILED write
            // never landed), so the resume completes the file instead of
            // failing on the row's key.
            let progress_row = if file.start_at > 0 {
                "INSERT INTO restore_progress (table_id, file_index, items_applied) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (table_id, file_index) DO UPDATE \
                 SET items_applied = EXCLUDED.items_applied"
            } else {
                "INSERT INTO restore_progress (table_id, file_index, items_applied) \
                 VALUES ($1, $2, $3)"
            };
            sqlx::query(progress_row)
                .bind(&target.table_id)
                .bind(i64::from(file.file_index))
                .bind(i64::try_from(file.start_at.saturating_add(applied)).unwrap_or(i64::MAX))
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    let duplicate = e
                        .as_database_error()
                        .and_then(|d| d.code())
                        .is_some_and(|code| code == "23505");
                    if duplicate {
                        StorageError::Validation(format!(
                            "{source} was applied before and its progress recorded: {e}"
                        ))
                    } else {
                        db_err(e)
                    }
                })?;
            tx.commit().await.map_err(db_err)?;
            Ok(applied)
        })
    }

    fn finish_restore<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        Box::pin(async move {
            // The key names the table; the flip itself is conditioned on the
            // table id, CREATING, RUNNING and the generation.
            let is_target: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE table_id = $1 AND account_id = $2 \
                 AND table_name = $3)",
            )
            .bind(&target.table_id)
            .bind(&target.account_id)
            .bind(&target.table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if !is_target {
                return Ok(Finished::NotMatched(format!(
                    "{} is not a table with id {} in account {}",
                    target.table_name, target.table_id, target.account_id
                )));
            }
            let activated = self
                .activate_restore_target(
                    &target.table_id,
                    claim.generation(),
                    i64::try_from(item_count).unwrap_or(i64::MAX),
                    i64::try_from(size_bytes).unwrap_or(i64::MAX),
                )
                .await?;
            if !activated {
                return Ok(Finished::NotMatched(format!(
                    "the job record of {} is not RUNNING under generation {}, or the table is \
                     not CREATING",
                    target.table_name,
                    claim.generation()
                )));
            }
            restore_pause_point(RESTORE_POSTGRES_BEFORE_PROGRESS_DELETE).await;
            // The flip has committed: the table is ACTIVE and the record DONE,
            // so the restore is complete whatever happens to the progress
            // rows. A failure here is logged, not returned: an `Err` would make
            // the engine fail a finished restore, and the rows are harmless
            // (the data database reads them for nothing once the record is
            // DONE) and the reconciler's `sweep_restore_progress` removes them.
            if let Err(e) = sqlx::query("DELETE FROM restore_progress WHERE table_id = $1")
                .bind(&target.table_id)
                .execute(&self.data_pool)
                .await
            {
                tracing::warn!(
                    "restore of table {}: the table is ACTIVE and its record DONE, but the \
                     restore_progress rows of table id {} could not be deleted: {e}; the progress \
                     rows are the reconciler's to sweep",
                    target.table_name,
                    target.table_id
                );
            }
            Ok(Finished::Active)
        })
    }

    fn count_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // The id names the data table; the key's account and name are
            // checked against the catalog so a bare id reads no other
            // account's table.
            self.restore_target_shape(key).await?;
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {}",
                data_table_name(&key.table_id)
            ))
            .fetch_one(&self.data_pool)
            .await
            .map_err(db_err)?;
            Ok(u64::try_from(count).unwrap_or_default())
        })
    }

    fn restore_target_ready<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The catalog row first, so a bare id reads no other account's
            // table; then the data table's existence (the registration
            // commits the catalog transaction before the data DDL).
            self.restore_target_shape(key).await?;
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(data_table_name(&key.table_id))
                .fetch_one(&self.data_pool)
                .await
                .map_err(db_err)?;
            Ok(exists)
        })
    }

    fn read_restore_progress<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Vec<(u32, u64)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(i64, i64)> = sqlx::query_as(
                "SELECT file_index, items_applied FROM restore_progress WHERE table_id = $1 \
                 ORDER BY file_index",
            )
            .bind(&key.table_id)
            .fetch_all(&self.data_pool)
            .await
            .map_err(db_err)?;
            Ok(rows
                .into_iter()
                .map(|(index, applied)| {
                    (
                        u32::try_from(index).unwrap_or(u32::MAX),
                        u64::try_from(applied).unwrap_or_default(),
                    )
                })
                .collect())
        })
    }

    fn sweep_restore_progress<'a>(
        &'a self,
        running: &'a [String],
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // A positive list, candidates before the live set (the trait
            // doc): the ids the progress rows name now, then the catalog's
            // RUNNING records; a target registered after `running` was
            // listed is RUNNING here, and one whose first row lands after
            // the first read is not among the candidates.
            let named: Vec<String> =
                sqlx::query_scalar("SELECT DISTINCT table_id FROM restore_progress")
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(db_err)?;
            if named.is_empty() {
                return Ok(0);
            }
            // A test may interpose here, after the candidate ids are read and
            // before the live RUNNING set, to commit a registration and a
            // progress row: a row of a record that is RUNNING at the read
            // below is protected (the two reads order candidates before the
            // live set, the trait doc).
            #[cfg(feature = "test-hooks")]
            sweep_pause::maybe_pause().await;
            let live: Vec<String> =
                sqlx::query_scalar("SELECT table_id FROM table_restores WHERE status = 'RUNNING'")
                    .fetch_all(&self.pool)
                    .await
                    .map_err(db_err)?;
            let protected: HashSet<&str> = running
                .iter()
                .chain(live.iter())
                .map(String::as_str)
                .collect();
            let stale: Vec<String> = named
                .into_iter()
                .filter(|id| !protected.contains(id.as_str()))
                .collect();
            if stale.is_empty() {
                return Ok(0);
            }
            let removed =
                sqlx::query("DELETE FROM restore_progress WHERE table_id = ANY($1::TEXT[])")
                    .bind(&stale)
                    .execute(&self.data_pool)
                    .await
                    .map_err(db_err)?
                    .rows_affected();
            Ok(removed)
        })
    }
}

/// A rendezvous a test arms to pause `sweep_restore_progress` between its
/// two reads (the candidate progress-row ids, then the live RUNNING set),
/// so it can commit a registration and a progress row in between and prove
/// that row survives the sweep. `test-hooks` only.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub mod sweep_pause {
    use std::sync::{LazyLock, Mutex};

    use tokio::sync::Notify;

    struct Rendezvous {
        arrived: std::sync::Arc<Notify>,
        proceed: std::sync::Arc<Notify>,
    }

    static PAUSE: LazyLock<Mutex<Option<Rendezvous>>> = LazyLock::new(|| Mutex::new(None));

    /// Arm the pause once. Returns `(arrived, proceed)`: the sweep notifies
    /// `arrived` when it reaches the point and waits on `proceed`.
    #[must_use]
    pub fn arm() -> (std::sync::Arc<Notify>, std::sync::Arc<Notify>) {
        let arrived = std::sync::Arc::new(Notify::new());
        let proceed = std::sync::Arc::new(Notify::new());
        *PAUSE.lock().unwrap() = Some(Rendezvous {
            arrived: arrived.clone(),
            proceed: proceed.clone(),
        });
        (arrived, proceed)
    }

    pub(crate) async fn maybe_pause() {
        let armed = PAUSE.lock().unwrap().take();
        if let Some(r) = armed {
            r.arrived.notify_one();
            r.proceed.notified().await;
        }
    }
}
