// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and point-in-time recovery implementation for `PostgreSQL` storage.

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription, GsiInput, Item,
    KeySchemaElement, LsiInput, PointInTimeRecoveryDescription, Projection, ScalarAttributeType,
    SourceTableDetails, TableDescription,
};
use extenddb_storage::backup_definition::{
    BACKUP_DEFINITION_VERSION, BackupTableDefinition, COPY_BATCH_BYTES, COPY_BATCH_ITEMS,
    ensure_single_part_base_key, throughput_from_catalog,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, composite_pk_to_text, parse_sk, sk_column_n};
use extenddb_storage::{BackupEngine, RestoreTableOverrides};
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::data::{
    all_sort_key_info, data_table_name, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index,
};

/// Current epoch milliseconds, used as the leading component of a backup id.
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Build the trailing `backup/<id>` component of a backup ARN.
///
/// `DynamoDB` formats this as `<epoch_millis>-<8 hex chars>` (for example
/// `01489602797149-73d8d5bc`). The random component is part of the identifier,
/// not decoration: a timestamp alone makes a backup id derivable by anyone who
/// knows roughly when the backup ran, so the id is only a usable handle for a
/// caller that was given it.
fn backup_id() -> String {
    use rand::Rng;
    let suffix: u32 = rand::rng().random();
    format!("{ts}-{suffix:08x}", ts = epoch_millis())
}

/// Convert a `PostgreSQL` `TIMESTAMPTZ` to epoch seconds as `f64`.
#[allow(clippy::cast_precision_loss)]
fn pg_timestamp_to_epoch(ts: time::OffsetDateTime) -> f64 {
    ts.unix_timestamp() as f64
}

/// Seed for the 64-bit restore lock key. Restore locks use the single-`bigint`
/// advisory lock form, which PostgreSQL keeps in a key space separate from the
/// two-`int4` form the migration and vector-build locks use, so they cannot
/// meet; the seed keeps this key space distinct from any other `bigint` user.
const RESTORE_LOCK_SEED: i64 = 0x0045_4452; // 'E', 'D', 'R'

/// Restores in flight at once in this process. Each holds one dedicated
/// connection for its lock on top of its pooled ones, so this bounds the
/// connections restores can open outside the configured pools. A restore
/// beyond the limit waits for a slot.
static RESTORE_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// How long a restore waits for a slot before it is refused with
/// `LimitExceededException`.
const RESTORE_SLOT_WAIT_SECS: u64 = 30;

/// How old an unowned restore target must be before it is treated as
/// abandoned. The owner takes its lock milliseconds after creating the
/// target, so this only has to cover that gap with a wide margin.
const ABANDONED_RESTORE_GRACE_SECS: i32 = 60;

/// A secondary index of a restore target, as the copy needs it.
struct RestoreIndex {
    table: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

/// Session-scoped ownership of one restore, held for the life of the copy.
///
/// A `pg_try_advisory_lock` on a dedicated connection: it dies with the
/// connection, so a crashed process's claim disappears on its own and the
/// abandoned-restore sweep can tell a dead restore from a running one.
///
/// If only this connection is lost while the copy carries on (its backend is
/// terminated, say), the sweep may claim the target after the grace period.
/// That cannot leave a wrong table: the claim and the restore's flip to
/// ACTIVE are both conditional on CREATING, so exactly one wins, and the
/// restore then fails with "deleted while it was being restored".
struct RestoreOwner {
    _conn: sqlx::PgConnection,
    _slot: Option<tokio::sync::SemaphorePermit<'static>>,
}

/// The advisory-lock key for a restore into `table_name` in `account_id`.
///
/// Keyed by name rather than table id so the restore can hold it before the
/// target exists: the sweep must never see an unowned target, including in
/// the window while `create_table_impl` is still running DDL. The key is a
/// 64-bit `hashtextextended`, so a collision is negligible, and one would
/// only make the sweep skip a table while the other name's lock is held,
/// never remove a table it should not.
fn restore_lock_key(account_id: &str, table_name: &str) -> String {
    format!("{account_id}/{table_name}")
}

async fn try_restore_lock(
    pool: &sqlx::PgPool,
    account_id: &str,
    table_name: &str,
    slot: Option<tokio::sync::SemaphorePermit<'static>>,
) -> Result<Option<RestoreOwner>, StorageError> {
    let options = pool.connect_options();
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect_with(&options)
        .await
        .map_err(|e| StorageError::Internal(format!("restore lock connection: {e}")))?;
    // This session sits idle for the whole copy. A server-side
    // idle_session_timeout would end it and release the lock while the
    // restore is still running, so disable it for this session when the
    // server permits. Some managed PostgreSQL services reject this SET;
    // ownership still works there, but remains subject to their idle timeout.
    if let Err(e) = sqlx::query("SET idle_session_timeout = 0")
        .execute(&mut conn)
        .await
    {
        tracing::warn!(
            "could not disable idle_session_timeout for the restore lock session; \
             continuing with the server setting: {e}"
        );
    }
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, $2))")
        .bind(restore_lock_key(account_id, table_name))
        .bind(RESTORE_LOCK_SEED)
        .fetch_one(&mut conn)
        .await
        .map_err(|e| StorageError::Internal(format!("restore lock: {e}")))?;
    Ok(taken.then_some(RestoreOwner {
        _conn: conn,
        _slot: slot,
    }))
}

/// Insert one buffered batch of backup rows in one statement and clear the
/// buffers. Returns the number written.
async fn insert_backup_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    backup_arn: &str,
    pks: &mut Vec<String>,
    datas: &mut Vec<String>,
) -> Result<i64, StorageError> {
    if pks.is_empty() {
        return Ok(0);
    }
    let n = i64::try_from(pks.len()).unwrap_or(i64::MAX);
    sqlx::query(
        "INSERT INTO backup_items (backup_arn, pk, item_data) \
         SELECT $1, pk, item_data::jsonb FROM UNNEST($2::text[], $3::text[]) AS t(pk, item_data)",
    )
    .bind(backup_arn)
    .bind(&*pks)
    .bind(&*datas)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    pks.clear();
    datas.clear();
    Ok(n)
}

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

impl PostgresEngine {
    /// Read the parts of a table's definition a restore recreates.
    async fn capture_table_definition(
        conn: &mut sqlx::PgConnection,
        table_id: &str,
    ) -> Result<(BackupTableDefinition, Vec<(String, String)>), StorageError> {
        let (billing_mode, pt, table_class, sse, on_demand): (
            String,
            Option<serde_json::Value>,
            Option<String>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
        ) = sqlx::query_as(
            "SELECT billing_mode, provisioned_throughput, table_class, sse_specification, \
             on_demand_throughput FROM tables WHERE table_id = $1",
        )
        .bind(table_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;

        let rows: Vec<(
            String,
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            Option<serde_json::Value>,
        )> = sqlx::query_as(
            "SELECT index_name, index_id, index_type, key_schema, projection, provisioned_throughput \
                 FROM indexes WHERE table_id = $1 ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        let mut gsis = Vec::new();
        let mut lsis = Vec::new();
        let mut gsi_ids = Vec::new();
        for (index_name, index_id, index_type, ks, proj, ipt) in rows {
            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks)
                .map_err(|e| StorageError::Internal(format!("index key schema: {e}")))?;
            let projection: Projection = serde_json::from_value(proj)
                .map_err(|e| StorageError::Internal(format!("index projection: {e}")))?;
            if index_type == "LSI" {
                lsis.push(LsiInput {
                    index_name,
                    key_schema,
                    projection,
                });
            } else {
                gsi_ids.push((index_name.clone(), index_id));
                gsis.push(GsiInput {
                    index_name,
                    key_schema,
                    projection,
                    provisioned_throughput: throughput_from_catalog(ipt.as_ref()),
                });
            }
        }

        let vector_index_names: Vec<String> = sqlx::query_scalar(
            "SELECT index_name FROM vector_indexes WHERE table_id = $1 ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;

        Ok((
            BackupTableDefinition {
                version: BACKUP_DEFINITION_VERSION,
                billing_mode,
                provisioned_throughput: throughput_from_catalog(pt.as_ref()),
                global_secondary_indexes: gsis,
                local_secondary_indexes: lsis,
                table_class,
                sse_specification: sse,
                on_demand_throughput: on_demand
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| StorageError::Internal(format!("on-demand throughput: {e}")))?,
                vector_index_names,
            },
            gsi_ids,
        ))
    }

    /// Copy a backup's items into a freshly created restore target, populate
    /// its secondary indexes, and mark it ACTIVE.
    ///
    /// Every key column is derived from the item itself, so the copy does not
    /// depend on how the source table's columns were laid out. Rows are plain
    /// INSERTs: two backup rows for one key are a corrupt backup and fail the
    /// restore rather than silently collapsing into one item. Items are read
    /// as a stream and written in batches, so memory is bounded by the batch
    /// size; the whole copy is one data transaction, so a failure part-way
    /// leaves the target empty. The target is CREATING throughout, which
    /// refuses every data-plane request, so nothing else writes to it.
    async fn copy_backup_items(
        &self,
        desc: &TableDescription,
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        use futures::TryStreamExt;

        let ddb_table = data_table_name(&desc.table_id);
        let sort_keys = all_sort_key_info(&desc.key_schema, &desc.attribute_definitions);
        let index_rows: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
            "SELECT index_id, key_schema, projection FROM indexes WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut indexes = Vec::with_capacity(index_rows.len());
        for (index_id, ks, proj) in index_rows {
            indexes.push(RestoreIndex {
                table: index_table_name(&index_id),
                key_schema: serde_json::from_value(ks)
                    .map_err(|e| StorageError::Internal(format!("index key schema: {e}")))?,
                projection: serde_json::from_value(proj)
                    .map_err(|e| StorageError::Internal(format!("index projection: {e}")))?,
            });
        }

        // The backup's rows are read from one catalog snapshot in which the
        // backup is still AVAILABLE. DeleteBackup removes the rows and marks
        // the backup DELETED in one transaction, so a concurrent delete is
        // either wholly visible here (the restore fails) or not at all (the
        // restore copies every row).
        let mut snapshot = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(db_err)?;
        let available: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM backups WHERE backup_arn = $1 \
             AND backup_status = 'AVAILABLE')",
        )
        .bind(backup_arn)
        .fetch_one(&mut *snapshot)
        .await
        .map_err(db_err)?;
        if !available {
            return Err(StorageError::Validation(format!(
                "Backup not found: {backup_arn}"
            )));
        }
        let mut tx = self.data_pool.begin().await.map_err(db_err)?;
        let mut rows = sqlx::query_scalar::<_, String>(
            "SELECT item_data::text FROM backup_items WHERE backup_arn = $1",
        )
        .bind(backup_arn)
        .fetch(&mut *snapshot);
        let mut batch: Vec<(Item, String)> = Vec::with_capacity(COPY_BATCH_ITEMS);
        let mut batch_bytes: usize = 0;
        let mut item_count: i64 = 0;
        while let Some(item_json) = rows.try_next().await.map_err(db_err)? {
            let item: Item = serde_json::from_str(&item_json)
                .map_err(|e| StorageError::Internal(format!("Parse backup item: {e}")))?;
            // The parsed item is kept for its keys and index projections; the
            // budget counts its stored text, which bounds both forms.
            batch_bytes += item_json.len();
            batch.push((item, item_json));
            if batch.len() == COPY_BATCH_ITEMS || batch_bytes >= COPY_BATCH_BYTES {
                self.write_restore_batch(
                    &mut tx, &ddb_table, desc, &sort_keys, &indexes, &batch, backup_arn,
                )
                .await?;
                item_count += i64::try_from(batch.len()).unwrap_or(i64::MAX);
                batch.clear();
                batch_bytes = 0;
            }
        }
        drop(rows);
        snapshot.commit().await.map_err(db_err)?;
        if !batch.is_empty() {
            self.write_restore_batch(
                &mut tx, &ddb_table, desc, &sort_keys, &indexes, &batch, backup_arn,
            )
            .await?;
            item_count += i64::try_from(batch.len()).unwrap_or(i64::MAX);
        }
        tx.commit().await.map_err(db_err)?;

        let (table_size,): (i64,) = sqlx::query_as(&format!(
            "SELECT COALESCE(pg_total_relation_size('{ddb_table}'), 0)"
        ))
        .fetch_one(&self.data_pool)
        .await
        .map_err(db_err)?;

        // Mark the restored table ACTIVE now that the copy has committed. The
        // table was created with the transition deferred, so this is the first
        // point it can become ACTIVE, by ordering rather than by timing.
        //
        // Only from CREATING. DeleteTable refuses a restore target, but the
        // abandoned-restore sweep can claim one whose lock session was lost
        // (moving it to DELETING); that claim wins, and flipping the row back
        // to ACTIVE would leave a table whose data tables are being dropped.
        let activated = sqlx::query(
            "UPDATE tables SET item_count = $1, table_size_bytes = $2, table_status = 'ACTIVE', \
             status_transition_at = NULL WHERE table_id = $3 AND table_status = 'CREATING'",
        )
        .bind(item_count)
        .bind(table_size)
        .bind(&desc.table_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if activated.rows_affected() == 0 {
            // The target was claimed for removal while the copy ran; the
            // copy's rows go with it.
            return Err(StorageError::TableNotFound(format!(
                "restore of {backup_arn} into {} did not complete: the table was deleted \
                 while it was being restored",
                desc.table_name
            )));
        }
        Ok(())
    }

    /// Write one batch of restored items: one multi-row INSERT into the base
    /// table, then each item's row in every secondary index it belongs to.
    #[allow(clippy::too_many_arguments)]
    async fn write_restore_batch(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ddb_table: &str,
        desc: &TableDescription,
        sort_keys: &[(&str, ScalarAttributeType)],
        indexes: &[RestoreIndex],
        batch: &[(Item, String)],
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        let mut cols = vec!["pk".to_owned()];
        cols.extend(
            sort_keys
                .iter()
                .enumerate()
                .map(|(i, &(_, t))| sk_column_n(i, t)),
        );
        cols.push("item_data".to_owned());
        let width = cols.len();
        // The last column is the item, bound as text and cast in SQL.
        let tuples: Vec<String> = (0..batch.len())
            .map(|r| {
                let ps: Vec<String> = (1..=width)
                    .map(|c| {
                        if c == width {
                            format!("${}::jsonb", r * width + c)
                        } else {
                            format!("${}", r * width + c)
                        }
                    })
                    .collect();
                format!("({})", ps.join(", "))
            })
            .collect();
        let sql = format!(
            "INSERT INTO {ddb_table} ({}) VALUES {}",
            cols.join(", "),
            tuples.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for (item, item_json) in batch {
            query = query.bind(composite_pk_to_text(item, &desc.key_schema)?);
            for &(name, sk_type) in sort_keys {
                let value = item.get(name).ok_or_else(|| {
                    StorageError::Internal(format!(
                        "backup {backup_arn} has an item without sort key {name}"
                    ))
                })?;
                query = match parse_sk(value, sk_type)? {
                    SortKeyValue::S(v) => query.bind(v),
                    SortKeyValue::N(v) => query.bind(v),
                    SortKeyValue::B(v) => query.bind(v),
                };
            }
            query = query.bind(item_json.as_str());
        }
        query.execute(&mut **tx).await.map_err(db_err)?;

        if indexes.is_empty() {
            return Ok(());
        }
        let base_sks = all_sort_key_info(&desc.key_schema, &desc.attribute_definitions);
        for idx in indexes {
            let idx_sks = all_sort_key_info(&idx.key_schema, &desc.attribute_definitions);
            for (item, _) in batch {
                if !item_has_index_keys(item, &idx.key_schema) {
                    continue;
                }
                let projected = project_item_for_index(
                    item,
                    &idx.key_schema,
                    &desc.key_schema,
                    &idx.projection,
                );
                insert_index_row_multi(
                    tx,
                    &idx.table,
                    item,
                    &projected,
                    &idx.key_schema,
                    &desc.key_schema,
                    &idx_sks,
                    &base_sks,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Remove a restore target whose copy failed or was abandoned.
    ///
    /// Keyed by table id, not name, so it can only ever remove the table that
    /// restore created, and synchronous regardless of the control-plane
    /// delay: the ordinary DeleteTable path would leave the name held in
    /// DELETING for that long.
    ///
    /// The target is first claimed by moving it from CREATING to DELETING in
    /// one statement. That is what makes the removal and the restore's own
    /// flip to ACTIVE mutually exclusive: both are conditional on CREATING, so
    /// exactly one wins. The claim also schedules the row for the ordinary
    /// DELETING removal, so if a drop below fails, the row stays DELETING with
    /// its index rows and the control plane finishes the job later.
    async fn abort_restore(&self, table_id: &str) -> Result<bool, StorageError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let claimed = sqlx::query(
            "UPDATE tables SET table_status = 'DELETING', status_transition_at = NOW() \
             WHERE table_id = $1 AND table_status = 'CREATING' AND status_transition_at IS NULL",
        )
        .bind(table_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected()
            > 0;
        let index_ids: Vec<String> =
            sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
                .bind(table_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        if !claimed {
            return Ok(false);
        }
        let mut drops = vec![data_table_name(table_id)];
        drops.extend(index_ids.iter().map(|id| index_table_name(id)));
        for t in drops {
            sqlx::query(&format!("DROP TABLE IF EXISTS {t}"))
                .execute(&self.data_pool)
                .await
                .map_err(db_err)?;
        }
        sqlx::query("DELETE FROM tables WHERE table_id = $1 AND table_status = 'DELETING'")
            .bind(table_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(true)
    }

    /// Remove restore targets whose restore died with its process.
    ///
    /// A restore target is the only table that sits CREATING with no
    /// scheduled transition. One older than the grace period whose restore
    /// lock is free has no live owner: the process that created it crashed or
    /// was killed mid-copy, and nothing else would ever move it on. Removing
    /// it frees the name; the client sees the table disappear, as it would
    /// after a failed restore. A target whose lock is held is being copied by
    /// a live process, here or on another instance, and is left alone.
    ///
    /// Returns the names of the tables removed.
    pub(crate) async fn sweep_abandoned_restores(&self) -> Result<Vec<String>, StorageError> {
        let candidates: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_id, account_id, table_name FROM tables \
             WHERE table_status = 'CREATING' AND status_transition_at IS NULL \
             AND creation_date_time < NOW() - make_interval(secs => $1)",
        )
        .bind(f64::from(ABANDONED_RESTORE_GRACE_SECS))
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut removed = Vec::new();
        for (table_id, account_id, table_name) in candidates {
            let Some(_owner) = try_restore_lock(&self.pool, &account_id, &table_name, None).await?
            else {
                continue;
            };
            if self.abort_restore(&table_id).await? {
                tracing::warn!(
                    "removed table {table_name} ({table_id}): its restore did not finish and \
                     no process owns it"
                );
                removed.push(table_name);
            }
        }
        Ok(removed)
    }
}

impl BackupEngine for PostgresEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let backup_name = backup_name.to_string();
        Box::pin(async move {
            // The table's definition and its items must describe one instant.
            // They live in different databases, so no single snapshot covers
            // both. Instead the table row is held FOR SHARE from the first
            // catalog read until the data snapshot has been taken: UpdateTable
            // and DeleteTable both take it FOR UPDATE, so no definition change
            // can commit in between, and the definition read here is the one
            // in force when the data snapshot starts.
            let mut meta = self.pool.begin().await.map_err(db_err)?;
            // Verify table exists and get metadata.
            let row: (
                String,
                String,
                serde_json::Value,
                serde_json::Value,
                String,
                i64,
                i64,
                String,
            ) = sqlx::query_as(
                "SELECT table_id, table_arn, key_schema, attribute_definitions, \
                 billing_mode, table_size_bytes, item_count, \
                 COALESCE(provisioned_throughput::text, '{}') \
                 FROM tables WHERE account_id = $1 AND table_name = $2 AND table_status = 'ACTIVE' \
                 FOR SHARE",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&mut *meta)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::TableNotFound(format!("Table not found: {table_name}")))?;

            let (
                table_id,
                _table_arn,
                key_schema,
                mut attr_defs,
                billing_mode,
                _size_bytes,
                _item_count,
                _prov,
            ) = row;

            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{id}",
                region = self.region,
                id = backup_id()
            );

            // Snapshot the source table's vector index configuration alongside
            // its key schema. Restore refuses a backup whose snapshot is
            // non-empty rather than silently dropping a declared index, and it
            // cannot ask the source table instead: the source may have been
            // deleted, which cascade-deletes its `vector_indexes` rows.
            //
            // Stored in the wire's own shape behind a version marker, not as a
            // copy of the catalog row. A snapshot outlives the schema that
            // produced it, so freezing physical column names into it would mean a
            // later rename or an added column silently changed the meaning of
            // snapshots already on disk. The lifecycle columns are deliberately
            // absent: a restored index is defined by its configuration, and its
            // build state belongs to the table it came from.
            let vector_indexes: Option<serde_json::Value> = sqlx::query_scalar(
                "SELECT jsonb_build_object('Version', 1, 'VectorIndexes', \
                     COALESCE(jsonb_agg(jsonb_build_object( \
                         'IndexName', index_name, \
                         'Dimensions', dimensions, \
                         'DistanceFunction', distance_function, \
                         'VectorAttribute', vector_attribute, \
                         'SearchSchema', search_schema, \
                         'Projection', projection) ORDER BY index_name), '[]'::jsonb)) \
                 FROM vector_indexes WHERE table_id = $1",
            )
            .bind(&table_id)
            .fetch_optional(&mut *meta)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let (mut definition, gsi_ids) =
                Self::capture_table_definition(&mut meta, &table_id).await?;

            // Begin the data transaction and hold ACCESS SHARE while the
            // catalog barrier still blocks UpdateTable and DeleteTable. LOCK
            // TABLE is a utility statement and does not establish a
            // REPEATABLE READ snapshot, so the real relation SELECT must run
            // before meta.commit(). This fixes the data snapshot at the same
            // instant as the captured definition; the lock then keeps a DROP
            // from removing the relation while its rows are read. The
            // control-plane drop path uses a short lock timeout and retries a
            // blocked DeleteTable rather than stalling its worker pass.
            let ddb_table = data_table_name(&table_id);
            let mut snapshot = self
                .data_pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await
                .map_err(db_err)?;
            sqlx::query(&format!("LOCK TABLE {ddb_table} IN ACCESS SHARE MODE"))
                .execute(&mut *snapshot)
                .await
                .map_err(db_err)?;
            let _: Option<i32> = sqlx::query_scalar(&format!("SELECT 1 FROM {ddb_table} LIMIT 1"))
                .fetch_optional(&mut *snapshot)
                .await
                .map_err(db_err)?;
            meta.commit().await.map_err(db_err)?;

            // UpdateTable commits a new index's catalog row before it creates
            // and fills the index's data table, and removes the row again if
            // that fails. An index whose data table is not there yet is
            // therefore not part of the table as of this snapshot; leave it
            // out rather than record an index that may never exist.
            let gsi_ids_len = gsi_ids.len();
            // The ids were read under the catalog barrier, so this probes
            // exactly the indexes the captured definition describes. The
            // to_regclass lookup reads pg_class rather than the pinned data
            // snapshot and can see later committed DDL; that remains consistent:
            // UpdateTable creates and fills a GSI in one data transaction, and
            // restore rebuilds its rows from the captured base items.
            let mut present = Vec::with_capacity(definition.global_secondary_indexes.len());
            for (gsi, (name, index_id)) in std::mem::take(&mut definition.global_secondary_indexes)
                .into_iter()
                .zip(gsi_ids)
            {
                debug_assert_eq!(gsi.index_name, name);
                let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                    .bind(index_table_name(&index_id))
                    .fetch_one(&mut *snapshot)
                    .await
                    .map_err(db_err)?;
                if exists {
                    present.push(gsi);
                }
            }
            let omitted = present.len() < gsi_ids_len;
            definition.global_secondary_indexes = present;
            // Leaving an index out can leave an attribute definition no key
            // uses, which CreateTable would refuse on the service; drop those.
            if omitted {
                let mut used: std::collections::HashSet<String> =
                    serde_json::from_value::<Vec<KeySchemaElement>>(key_schema.clone())
                        .map_err(|e| StorageError::Internal(format!("key schema: {e}")))?
                        .into_iter()
                        .map(|k| k.attribute_name)
                        .collect();
                for k in definition
                    .global_secondary_indexes
                    .iter()
                    .flat_map(|g| &g.key_schema)
                    .chain(
                        definition
                            .local_secondary_indexes
                            .iter()
                            .flat_map(|l| &l.key_schema),
                    )
                {
                    used.insert(k.attribute_name.clone());
                }
                if let Some(defs) = attr_defs.as_array_mut() {
                    defs.retain(|d| {
                        d.get("AttributeName")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|n| used.contains(n))
                    });
                }
            }
            let definition = definition.to_json()?;

            // All catalog-side writes are one transaction, so a crash cannot
            // leave a backup AVAILABLE with partial items. The items are read
            // from one REPEATABLE READ snapshot of the data table, streamed,
            // and written in batches, so the backup is consistent as of one
            // instant and memory is bounded by the batch size.
            let mut tx = self.pool.begin().await.map_err(db_err)?;

            sqlx::query(
                "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
             billing_mode, vector_indexes) \
             VALUES ($1, $2, $3, $4, $5, 'AVAILABLE', 0, 0, $6, $7, $8, $9)",
            )
            .bind(&backup_arn)
            .bind(&backup_name)
            .bind(&table_id)
            .bind(&table_name)
            .bind(&account_id)
            .bind(&key_schema)
            .bind(&attr_defs)
            .bind(&billing_mode)
            .bind(&vector_indexes)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
            sqlx::query("INSERT INTO backup_definitions (backup_arn, definition) VALUES ($1, $2)")
                .bind(&backup_arn)
                .bind(&definition)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;

            // `backup_items.sk` stays NULL: `item_data` is the whole item, key
            // attributes included, and restore derives every key column from
            // it. Reading the typed columns here instead would tie the backup
            // to this table's physical layout.
            let (actual_count, size_bytes) = {
                use futures::TryStreamExt;
                let mut count: i64 = 0;
                let mut size: i64 = 0;
                let mut pks: Vec<String> = Vec::with_capacity(COPY_BATCH_ITEMS);
                let mut datas: Vec<String> = Vec::with_capacity(COPY_BATCH_ITEMS);
                let mut batch_bytes: usize = 0;
                // Rows come back as JSON text and are buffered as text, so the
                // batch budget counts what is actually held. Each item is
                // parsed only long enough to size it.
                let select_sql = format!("SELECT pk, item_data::text FROM {ddb_table}");
                {
                    let mut rows =
                        sqlx::query_as::<_, (String, String)>(&select_sql).fetch(&mut *snapshot);
                    while let Some((pk, data)) = rows.try_next().await.map_err(db_err)? {
                        let item: Item = serde_json::from_str(&data)
                            .map_err(|e| StorageError::Internal(format!("Parse item: {e}")))?;
                        let item_bytes = extenddb_core::types::item_size_bytes(&item);
                        drop(item);
                        size += i64::try_from(item_bytes).unwrap_or(i64::MAX);
                        batch_bytes += data.len();
                        pks.push(pk);
                        datas.push(data);
                        if pks.len() == COPY_BATCH_ITEMS || batch_bytes >= COPY_BATCH_BYTES {
                            batch_bytes = 0;
                            count +=
                                insert_backup_batch(&mut tx, &backup_arn, &mut pks, &mut datas)
                                    .await?;
                        }
                    }
                }
                count += insert_backup_batch(&mut tx, &backup_arn, &mut pks, &mut datas).await?;
                snapshot.commit().await.map_err(db_err)?;
                (count, size)
            };
            sqlx::query(
                "UPDATE backups SET item_count = $1, backup_size_bytes = $2 WHERE backup_arn = $3",
            )
            .bind(actual_count)
            .bind(size_bytes)
            .bind(&backup_arn)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

            // Read back the creation timestamp assigned by the database.
            let created_at: time::OffsetDateTime =
                sqlx::query_scalar("SELECT created_at FROM backups WHERE backup_arn = $1")
                    .bind(&backup_arn)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            Ok(BackupDetails {
                backup_arn,
                backup_name: backup_name.clone(),
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: size_bytes,
                backup_creation_date_time: pg_timestamp_to_epoch(created_at),
            })
        })
    }

    fn describe_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let row: (
                String,
                String,
                String,
                String,
                String,
                i64,
                i64,
                serde_json::Value,
                String,
                String,
                time::OffsetDateTime,
                time::OffsetDateTime,
            ) = sqlx::query_as(
                "SELECT b.backup_name, b.backup_status, b.table_id, b.table_name, b.account_id, \
                 b.backup_size_bytes, b.item_count, b.key_schema, b.billing_mode, \
                 COALESCE(t.table_arn, \
                   'arn:aws:dynamodb:' || $2 || ':' || b.account_id || ':table/' || b.table_name), \
                 b.created_at, \
                 COALESCE(t.creation_date_time, b.created_at) \
                 FROM backups b \
                 LEFT JOIN tables t ON t.table_id = b.table_id \
                 WHERE b.backup_arn = $1 AND b.account_id = $3 AND b.backup_status != 'DELETED'",
            )
            .bind(&backup_arn)
            .bind(&self.region)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            let (
                name,
                status,
                table_id,
                table_name,
                _account_id,
                size,
                count,
                ks_json,
                billing,
                table_arn,
                backup_created_at,
                table_created_at,
            ) = row;

            let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                serde_json::from_value(ks_json)
                    .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.clone(),
                    backup_name: name,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: pg_timestamp_to_epoch(backup_created_at),
                },
                source_table_details: SourceTableDetails {
                    table_name,
                    table_id,
                    table_arn,
                    key_schema,
                    item_count: count,
                    table_size_bytes: size,
                    billing_mode: Some(billing),
                    table_creation_date_time: pg_timestamp_to_epoch(table_created_at),
                },
            })
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            let rows: Vec<(
                String,
                String,
                String,
                String,
                i64,
                String,
                time::OffsetDateTime,
            )> = if let Some(tn) = table_name {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     b.backup_size_bytes, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || $3 || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = $1 AND b.table_name = $2 AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&account_id)
                .bind(tn)
                .bind(&self.region)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            } else {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     b.backup_size_bytes, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || $2 || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = $1 AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&account_id)
                .bind(&self.region)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            };

            Ok(rows
                .into_iter()
                .map(
                    |(arn, name, tn, status, size, table_arn, created_at)| BackupSummary {
                        backup_arn: arn,
                        backup_name: name,
                        table_name: tn,
                        table_arn,
                        backup_status: status,
                        backup_type: "USER".to_owned(),
                        backup_size_bytes: size,
                        backup_creation_date_time: pg_timestamp_to_epoch(created_at),
                    },
                )
                .collect())
        })
    }

    fn delete_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            // Resolves account-scoped, so a backup owned by another account is
            // reported missing here and the writes below never run.
            let desc = self.describe_backup(&account_id, &backup_arn).await?;

            // One transaction, so a restore reading its snapshot sees either
            // the whole backup or a DELETED one, never AVAILABLE with its rows
            // gone. The account predicate is repeated on every write rather
            // than relying on the lookup above, so the statements are correct
            // on their own terms.
            let mut tx = self.pool.begin().await.map_err(db_err)?;
            // Refuse while a restore from this backup is still running, as the
            // service does. The backup row is locked first; a restore records
            // itself in `table_restores` while holding the same row FOR SHARE,
            // so one of the two always sees the other.
            sqlx::query(
                "SELECT 1 FROM backups WHERE backup_arn = $1 AND account_id = $2 FOR UPDATE",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
            let restoring: Option<String> = sqlx::query_scalar(
                "SELECT t.table_name FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.source_backup_arn = $1 AND t.table_status = 'CREATING' LIMIT 1",
            )
            .bind(&backup_arn)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
            if let Some(table) = restoring {
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {table}: {backup_arn}"
                )));
            }
            sqlx::query(
                "DELETE FROM backup_items WHERE backup_arn = $1 AND EXISTS (\
                 SELECT 1 FROM backups b WHERE b.backup_arn = $1 AND b.account_id = $2)",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query(
                "DELETE FROM backup_definitions WHERE backup_arn = $1 AND EXISTS (\
                 SELECT 1 FROM backups b WHERE b.backup_arn = $1 AND b.account_id = $2)",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query(
                "UPDATE backups SET backup_status = 'DELETED' \
                 WHERE backup_arn = $1 AND account_id = $2",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            tx.commit().await.map_err(db_err)?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..desc.backup_details
                },
                source_table_details: desc.source_table_details,
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
        overrides: RestoreTableOverrides,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let target_table_name = target_table_name.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            #[allow(clippy::type_complexity)]
            let backup_row: (
                serde_json::Value,
                serde_json::Value,
                String,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
            ) = sqlx::query_as(
                "SELECT b.key_schema, b.attribute_definitions, b.billing_mode, \
                 b.vector_indexes, d.definition \
                 FROM backups b LEFT JOIN backup_definitions d ON d.backup_arn = b.backup_arn \
                 WHERE b.backup_arn = $1 AND b.account_id = $2 AND b.backup_status = 'AVAILABLE'",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            let (ks_json, ad_json, billing, vector_indexes, definition) = backup_row;

            // Vector indexes are not restored on this backend. For a source
            // table that had them, a restore without them would be silent loss
            // of a declared index: the restored table would answer every
            // request except a search, and the client would only find out on
            // the first one. The service preserves vector indexes through
            // backup and restore (measured 2026-08-19), so the conformant end
            // state is to restore them; until then this refuses, because a
            // typed refusal is recoverable and silent loss is not.
            //
            // A NULL snapshot is a backup taken before the column existed, which
            // cannot have carried vector indexes: this backend could not create
            // them then. An unrecognised version is refused rather than guessed
            // at, for the same reason a declared index must not be dropped
            // silently.
            let vector_index_count = match vector_indexes.as_ref() {
                None => 0,
                Some(snapshot) => {
                    let version = snapshot.get("Version").and_then(serde_json::Value::as_u64);
                    if version != Some(1) {
                        return Err(StorageError::Unsupported(format!(
                            "backup {backup_arn} carries a vector index snapshot this build \
                             cannot read (version {version:?})"
                        )));
                    }
                    snapshot
                        .get("VectorIndexes")
                        .and_then(serde_json::Value::as_array)
                        .map_or(0, Vec::len)
                }
            };
            if vector_index_count > 0 {
                return Err(StorageError::Unsupported(format!(
                    "backup {backup_arn} has {vector_index_count} vector index(es); \
                     restoring a table with vector indexes is not supported by this \
                     storage backend"
                )));
            }
            let definition = definition
                .map(|d| BackupTableDefinition::from_json(d, &backup_arn))
                .transpose()?;
            if let Some(d) = &definition {
                d.ensure_restorable(&backup_arn)?;
            }

            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;
            ensure_single_part_base_key(&key_schema, &backup_arn)?;
            let attr_defs: Vec<extenddb_core::types::AttributeDefinition> =
                serde_json::from_value(ad_json)
                    .map_err(|e| StorageError::Internal(format!("Parse attr defs: {e}")))?;

            let mut create_input = extenddb_core::types::CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema,
                attribute_definitions: attr_defs,
                ..Default::default()
            };
            match definition {
                Some(d) => d.apply_to(&mut create_input, &overrides)?,
                None => {
                    // A backup taken before table definitions were recorded
                    // carries only its keys and billing mode, so it restores as
                    // it always has: no secondary indexes, and 5/5 throughput for
                    // a provisioned table since the source's is unknown.
                    let on_demand = billing == "PAY_PER_REQUEST";
                    create_input.billing_mode = Some(if on_demand {
                        extenddb_core::types::BillingMode::PayPerRequest
                    } else {
                        extenddb_core::types::BillingMode::Provisioned
                    });
                    create_input.provisioned_throughput =
                        (!on_demand).then_some(extenddb_core::types::ProvisionedThroughput {
                            read_capacity_units: 5,
                            write_capacity_units: 5,
                        });
                    overrides.apply_to_create_input(&mut create_input);
                }
            }

            // Ownership first, before the target exists, so the
            // abandoned-restore sweep never sees this target without an owner,
            // including while the DDL below runs. It is held until the copy
            // ends; a crash anywhere releases it with the connection, and the
            // sweep then removes the target. A lock already held means another
            // restore into this name is in flight right now; let it report the
            // name as taken, as CreateTable would.
            let slot = match tokio::time::timeout(
                std::time::Duration::from_secs(RESTORE_SLOT_WAIT_SECS),
                RESTORE_SLOTS.acquire(),
            )
            .await
            {
                Ok(slot) => {
                    slot.map_err(|e| StorageError::Internal(format!("restore slot: {e}")))?
                }
                Err(_) => {
                    return Err(StorageError::LimitExceeded(
                        "Too many restores are in progress on this server; retry later".to_owned(),
                    ));
                }
            };
            let Some(_owner) =
                try_restore_lock(&self.pool, &account_id, &target_table_name, Some(slot)).await?
            else {
                return Err(StorageError::TableAlreadyExists(target_table_name.clone()));
            };

            // Create the table and register its source backup in one catalog
            // transaction. The transaction holds the AVAILABLE backup row FOR
            // SHARE until the CREATING target and its table_restores row commit,
            // so DeleteBackup can neither remove the source between those rows
            // nor observe an unregistered target.
            let desc = self
                .create_table_for_restore(&account_id, create_input, &backup_arn)
                .await?;

            // From here on a failure must not leave the target behind: it is
            // CREATING with no scheduled transition, so nothing else would
            // move it on, and the name would stay taken. Deleting the target
            // cascades its table_restores row.
            let copied = self.copy_backup_items(&desc, &backup_arn).await;
            if let Err(e) = copied {
                match self.abort_restore(&desc.table_id).await {
                    // Not ours to remove: it was already claimed for removal
                    // while it was being copied into, and that is why the copy
                    // failed. Report it as such rather than as a server fault a
                    // client would retry.
                    Ok(false) => {
                        return Err(StorageError::TableNotFound(format!(
                            "restore of {backup_arn} into {target_table_name} did not \
                             complete: the table was deleted while it was being restored"
                        )));
                    }
                    Ok(true) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed and the \
                         partial table was removed: {e}"
                    ),
                    Err(cleanup) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed ({e}), and \
                         the partial table ({}) could not be removed yet; the control plane \
                         retries: {cleanup}",
                        desc.table_id
                    ),
                }
                return Err(e);
            }

            // Return CREATING — the API response shows the initial status,
            // but the table is already ACTIVE by the time the caller polls.
            // The summary carries the recorded restore time, so the response
            // and later DescribeTable calls agree.
            let mut desc = desc;
            desc.restore_summary = self
                .restore_summary(&desc.table_id, &desc.table_status)
                .await?;
            Ok(desc)
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = $1 AND table_name = $2)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            let pitr_row: Option<(bool,)> = sqlx::query_as(
                "SELECT pitr_enabled FROM continuous_backups \
             WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let pitr_enabled = pitr_row.is_some_and(|r| r.0);

            #[allow(clippy::cast_precision_loss)]
            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as f64;

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: if pitr_enabled {
                        Some(now_epoch - 35.0 * 24.0 * 3600.0)
                    } else {
                        None
                    },
                    latest_restorable_date_time: if pitr_enabled { Some(now_epoch) } else { None },
                }),
            })
        })
    }

    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = $1 AND table_name = $2)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            sqlx::query(
                "INSERT INTO continuous_backups (account_id, table_name, pitr_enabled) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (account_id, table_name) DO UPDATE SET pitr_enabled = $3",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(pitr_enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }

    // TODO(cleanup): This method is unreachable — the engine handler returns
    // ValidationException("not yet supported") before calling storage. Remove
    // when real PITR is implemented or during the next storage trait cleanup.
    fn restore_table_to_point_in_time(
        &self,
        account_id: &str,
        source_table_name: &str,
        target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let source_table_name = source_table_name.to_string();
        let target_table_name = target_table_name.to_string();
        Box::pin(async move {
            let backup = self
                .create_backup(&account_id, &source_table_name, "__pitr_restore__")
                .await?;
            let desc = self
                .restore_table_from_backup(
                    &account_id,
                    &target_table_name,
                    &backup.backup_arn,
                    RestoreTableOverrides::default(),
                )
                .await?;
            let _ = self.delete_backup(&account_id, &backup.backup_arn).await;
            Ok(desc)
        })
    }
}
