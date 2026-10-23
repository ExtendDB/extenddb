// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and restore implementation for `PostgreSQL` storage.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription, GsiInput, Item,
    KeySchemaElement, LsiInput, PointInTimeRecoveryDescription, Projection, RestoreSummary,
    ScalarAttributeType, SourceTableDetails, StreamSpecification, TableDescription,
};
use extenddb_storage::backup_definition::{
    BACKUP_DEFINITION_VERSION, BackupTableDefinition, COPY_BATCH_BYTES, COPY_BATCH_ITEMS,
    throughput_from_catalog,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    SortKeyValue, backup_arn, backup_id_epoch_millis, composite_pk_to_text, new_backup_id,
    parse_sk, sk_column_n,
};
use extenddb_storage::{BackupEngine, BackupRestoreSource, RestoreRequest};
use futures::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::PostgresEngine;
use crate::create_table::RestoreRegistration;
use crate::data::{
    all_sort_key_info, data_table_name, index_table_name, insert_index_rows_multi,
    item_has_index_keys, project_item_for_index,
};

/// Convert a `PostgreSQL` `TIMESTAMPTZ` to epoch seconds as `f64`, keeping the
/// sub-second part.
///
/// A whole-millisecond instant (every backup created since the shared backup
/// id) is converted as `millis / 1000`, the same expression
/// `util::backup_creation_date_time` uses on the id, so the time reported for
/// a backup and the time embedded in its ARN are the same `f64`.
#[allow(clippy::cast_precision_loss)] // epoch ns/ms fit f64 to well under a microsecond
fn pg_timestamp_to_epoch(ts: time::OffsetDateTime) -> f64 {
    let nanos = ts.unix_timestamp_nanos();
    if nanos % 1_000_000 == 0 {
        (nanos / 1_000_000) as f64 / 1000.0
    } else {
        nanos as f64 / 1e9
    }
}

/// Seed for the 64-bit restore lock key. Restore locks use the single-`bigint`
/// advisory lock form, which PostgreSQL keeps in a key space separate from the
/// two-`int4` form the migration and vector-build locks use, so they cannot
/// meet; the seed keeps this key space distinct from any other `bigint` user.
const RESTORE_LOCK_SEED: i64 = 0x0045_4452; // 'E', 'D', 'R'

/// Restores one account may have in flight at once in this process. Each
/// holds one dedicated connection for its lock on top of its pooled ones. A
/// restore beyond the limit waits for one of its own account's slots, so one
/// account cannot hold every other account's restores back.
const RESTORE_SLOTS_PER_ACCOUNT: usize = 4;

/// Upper limit on [`restore_server_slot_count`].
const RESTORE_SERVER_SLOTS_MAX: usize = 16;

/// Restores the whole process may have in flight at once, across accounts:
/// `min(16, pool_size / 2)`, at least 1.
///
/// Each running copy holds one connection of the engine's catalog pool
/// (`pool_size`) for its whole duration, for the REPEATABLE READ snapshot it
/// reads the backup's items from. Without a process-wide bound, enough
/// accounts each running their per-account allowance would hold every
/// catalog connection and stall every other request that needs the catalog.
/// Half the pool stays free for them. A restore takes its account slot
/// first, then one of these; the two waits share one deadline of
/// [`RESTORE_SLOT_WAIT_SECS`].
pub(crate) fn restore_server_slot_count(pool_size: u32) -> usize {
    usize::try_from(pool_size / 2)
        .unwrap_or(usize::MAX)
        .clamp(1, RESTORE_SERVER_SLOTS_MAX)
}

/// The two slots a running restore holds until its copy ends.
struct RestorePermits {
    _account: OwnedSemaphorePermit,
    _server: OwnedSemaphorePermit,
}

/// Take an account slot, then a server slot, waiting at most `wait` for
/// both together. Running out of time on either is `LimitExceeded`, with a
/// message naming the limit that was full.
async fn acquire_restore_permits(
    account: Arc<Semaphore>,
    server: Arc<Semaphore>,
    server_slots: usize,
    wait: std::time::Duration,
) -> Result<RestorePermits, StorageError> {
    let deadline = tokio::time::Instant::now() + wait;
    let closed =
        |e: tokio::sync::AcquireError| StorageError::Internal(format!("restore slot: {e}"));
    let account_permit = tokio::time::timeout_at(deadline, account.acquire_owned())
        .await
        .map_err(|_| {
            StorageError::LimitExceeded(format!(
                "Too many restores are in progress for this account (at most \
                 {RESTORE_SLOTS_PER_ACCOUNT} at once); retry later"
            ))
        })?
        .map_err(closed)?;
    let server_permit = tokio::time::timeout_at(deadline, server.acquire_owned())
        .await
        .map_err(|_| {
            StorageError::LimitExceeded(format!(
                "Too many restores are in progress on this server (at most {server_slots} at \
                 once); retry later"
            ))
        })?
        .map_err(closed)?;
    Ok(RestorePermits {
        _account: account_permit,
        _server: server_permit,
    })
}

/// Per-account restore slots of one engine (shared by its clones, so one
/// per server process). An entry nobody holds (no permit, no waiter) is
/// pruned the next time any account takes a slot, so the map does not grow
/// with every account that ever restored.
#[derive(Clone, Default)]
pub(crate) struct RestoreAccountSlots(Arc<std::sync::Mutex<HashMap<String, Arc<Semaphore>>>>);

impl RestoreAccountSlots {
    /// The slot semaphore for `account_id`.
    pub(crate) fn for_account(&self, account_id: &str) -> Arc<Semaphore> {
        let mut slots = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A permit and a waiter each hold a clone, so a count of one means
        // the account has nothing in flight.
        slots.retain(|_, s| Arc::strong_count(s) > 1);
        Arc::clone(
            slots
                .entry(account_id.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(RESTORE_SLOTS_PER_ACCOUNT))),
        )
    }
}

/// How long a restore waits for its account slot and a server slot,
/// together, before it is refused with `LimitExceededException`.
const RESTORE_SLOT_WAIT_SECS: u64 = 30;

/// How often the restore's lock session is exercised while its copy runs.
/// Shorter than the idle-session timeouts managed services apply (minutes),
/// and a quarter of [`ABANDONED_RESTORE_GRACE_SECS`].
const RESTORE_HEARTBEAT_SECS: u64 = 15;

/// How old an unowned restore target must be before it is treated as
/// abandoned. The owner takes its lock before creating the target, so this
/// only has to cover clock skew between the catalog and the sweep, with a
/// wide margin.
const ABANDONED_RESTORE_GRACE_SECS: i32 = 60;

/// A secondary index of a restore target, as the copy needs it.
struct RestoreIndex {
    table: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

/// Session-scoped ownership of one restore, held for the life of the copy.
///
/// A session-level `pg_try_advisory_lock` on a dedicated connection: it dies
/// with the session, so a crashed process's claim disappears on its own and
/// the abandoned-restore sweep can tell a dead restore from a running one.
///
/// This needs the connection to be one server session for its whole life:
/// a direct connection to the catalog database, or one through a
/// session-mode pooler. Behind a transaction-mode pooler (PgBouncer
/// `pool_mode = transaction`) the lock stays on whichever server session ran
/// the statement, is not released when this connection closes, and can be
/// observed as free by the sweep on that same session; restore is not
/// supported there.
///
/// The connection would otherwise sit idle for the whole copy, and some
/// servers end idle sessions (`idle_session_timeout`, or a managed service's
/// own reaping). [`RestoreOwner::heartbeat`] issues
/// a statement on it every [`RESTORE_HEARTBEAT_SECS`]; when one fails, the
/// copy is stopped and the target removed, rather than copying on into a
/// target the sweep may claim. The sweep's claim and the copy's flip to
/// ACTIVE are both conditional on CREATING, so if the two still race (the
/// session ends between heartbeats), exactly one wins.
struct RestoreOwner {
    conn: sqlx::PgConnection,
    _slots: Option<RestorePermits>,
}

impl RestoreOwner {
    /// Exercise the lock session every [`RESTORE_HEARTBEAT_SECS`] until a
    /// statement on it fails; returns that failure. Never returns while the
    /// session is alive.
    async fn heartbeat(&mut self) -> sqlx::Error {
        let period = std::time::Duration::from_secs(RESTORE_HEARTBEAT_SECS);
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = sqlx::query("SELECT 1").execute(&mut self.conn).await {
                return e;
            }
        }
    }
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

/// Take the restore lock for `table_name` on a new dedicated connection.
/// `Ok(None)`: another session holds it.
///
/// A connection the server refuses for lack of connection slots is reported
/// as `LimitExceeded`; any other failure to open it is `Internal` (see
/// [`lock_connection_error`]).
async fn try_restore_lock(
    pool: &sqlx::PgPool,
    account_id: &str,
    table_name: &str,
    slots: Option<RestorePermits>,
) -> Result<Option<RestoreOwner>, StorageError> {
    let options = pool.connect_options();
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect_with(&options)
        .await
        .map_err(|e| {
            tracing::warn!(
                "could not open the restore lock connection for {account_id}/{table_name}: {e}"
            );
            lock_connection_error(&e)
        })?;
    // A server-side idle_session_timeout would end this session between
    // heartbeats if it were shorter than the heartbeat period; disable it for
    // this session when the server permits. Some managed PostgreSQL services
    // reject this SET; there the heartbeat is what keeps the session from
    // sitting idle.
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
        conn,
        _slots: slots,
    }))
}

/// SQLSTATE `too_many_connections`: the server (or the role or database)
/// has no connection slot left.
const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";

/// Map a failure to open the restore lock connection. Only the server's
/// `too_many_connections` refusal is `LimitExceeded` (a client should back
/// off and retry); authentication, TLS, DNS, network, and every other
/// failure, including one that carries no SQLSTATE, is `Internal`.
fn lock_connection_error(e: &sqlx::Error) -> StorageError {
    let code = e.as_database_error().and_then(|d| d.code());
    if code.as_deref() == Some(SQLSTATE_TOO_MANY_CONNECTIONS) {
        StorageError::LimitExceeded(
            "Too many restores are in progress on this server; retry later".to_owned(),
        )
    } else {
        StorageError::Internal(format!("restore lock connection: {e}"))
    }
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

/// Map a failed read as [`db_err`] does, except that a failure to reach the
/// database is [`StorageError::Transient`]: an I/O or TLS error, a pool that
/// timed out, closed, or lost its worker, and a server reply of class 08
/// (connection exception), 57P01-57P03 (admin shutdown, crash shutdown,
/// cannot connect now) or 53300 (too many connections). DescribeBackup
/// answers without feature details when the definition read fails with
/// `Internal`; a failover or a pool timeout is not a damaged definition and
/// must reach the client as a retryable error instead.
fn read_err(e: sqlx::Error) -> StorageError {
    let unreachable = match &e {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(d) => d.code().is_some_and(|code| {
            code.starts_with("08")
                || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
                || code == SQLSTATE_TOO_MANY_CONNECTIONS
        }),
        _ => false,
    };
    if unreachable {
        StorageError::Transient(format!("Database error: {e}"))
    } else {
        db_err(e)
    }
}

impl PostgresEngine {
    /// Read the parts of a table's definition a restore recreates.
    async fn capture_table_definition(
        conn: &mut sqlx::PgConnection,
        table_id: &str,
    ) -> Result<(BackupTableDefinition, Vec<(String, String)>), StorageError> {
        #[allow(clippy::type_complexity)]
        let (billing_mode, pt, table_class, sse, on_demand, stream, ttl_attr, ttl_ready): (
            String,
            Option<serde_json::Value>,
            Option<String>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            Option<bool>,
        ) = sqlx::query_as(
            "SELECT billing_mode, provisioned_throughput, table_class, sse_specification, \
             on_demand_throughput, stream_specification, ttl_attribute, ttl_index_ready \
             FROM tables WHERE table_id = $1",
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
                // As DescribeTimeToLive and DescribeTable report them, read
                // under the same row lock as the rest of the definition.
                time_to_live: Some(crate::metadata_engine::ttl_description(ttl_attr, ttl_ready)),
                stream_specification: stream
                    .map(serde_json::from_value::<StreamSpecification>)
                    .transpose()
                    .map_err(|e| StorageError::Internal(format!("stream specification: {e}")))?,
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
    /// as a stream and written in batches of at most `COPY_BATCH_ITEMS` items
    /// or `COPY_BATCH_BYTES` of item text: per batch, one multi-row INSERT
    /// into the base table and one into each secondary index, committed as
    /// one data transaction. Committing per batch rather than once at the end
    /// keeps no data-side transaction open across the copy. The catalog side
    /// does keep one read-only REPEATABLE READ transaction open from the
    /// first item read until the last full batch is written (see below), so
    /// the copy holds one catalog pool connection and the catalog's xmin for
    /// that long. A failure
    /// part-way leaves some batches committed, which is harmless because the
    /// target is CREATING throughout (every data-plane request is refused)
    /// and the failure path drops it whole.
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
        // backup is still AVAILABLE. DeleteBackup refuses while this restore
        // is registered and CREATING; the snapshot keeps the read whole even
        // so: DeleteBackup removes the rows and marks the backup DELETED in
        // one transaction, so a delete is either wholly visible here (the
        // restore fails) or not at all (the restore copies every row).
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
                    &ddb_table, desc, &sort_keys, &indexes, &batch, backup_arn,
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
            self.write_restore_batch(&ddb_table, desc, &sort_keys, &indexes, &batch, backup_arn)
                .await?;
            item_count += i64::try_from(batch.len()).unwrap_or(i64::MAX);
        }

        let (table_size,): (i64,) = sqlx::query_as(&format!(
            "SELECT COALESCE(pg_total_relation_size('{ddb_table}'), 0)"
        ))
        .fetch_one(&self.data_pool)
        .await
        .map_err(db_err)?;

        // Mark the restored table ACTIVE now that every batch has committed.
        // The table was created with the transition deferred, so this is the
        // first point it can become ACTIVE, by ordering rather than by timing.
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

    /// Write and commit one batch of restored items: one multi-row INSERT
    /// into the base table, then one multi-row INSERT into each secondary
    /// index for the batch's items that carry that index's keys.
    async fn write_restore_batch(
        &self,
        ddb_table: &str,
        desc: &TableDescription,
        sort_keys: &[(&str, ScalarAttributeType)],
        indexes: &[RestoreIndex],
        batch: &[(Item, String)],
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.data_pool.begin().await.map_err(db_err)?;
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
        query.execute(&mut *tx).await.map_err(db_err)?;

        for idx in indexes {
            let idx_sks = all_sort_key_info(&idx.key_schema, &desc.attribute_definitions);
            let projected: Vec<(&Item, Item)> = batch
                .iter()
                .filter(|(item, _)| item_has_index_keys(item, &idx.key_schema))
                .map(|(item, _)| {
                    (
                        item,
                        project_item_for_index(
                            item,
                            &idx.key_schema,
                            &desc.key_schema,
                            &idx.projection,
                        ),
                    )
                })
                .collect();
            let rows: Vec<(&Item, &Item)> = projected.iter().map(|(i, p)| (*i, p)).collect();
            insert_index_rows_multi(
                &mut tx,
                &idx.table,
                &rows,
                &idx.key_schema,
                &desc.key_schema,
                &idx_sks,
                sort_keys,
            )
            .await?;
        }
        tx.commit().await.map_err(db_err)?;
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
    /// was killed mid-copy, or its lock session ended, and nothing else would
    /// ever move it on. Removing it frees the name; the client sees the table
    /// disappear, as it would after a failed restore. A target whose lock is
    /// held is being copied by a live process, here or on another instance,
    /// and is left alone.
    ///
    /// Each removal is logged at warn with the account, the table, and the
    /// backup it was being restored from, so a table that vanishes from
    /// ListTables has a server record of why.
    ///
    /// Returns the names of the tables removed.
    pub(crate) async fn sweep_abandoned_restores(&self) -> Result<Vec<String>, StorageError> {
        let candidates: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT t.table_id, t.account_id, t.table_name, r.source_backup_arn FROM tables t \
             LEFT JOIN table_restores r ON r.table_id = t.table_id \
             WHERE t.table_status = 'CREATING' AND t.status_transition_at IS NULL \
             AND t.creation_date_time < NOW() - make_interval(secs => $1)",
        )
        .bind(f64::from(ABANDONED_RESTORE_GRACE_SECS))
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut removed = Vec::new();
        for (table_id, account_id, table_name, source_backup_arn) in candidates {
            let Some(_owner) = try_restore_lock(&self.pool, &account_id, &table_name, None).await?
            else {
                continue;
            };
            if self.abort_restore(&table_id).await? {
                tracing::warn!(
                    "removed abandoned restore target {table_name} ({table_id}) in account \
                     {account_id}, restoring from {}: its restore did not finish and no \
                     process owns it",
                    source_backup_arn
                        .as_deref()
                        .unwrap_or("<no table_restores row>")
                );
                removed.push(table_name);
            }
        }
        Ok(removed)
    }

    /// The detached task of RestoreTableFromBackup: wait for the account and
    /// server slots, take the restore lock, register the CREATING target,
    /// report the registration to the request (which may be gone), then copy.
    async fn run_restore(
        self,
        account_id: String,
        request: RestoreRequest,
        registered: tokio::sync::oneshot::Sender<Result<TableDescription, StorageError>>,
    ) {
        let RestoreRequest {
            backup_arn,
            source_table_arn,
            restore_date_time,
            input,
        } = request;
        let target = input.table_name.clone();
        // A refusal the request is no longer there to receive (its client
        // went away) would otherwise vanish, so it is logged. A refused restore
        // removes what it created; a cleanup that failed is logged on its own,
        // and the abandoned-restore sweep finishes it.
        let refuse = |registered: tokio::sync::oneshot::Sender<_>, e: StorageError| {
            if let Err(Err(e)) = registered.send(Err(e)) {
                tracing::warn!(
                    "restore of {backup_arn} into {target} (account {account_id}) was refused \
                     after its request ended: {e}"
                );
            }
        };
        // The name first, unfenced, as on the other backends and before a
        // slot is taken: a restore into a name that is a CREATING restore
        // target is refused as the name (TableBeingRestored, the service's
        // TableInUseException) at once, whatever state the backup is in and
        // whether or not that target's restore is still running (an
        // abandoned target holds no restore lock until the sweep removes
        // it). A client retrying its own restore is answered here instead of
        // waiting for a slot. The restore lock and the target insert stay
        // the fence.
        if let Some(source_backup_arn) = self.restoring_into(&account_id, &target).await {
            refuse(
                registered,
                StorageError::TableBeingRestored {
                    table_name: target.clone(),
                    backup_arn: source_backup_arn,
                },
            );
            return;
        }
        // Admission: at most RESTORE_SLOTS_PER_ACCOUNT restores per account
        // and restore_server_slot_count per process at once. The permits
        // travel with the lock owner and are released when the copy ends.
        let slots = match acquire_restore_permits(
            self.restore_account_slots.for_account(&account_id),
            Arc::clone(&self.restore_server_slots),
            self.restore_server_slot_count,
            std::time::Duration::from_secs(RESTORE_SLOT_WAIT_SECS),
        )
        .await
        {
            Ok(slots) => slots,
            Err(e) => {
                refuse(registered, e);
                return;
            }
        };
        // Ownership first, before the target exists, so the abandoned-restore
        // sweep never sees this target without an owner, including while the
        // DDL runs. It is held until the copy ends; a crash anywhere releases
        // it with the connection, and the sweep then removes the target. A
        // lock already held means another restore into this name is in
        // flight right now: TableBeingRestored naming its source backup once
        // its CREATING row is visible, else the name is taken as CreateTable
        // would report it.
        let owner = match try_restore_lock(&self.pool, &account_id, &target, Some(slots)).await {
            Ok(Some(owner)) => owner,
            Ok(None) => {
                let error = self.name_taken_error(&account_id, &target, true).await;
                refuse(registered, error);
                return;
            }
            Err(e) => {
                refuse(registered, e);
                return;
            }
        };
        // Create the table and register its source backup in one catalog
        // transaction. The transaction holds the AVAILABLE backup row FOR
        // SHARE until the CREATING target and its table_restores row commit,
        // so DeleteBackup can neither remove the source between those rows
        // nor observe an unregistered target.
        let registration = RestoreRegistration {
            backup_arn: &backup_arn,
            source_table_arn: &source_table_arn,
            restore_date_time,
        };
        let mut desc = match self
            .create_table_for_restore(&account_id, input, &registration)
            .await
        {
            Ok(desc) => desc,
            Err(e) => {
                refuse(registered, e);
                return;
            }
        };
        desc.restore_summary = Some(RestoreSummary {
            source_backup_arn: Some(backup_arn.clone()),
            source_table_arn: Some(source_table_arn.clone()),
            restore_date_time,
            restore_in_progress: true,
        });
        if registered.send(Ok(desc.clone())).is_err() {
            tracing::info!(
                "restore of {backup_arn} into {target} (account {account_id}): the request \
                 ended before the CREATING response was sent; the restore continues"
            );
        }
        self.run_restore_copy(owner, account_id, desc, backup_arn)
            .await;
    }

    /// The detached half of RestoreTableFromBackup: copy the backup into the
    /// registered CREATING target while heartbeating the lock session, then
    /// flip the target ACTIVE, or remove it on failure. Owns everything it
    /// uses; the request that spawned it may be gone.
    async fn run_restore_copy(
        self,
        mut owner: RestoreOwner,
        account_id: String,
        desc: TableDescription,
        backup_arn: String,
    ) {
        let table_name = desc.table_name.clone();
        let outcome = {
            let copy = self.copy_backup_items(&desc, &backup_arn);
            let heartbeat = owner.heartbeat();
            tokio::select! {
                copied = copy => copied,
                lost = heartbeat => {
                    tracing::error!(
                        "restore of {backup_arn} into {table_name} (account {account_id}): the \
                         restore lock session failed its heartbeat ({lost}); stopping the copy"
                    );
                    Err(StorageError::Internal(format!(
                        "restore lock session lost: {lost}"
                    )))
                }
            }
        };
        let e = match outcome {
            Ok(()) => {
                tracing::info!(
                    "restore of {backup_arn} into {table_name} (account {account_id}) completed"
                );
                return;
            }
            Err(e) => e,
        };
        // The target must not stay behind: it is CREATING with no scheduled
        // transition, so nothing else would move it on, and the name would
        // stay taken. Removing it cascades its table_restores row. The lock
        // is still held here (unless its session is what failed), so the
        // sweep does not race this removal; both are conditional on CREATING
        // in any case.
        match self.abort_restore(&desc.table_id).await {
            Ok(true) => tracing::error!(
                "restore of {backup_arn} into {table_name} (account {account_id}) failed and \
                 the partial table was removed: {e}"
            ),
            // Already claimed by the sweep (whose own removal is logged), or
            // the activation committed before the copy was stopped.
            Ok(false) => tracing::warn!(
                "restore of {backup_arn} into {table_name} (account {account_id}) ended with \
                 \"{e}\"; the target ({}) was no longer CREATING, so it was left to its \
                 current owner",
                desc.table_id
            ),
            Err(cleanup) => tracing::error!(
                "restore of {backup_arn} into {table_name} (account {account_id}) failed ({e}), \
                 and the partial table ({}) could not be removed yet; the control plane (if it \
                 reached DELETING) or the abandoned-restore sweep (if still CREATING) finishes \
                 the removal: {cleanup}",
                desc.table_id
            ),
        }
        drop(owner);
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

            // The id embeds the creation instant to the millisecond; that
            // instant is stored as created_at, so the ARN, the reported
            // BackupCreationDateTime, and the ListBackups cursor agree.
            let backup_id = new_backup_id();
            let backup_arn = backup_arn(&self.region, &account_id, &table_name, &backup_id);
            let created_millis = backup_id_epoch_millis(&backup_id).ok_or_else(|| {
                StorageError::Internal(format!("backup id {backup_id} has no creation time"))
            })?;
            let created_at = time::OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(created_millis) * 1_000_000,
            )
            .map_err(|e| StorageError::Internal(format!("backup creation time: {e}")))?;

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
             billing_mode, vector_indexes, created_at) \
             VALUES ($1, $2, $3, $4, $5, 'AVAILABLE', 0, 0, $6, $7, $8, $9, $10)",
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
            .bind(created_at)
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
                    ..Default::default()
                },
                source_table_feature_details: None,
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
            #[allow(clippy::type_complexity)]
            let rows: Vec<(
                String,
                String,
                String,
                String,
                String,
                i64,
                String,
                time::OffsetDateTime,
            )> = if let Some(tn) = table_name {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.table_id, \
                     b.backup_status, b.backup_size_bytes, \
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
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.table_id, \
                     b.backup_status, b.backup_size_bytes, \
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
                    |(arn, name, tn, table_id, status, size, table_arn, created_at)| {
                        BackupSummary {
                            backup_arn: arn,
                            backup_name: name,
                            table_name: tn,
                            table_id,
                            table_arn,
                            backup_status: status,
                            backup_type: "USER".to_owned(),
                            backup_size_bytes: size,
                            backup_creation_date_time: pg_timestamp_to_epoch(created_at),
                        }
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
            // itself in `table_restores` while holding the same row FOR UPDATE,
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
                ..desc
            })
        })
    }

    fn backup_restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            #[allow(clippy::type_complexity)]
            let row: Option<(
                String,
                String,
                String,
                serde_json::Value,
                serde_json::Value,
                String,
                Option<serde_json::Value>,
                i64,
                i64,
                time::OffsetDateTime,
                time::OffsetDateTime,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
            )> = sqlx::query_as(
                "SELECT b.table_name, b.table_id, \
                 COALESCE(t.table_arn, \
                   'arn:aws:dynamodb:' || $3 || ':' || b.account_id || ':table/' || b.table_name), \
                 b.key_schema, b.attribute_definitions, b.billing_mode, \
                 b.provisioned_throughput, b.item_count, b.backup_size_bytes, b.created_at, \
                 COALESCE(t.creation_date_time, b.created_at), b.vector_indexes, d.definition \
                 FROM backups b \
                 LEFT JOIN tables t ON t.table_id = b.table_id \
                 LEFT JOIN backup_definitions d ON d.backup_arn = b.backup_arn \
                 WHERE b.backup_arn = $1 AND b.account_id = $2 AND b.backup_status = 'AVAILABLE'",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .bind(&self.region)
            .fetch_optional(&self.pool)
            .await
            .map_err(read_err)?;
            let Some((
                source_table_name,
                source_table_id,
                source_table_arn,
                ks_json,
                ad_json,
                billing_mode,
                row_throughput,
                item_count,
                size_bytes,
                created_at,
                table_created_at,
                vector_indexes,
                definition,
            )) = row
            else {
                return Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                )));
            };

            let definition = definition
                .map(|d| BackupTableDefinition::from_json(d, &backup_arn))
                .transpose()?;
            // A backup taken before catalog 0.0.4 has no definition; its
            // vector indexes are known only from the `backups.vector_indexes`
            // snapshot. A NULL snapshot is a backup taken before that column
            // existed, which cannot have carried vector indexes: this backend
            // could not create them then. An unrecognised version is refused
            // rather than guessed at, so a declared index is never dropped
            // silently. The engine refuses a non-zero count.
            let legacy_vector_index_count = match (&definition, vector_indexes.as_ref()) {
                (Some(_), _) | (None, None) => 0,
                (None, Some(snapshot)) => {
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
            let provisioned_throughput = match &definition {
                Some(d) => d.provisioned_throughput.clone(),
                None => throughput_from_catalog(row_throughput.as_ref()),
            };

            Ok(BackupRestoreSource {
                backup_arn: backup_arn.clone(),
                backup_creation_date_time: pg_timestamp_to_epoch(created_at),
                source_table_name,
                source_table_arn,
                source_table_id,
                key_schema: serde_json::from_value(ks_json)
                    .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?,
                attribute_definitions: serde_json::from_value(ad_json)
                    .map_err(|e| StorageError::Internal(format!("Parse attr defs: {e}")))?,
                billing_mode,
                provisioned_throughput,
                item_count,
                table_size_bytes: size_bytes,
                table_creation_date_time: pg_timestamp_to_epoch(table_created_at),
                definition,
                legacy_vector_index_count,
            })
        })
    }

    /// Register the CREATING target and return its description; the copy
    /// runs in a detached task.
    ///
    /// The request hands everything to a `tokio::spawn`ed task that owns its
    /// own clones: wait for a slot of its account's
    /// ([`RESTORE_SLOTS_PER_ACCOUNT`]) and then a slot of the server's
    /// ([`restore_server_slot_count`]), take the restore lock, create the
    /// target and its `table_restores` row in one catalog transaction (which
    /// re-checks the backup is AVAILABLE and owned by the account, holding it
    /// FOR UPDATE), report that result back through a channel, then copy.
    /// Dropping the request future at any point therefore changes nothing:
    /// a restore refused for want of a slot creates nothing, the
    /// registration still commits or fails on its own, and a registered
    /// target is still copied and flipped ACTIVE, or removed on failure.
    fn restore_table_from_backup(
        &self,
        account_id: &str,
        request: RestoreRequest,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
            let engine = self.clone();
            tokio::spawn(engine.run_restore(account_id, request, registered_tx));
            registered_rx.await.map_err(|_| {
                StorageError::Internal("the restore task ended before registering".to_owned())
            })?
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
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::Duration;

    use super::*;

    /// A server error reply carrying `code` as its SQLSTATE.
    #[derive(Debug)]
    struct Reply {
        code: Option<&'static str>,
    }

    impl std::fmt::Display for Reply {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "server reply {:?}", self.code)
        }
    }

    impl std::error::Error for Reply {}

    impl sqlx::error::DatabaseError for Reply {
        fn message(&self) -> &str {
            "server reply"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn reply(code: Option<&'static str>) -> sqlx::Error {
        sqlx::Error::Database(Box::new(Reply { code }))
    }

    #[test]
    fn only_too_many_connections_is_limit_exceeded() {
        assert!(matches!(
            lock_connection_error(&reply(Some("53300"))),
            StorageError::LimitExceeded(_)
        ));
        // invalid_password, invalid_catalog_name, cannot_connect_now,
        // insufficient_resources (53000, same class), and no SQLSTATE.
        for code in [
            Some("28P01"),
            Some("3D000"),
            Some("57P03"),
            Some("53000"),
            None,
        ] {
            assert!(
                matches!(
                    lock_connection_error(&reply(code)),
                    StorageError::Internal(_)
                ),
                "{code:?} must be Internal"
            );
        }
        for e in [
            sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused",
            )),
            sqlx::Error::Tls("bad certificate".into()),
            sqlx::Error::PoolTimedOut,
            sqlx::Error::Protocol("unexpected".to_owned()),
        ] {
            assert!(
                matches!(lock_connection_error(&e), StorageError::Internal(_)),
                "{e} must be Internal"
            );
        }
    }

    #[test]
    fn a_read_that_cannot_reach_the_database_is_transient() {
        // connection_failure, admin_shutdown, crash_shutdown,
        // cannot_connect_now, too_many_connections, connection_exception.
        for code in ["08006", "57P01", "57P02", "57P03", "53300", "08000"] {
            assert!(
                matches!(read_err(reply(Some(code))), StorageError::Transient(_)),
                "{code} must be Transient"
            );
        }
        for e in [
            sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            )),
            sqlx::Error::Tls("bad record".into()),
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::WorkerCrashed,
        ] {
            let what = e.to_string();
            assert!(
                matches!(read_err(e), StorageError::Transient(_)),
                "{what} must be Transient"
            );
        }
        // undefined_column, insufficient_privilege, query_canceled,
        // insufficient_resources (53000), no SQLSTATE, a protocol error,
        // and a decode error stay Internal.
        for code in [
            Some("42703"),
            Some("42501"),
            Some("57014"),
            Some("53000"),
            None,
        ] {
            assert!(
                matches!(read_err(reply(code)), StorageError::Internal(_)),
                "{code:?} must be Internal"
            );
        }
        for e in [
            sqlx::Error::Protocol("unexpected".to_owned()),
            sqlx::Error::ColumnNotFound("definition".to_owned()),
        ] {
            let what = e.to_string();
            assert!(
                matches!(read_err(e), StorageError::Internal(_)),
                "{what} must be Internal"
            );
        }
    }

    #[test]
    fn the_server_bound_is_half_the_pool_within_one_and_sixteen() {
        assert_eq!(restore_server_slot_count(0), 1);
        assert_eq!(restore_server_slot_count(1), 1);
        assert_eq!(restore_server_slot_count(3), 1);
        assert_eq!(restore_server_slot_count(10), 5);
        assert_eq!(restore_server_slot_count(20), 10);
        assert_eq!(restore_server_slot_count(32), 16);
        assert_eq!(restore_server_slot_count(33), 16);
        assert_eq!(restore_server_slot_count(1000), 16);
    }

    /// A full server bound refuses with LimitExceeded naming the server, a
    /// full account with LimitExceeded naming the account; a free slot of
    /// each is granted, and releasing a held one lets the next restore in.
    #[tokio::test]
    async fn restore_permits_wait_then_limit_exceeded() {
        let wait = Duration::from_millis(50);
        let account = Arc::new(Semaphore::new(RESTORE_SLOTS_PER_ACCOUNT));
        let server = Arc::new(Semaphore::new(1));

        let held = acquire_restore_permits(Arc::clone(&account), Arc::clone(&server), 1, wait)
            .await
            .expect("the first restore gets both slots");
        let other_account = Arc::new(Semaphore::new(RESTORE_SLOTS_PER_ACCOUNT));
        match acquire_restore_permits(Arc::clone(&other_account), Arc::clone(&server), 1, wait)
            .await
        {
            Err(StorageError::LimitExceeded(m)) => {
                assert!(m.contains("on this server (at most 1"), "{m}");
            }
            other => panic!("a full server bound must refuse: {:?}", other.err()),
        }
        // The refused restore gave its account slot back.
        assert_eq!(other_account.available_permits(), RESTORE_SLOTS_PER_ACCOUNT);
        drop(held);
        acquire_restore_permits(Arc::clone(&other_account), Arc::clone(&server), 1, wait)
            .await
            .expect("a freed server slot is granted");

        let full_account = Arc::new(Semaphore::new(0));
        let roomy_server = Arc::new(Semaphore::new(4));
        match acquire_restore_permits(full_account, Arc::clone(&roomy_server), 4, wait).await {
            Err(StorageError::LimitExceeded(m)) => {
                assert!(m.contains("for this account"), "{m}");
            }
            other => panic!("a full account must refuse: {:?}", other.err()),
        }
        assert_eq!(roomy_server.available_permits(), 4);
    }

    /// One engine's account slots are shared by its clones and by nothing
    /// else: a second engine in the same process has its own full allowance
    /// for the same account.
    #[test]
    fn account_restore_slots_belong_to_one_engine() {
        let slots = RestoreAccountSlots::default();
        let held = slots
            .for_account("123456789012")
            .try_acquire_owned()
            .expect("a free slot");
        assert_eq!(
            slots
                .clone()
                .for_account("123456789012")
                .available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT - 1,
            "a clone sees the same account map"
        );
        assert_eq!(
            RestoreAccountSlots::default()
                .for_account("123456789012")
                .available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT,
            "another engine has its own"
        );
        drop(held);
        // Nothing in flight for the account: its entry is pruned on the next
        // lookup of any account, and a fresh one has the full allowance.
        slots.for_account("other");
        assert_eq!(slots.0.lock().unwrap().len(), 1);
        assert_eq!(
            slots.for_account("123456789012").available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT
        );
    }
}
