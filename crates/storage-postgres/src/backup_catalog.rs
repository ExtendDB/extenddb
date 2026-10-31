// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on `PostgreSQL`: `backup_index`, the restore job fields
//! of `table_restores`, and `backup_maintenance`.
//!
//! The claim of a backup job is `pg_try_advisory_lock(hashtextextended(
//! 'backup:' || backup_id, RESTORE_LOCK_SEED))` on a dedicated, non-pooled
//! connection to the catalog database (a [`ClaimSession`]), taken before the
//! row is inserted and held for the job's life; the backup job's heartbeats
//! and status writes go through that connection and carry the claim's
//! `owner_generation`, so a session the server dropped surfaces as a failed
//! heartbeat and a row a successor took over matches no write (the legacy
//! restore heartbeats through its session too, and makes its DONE and FAILED
//! writes on pooled connections, each conditioned on RUNNING and the
//! generation). The lock on a row for DeleteBackup and a restore's
//! registration is `FOR UPDATE` on the `backup_index` row. Staleness is
//! measured on the catalog's clock.
//!
//! The maintenance claim is the same mechanism on the `backup_maintenance`
//! row: an advisory lock (`backup_maintenance` in the same key space) on a
//! dedicated connection is the proof of a live holder, so a second command
//! finds it taken and a holder whose session ended has released it; the row
//! carries the holder, the generation and the heartbeat.
//! `check_maintenance_claim` reports the claim held while a session holds
//! the lock (a transaction-scoped shared try-lock in one autocommit
//! statement fails while the exclusive lock is held, and holds nothing past
//! its statement, so a request dropped mid-check leaves no lock on the
//! pool), whatever the row's heartbeat says, and otherwise while the row's
//! heartbeat is younger than the stale window, so a dead holder's claim
//! expires for the jobs that read it without anyone taking it over. `release` deletes the row and ends the session.
//!
//! A conditional write whose error is of the connection class (an I/O or TLS
//! error, a pool or worker failure, SQLSTATE class 08, or 57P01 to 57P03)
//! has an unknown outcome (`WriteOutcome::Unknown`: the statement may have
//! reached the server); a statement error, a `statement_timeout` included,
//! is `Err`. The heartbeat period is
//! [`extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL`] (15 s), the
//! constant every backend runs at until `[backup] heartbeat_interval_secs`
//! reaches the jobs in a later release.

use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreTargetKey, StatusFields,
    StoreIdentity, StoreKind, TimeBounds, WriteOutcome,
};
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::PgExecutor;
use tokio::sync::Mutex;

use crate::PostgresEngine;
use crate::backup_engine::{ClaimSession, backup_lock_key, generation_column, try_claim};

/// The name of a CREATING restore target registered from `backup_arn`, if
/// any: the one in-use rule DeleteBackup, a restore's registration and
/// `backup_in_use` share. Reads the target's status, not the job's.
pub(crate) async fn creating_restore_target<'e, E: PgExecutor<'e>>(
    executor: E,
    backup_arn: &str,
) -> Result<Option<String>, StorageError> {
    sqlx::query_scalar(
        "SELECT t.table_name FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
         WHERE r.source_backup_arn = $1 AND t.table_status = 'CREATING' LIMIT 1",
    )
    .bind(backup_arn)
    .fetch_optional(executor)
    .await
    .map_err(db_err)
}

/// The error for a backup a CREATING restore target still reads: the shared
/// text, [`extenddb_storage::backup_catalog::backup_in_use`].
pub(crate) fn backup_in_use(table: &str, backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_in_use(table, backup_arn)
}

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

fn not_found(backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_not_found(backup_arn)
}

/// The columns of one `backup_index` row, in the order every read selects
/// them.
const ROW_COLUMNS: &str = "backup_id, account_id, table_name, backup_arn, backup_name, status, \
     store_kind, store_root, store_id, size_bytes, item_count, \
     EXTRACT(EPOCH FROM created_at)::FLOAT8 AS created_at, \
     EXTRACT(EPOCH FROM completed_at)::FLOAT8 AS completed_at, \
     failure_reason, source_table_details_json, owner_generation, \
     EXTRACT(EPOCH FROM heartbeat_at)::FLOAT8 AS heartbeat_at";

/// One `backup_index` row as the driver reads it.
#[derive(sqlx::FromRow)]
struct RowTuple {
    backup_id: String,
    account_id: String,
    table_name: String,
    backup_arn: String,
    backup_name: String,
    status: String,
    store_kind: String,
    store_root: String,
    store_id: String,
    size_bytes: i64,
    item_count: i64,
    created_at: f64,
    completed_at: Option<f64>,
    failure_reason: Option<String>,
    source_table_details_json: serde_json::Value,
    owner_generation: i64,
    heartbeat_at: f64,
}

fn row_from_tuple(t: RowTuple) -> Result<BackupRow, StorageError> {
    let RowTuple {
        backup_id,
        account_id,
        table_name,
        backup_arn,
        backup_name,
        status,
        store_kind,
        store_root,
        store_id,
        size_bytes,
        item_count,
        created_at,
        completed_at,
        failure_reason,
        source_table_details_json,
        owner_generation,
        heartbeat_at,
    } = t;
    let store_kind = StoreKind::parse(&store_kind).ok_or_else(|| {
        StorageError::Internal(format!(
            "backup {backup_arn} carries an unknown store kind {store_kind:?}"
        ))
    })?;
    Ok(BackupRow {
        backup_id,
        account_id,
        table_name,
        backup_arn,
        backup_name,
        status: BackupStatus::parse(&status)?,
        store: StoreIdentity {
            store_kind,
            store_root,
            store_id,
        },
        size_bytes,
        item_count,
        created_at,
        completed_at,
        failure_reason,
        source_table_details_json,
        owner_generation: u64::try_from(owner_generation).unwrap_or_default(),
        heartbeat_at,
    })
}

/// The rows `read` fetches, as a stream: the read runs once, on the first
/// poll, and a failed read is one `Err` item.
fn rows_stream<'a, F>(read: F) -> BoxStream<'a, Result<BackupRow, StorageError>>
where
    F: std::future::Future<Output = Result<Vec<RowTuple>, StorageError>> + Send + 'a,
{
    use futures::StreamExt;
    Box::pin(
        futures::stream::once(async move {
            match read.await {
                Ok(rows) => rows.into_iter().map(row_from_tuple).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        })
        .flat_map(futures::stream::iter),
    )
}

/// A backup job's handle on its `backup_index` row.
struct PostgresBackupClaim {
    session: Mutex<ClaimSession>,
    generation: u64,
    key: BackupKey,
    pool: sqlx::PgPool,
}

/// The heartbeat of a backup job: refresh its row while it carries this
/// generation.
const BACKUP_HEARTBEAT: &str = "UPDATE backup_index SET heartbeat_at = now() \
     WHERE account_id = $1 AND backup_id = $2 AND owner_generation = $3";

/// Whether a write's outcome cannot be told from its error: the statement
/// may have reached the server before the connection failed.
fn outcome_unknown(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(d) => d.code().is_some_and(|code| {
            code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
        }),
        _ => false,
    }
}

impl JobClaim for PostgresBackupClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let confirmed = session
                .confirm(
                    BACKUP_HEARTBEAT,
                    &[&self.key.account_id, &self.key.backup_id],
                )
                .await
                .map_err(|e| StorageError::Internal(format!("claim session: {e}")))?;
            Ok(if confirmed {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    fn set_status<'a>(
        &'a self,
        from: JobStatus,
        to: JobStatus,
        fields: &'a StatusFields,
    ) -> BoxFuture<'a, Result<WriteOutcome, StorageError>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let written = sqlx::query(
                "UPDATE backup_index SET status = $4, \
                 size_bytes = COALESCE($5, size_bytes), \
                 item_count = COALESCE($6, item_count), \
                 failure_reason = COALESCE($7, failure_reason), \
                 completed_at = CASE WHEN $8 THEN now() ELSE completed_at END \
                 WHERE account_id = $1 AND backup_id = $2 AND owner_generation = $3 \
                 AND status = $9",
            )
            .bind(&self.key.account_id)
            .bind(&self.key.backup_id)
            .bind(generation_column(self.generation))
            .bind(to.as_str())
            .bind(fields.size_bytes)
            .bind(fields.item_count)
            .bind(fields.failure_reason.as_deref())
            .bind(fields.completed)
            .bind(from.as_str())
            .execute(session.connection())
            .await;
            match written {
                Ok(result) if result.rows_affected() == 1 => Ok(WriteOutcome::Matched),
                Ok(_) => Ok(WriteOutcome::NotMatched),
                Err(e) if outcome_unknown(&e) => Ok(WriteOutcome::Unknown),
                Err(e) => Err(db_err(e)),
            }
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let row: Option<(String, i64, f64, Option<String>)> = sqlx::query_as(
                "SELECT status, owner_generation, EXTRACT(EPOCH FROM heartbeat_at)::FLOAT8, \
                 failure_reason FROM backup_index WHERE account_id = $1 AND backup_id = $2",
            )
            .bind(&self.key.account_id)
            .bind(&self.key.backup_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            let (status, generation, heartbeat_at, failure_reason) =
                row.ok_or_else(|| not_found(&self.key.backup_arn))?;
            Ok(JobRow {
                status: JobStatus::parse(&status)?,
                owner_generation: u64::try_from(generation).unwrap_or_default(),
                heartbeat_at,
                failure_reason,
            })
        })
    }
}

impl BackupCatalog for PostgresEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let id: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(db_err)?;
            id.ok_or_else(|| {
                StorageError::Internal(
                    "the catalog has no catalog_id; run extenddb migrate --yes".to_owned(),
                )
            })
        })
    }

    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        Box::pin(async move {
            let Some(mut session) =
                try_claim(&self.pool, &backup_lock_key(&row.backup_id), None).await?
            else {
                return Err(StorageError::AlreadyExists(format!(
                    "a live job owns backup {}",
                    row.backup_arn
                )));
            };
            let inserted = sqlx::query(
                "INSERT INTO backup_index (backup_id, account_id, table_name, backup_arn, \
                 backup_name, status, store_kind, store_root, store_id, size_bytes, item_count, \
                 created_at, source_table_details_json, owner_generation, heartbeat_at) \
                 VALUES ($1, $2, $3, $4, $5, 'CREATING', $6, $7, $8, $9, $10, to_timestamp($11), \
                 $12, $13, now()) ON CONFLICT DO NOTHING",
            )
            .bind(&row.backup_id)
            .bind(&row.account_id)
            .bind(&row.table_name)
            .bind(&row.backup_arn)
            .bind(&row.backup_name)
            .bind(row.store.store_kind.as_str())
            .bind(&row.store.store_root)
            .bind(&row.store.store_id)
            .bind(row.size_bytes)
            .bind(row.item_count)
            .bind(row.created_at)
            .bind(&row.source_table_details_json)
            .bind(generation_column(session.generation()))
            .execute(session.connection())
            .await
            .map_err(db_err)?
            .rows_affected();
            if inserted == 0 {
                return Err(StorageError::AlreadyExists(format!(
                    "backup {} already has a row",
                    row.backup_arn
                )));
            }
            let generation = session.generation();
            Ok(Box::new(PostgresBackupClaim {
                session: Mutex::new(session),
                generation,
                key: row.key(),
                pool: self.pool.clone(),
            }) as Box<dyn JobClaim>)
        })
    }

    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = $1 AND store_root = $2 AND store_id = $3 \
                 AND ($4::TEXT IS NULL OR status = $4) ORDER BY created_at, backup_arn"
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .bind(status.map(BackupStatus::as_str))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index WHERE account_id = $1 \
                 AND ($2::TEXT IS NULL OR table_name = $2) \
                 AND ($3::FLOAT8 IS NULL OR created_at >= to_timestamp($3)) \
                 AND ($4::FLOAT8 IS NULL OR created_at < to_timestamp($4)) \
                 ORDER BY created_at, backup_arn"
            ))
            .bind(account_id)
            .bind(table_name)
            .bind(bounds.lower)
            .bind(bounds.upper)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RowTuple> = sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index WHERE account_id = $1 AND backup_id = $2 \
                 AND table_name = $3 AND backup_arn = $4"
            ))
            .bind(arn.account_id())
            .bind(arn.backup_id())
            .bind(arn.table_name())
            .bind(arn.to_arn_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(row_from_tuple).transpose()
        })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(db_err)?;
            // FOR UPDATE is the contract a restore's registration of this
            // row must meet: it locks the same row before its own in-use
            // check, so the two order wholly one before the other (a
            // registration that commits first leaves this `InUse`; one that
            // waits behind this finds the row DELETING). On this head the
            // legacy registration locks the legacy `backups` row and no
            // restore of a `backup_index` row exists; PR 9a's registration
            // is the first to take this lock.
            let status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM backup_index WHERE account_id = $1 AND backup_id = $2 \
                 FOR UPDATE",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
            let status = BackupStatus::parse(&status.ok_or_else(|| not_found(&key.backup_arn))?)?;
            if status != BackupStatus::Available {
                return Ok(DeleteStart::Status(status));
            }
            if let Some(table) = creating_restore_target(&mut *tx, &key.backup_arn).await? {
                return Ok(DeleteStart::InUse(table));
            }
            sqlx::query(
                "UPDATE backup_index SET status = 'DELETING' \
                 WHERE account_id = $1 AND backup_id = $2 AND status = 'AVAILABLE'",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
            tx.commit().await.map_err(db_err)?;
            Ok(DeleteStart::Deleting)
        })
    }

    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let affected = sqlx::query(
                "UPDATE backup_index SET status = 'DELETED', completed_at = now() \
                 WHERE account_id = $1 AND backup_id = $2 AND status = 'DELETING'",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let affected = sqlx::query(
                "DELETE FROM backup_index WHERE account_id = $1 AND backup_id = $2 \
                 AND status IN ('DELETED', 'FAILED')",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let affected = sqlx::query(
                "DELETE FROM backup_index WHERE account_id = $1 AND backup_id = $2 \
                 AND status = 'CREATING' \
                 AND heartbeat_at < now() - make_interval(secs => $3)",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .bind(stale_after.as_secs_f64())
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move { creating_restore_target(&self.pool, &key.backup_arn).await })
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The key must name a CREATING restore target with this table id;
            // a table re-created under the name has another id and is not
            // touched. `abort_restore` then claims it from CREATING in one
            // statement, so it and the ACTIVE flip exclude each other.
            let is_target: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables t JOIN table_restores r \
                 ON r.table_id = t.table_id WHERE t.table_id = $1 AND t.account_id = $2 \
                 AND t.table_name = $3 AND t.table_status = 'CREATING')",
            )
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if !is_target {
                return Ok(false);
            }
            // Progress rows belong to a later release's restore path; a
            // catalog without that table has none to delete.
            let has_progress: bool =
                sqlx::query_scalar("SELECT to_regclass('restore_progress') IS NOT NULL")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db_err)?;
            if has_progress {
                sqlx::query("DELETE FROM restore_progress WHERE table_id = $1")
                    .bind(&key.table_id)
                    .execute(&self.pool)
                    .await
                    .map_err(db_err)?;
            }
            self.abort_restore(&key.table_id).await
        })
    }

    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // Two arms: a RUNNING record whose heartbeat is stale (a dead
            // owner), and a FAILED record whose table is still a CREATING
            // restore target once the record is stale (the FAILED write
            // landed and the removal did not; `fail_restore_job` and the
            // job's own failure path stamp `completed_at`, and a record
            // without one is aged by its heartbeat). The delay is what
            // leaves a job still on its own failure path to finish it.
            #[allow(clippy::type_complexity)]
            let rows: Vec<(String, String, String, String, String, String, i64, f64)> =
                sqlx::query_as(
                    "SELECT r.table_id, t.account_id, t.table_name, r.source_backup_arn, \
                     r.kind, r.status, r.owner_generation, \
                     EXTRACT(EPOCH FROM r.heartbeat_at)::FLOAT8 \
                     FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                     WHERE (r.status = 'RUNNING' \
                            AND r.heartbeat_at < now() - make_interval(secs => $1)) \
                        OR (r.status = 'FAILED' AND t.table_status = 'CREATING' \
                            AND COALESCE(r.completed_at, r.heartbeat_at) \
                                < now() - make_interval(secs => $1)) \
                     ORDER BY t.account_id, t.table_name",
                )
                .bind(stale_after.as_secs_f64())
                .fetch_all(&self.pool)
                .await
                .map_err(db_err)?;
            rows.into_iter()
                .map(
                    |(
                        table_id,
                        account_id,
                        table_name,
                        source_backup_arn,
                        kind,
                        status,
                        owner_generation,
                        heartbeat_at,
                    )| {
                        Ok(RestoreJobRow {
                            target: RestoreTargetKey {
                                account_id,
                                table_name,
                                table_id,
                            },
                            source_backup_arn,
                            kind: RestoreKind::parse(&kind)?,
                            status: JobStatus::parse(&status)?,
                            owner_generation: u64::try_from(owner_generation).unwrap_or_default(),
                            heartbeat_at,
                        })
                    },
                )
                .collect()
        })
    }

    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let affected = sqlx::query(
                "UPDATE table_restores r SET status = 'FAILED', failure_reason = $4, \
                 completed_at = now() FROM tables t \
                 WHERE t.table_id = r.table_id AND r.table_id = $1 AND t.account_id = $2 \
                 AND t.table_name = $3 AND r.status = 'RUNNING'",
            )
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .bind(reason)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            sqlx::query_scalar(
                "SELECT DISTINCT store_id FROM backup_index \
                 WHERE store_kind = $1 AND store_root = $2 ORDER BY store_id",
            )
            .bind(store_kind.as_str())
            .bind(store_root)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        Box::pin(async move {
            // The advisory lock is the live-holder proof, as a job's is: a
            // second command on any instance finds it taken while the first
            // runs, and a holder whose session ended has released it. The
            // row carries the generation the heartbeats and the release are
            // conditioned on, and the heartbeat age jobs read.
            let Some(mut session) = try_claim(&self.pool, MAINTENANCE_LOCK_KEY, None).await? else {
                let current: Option<String> =
                    sqlx::query_scalar("SELECT holder FROM backup_maintenance")
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(db_err)?;
                return Err(StorageError::AlreadyExists(format!(
                    "the backup maintenance claim is held by {}",
                    current.as_deref().unwrap_or("another maintenance command")
                )));
            };
            // With the lock held, a row whose heartbeat is still young is a
            // holder whose lock session ended within the stale window: dead
            // by this backend's rule (4(e)), so the row is taken over
            // whatever its age. `stale_after` is the other backends' rule and
            // the one `check_maintenance_claim` applies on every backend.
            let _ = stale_after;
            let generation = session.generation();
            sqlx::query(
                "INSERT INTO backup_maintenance (singleton, holder, owner_generation) \
                 VALUES (TRUE, $1, $2) \
                 ON CONFLICT (singleton) DO UPDATE SET holder = EXCLUDED.holder, \
                 owner_generation = EXCLUDED.owner_generation, claimed_at = now(), \
                 heartbeat_at = now()",
            )
            .bind(holder)
            .bind(generation_column(generation))
            .execute(session.connection())
            .await
            .map_err(db_err)?;
            Ok(Box::new(PostgresMaintenanceClaim {
                session: Mutex::new(Some(session)),
                generation,
            }) as Box<dyn MaintenanceClaim>)
        })
    }

    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        Box::pin(async move {
            // A live holder's session holds the exclusive lock, so a shared
            // try-lock fails; it succeeds when no session does. The lock is
            // taken at transaction scope in one autocommit statement, so it
            // ends with the statement and nothing is held on the pooled
            // connection when the caller's future is dropped between awaits
            // (this runs inside every CreateBackup and RestoreTableFromBackup
            // request). Then the row a dead session left is held until its
            // heartbeat ages past the window.
            let unlocked: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, $2))",
            )
            .bind(MAINTENANCE_LOCK_KEY)
            .bind(crate::backup_engine::RESTORE_LOCK_SEED)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if !unlocked {
                return Ok(true);
            }
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM backup_maintenance \
                 WHERE heartbeat_at >= now() - make_interval(secs => $1))",
            )
            .bind(stale_after.as_secs_f64())
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)
        })
    }
}

/// The advisory-lock key of the maintenance claim, in the one lock key space
/// (`RESTORE_LOCK_SEED`) the job claims use.
const MAINTENANCE_LOCK_KEY: &str = "backup_maintenance";

/// The holder's handle on the `backup_maintenance` row: the lock session
/// (`None` once released) and the generation its writes are conditioned on.
struct PostgresMaintenanceClaim {
    session: Mutex<Option<ClaimSession>>,
    generation: u64,
}

impl MaintenanceClaim for PostgresMaintenanceClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let Some(session) = session.as_mut() else {
                return Ok(Heartbeat::Lost);
            };
            let confirmed = session
                .confirm(
                    "UPDATE backup_maintenance SET heartbeat_at = now() WHERE owner_generation = $1",
                    &[],
                )
                .await
                .map_err(|e| StorageError::Internal(format!("claim session: {e}")))?;
            Ok(if confirmed {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    /// Delete the row and end the lock session, which releases the advisory
    /// lock; a second release finds no session and does nothing.
    fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let mut slot = self.session.lock().await;
            let Some(mut session) = slot.take() else {
                return Ok(());
            };
            sqlx::query("DELETE FROM backup_maintenance WHERE owner_generation = $1")
                .bind(generation_column(self.generation))
                .execute(session.connection())
                .await
                .map_err(db_err)?;
            session.close().await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_tuple_with_an_unknown_store_kind_or_status_is_internal() {
        let good = || RowTuple {
            backup_id: "01757500000123-0badcafe".to_owned(),
            account_id: "123456789012".to_owned(),
            table_name: "Music".to_owned(),
            backup_arn: "arn".to_owned(),
            backup_name: "b".to_owned(),
            status: "AVAILABLE".to_owned(),
            store_kind: "s3".to_owned(),
            store_root: "s3://bucket/prefix".to_owned(),
            store_id: "store-1".to_owned(),
            size_bytes: 1,
            item_count: 2,
            created_at: 3.0,
            completed_at: None,
            failure_reason: None,
            source_table_details_json: serde_json::json!({}),
            owner_generation: 7,
            heartbeat_at: 4.0,
        };
        let row = row_from_tuple(good()).unwrap();
        assert_eq!(row.store.store_kind, StoreKind::S3);
        assert_eq!(row.owner_generation, 7);
        let mut bad_kind = good();
        bad_kind.store_kind = "gcs".to_owned();
        assert!(matches!(
            row_from_tuple(bad_kind),
            Err(StorageError::Internal(_))
        ));
        let mut bad_status = good();
        bad_status.status = "ACTIVE".to_owned();
        assert!(matches!(
            row_from_tuple(bad_status),
            Err(StorageError::Internal(_))
        ));
    }

    #[test]
    fn only_a_connection_class_failure_leaves_a_write_unknown() {
        assert!(outcome_unknown(&sqlx::Error::PoolClosed));
        assert!(outcome_unknown(&sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(!outcome_unknown(&sqlx::Error::Protocol("x".to_owned())));
        assert!(!outcome_unknown(&sqlx::Error::ColumnNotFound(
            "status".to_owned()
        )));
    }
}
