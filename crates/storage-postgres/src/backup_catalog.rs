// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on `PostgreSQL`: `backup_index`, the restore job fields
//! of `table_restores`, and `backup_maintenance`. The data-plane side of a
//! store-backed restore is `restore_engine.rs`.
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
use extenddb_core::types::{CreateTableInput, RestoreSummary};
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreRegistered,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity, StoreKind,
    TimeBounds, WriteOutcome, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::PgExecutor;
use tokio::sync::Mutex;

use crate::PostgresEngine;
use crate::backup_engine::{
    ClaimSession, RESTORE_HEARTBEAT, backup_lock_key, generation_column, restore_lock_key,
    try_claim, try_claim_with,
};
use crate::create_table::RestoreSource;

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
                 completed_at = CASE WHEN $8 THEN now() ELSE completed_at END, \
                 source_table_details_json = COALESCE($10, source_table_details_json) \
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
            .bind(fields.source_table_details_json.as_ref())
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

/// A store-backed restore job's handle on its `table_restores` record: the
/// advisory lock on the target name (`restore_lock_key`) on the claim
/// session, the generation, and the pool for reads. The heartbeat and the
/// status writes go through the session, so a session the server dropped
/// surfaces as a lost heartbeat or an unknown write outcome.
struct PostgresRestoreClaim {
    session: Mutex<ClaimSession>,
    generation: u64,
    table_id: String,
    pool: sqlx::PgPool,
}

impl JobClaim for PostgresRestoreClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let confirmed = session
                .confirm(RESTORE_HEARTBEAT, &[&self.table_id])
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
                "UPDATE table_restores SET status = $3, \
                 failure_reason = COALESCE($4, failure_reason), \
                 completed_at = CASE WHEN $5 THEN now() ELSE completed_at END \
                 WHERE table_id = $1 AND owner_generation = $2 AND status = $6",
            )
            .bind(&self.table_id)
            .bind(generation_column(self.generation))
            .bind(to.as_str())
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
                 failure_reason FROM table_restores WHERE table_id = $1",
            )
            .bind(&self.table_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            let (status, generation, heartbeat_at, failure_reason) = row.ok_or_else(|| {
                StorageError::TableNotFound(format!(
                    "restore target {} has no job record",
                    self.table_id
                ))
            })?;
            Ok(JobRow {
                status: JobStatus::parse(&status)?,
                owner_generation: u64::try_from(generation).unwrap_or_default(),
                heartbeat_at,
                failure_reason,
            })
        })
    }
}

/// The columns of one restore job record, joined with its table, in the
/// order [`RestoreJobTuple`] reads them.
const RESTORE_ROW_COLUMNS: &str = "r.table_id, t.account_id, t.table_name, r.source_backup_arn, \
     r.kind, r.status, r.owner_generation, EXTRACT(EPOCH FROM r.heartbeat_at)::FLOAT8, \
     r.store_kind, r.store_root, r.store_id, r.spool_host, r.file_index, r.items_applied, \
     r.failure_reason, EXTRACT(EPOCH FROM r.completed_at)::FLOAT8";

/// One restore job record as the driver reads it.
#[allow(clippy::type_complexity)]
type RestoreJobTuple = (
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    f64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<f64>,
);

fn restore_job_from_tuple(t: RestoreJobTuple) -> Result<RestoreJobRow, StorageError> {
    let (
        table_id,
        account_id,
        table_name,
        source_backup_arn,
        kind,
        status,
        owner_generation,
        heartbeat_at,
        store_kind,
        store_root,
        store_id,
        spool_host,
        file_index,
        items_applied,
        failure_reason,
        completed_at,
    ) = t;
    let store = match (store_kind, store_root, store_id) {
        (Some(kind), Some(store_root), Some(store_id)) => Some(StoreIdentity {
            store_kind: StoreKind::parse(&kind).ok_or_else(|| {
                StorageError::Internal(format!(
                    "restore of {source_backup_arn} carries an unknown store kind {kind:?}"
                ))
            })?,
            store_root,
            store_id,
        }),
        _ => None,
    };
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
        store,
        spool_host,
        file_index: file_index.and_then(|i| u32::try_from(i).ok()),
        items_applied: items_applied.and_then(|i| u64::try_from(i).ok()),
        failure_reason,
        completed_at,
    })
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

    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RowTuple> = sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = $1 AND store_root = $2 AND store_id = $3 \
                 AND status = 'AVAILABLE' ORDER BY created_at DESC, backup_arn DESC LIMIT 1"
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(row_from_tuple).transpose()
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
            // waits behind this finds the row DELETING). `claim_restore`
            // (`create_table.rs`, the manifest arm) takes this lock; the
            // legacy registration locks the legacy `backups` row instead.
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

    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The same advisory lock the owner took: a session that still
            // holds it is alive whatever its heartbeat says (the lock goes
            // with the session), and a second taker is refused here.
            let Some(mut session) =
                try_claim(&self.pool, &backup_lock_key(&key.backup_id), None).await?
            else {
                return Ok(None);
            };
            let generation = session.generation();
            let taken = sqlx::query(
                "UPDATE backup_index SET owner_generation = $3 \
                 WHERE account_id = $1 AND backup_id = $2 AND status = 'CREATING' \
                 AND heartbeat_at < now() - make_interval(secs => $4)",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .bind(generation_column(generation))
            .bind(stale_after.as_secs_f64())
            .execute(session.connection())
            .await
            .map_err(db_err)?
            .rows_affected();
            if taken != 1 {
                session.close().await;
                return Ok(None);
            }
            Ok(Some(Box::new(PostgresBackupClaim {
                session: Mutex::new(session),
                generation,
                key: key.clone(),
                pool: self.pool.clone(),
            }) as Box<dyn JobClaim>))
        })
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: BackupStatus,
        store: &'a StoreIdentity,
        clock: StaleClock,
        age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        let column = match clock {
            StaleClock::Heartbeat => "heartbeat_at",
            StaleClock::Completed => "completed_at",
        };
        rows_stream(async move {
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = $1 AND store_root = $2 AND store_id = $3 AND status = $4 \
                 AND {column} < now() - make_interval(secs => $5) \
                 ORDER BY created_at, backup_arn"
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .bind(status.as_str())
            .bind(age.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
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
        Box::pin(async move { self.remove_restore_target_locked(key).await })
    }

    fn remove_restore_target_exclusive<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The command path (`remove-target`) takes the target name's
            // advisory lock first, the one the owner and `claim_restore_dead`
            // take, so a removal and a takeover of the same target exclude
            // each other, and a frozen owner whose claim session still holds
            // the lock (its heartbeat stale but its session alive) keeps its
            // target. The job's own failure path and the
            // reconciler's resume call the plain `remove_restore_target`
            // while they already hold this lock through their claim, so the
            // lock lives only on this command-only variant.
            let Some(session) = try_claim(
                &self.pool,
                &restore_lock_key(&key.account_id, &key.table_name),
                None,
            )
            .await?
            else {
                return Ok(false);
            };
            let outcome = self.remove_restore_target_locked(key).await;
            session.close().await;
            outcome
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
            // leaves a job still on its own failure path to finish it. A row
            // this build cannot read (a kind or status no release writes) is
            // logged and skipped, as ListBackups skips a row it cannot read,
            // so one such row does not stop the cycle.
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE (r.status = 'RUNNING' \
                        AND r.heartbeat_at < now() - make_interval(secs => $1)) \
                    OR (r.status = 'FAILED' AND t.table_status = 'CREATING' \
                        AND COALESCE(r.completed_at, r.heartbeat_at) \
                            < now() - make_interval(secs => $1)) \
                 ORDER BY t.account_id, t.table_name"
            ))
            .bind(stale_after.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let (account_id, table_name) = (row.1.clone(), row.2.clone());
                    match restore_job_from_tuple(row) {
                        Ok(job) => Some(job),
                        Err(e) => {
                            tracing::warn!(
                                "reconciler: restore job record of {table_name} (account \
                                 {account_id}) skipped: {e}"
                            );
                            None
                        }
                    }
                })
                .collect())
        })
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // By status alone: a record stamped in this statement's instant,
            // or ahead of the server's clock, is RUNNING too.
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.status = 'RUNNING' ORDER BY t.account_id, t.table_name"
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            rows.into_iter().map(restore_job_from_tuple).collect()
        })
    }

    /// Conditioned on RUNNING, on the listed `owner_generation` and on the
    /// listed `heartbeat_at` (compared through the same `EXTRACT(EPOCH ...)`
    /// expression the listing read it with, so the comparison is exact), and
    /// written with a fresh generation (design 4(e)).
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let affected = sqlx::query(
                "UPDATE table_restores r SET status = 'FAILED', failure_reason = $4, \
                 completed_at = now(), owner_generation = $7 FROM tables t \
                 WHERE t.table_id = r.table_id AND r.table_id = $1 AND t.account_id = $2 \
                 AND t.table_name = $3 AND r.status = 'RUNNING' AND r.owner_generation = $5 \
                 AND EXTRACT(EPOCH FROM r.heartbeat_at)::FLOAT8 = $6",
            )
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .bind(reason)
            .bind(generation_column(owner_generation))
            .bind(heartbeat_at)
            .bind(generation_column(new_generation()))
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        Box::pin(async move {
            let table_name = input.table_name.clone();
            // Ownership first, before the target exists, on the lock the
            // legacy restore takes for a name (`restore_lock_key`), so a
            // legacy and a store-backed restore into one name exclude each
            // other too. A lock already held is another restore into this
            // name in flight: TableBeingRestored naming its source once its
            // CREATING row is visible, else the name is taken as CreateTable
            // would report it. The record's `heartbeat_at` is `now()` of the
            // registration transaction, its start (`create_table.rs`), so a
            // registration that waited on the backup's row lock or whose data
            // DDL took long returns a record already that old; the engine's
            // first beat refreshes it before the reply.
            let Some(session) =
                try_claim(&self.pool, &restore_lock_key(account_id, &table_name), None).await?
            else {
                return Err(self.name_taken_error(account_id, &table_name, true).await);
            };
            let generation = session.generation();
            let registration = RestoreRegistrationRow {
                owner_generation: generation,
                ..registration.clone()
            };
            let mut desc = match self
                .create_table_for_restore(account_id, input, RestoreSource::Manifest(&registration))
                .await
            {
                Ok(desc) => desc,
                Err(e) => {
                    session.close().await;
                    return Err(e);
                }
            };
            desc.restore_summary = Some(RestoreSummary {
                source_backup_arn: Some(registration.backup.backup_arn.clone()),
                source_table_arn: Some(registration.source_table_arn.clone()),
                restore_date_time: registration.restore_date_time,
                restore_in_progress: true,
            });
            let table_id = desc.table_id.clone();
            Ok((
                desc,
                Box::new(PostgresRestoreClaim {
                    session: Mutex::new(session),
                    generation,
                    table_id,
                    pool: self.pool.clone(),
                }) as Box<dyn JobClaim>,
            ))
        })
    }

    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The same advisory lock the owner took on the target name: a
            // session that still holds it is alive whatever its heartbeat
            // says, and a second taker is refused here.
            let Some(mut session) = try_claim(
                &self.pool,
                &restore_lock_key(&key.account_id, &key.table_name),
                None,
            )
            .await?
            else {
                return Ok(None);
            };
            let generation = session.generation();
            // The generation, the claimant's spool and a fresh heartbeat in
            // one write: a second taker, on this or another instance, finds
            // the record live at once (the trait doc says why a restore
            // record is refreshed where a backup row is not).
            let taken = sqlx::query(
                "UPDATE table_restores r SET owner_generation = $4, spool_host = $5, \
                 heartbeat_at = now() \
                 FROM tables t WHERE t.table_id = r.table_id AND r.table_id = $1 \
                 AND t.account_id = $2 AND t.table_name = $3 \
                 AND r.status = 'RUNNING' AND r.kind = 'manifest' \
                 AND r.heartbeat_at < now() - make_interval(secs => $6)",
            )
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .bind(generation_column(generation))
            .bind(spool_host)
            .bind(stale_after.as_secs_f64())
            .execute(session.connection())
            .await
            .map_err(db_err)?
            .rows_affected();
            if taken != 1 {
                session.close().await;
                return Ok(None);
            }
            Ok(Some(Box::new(PostgresRestoreClaim {
                session: Mutex::new(session),
                generation,
                table_id: key.table_id.clone(),
                pool: self.pool.clone(),
            }) as Box<dyn JobClaim>))
        })
    }

    fn read_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.table_id = $1 AND t.account_id = $2 AND t.table_name = $3"
            ))
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(restore_job_from_tuple).transpose()
        })
    }

    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE ($1::TEXT IS NULL OR r.status = $1) \
                 ORDER BY t.account_id, t.table_name"
            ))
            .bind(status.map(JobStatus::as_str))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            rows.into_iter().map(restore_job_from_tuple).collect()
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
            let Some(mut session) = try_claim_with(
                &self.pool,
                MAINTENANCE_LOCK_KEY,
                None,
                MAINTENANCE_TAKE_TRIES,
                MAINTENANCE_TAKE_PAUSE,
            )
            .await?
            else {
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

/// How many times `maintenance_claim` tries the exclusive lock (on its one
/// claim connection), and the pause between tries. `check_maintenance_claim` holds the key's shared lock for
/// one statement, and every CreateBackup and RestoreTableFromBackup runs one,
/// so a take that lands inside such a statement fails with no holder to
/// name; the tries span about 10 ms, past any such statement, so only a live
/// holder's session refuses the take.
const MAINTENANCE_TAKE_TRIES: u32 = 6;
const MAINTENANCE_TAKE_PAUSE: Duration = Duration::from_millis(2);

impl crate::PostgresEngine {
    /// The lock step of `maintenance_claim` (the tries on one claim
    /// connection) with `tries` and `pause` in place of the take's own, so a
    /// test can hold the key's shared lock past the first try and release it
    /// while the take still tries. `true` when the lock was taken (it is
    /// released again as the session closes).
    ///
    /// # Errors
    ///
    /// As `maintenance_claim`'s connection and lock errors.
    #[cfg(feature = "test-util")]
    pub async fn maintenance_lock_for_tests(
        &self,
        tries: u32,
        pause: Duration,
    ) -> Result<bool, StorageError> {
        Ok(
            try_claim_with(&self.pool, MAINTENANCE_LOCK_KEY, None, tries, pause)
                .await?
                .is_some(),
        )
    }
}

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

    /// Delete the row, release the advisory lock, then end the lock session.
    /// The unlock is explicit: `PgConnection::close` returns before the
    /// server backend exits and drops the session's locks, so a
    /// `check_maintenance_claim` on another connection right after the
    /// release would otherwise still find the lock held and answer held. A
    /// second release finds no session and does nothing.
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
            sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, $2))")
                .bind(MAINTENANCE_LOCK_KEY)
                .bind(crate::backup_engine::RESTORE_LOCK_SEED)
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
