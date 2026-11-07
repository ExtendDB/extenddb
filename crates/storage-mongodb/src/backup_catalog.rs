// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on MongoDB: the `backup_index` collection, the restore
//! job record (`restore_job`) on a restore target's `tables` document, the
//! `backup_maintenance` collection and the `catalog_id` settings document.
//!
//! The claim of a backup job is the row itself: it is born owned by an
//! `update_one` with `upsert: true`, a filter on `owner_generation: {$exists:
//! false}` and an update pipeline that sets the generation and `heartbeat_at:
//! "$$NOW"`, so no row ever exists unowned and a second claim of the same
//! backup trips the unique index on (`account_id`, `backup_id`). Every write
//! the job makes carries its generation in the filter; a heartbeat that
//! matches other than exactly one document means the claim is lost. The
//! heartbeat is also the liveness test: the reconciler takes over a CREATING
//! row whose heartbeat is older than the stale window on the server's clock
//! with one conditional update that sets a fresh generation and leaves
//! `heartbeat_at` as the dead owner wrote it.
//!
//! The lock on a row for DeleteBackup is a session transaction that `$inc`s
//! `lock_seq` on the row before it reads the status and the in-use rule, so
//! two such transactions on one row conflict and order wholly one before the
//! other; a restore's registration over a `backup_index` row takes the same
//! lock. Staleness is measured on the server's clock (`$$NOW`).
//!
//! A conditional write whose error is the driver's network class
//! (`ErrorKind::Io`, `ConnectionPoolCleared`) has an unknown outcome
//! (`WriteOutcome::Unknown`: the statement may have reached the server);
//! a server error (a command or write error) is `Err`, as a statement error
//! is on PostgreSQL.
//!
//! The maintenance claim is one document (`_id: "claim"`) in
//! `backup_maintenance`: a record whose heartbeat is older than the stale
//! window is taken over by a conditional update, a missing one is inserted by
//! an upsert whose filter a live record never matches (its insert then trips
//! the `_id` index and the claim is refused), and heartbeats and the release
//! are conditioned on the generation. `$expr` is not allowed in an upsert's
//! filter, which is why the takeover and the insert are two writes.

use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::{CreateTableInput, RestoreSummary, TableDescription};
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreRegistered,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity, StoreKind,
    TimeBounds, WriteOutcome, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mongodb::bson::{self, Bson, Document, doc};

use crate::MongoEngine;
use crate::backup_engine::{
    RestoreRecord, RestoreRegistration, age_is_stale, restoring_target_filter,
};
use crate::data_engine::{
    CommitOutcome, TRANSIENT_RETRY_ATTEMPTS, TxErr, backoff_sleep, is_duplicate_key,
};

/// The catalog collection of backup rows.
pub(crate) const BACKUP_INDEX: &str = "backup_index";
/// The catalog collection of the maintenance claim record.
pub(crate) const BACKUP_MAINTENANCE: &str = "backup_maintenance";
/// The settings document holding the catalog's random id.
pub(crate) const CATALOG_ID: &str = "catalog_id";
/// The counter a row-locking transaction `$inc`s so two of them conflict.
pub(crate) const LOCK_SEQ: &str = "lock_seq";

/// The restore job record, a subdocument of a restore target's `tables`
/// document, with the field names the job record has on every backend.
pub(crate) const RESTORE_JOB: &str = "restore_job";
pub(crate) const RESTORE_JOB_STATUS: &str = "restore_job.status";
pub(crate) const RESTORE_JOB_KIND: &str = "restore_job.kind";
pub(crate) const RESTORE_JOB_GENERATION: &str = "restore_job.owner_generation";
pub(crate) const RESTORE_JOB_HEARTBEAT: &str = "restore_job.heartbeat_at";
pub(crate) const RESTORE_JOB_FAILURE_REASON: &str = "restore_job.failure_reason";
pub(crate) const RESTORE_JOB_COMPLETED_AT: &str = "restore_job.completed_at";
pub(crate) const RESTORE_JOB_SPOOL_HOST: &str = "restore_job.spool_host";
pub(crate) const RESTORE_JOB_FILE_INDEX: &str = "restore_job.file_index";
/// Items of `file_index` applied so far, written with each batch's inserts
/// in one transaction (`restore_engine.rs`).
pub(crate) const RESTORE_JOB_ITEMS_APPLIED: &str = "restore_job.items_applied";
/// Whether the registration's DDL finished: `false` from the record's
/// insert, `true` once the target's collections and index metadata exist;
/// the reconciler's resume refuses a record left `false`.
pub(crate) const RESTORE_JOB_DDL_DONE: &str = "restore_job.ddl_done";

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn not_found(backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_not_found(backup_arn)
}

/// The error for a backup a CREATING restore target still reads: the shared
/// text, [`extenddb_storage::backup_catalog::backup_in_use`].
pub(crate) fn backup_in_use(table: &str, backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_in_use(table, backup_arn)
}

/// An `owner_generation` as the catalog stores it (a 64-bit integer);
/// generations are minted below `i64::MAX`.
pub(crate) fn generation_field(generation: u64) -> i64 {
    i64::try_from(generation).unwrap_or(i64::MAX)
}

fn generation_from_field(doc: &Document, key: &str) -> u64 {
    match doc.get(key) {
        Some(Bson::Int64(g)) => u64::try_from(*g).unwrap_or_default(),
        Some(Bson::Int32(g)) => u64::try_from(*g).unwrap_or_default(),
        Some(Bson::Double(g)) => {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let g = *g as u64;
            g
        }
        _ => 0,
    }
}

#[allow(clippy::cast_precision_loss)]
fn datetime_epoch_secs(dt: bson::DateTime) -> f64 {
    dt.timestamp_millis() as f64 / 1000.0
}

#[allow(clippy::cast_possible_truncation)]
fn epoch_secs_datetime(secs: f64) -> bson::DateTime {
    bson::DateTime::from_millis((secs * 1000.0).round() as i64)
}

fn optional_datetime(doc: &Document, key: &str) -> Option<f64> {
    doc.get_datetime(key)
        .ok()
        .map(|dt| datetime_epoch_secs(*dt))
}

fn required_str(doc: &Document, key: &str) -> Result<String, StorageError> {
    doc.get_str(key)
        .map(str::to_owned)
        .map_err(|_| StorageError::Internal(format!("backup row is missing {key}")))
}

/// Whether a write's outcome cannot be told from its error: the statement
/// may have reached the server before the connection failed.
fn outcome_unknown(e: &mongodb::error::Error) -> bool {
    matches!(
        *e.kind,
        mongodb::error::ErrorKind::Io(_) | mongodb::error::ErrorKind::ConnectionPoolCleared { .. }
    )
}

/// One `backup_index` document as a [`BackupRow`].
fn row_from_doc(doc: &Document) -> Result<BackupRow, StorageError> {
    let backup_arn = required_str(doc, "backup_arn")?;
    let store_kind = required_str(doc, "store_kind")?;
    let store_kind = StoreKind::parse(&store_kind).ok_or_else(|| {
        StorageError::Internal(format!(
            "backup {backup_arn} carries an unknown store kind {store_kind:?}"
        ))
    })?;
    let source_table_details_json: serde_json::Value = doc
        .get("source_table_details_json")
        .cloned()
        .map(bson::from_bson)
        .transpose()
        .map_err(|e| StorageError::Internal(format!("parse source_table_details_json: {e}")))?
        .ok_or_else(|| {
            StorageError::Internal(format!(
                "backup {backup_arn} is missing source_table_details_json"
            ))
        })?;
    Ok(BackupRow {
        backup_id: required_str(doc, "backup_id")?,
        account_id: required_str(doc, "account_id")?,
        table_name: required_str(doc, "table_name")?,
        backup_arn,
        backup_name: required_str(doc, "backup_name")?,
        status: BackupStatus::parse(&required_str(doc, "status")?)?,
        store: StoreIdentity {
            store_kind,
            store_root: required_str(doc, "store_root")?,
            store_id: required_str(doc, "store_id")?,
        },
        size_bytes: doc.get_i64("size_bytes").unwrap_or(0),
        item_count: doc.get_i64("item_count").unwrap_or(0),
        created_at: optional_datetime(doc, "created_at").unwrap_or(0.0),
        completed_at: optional_datetime(doc, "completed_at"),
        failure_reason: doc.get_str("failure_reason").ok().map(str::to_owned),
        source_table_details_json,
        owner_generation: generation_from_field(doc, "owner_generation"),
        heartbeat_at: optional_datetime(doc, "heartbeat_at").unwrap_or(0.0),
    })
}

/// The fields of `row` the claim's insert sets, status CREATING; the
/// generation and the heartbeat are added by the caller.
fn row_fields(row: &BackupRow) -> Result<Document, StorageError> {
    let source = bson::to_bson(&row.source_table_details_json)
        .map_err(|e| StorageError::Internal(format!("serialize source_table_details_json: {e}")))?;
    Ok(doc! {
        "table_name": &row.table_name,
        "backup_arn": &row.backup_arn,
        "backup_name": &row.backup_name,
        "status": BackupStatus::Creating.as_str(),
        "store_kind": row.store.store_kind.as_str(),
        "store_root": &row.store.store_root,
        "store_id": &row.store.store_id,
        "size_bytes": row.size_bytes,
        "item_count": row.item_count,
        "created_at": epoch_secs_datetime(row.created_at),
        "source_table_details_json": source,
        LOCK_SEQ: 0_i64,
    })
}

/// The filter for the row `key` names.
fn key_filter(key: &BackupKey) -> Document {
    doc! { "account_id": &key.account_id, "backup_id": &key.backup_id }
}

/// `set` as the `$set` stage of a pipeline update: `$$NOW` stays the server's
/// clock and every other value is wrapped in `$literal`, so a string, or a
/// key inside the source table object, is never read as a field path.
pub(crate) fn literal_set(set: Document) -> Document {
    set.into_iter()
        .map(|(k, v)| {
            let v = match v {
                Bson::String(s) if s == "$$NOW" => Bson::String(s),
                other => doc! { "$literal": other }.into(),
            };
            (k, v)
        })
        .collect()
}

/// The filter for the rows of `store`.
fn store_filter(store: &StoreIdentity) -> Document {
    doc! {
        "store_kind": store.store_kind.as_str(),
        "store_root": &store.store_root,
        "store_id": &store.store_id,
    }
}

/// The `$expr` of [`BackupCatalog::list_backup_rows_older_than`]: the
/// `clock` column older than `age` on the server's clock. `age_is_stale`
/// reads a missing column as the epoch, which is the rule the dead-job
/// listing wants and not this one: a row with `completed_at` unset has not
/// reached its terminal status's clock, so the column must also be present.
fn older_than_filter(clock: StaleClock, age: Duration) -> Document {
    let column = match clock {
        StaleClock::Heartbeat => "heartbeat_at",
        StaleClock::Completed => "completed_at",
    };
    doc! {
        column: { "$type": "date" },
        "$expr": age_is_stale(&[column], age),
    }
}

/// The sort every listing applies: `(created_at, backup_arn)`.
fn listing_sort() -> Document {
    doc! { "created_at": 1, "backup_arn": 1 }
}

/// The rows `read` fetches, as a stream: the read runs once, on the first
/// poll, and a failed read is one `Err` item.
fn rows_stream<'a, F>(read: F) -> BoxStream<'a, Result<BackupRow, StorageError>>
where
    F: std::future::Future<Output = Result<Vec<Document>, StorageError>> + Send + 'a,
{
    use futures::StreamExt;
    Box::pin(
        futures::stream::once(async move {
            match read.await {
                Ok(docs) => docs.iter().map(row_from_doc).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        })
        .flat_map(futures::stream::iter),
    )
}

/// The filter of the restore job records the reconciler acts on: a RUNNING
/// record whose heartbeat is older than `stale_after` on the server's clock
/// (a dead owner), and a FAILED record whose table is still a CREATING
/// restore target once `completed_at` (or, where none was stamped, the
/// heartbeat) is that old: the FAILED write landed and the removal did not,
/// and the delay leaves a task still on its own failure path to finish it.
pub(crate) fn dead_restore_job_filter(stale_after: Duration) -> Document {
    doc! {
        "$or": [
            {
                RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
                "$expr": age_is_stale(&[RESTORE_JOB_HEARTBEAT], stale_after),
            },
            {
                RESTORE_JOB_STATUS: JobStatus::Failed.as_str(),
                "table_status": "CREATING",
                "$expr": age_is_stale(
                    &[RESTORE_JOB_COMPLETED_AT, RESTORE_JOB_HEARTBEAT],
                    stale_after,
                ),
            },
        ]
    }
}

/// A backup job's handle on its `backup_index` document.
struct MongoBackupClaim {
    index: mongodb::Collection<Document>,
    key: BackupKey,
    generation: u64,
}

impl MongoBackupClaim {
    /// The filter of the row while it carries this claim.
    fn owned_filter(&self) -> Document {
        let mut filter = key_filter(&self.key);
        filter.insert("owner_generation", generation_field(self.generation));
        filter
    }
}

impl JobClaim for MongoBackupClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let result = self
                .index
                .update_one(
                    self.owned_filter(),
                    vec![doc! { "$set": { "heartbeat_at": "$$NOW" } }],
                )
                .await
                .map_err(|e| StorageError::Internal(format!("backup heartbeat: {e}")))?;
            Ok(if result.matched_count == 1 {
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
            let mut filter = self.owned_filter();
            filter.insert("status", from.as_str());
            let mut set = doc! { "status": to.as_str() };
            if let Some(size) = fields.size_bytes {
                set.insert("size_bytes", size);
            }
            if let Some(count) = fields.item_count {
                set.insert("item_count", count);
            }
            if let Some(reason) = &fields.failure_reason {
                set.insert("failure_reason", reason.as_str());
            }
            if let Some(source) = &fields.source_table_details_json {
                let source = bson::to_bson(source).map_err(|e| {
                    StorageError::Internal(format!("serialize source_table_details_json: {e}"))
                })?;
                set.insert("source_table_details_json", source);
            }
            if fields.completed {
                set.insert("completed_at", "$$NOW");
            }
            // A pipeline update, so `$$NOW` is the server's clock; every other
            // value is wrapped in `$literal` so the pipeline does not read a
            // string, or a key of the source table object, as a field path.
            let set = literal_set(set);
            match self
                .index
                .update_one(filter, vec![doc! { "$set": set }])
                .await
            {
                Ok(result) if result.matched_count == 1 => Ok(WriteOutcome::Matched),
                Ok(_) => Ok(WriteOutcome::NotMatched),
                Err(e) if outcome_unknown(&e) => Ok(WriteOutcome::Unknown),
                Err(e) => Err(internal(e)),
            }
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let doc = self
                .index
                .find_one(key_filter(&self.key))
                .await
                .map_err(internal)?
                .ok_or_else(|| not_found(&self.key.backup_arn))?;
            Ok(JobRow {
                status: JobStatus::parse(&required_str(&doc, "status")?)?,
                owner_generation: generation_from_field(&doc, "owner_generation"),
                heartbeat_at: optional_datetime(&doc, "heartbeat_at").unwrap_or(0.0),
                failure_reason: doc.get_str("failure_reason").ok().map(str::to_owned),
            })
        })
    }
}

/// The filter of the `tables` document `key` names: the `_id` and the table
/// id, so a table re-created under the name is never the one addressed.
fn target_filter(key: &RestoreTargetKey) -> Document {
    doc! {
        "_id": { "account_id": &key.account_id, "table_name": &key.table_name },
        "table_id": &key.table_id,
    }
}

/// One restore target's `tables` document as a [`RestoreJobRow`]: the key,
/// the provenance and every member of the `restore_job` subdocument; the
/// store is read when all three of its fields are there.
pub(crate) fn restore_job_from_doc(doc: &Document) -> Result<RestoreJobRow, StorageError> {
    let id = doc
        .get_document("_id")
        .map_err(|_| StorageError::Internal("table is missing _id".to_owned()))?;
    let job = doc.get_document(RESTORE_JOB).map_err(|_| {
        StorageError::Internal("restore target is missing its job record".to_owned())
    })?;
    let source_backup_arn = required_str(doc, "restore_source_backup_arn")?;
    let store = match (
        job.get_str("store_kind"),
        job.get_str("store_root"),
        job.get_str("store_id"),
    ) {
        (Ok(kind), Ok(store_root), Ok(store_id)) => Some(StoreIdentity {
            store_kind: StoreKind::parse(kind).ok_or_else(|| {
                StorageError::Internal(format!(
                    "restore of {source_backup_arn} carries an unknown store kind {kind:?}"
                ))
            })?,
            store_root: store_root.to_owned(),
            store_id: store_id.to_owned(),
        }),
        _ => None,
    };
    Ok(RestoreJobRow {
        target: RestoreTargetKey {
            account_id: required_str(id, "account_id")?,
            table_name: required_str(id, "table_name")?,
            table_id: required_str(doc, "table_id")?,
        },
        source_backup_arn,
        kind: RestoreKind::parse(&required_str(job, "kind")?)?,
        status: JobStatus::parse(&required_str(job, "status")?)?,
        owner_generation: generation_from_field(job, "owner_generation"),
        heartbeat_at: optional_datetime(job, "heartbeat_at").unwrap_or(0.0),
        store,
        spool_host: job.get_str("spool_host").ok().map(str::to_owned),
        file_index: match job.get("file_index") {
            Some(Bson::Int32(i)) => u32::try_from(*i).ok(),
            Some(Bson::Int64(i)) => u32::try_from(*i).ok(),
            _ => None,
        },
        items_applied: match job.get("items_applied") {
            Some(Bson::Int32(i)) => u64::try_from(*i).ok(),
            Some(Bson::Int64(i)) => u64::try_from(*i).ok(),
            _ => None,
        },
        failure_reason: job.get_str("failure_reason").ok().map(str::to_owned),
        completed_at: optional_datetime(job, "completed_at"),
    })
}

/// A store-backed restore job's handle on the job record of its target's
/// `tables` document: every write carries the target's key, its table id
/// and the generation, and the heartbeat and the status writes also require
/// the record RUNNING (or the `from` status).
struct MongoRestoreClaim {
    tables: mongodb::Collection<Document>,
    key: RestoreTargetKey,
    generation: u64,
}

impl MongoRestoreClaim {
    /// The filter of the record while it is in `status` under this claim.
    fn owned_filter(&self, status: JobStatus) -> Document {
        let mut filter = target_filter(&self.key);
        filter.insert(RESTORE_JOB_STATUS, status.as_str());
        filter.insert(RESTORE_JOB_GENERATION, generation_field(self.generation));
        filter
    }
}

impl JobClaim for MongoRestoreClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let result = self
                .tables
                .update_one(
                    self.owned_filter(JobStatus::Running),
                    vec![doc! { "$set": { RESTORE_JOB_HEARTBEAT: "$$NOW" } }],
                )
                .await
                .map_err(|e| StorageError::Internal(format!("restore heartbeat: {e}")))?;
            Ok(if result.matched_count == 1 {
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
            let mut set = doc! { RESTORE_JOB_STATUS: to.as_str() };
            if let Some(reason) = &fields.failure_reason {
                set.insert(RESTORE_JOB_FAILURE_REASON, reason.as_str());
            }
            if fields.completed {
                set.insert(RESTORE_JOB_COMPLETED_AT, "$$NOW");
            }
            let set = literal_set(set);
            match self
                .tables
                .update_one(self.owned_filter(from), vec![doc! { "$set": set }])
                .await
            {
                Ok(result) if result.matched_count == 1 => Ok(WriteOutcome::Matched),
                Ok(_) => Ok(WriteOutcome::NotMatched),
                Err(e) if outcome_unknown(&e) => Ok(WriteOutcome::Unknown),
                Err(e) => Err(internal(e)),
            }
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let doc = self
                .tables
                .find_one(target_filter(&self.key))
                .await
                .map_err(internal)?
                .ok_or_else(|| {
                    StorageError::TableNotFound(format!(
                        "restore target {} (table id {}) has no catalog document",
                        self.key.table_name, self.key.table_id
                    ))
                })?;
            let row = restore_job_from_doc(&doc)?;
            Ok(JobRow {
                status: row.status,
                owner_generation: row.owner_generation,
                heartbeat_at: row.heartbeat_at,
                failure_reason: row.failure_reason,
            })
        })
    }
}

impl MongoEngine {
    pub(crate) fn backup_index(&self) -> mongodb::Collection<Document> {
        self.catalog_db.collection::<Document>(BACKUP_INDEX)
    }

    fn tables(&self) -> mongodb::Collection<Document> {
        self.catalog_db.collection::<Document>("tables")
    }

    /// The name of a CREATING restore target registered from `backup_arn`,
    /// if any: the one in-use rule DeleteBackup, a restore's registration and
    /// `backup_in_use` share. Reads the target's status, not the job's. The
    /// driver's error is returned as it is, so a read inside a transaction
    /// keeps its transient label for the caller's retry.
    pub(crate) async fn creating_restore_target(
        &self,
        account_id: &str,
        backup_arn: &str,
        session: Option<&mut mongodb::ClientSession>,
    ) -> Result<Option<String>, mongodb::error::Error> {
        let filter = restoring_target_filter(account_id, backup_arn);
        let tables = self.tables();
        let find = tables.find_one(filter);
        let target = match session {
            Some(session) => find.session(session).await,
            None => find.await,
        }?;
        Ok(target.map(|t| {
            t.get_document("_id")
                .ok()
                .and_then(|id| id.get_str("table_name").ok())
                .unwrap_or("?")
                .to_owned()
        }))
    }

    /// DeleteBackup's first step as one session transaction: `$inc` the
    /// row's `lock_seq` (the lock), read the status, run the in-use rule,
    /// write DELETING. A transaction that lost a write conflict is re-run.
    async fn start_backup_delete_tx(&self, key: &BackupKey) -> Result<DeleteStart, StorageError> {
        let index = self.backup_index();
        let mut session = self.client.start_session().await.map_err(internal)?;
        let tx_options = self.write_transaction_options();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(internal)?;
            let body: Result<DeleteStart, TxErr> = async {
                let locked = index
                    .find_one_and_update(key_filter(key), doc! { "$inc": { LOCK_SEQ: 1_i64 } })
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?
                    .ok_or_else(|| TxErr::Fatal(not_found(&key.backup_arn)))?;
                let status = BackupStatus::parse(&required_str(&locked, "status")?)?;
                if status != BackupStatus::Available {
                    return Ok(DeleteStart::Status(status));
                }
                if let Some(table) = self
                    .creating_restore_target(&key.account_id, &key.backup_arn, Some(&mut session))
                    .await
                    .map_err(TxErr::from)?
                {
                    return Ok(DeleteStart::InUse(table));
                }
                let mut filter = key_filter(key);
                filter.insert("status", BackupStatus::Available.as_str());
                index
                    .update_one(
                        filter,
                        doc! { "$set": { "status": BackupStatus::Deleting.as_str() } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                Ok(DeleteStart::Deleting)
            }
            .await;
            match body {
                Ok(outcome) => match self.commit_write_transaction(&mut session, None).await? {
                    CommitOutcome::Committed => return Ok(outcome),
                    CommitOutcome::RerunBody(_) => backoff_sleep(attempt).await,
                },
                Err(TxErr::Transient(_)) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }
        Err(StorageError::Internal(format!(
            "DeleteBackup of {} did not commit after {TRANSIENT_RETRY_ATTEMPTS} conflicting \
             attempts",
            key.backup_arn
        )))
    }

    /// A store-backed restore's registration (the design's 5.3 step 4) as
    /// one session transaction: `$inc` the `backup_index` row's `lock_seq`
    /// while it is AVAILABLE (the lock `start_backup_delete` takes, so the
    /// two order wholly one before the other), run the in-use rule, insert
    /// the CREATING target with its provenance and its RUNNING `kind =
    /// manifest` record under a fresh generation, the store and the
    /// claimant's spool, with the server's clock on its heartbeat. The
    /// transaction commits right after the insert, before the target's
    /// collections and indexes are created; one that lost a write conflict
    /// (a DeleteBackup or another registration of the backup) is re-run and
    /// sees the other's result.
    ///
    /// Refusals: a missing or non-AVAILABLE row is `Backup not found`; a
    /// CREATING target of the backup is `BackupInUse` naming it; a taken
    /// name is what `create_table_for_restore` reports (`TableAlreadyExists`,
    /// or `TableBeingRestored` when the name is a CREATING restore target).
    async fn claim_restore_tx(
        &self,
        account_id: &str,
        input: CreateTableInput,
        registration: &RestoreRegistrationRow,
    ) -> Result<RestoreRegistered, StorageError> {
        let target_table_name = input.table_name.clone();
        let backup_arn = registration.backup.backup_arn.as_str();
        let index = self.backup_index();
        let mut session = self.client.start_session().await.map_err(internal)?;
        let tx_options = self.write_transaction_options();
        let generation = new_generation();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(internal)?;
            let body: Result<Result<TableDescription, StorageError>, TxErr> = async {
                let mut lock = key_filter(&registration.backup);
                lock.insert("status", BackupStatus::Available.as_str());
                let locked = index
                    .find_one_and_update(lock, doc! { "$inc": { LOCK_SEQ: 1_i64 } })
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                if locked.is_none() {
                    return Ok(Err(not_found(backup_arn)));
                }
                if let Some(restoring) = self
                    .creating_restore_target(account_id, backup_arn, Some(&mut session))
                    .await
                    .map_err(TxErr::from)?
                {
                    return Ok(Err(backup_in_use(&restoring, backup_arn)));
                }
                let mut target = RestoreRegistration {
                    backup_arn,
                    source_table_arn: &registration.source_table_arn,
                    restore_date_time: epoch_secs_datetime(registration.restore_date_time),
                    owner_generation: generation,
                    // This process's clock, replaced by the server's inside
                    // the transaction (`stamp_server_clock`); test builds set
                    // it behind to prove the replacement.
                    first_heartbeat: self.local_now(),
                    record: RestoreRecord::Manifest {
                        store: &registration.store,
                        spool_host: registration.spool_host.as_deref(),
                    },
                    session: Some(&mut session),
                };
                match self
                    .create_table_for_restore(account_id, input.clone(), &mut target)
                    .await
                {
                    Ok(desc) => Ok(Ok(desc)),
                    Err(StorageError::Transient(_)) => Err(TxErr::Transient),
                    Err(e) => {
                        // A creation that failed after its insert committed
                        // (a collection or index could not be created) left a
                        // registered target under this generation; a name
                        // refusal left nothing, and the removal matches
                        // nothing.
                        if let Err(cleanup) = self
                            .remove_target_of_generation(account_id, &target_table_name, generation)
                            .await
                        {
                            tracing::error!(
                                "restore of {backup_arn} into {target_table_name} failed during \
                                 target creation ({e}), and cleanup failed: {cleanup}"
                            );
                        }
                        Ok(Err(e))
                    }
                }
            }
            .await;
            match body {
                // The insert committed its transaction before the target's
                // collections were created; nothing is left to commit.
                Ok(Ok(mut desc)) => {
                    desc.restore_summary = Some(RestoreSummary {
                        source_backup_arn: Some(backup_arn.to_owned()),
                        source_table_arn: Some(registration.source_table_arn.clone()),
                        restore_date_time: registration.restore_date_time,
                        restore_in_progress: true,
                    });
                    let key = RestoreTargetKey {
                        account_id: account_id.to_owned(),
                        table_name: target_table_name,
                        table_id: desc.table_id.clone(),
                    };
                    return Ok((
                        desc,
                        Box::new(MongoRestoreClaim {
                            tables: self.tables(),
                            key,
                            generation,
                        }) as Box<dyn JobClaim>,
                    ));
                }
                Ok(Err(refusal)) => {
                    let _ = session.abort_transaction().await;
                    return Err(refusal);
                }
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }
        Err(StorageError::Internal(format!(
            "the registration of the restore of {backup_arn} into {target_table_name} did not \
             commit after {TRANSIENT_RETRY_ATTEMPTS} conflicting attempts"
        )))
    }
}

impl BackupCatalog for MongoEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let id = self
                .catalog_db
                .collection::<Document>("settings")
                .find_one(doc! { "_id": CATALOG_ID })
                .await
                .map_err(internal)?
                .and_then(|d| d.get_str("value").ok().map(str::to_owned));
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
            let generation = new_generation();
            let mut filter = key_filter(&row.key());
            filter.insert("owner_generation", doc! { "$exists": false });
            // The row is the claim. The upsert inserts it owned, with the
            // server's clock on the heartbeat; an existing row never matches
            // the filter (every row carries a generation), so the upsert's
            // insert hits the unique index and the claim is refused.
            let mut set = row_fields(row)?;
            set.insert("owner_generation", generation_field(generation));
            set.insert("heartbeat_at", "$$NOW");
            let set = literal_set(set);
            match self
                .backup_index()
                .update_one(filter, vec![doc! { "$set": set }])
                .upsert(true)
                .await
            {
                Ok(result) if result.upserted_id.is_some() => {}
                Ok(_) => {
                    return Err(StorageError::AlreadyExists(format!(
                        "backup {} already has a row",
                        row.backup_arn
                    )));
                }
                Err(e) if is_duplicate_key(&e) => {
                    return Err(StorageError::AlreadyExists(format!(
                        "a live job owns backup {}",
                        row.backup_arn
                    )));
                }
                Err(e) => return Err(internal(e)),
            }
            Ok(Box::new(MongoBackupClaim {
                index: self.backup_index(),
                key: row.key(),
                generation,
            }) as Box<dyn JobClaim>)
        })
    }

    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            let mut filter = store_filter(store);
            if let Some(status) = status {
                filter.insert("status", status.as_str());
            }
            self.backup_index()
                .find(filter)
                .sort(listing_sort())
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)
        })
    }

    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let mut filter = store_filter(store);
            filter.insert("status", BackupStatus::Available.as_str());
            self.backup_index()
                .find_one(filter)
                .sort(doc! { "created_at": -1, "backup_arn": -1 })
                .await
                .map_err(internal)?
                .as_ref()
                .map(row_from_doc)
                .transpose()
        })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            let mut filter = doc! { "account_id": account_id };
            if let Some(table_name) = table_name {
                filter.insert("table_name", table_name);
            }
            let mut created = Document::new();
            if let Some(lower) = bounds.lower {
                created.insert("$gte", epoch_secs_datetime(lower));
            }
            if let Some(upper) = bounds.upper {
                created.insert("$lt", epoch_secs_datetime(upper));
            }
            if !created.is_empty() {
                filter.insert("created_at", created);
            }
            self.backup_index()
                .find(filter)
                .sort(listing_sort())
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)
        })
    }

    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let doc = self
                .backup_index()
                .find_one(doc! {
                    "account_id": arn.account_id(),
                    "backup_id": arn.backup_id(),
                    "table_name": arn.table_name(),
                    "backup_arn": arn.to_arn_string(),
                })
                .await
                .map_err(internal)?;
            doc.as_ref().map(row_from_doc).transpose()
        })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        Box::pin(self.start_backup_delete_tx(key))
    }

    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let mut filter = key_filter(key);
            filter.insert("status", BackupStatus::Deleting.as_str());
            let result = self
                .backup_index()
                .update_one(
                    filter,
                    vec![doc! { "$set": {
                        "status": { "$literal": BackupStatus::Deleted.as_str() },
                        "completed_at": "$$NOW",
                    } }],
                )
                .await
                .map_err(internal)?;
            Ok(result.matched_count == 1)
        })
    }

    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let mut filter = key_filter(key);
            filter.insert(
                "status",
                doc! { "$in": [BackupStatus::Deleted.as_str(), BackupStatus::Failed.as_str()] },
            );
            let result = self
                .backup_index()
                .delete_one(filter)
                .await
                .map_err(internal)?;
            Ok(result.deleted_count == 1)
        })
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let mut filter = key_filter(key);
            filter.insert("status", BackupStatus::Creating.as_str());
            filter.insert("$expr", age_is_stale(&["heartbeat_at"], stale_after));
            let result = self
                .backup_index()
                .delete_one(filter)
                .await
                .map_err(internal)?;
            Ok(result.deleted_count == 1)
        })
    }

    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The heartbeat is the liveness test: a live owner refreshes it
            // every period, so a CREATING row whose heartbeat is older than
            // the window on the server's clock has no owner, and the one
            // write below both tests that and takes the row. The filter
            // binds no generation, so the write takes a row whatever
            // generation it carries; `heartbeat_at` is left as the dead
            // owner wrote it. A second taker before this one resolves the
            // row (another process's reconciler; one process runs one
            // reconciler) finds the same stale heartbeat and takes the row
            // again under its own generation, which fences this claim's
            // status write as any takeover fences the dead owner's.
            let generation = new_generation();
            let mut filter = key_filter(key);
            filter.insert("status", BackupStatus::Creating.as_str());
            filter.insert("$expr", age_is_stale(&["heartbeat_at"], stale_after));
            let update = vec![doc! { "$set": literal_set(doc! {
                "owner_generation": generation_field(generation),
            }) }];
            let taken = self
                .backup_index()
                .find_one_and_update(filter, update)
                .await
                .map_err(internal)?;
            Ok(taken.map(|_| {
                Box::new(MongoBackupClaim {
                    index: self.backup_index(),
                    key: key.clone(),
                    generation,
                }) as Box<dyn JobClaim>
            }))
        })
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: BackupStatus,
        store: &'a StoreIdentity,
        clock: StaleClock,
        age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            let mut filter = store_filter(store);
            filter.insert("status", status.as_str());
            filter.extend(older_than_filter(clock, age));
            self.backup_index()
                .find(filter)
                .sort(listing_sort())
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)
        })
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move {
            self.creating_restore_target(&key.account_id, &key.backup_arn, None)
                .await
                .map_err(internal)
        })
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The key must name a CREATING restore target with this table id;
            // a table re-created under the name has another id and is not
            // touched. The delete is one atomic step on the document, so it
            // and the ACTIVE flip (conditioned on CREATING) exclude each
            // other. The catalog document goes first: once it is gone no
            // provenance keeps the source backup in use, whatever the
            // collection drops do, and the target counts as removed: a drop
            // that fails then is logged, naming the collections, and leaves
            // them for `backup sync` (no record remains for a later cycle to
            // retry from).
            let removed = self
                .tables()
                .find_one_and_delete(doc! {
                    "_id": { "account_id": &key.account_id, "table_name": &key.table_name },
                    "table_id": &key.table_id,
                    "table_status": "CREATING",
                    "restore_source_backup_arn": { "$exists": true },
                })
                .await
                .map_err(internal)?;
            if removed.is_none() {
                return Ok(false);
            }
            let dropped = self.remove_target_collections(&key.table_id).await;
            #[cfg(test)]
            let dropped = if self.backup_liveness.target_drop_fails {
                Err(StorageError::Internal(
                    "test: the reply to the target's collection drops was lost".to_owned(),
                ))
            } else {
                dropped
            };
            if let Err(e) = dropped {
                tracing::error!(
                    "the restore target {} (account {}) was removed from the catalog, but \
                     dropping its data collection {} or the index collections of table id {} \
                     failed; they are left in the data database for `backup sync` to sweep: {e}",
                    key.table_name,
                    key.account_id,
                    crate::data::data_collection_name(&key.table_id),
                    key.table_id
                );
            }
            Ok(true)
        })
    }

    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let docs: Vec<Document> = self
                .tables()
                .find(dead_restore_job_filter(stale_after))
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)?;
            // A document this build cannot read (a kind or status no release
            // writes) is logged and skipped, as ListBackups skips a row it
            // cannot read, so one such document does not stop the cycle.
            Ok(docs
                .iter()
                .filter_map(|doc| match restore_job_from_doc(doc) {
                    Ok(row) => Some(row),
                    Err(e) => {
                        tracing::warn!(
                            "reconciler: restore job record {:?} skipped: {e}",
                            doc.get("_id")
                        );
                        None
                    }
                })
                .collect())
        })
    }

    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        Box::pin(self.claim_restore_tx(account_id, input, registration))
    }

    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The heartbeat is the liveness test, as for a backup row: a
            // RUNNING manifest record whose heartbeat is older than the window
            // on the server's clock has no owner, and the one write below
            // tests that and takes the record under a fresh generation with
            // the claimant's spool and `heartbeat_at` at the server's clock,
            // so a second taker (another instance's reconciler in the period
            // before the successor's first beat) finds it live; the trait doc
            // says why a restore record is refreshed where a backup row is
            // not. A legacy record is never taken over (the reconciler fails
            // it), and a record that is not RUNNING is its owner's or
            // finished.
            let generation = new_generation();
            let mut filter = target_filter(key);
            filter.insert(RESTORE_JOB_STATUS, JobStatus::Running.as_str());
            filter.insert(RESTORE_JOB_KIND, RestoreKind::Manifest.as_str());
            filter.insert("$expr", age_is_stale(&[RESTORE_JOB_HEARTBEAT], stale_after));
            let mut update = vec![doc! { "$set": literal_set(doc! {
                RESTORE_JOB_GENERATION: generation_field(generation),
                RESTORE_JOB_HEARTBEAT: "$$NOW",
            }) }];
            match spool_host {
                Some(host) => update.push(doc! { "$set": literal_set(doc! {
                    RESTORE_JOB_SPOOL_HOST: host,
                }) }),
                None => update.push(doc! { "$unset": RESTORE_JOB_SPOOL_HOST }),
            }
            let taken = self
                .tables()
                .find_one_and_update(filter, update)
                .await
                .map_err(internal)?;
            Ok(taken.map(|_| {
                Box::new(MongoRestoreClaim {
                    tables: self.tables(),
                    key: key.clone(),
                    generation,
                }) as Box<dyn JobClaim>
            }))
        })
    }

    fn read_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let mut filter = target_filter(key);
            filter.insert(RESTORE_JOB, doc! { "$exists": true });
            let doc = self.tables().find_one(filter).await.map_err(internal)?;
            doc.as_ref().map(restore_job_from_doc).transpose()
        })
    }

    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let mut filter = doc! { RESTORE_JOB: { "$exists": true } };
            if let Some(status) = status {
                filter.insert(RESTORE_JOB_STATUS, status.as_str());
            }
            let docs: Vec<Document> = self
                .tables()
                .find(filter)
                .sort(doc! { "_id": 1 })
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)?;
            docs.iter().map(restore_job_from_doc).collect()
        })
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // By status alone: a record stamped in this read's instant, or
            // ahead of the server's clock, is RUNNING too.
            let docs: Vec<Document> = self
                .tables()
                .find(doc! { RESTORE_JOB_STATUS: JobStatus::Running.as_str() })
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)?;
            docs.iter().map(restore_job_from_doc).collect()
        })
    }

    /// Conditioned on RUNNING, on the listed `owner_generation` and on the
    /// listed `heartbeat_at` (a `Date`, millisecond precision, rebuilt from
    /// the listed epoch seconds exactly), and written with a fresh generation
    /// (design 4(e)).
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let result = self
                .tables()
                .update_one(
                    doc! {
                        "_id": { "account_id": &key.account_id, "table_name": &key.table_name },
                        "table_id": &key.table_id,
                        RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
                        RESTORE_JOB_GENERATION: generation_field(owner_generation),
                        RESTORE_JOB_HEARTBEAT: epoch_secs_datetime(heartbeat_at),
                    },
                    vec![doc! { "$set": {
                        RESTORE_JOB_STATUS: { "$literal": JobStatus::Failed.as_str() },
                        RESTORE_JOB_FAILURE_REASON: { "$literal": reason },
                        RESTORE_JOB_COMPLETED_AT: "$$NOW",
                        RESTORE_JOB_GENERATION: { "$literal": generation_field(new_generation()) },
                    } }],
                )
                .await
                .map_err(internal)?;
            Ok(result.matched_count == 1)
        })
    }

    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            let ids = self
                .backup_index()
                .distinct(
                    "store_id",
                    doc! { "store_kind": store_kind.as_str(), "store_root": store_root },
                )
                .await
                .map_err(internal)?;
            let mut ids: Vec<String> = ids
                .into_iter()
                .filter_map(|v| match v {
                    Bson::String(s) => Some(s),
                    _ => None,
                })
                .collect();
            ids.sort();
            Ok(ids)
        })
    }

    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        Box::pin(async move {
            // The record is the claim, as a backup row is: one document under
            // a fixed `_id`. Two writes: the first takes over a record whose
            // heartbeat is stale (a dead holder's), the second inserts the
            // record when there is none. A live record matches neither
            // filter, so the second write's upsert trips the `_id` index and
            // the claim is refused; two commands racing for a stale record
            // both run the first write, and the one whose filter no longer
            // matches falls to the second and is refused the same way.
            // (`$expr` is not allowed in an upsert's filter, which is why
            // the two are not one write.)
            let generation = new_generation();
            let collection = self.maintenance();
            let set = doc! { "$set": {
                "holder": { "$literal": holder },
                "owner_generation": { "$literal": generation_field(generation) },
                "claimed_at": "$$NOW",
                "heartbeat_at": "$$NOW",
            } };
            let refused = |current: Option<String>| {
                StorageError::AlreadyExists(format!(
                    "the backup maintenance claim is held by {}",
                    current.as_deref().unwrap_or("another maintenance command")
                ))
            };
            let taken_over = collection
                .update_one(
                    doc! {
                        "_id": MAINTENANCE_CLAIM_ID,
                        "$expr": age_is_stale(&["heartbeat_at"], stale_after),
                    },
                    vec![set.clone()],
                )
                .await
                .map_err(internal)?
                .matched_count
                == 1;
            if !taken_over {
                match collection
                    .update_one(
                        doc! {
                            "_id": MAINTENANCE_CLAIM_ID,
                            "owner_generation": { "$exists": false },
                        },
                        vec![set],
                    )
                    .upsert(true)
                    .await
                {
                    // The upsert matched a document with no
                    // `owner_generation` (an operator's insert, say, not a
                    // release's) and overwrote it with this call's
                    // generation: the claim is this call's, and refusing here
                    // would leave a document nobody heartbeats until it goes
                    // stale.
                    Ok(result) if result.upserted_id.is_some() || result.matched_count == 1 => {}
                    Ok(_) => return Err(refused(None)),
                    Err(e) if is_duplicate_key(&e) => {
                        let current = collection
                            .find_one(doc! { "_id": MAINTENANCE_CLAIM_ID })
                            .await
                            .map_err(internal)?
                            .and_then(|d| d.get_str("holder").ok().map(str::to_owned));
                        return Err(refused(current));
                    }
                    Err(e) => return Err(internal(e)),
                }
            }
            Ok(Box::new(MongoMaintenanceClaim {
                collection,
                generation,
            }) as Box<dyn MaintenanceClaim>)
        })
    }

    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        Box::pin(async move {
            let held = self
                .maintenance()
                .find_one(doc! {
                    "$nor": [ { "$expr": age_is_stale(&["heartbeat_at"], stale_after) } ],
                })
                .await
                .map_err(internal)?;
            Ok(held.is_some())
        })
    }
}

/// The `_id` of the one maintenance claim document.
const MAINTENANCE_CLAIM_ID: &str = "claim";

impl MongoEngine {
    fn maintenance(&self) -> mongodb::Collection<Document> {
        self.catalog_db.collection::<Document>(BACKUP_MAINTENANCE)
    }
}

/// The holder's handle on the maintenance claim document.
struct MongoMaintenanceClaim {
    collection: mongodb::Collection<Document>,
    generation: u64,
}

impl MongoMaintenanceClaim {
    fn owned_filter(&self) -> Document {
        doc! {
            "_id": MAINTENANCE_CLAIM_ID,
            "owner_generation": generation_field(self.generation),
        }
    }
}

impl MaintenanceClaim for MongoMaintenanceClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let result = self
                .collection
                .update_one(
                    self.owned_filter(),
                    vec![doc! { "$set": { "heartbeat_at": "$$NOW" } }],
                )
                .await
                .map_err(|e| StorageError::Internal(format!("maintenance heartbeat: {e}")))?;
            Ok(if result.matched_count == 1 {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            self.collection
                .delete_one(self.owned_filter())
                .await
                .map_err(internal)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_source_table() -> serde_json::Value {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../core/testdata/extenddb-manifest/v1.json"
        ))
        .expect("fixture parses");
        manifest["source_table"].clone()
    }

    fn row() -> BackupRow {
        BackupRow {
            backup_id: "01757500000123-0badcafe".to_owned(),
            account_id: "123456789012".to_owned(),
            table_name: "Music".to_owned(),
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "music-nightly".to_owned(),
            status: BackupStatus::Creating,
            store: StoreIdentity {
                store_kind: StoreKind::S3,
                store_root: "s3://bucket/prefix".to_owned(),
                store_id: "store-1".to_owned(),
            },
            size_bytes: 61828,
            item_count: 100_000,
            created_at: 1_757_500_000.123,
            completed_at: None,
            failure_reason: None,
            source_table_details_json: fixture_source_table(),
            owner_generation: 7,
            heartbeat_at: 1_757_500_001.0,
        }
    }

    /// The document the claim writes reads back as the row it was built
    /// from, the source table object included, with the times kept to the
    /// millisecond.
    #[test]
    fn a_row_round_trips_through_its_document() {
        let row = row();
        let mut doc = row_fields(&row).unwrap();
        doc.insert("account_id", &row.account_id);
        doc.insert("backup_id", &row.backup_id);
        doc.insert("owner_generation", generation_field(row.owner_generation));
        doc.insert("heartbeat_at", epoch_secs_datetime(row.heartbeat_at));
        let read = row_from_doc(&doc).unwrap();
        assert_eq!(read, row);
        assert_eq!(read.source_table().unwrap().table_name, "Music");
    }

    #[test]
    fn a_document_with_an_unknown_store_kind_or_status_is_internal() {
        let row = row();
        let mut doc = row_fields(&row).unwrap();
        doc.insert("account_id", &row.account_id);
        doc.insert("backup_id", &row.backup_id);
        doc.insert("owner_generation", 7_i64);
        doc.insert("heartbeat_at", epoch_secs_datetime(1.0));
        let mut bad_kind = doc.clone();
        bad_kind.insert("store_kind", "gcs");
        assert!(matches!(
            row_from_doc(&bad_kind),
            Err(StorageError::Internal(_))
        ));
        let mut bad_status = doc;
        bad_status.insert("status", "ACTIVE");
        assert!(matches!(
            row_from_doc(&bad_status),
            Err(StorageError::Internal(_))
        ));
    }

    #[test]
    fn only_a_connection_class_failure_leaves_a_write_unknown() {
        let io: mongodb::error::Error = mongodb::error::ErrorKind::Io(std::sync::Arc::new(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"),
        ))
        .into();
        assert!(outcome_unknown(&io));
        let command: mongodb::error::Error = mongodb::error::ErrorKind::Command(
            bson::from_document(doc! { "code": 112, "codeName": "WriteConflict", "errmsg": "x" })
                .expect("a command error document"),
        )
        .into();
        assert!(!outcome_unknown(&command));
    }

    /// The older-than filter reads the named column alone against the
    /// server's clock and requires the column to be a date, so a row whose
    /// `completed_at` was never stamped is not read as old as the epoch.
    #[test]
    fn the_older_than_filter_names_one_clock_column_and_requires_it_set() {
        let heartbeat = older_than_filter(StaleClock::Heartbeat, Duration::from_secs(90));
        assert_eq!(
            heartbeat.get_document("heartbeat_at").unwrap(),
            &doc! { "$type": "date" }
        );
        assert!(heartbeat.get("completed_at").is_none());
        let expr = heartbeat.get_document("$expr").unwrap().to_string();
        assert!(expr.contains("$heartbeat_at"), "{expr}");
        assert!(!expr.contains("completed_at"), "{expr}");
        assert!(expr.contains("$$NOW") && expr.contains("90000"), "{expr}");

        let completed = older_than_filter(StaleClock::Completed, Duration::from_secs(90));
        assert_eq!(
            completed.get_document("completed_at").unwrap(),
            &doc! { "$type": "date" }
        );
        assert!(completed.get("heartbeat_at").is_none());
        let expr = completed.get_document("$expr").unwrap().to_string();
        assert!(expr.contains("$completed_at"), "{expr}");
        assert!(!expr.contains("heartbeat_at"), "{expr}");
    }

    /// The dead-job filter selects, by the server's clock, RUNNING records
    /// with a stale heartbeat and FAILED records of a CREATING table whose
    /// `completed_at`, or heartbeat where none, is stale.
    #[test]
    fn the_dead_job_filter_reads_both_kinds_of_record_by_the_server_clock() {
        let filter = dead_restore_job_filter(Duration::from_secs(300));
        let arms = filter.get_array("$or").expect("$or");
        assert_eq!(arms.len(), 2);
        let running = arms[0].as_document().expect("arm");
        assert_eq!(running.get_str(RESTORE_JOB_STATUS), Ok("RUNNING"));
        assert!(running.get("table_status").is_none());
        let expr = running.get_document("$expr").expect("$expr").to_string();
        assert!(expr.contains("$restore_job.heartbeat_at"), "{expr}");
        assert!(!expr.contains("completed_at"), "{expr}");
        assert!(expr.contains("$$NOW") && expr.contains("300000"), "{expr}");
        let failed = arms[1].as_document().expect("arm");
        assert_eq!(failed.get_str(RESTORE_JOB_STATUS), Ok("FAILED"));
        assert_eq!(failed.get_str("table_status"), Ok("CREATING"));
        let expr = failed.get_document("$expr").expect("$expr").to_string();
        let completed = expr
            .find("$restore_job.completed_at")
            .expect("completed_at");
        let heartbeat = expr
            .find("$restore_job.heartbeat_at")
            .expect("heartbeat_at");
        assert!(completed < heartbeat, "completed_at is read first: {expr}");
        assert!(expr.contains("$$NOW") && expr.contains("300000"), "{expr}");
    }

    #[test]
    fn generations_round_trip_through_the_integer_field() {
        let g = new_generation();
        let doc = doc! { "owner_generation": generation_field(g) };
        assert_eq!(generation_from_field(&doc, "owner_generation"), g);
        assert_eq!(generation_from_field(&doc! {}, "owner_generation"), 0);
    }

    /// A pipeline `$set` keeps `$$NOW` as the server's clock and wraps every
    /// other value, the source table object included, so none is read as a
    /// field path.
    #[test]
    fn a_pipeline_set_wraps_every_value_but_now_in_a_literal() {
        let source = bson::to_bson(&fixture_source_table()).unwrap();
        let set = literal_set(doc! {
            "status": "AVAILABLE",
            "completed_at": "$$NOW",
            "owner_generation": 7_i64,
            "source_table_details_json": source.clone(),
        });
        assert_eq!(set.get_str("completed_at"), Ok("$$NOW"));
        assert_eq!(
            set.get_document("status").unwrap(),
            &doc! { "$literal": "AVAILABLE" }
        );
        assert_eq!(
            set.get_document("owner_generation").unwrap(),
            &doc! { "$literal": 7_i64 }
        );
        assert_eq!(
            set.get_document("source_table_details_json").unwrap(),
            &doc! { "$literal": source }
        );
    }
}
