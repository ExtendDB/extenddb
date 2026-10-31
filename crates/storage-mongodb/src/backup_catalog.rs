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
//! matches other than exactly one document means the claim is lost.
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

use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, RestoreJobRow, RestoreKind, RestoreTargetKey, StatusFields, StoreIdentity,
    StoreKind, TimeBounds, WriteOutcome, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mongodb::bson::{self, Bson, Document, doc};

use crate::MongoEngine;
use crate::backup_engine::{age_is_stale, restoring_target_filter};
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
pub(crate) const RESTORE_JOB_GENERATION: &str = "restore_job.owner_generation";
pub(crate) const RESTORE_JOB_HEARTBEAT: &str = "restore_job.heartbeat_at";
pub(crate) const RESTORE_JOB_FAILURE_REASON: &str = "restore_job.failure_reason";
pub(crate) const RESTORE_JOB_COMPLETED_AT: &str = "restore_job.completed_at";

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
            if fields.completed {
                set.insert("completed_at", "$$NOW");
            }
            // A pipeline update, so `$$NOW` is the server's clock; the
            // literal strings are wrapped so the pipeline does not read them
            // as field paths.
            let set: Document = set
                .into_iter()
                .map(|(k, v)| {
                    let v = match v {
                        Bson::String(s) if s == "$$NOW" => Bson::String(s),
                        other => doc! { "$literal": other }.into(),
                    };
                    (k, v)
                })
                .collect();
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
                    CommitOutcome::RerunBody => backoff_sleep(attempt).await,
                },
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
            "DeleteBackup of {} did not commit after {TRANSIENT_RETRY_ATTEMPTS} conflicting \
             attempts",
            key.backup_arn
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
            let set: Document = set
                .into_iter()
                .map(|(k, v)| {
                    let v = match v {
                        Bson::String(s) if s == "$$NOW" => Bson::String(s),
                        other => doc! { "$literal": other }.into(),
                    };
                    (k, v)
                })
                .collect();
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
            let mut filter = doc! {
                "store_kind": store.store_kind.as_str(),
                "store_root": &store.store_root,
                "store_id": &store.store_id,
            };
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
            docs.iter()
                .map(|doc| {
                    let id = doc
                        .get_document("_id")
                        .map_err(|_| StorageError::Internal("table is missing _id".to_owned()))?;
                    let job = doc.get_document(RESTORE_JOB).map_err(|_| {
                        StorageError::Internal(
                            "restore target is missing its job record".to_owned(),
                        )
                    })?;
                    Ok(RestoreJobRow {
                        target: RestoreTargetKey {
                            account_id: required_str(id, "account_id")?,
                            table_name: required_str(id, "table_name")?,
                            table_id: required_str(doc, "table_id")?,
                        },
                        source_backup_arn: required_str(doc, "restore_source_backup_arn")?,
                        kind: RestoreKind::parse(&required_str(job, "kind")?)?,
                        status: JobStatus::parse(&required_str(job, "status")?)?,
                        owner_generation: generation_from_field(job, "owner_generation"),
                        heartbeat_at: optional_datetime(job, "heartbeat_at").unwrap_or(0.0),
                    })
                })
                .collect()
        })
    }

    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
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
                    },
                    vec![doc! { "$set": {
                        RESTORE_JOB_STATUS: { "$literal": JobStatus::Failed.as_str() },
                        RESTORE_JOB_FAILURE_REASON: { "$literal": reason },
                        RESTORE_JOB_COMPLETED_AT: "$$NOW",
                    } }],
                )
                .await
                .map_err(internal)?;
            Ok(result.matched_count == 1)
        })
    }

    fn check_maintenance_claim(&self) -> BoxFuture<'_, Result<bool, StorageError>> {
        Box::pin(async move {
            let held = self
                .catalog_db
                .collection::<Document>(BACKUP_MAINTENANCE)
                .find_one(doc! {})
                .await
                .map_err(internal)?;
            Ok(held.is_some())
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
}
