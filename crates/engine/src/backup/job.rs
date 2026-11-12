// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The store-backed CreateBackup job: the detached task the request spawns
//! once its checks have passed and the permit is held.
//!
//! The task ([`start`]) opens the table snapshot, mints the backup id from
//! the millisecond the open returned, reads the table's TTL, builds the
//! manifest's `source_table` from the snapshot's description
//! ([`source_table_schema`]), runs the deletion fence (a root another
//! catalog adopted since the store's last check is refused before the row,
//! `StoreFence`; a fence read that recorded a problem answers the
//! unhealthy-store refusal, one that failed in transport an internal
//! error), inserts the CREATING row under the job's claim
//! (`claim_backup`), answers the request through a one-shot channel
//! (`BackupDetails` CREATING, or the refusal) and then goes on as the job
//! ([`run`]) with the same owned values: nothing crosses a thread or a
//! channel between the request and the job but the response (the
//! pipeline's item and chunk channels are the job's own), and a client that
//! disconnects mid-request cannot leave a row whose owner was never
//! spawned. The maintenance claim is checked by the request before the
//! permit, as the in-catalog paths check it, and again immediately before
//! `claim_backup`, so a held claim refuses the request and inserts no row
//! (a FAILED row written for the refusal was counted by `backup purge`'s
//! wait as a live job for the stale window), and a claim taken while the
//! open waited (on a lock the table holds, say) refuses it too.
//! The open and the insert are selected against the shutdown token, so a
//! server that is stopping neither waits on an open the table's lock holds
//! nor inserts a row it would fail at once. The `ServiceUnavailable` that
//! refusal answers reaches a client only when the token is cancelled while
//! the listener still serves, which is a registration loss (the restartable
//! worker set stopping calls `BackupRuntime::cancel_jobs`); on SIGTERM the
//! listener's 5 s connection drain closes the connection before
//! `drain_then_release` cancels the token (the maintainer measured a
//! `CreateBackup` waiting in the open: its connection closed 8.2 s after it
//! was sent), so there the refusal's effect is the row not written.
//!
//! The job: the fence once more, right before the first put (a root
//! adopted between the request's read and the job's first write gets no
//! object of this job's; the refusal fails the row with the fence's reason
//! and makes no store write), the data files through the pipeline
//! ([`writer`]), then the
//! three documents `BackupDocuments::build` returns in its order, a re-check
//! of the claim, the manifest (the commit record), and the AVAILABLE write
//! with the size, the count and the manifest's `source_table`
//! ([`failure::write_status`] gives every status write one meaning; an
//! AVAILABLE write whose outcome is unknown is re-read, one that matches no
//! row ends the job with no store access). Before every put the job checks
//! the store as the runtime holds it: healthy, and its identity the one the
//! row records ([`writer::Pipeline::store_check`]); a store the reconciler
//! has marked unhealthy, or whose identity file now names another store (a
//! root swapped under a running job), fails the row with the store's reason
//! and makes no store call, the put or the prefix's deletion included (the
//! FAILED-prefix duty deletes the prefix once the store is healthy again).
//! The heartbeat runs on a timer for the job's life ([`heartbeat`]); a
//! heartbeat that does not confirm the claim, or does not return within one
//! period, ends the job with no further store or catalog access. Any other
//! failure, and the shutdown token (reason `shutdown`), runs the failure
//! path ([`failure::fail_row`]): FAILED, then the prefix's deletion only
//! when that write matched and the fence passed.
//!
//! Each job records `BackupJobs` and `BackupJobDuration` (microseconds,
//! from the snapshot's open to the end of its last catalog or store call:
//! the AVAILABLE write, or the FAILED write and the prefix's deletion)
//! under its outcome, `BackupBytesUploaded` once it ended AVAILABLE,
//! `BackupSnapshotAge` at each heartbeat, and the worker metrics under
//! `QuerySource::BackupJob` (`WorkerLastSuccess` on success,
//! `WorkerErrorCount` on a failure; a job the shutdown cancelled is neither);
//! one info line at start (ARN, the table's item count as the stats pass
//! last counted it, the order) and at the end (ARN, the items written,
//! bytes, duration), one error line per failure.
//!
//! Kill points ([`kill_point`]) pause the job for the binary-level crash
//! tests under the `backup-test-hooks` feature and are no-ops without it.
//! The restore job ([`super::restore`]) shares this module's environment
//! ([`JobEnv`]), its kill points, its heartbeat and its status writes.

pub(crate) mod failure;
pub(crate) mod heartbeat;
pub(crate) mod writer;

use std::sync::Arc;

use extenddb_backup_store::{BackupStore, byte_stream_from};
use extenddb_core::backup_format::{
    BackupArn, BackupDocuments, ItemOrder, KeyOrder, KeyRangeTracker, ManifestHeader,
    SourceTableSchema, backup_creation_date_time,
};
use extenddb_core::error::DynamoDbError;
use extenddb_core::metrics::{BackupJobOutcome, MetricsCollector, QuerySource};
use extenddb_core::types::BackupDetails;
use extenddb_core::validation::{RestorableKeySchema, RestorableLimits};
use extenddb_storage::backup_catalog::{
    BACKUP_TYPE_USER, BackupRow, BackupStatus, JobStatus, StatusFields, StoreIdentity,
    UNOWNED_GENERATION,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::snapshot::source_table_schema;
use extenddb_storage::{
    BackupCatalog, CancellationToken, JobClaim, JobPermit, MetadataEngine, SnapshotEngine,
    TableSnapshot,
};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::runtime::{BackupRuntime, ConfiguredStore, JobSettings, Spool, StoreFence};
use super::{create_backup_storage_err_to_dynamo, maintenance_held, storage_err_to_dynamo};
use failure::{FailurePath, Written};
use heartbeat::{HeartbeatHandle, HeartbeatMetric};
use writer::{DataFiles, Pipeline, Stop};

/// The reason a restore record carries when the maintenance claim was held
/// as the job started (the design's 5.3 step 4; the backup job checks the
/// claim before its row exists and writes no such row).
pub(crate) const MAINTENANCE: &str = "maintenance";

/// The reason a row carries when the server's shutdown stopped the job.
pub(crate) const SHUTDOWN: &str = "shutdown";

/// What the runtime hands a job: the store as the runtime holds it (its
/// health and identity, read before every put), its identity as the row
/// records it and its manifest names, the restore spool, the settings, the
/// collector, the shutdown token and the source names.
#[derive(Clone)]
pub(crate) struct JobEnv {
    pub configured: Arc<ConfiguredStore>,
    pub store: Arc<dyn BackupStore>,
    /// The store's deletion fence, which CreateBackup runs before its row,
    /// the job right before its first put and the failure path before its
    /// deletion.
    pub fence: StoreFence,
    pub identity: StoreIdentity,
    pub bucket: String,
    pub key_prefix: String,
    /// The restore spool of an S3 store; `None` for a filesystem store,
    /// whose files a restore reads in place.
    pub spool: Option<Spool>,
    pub settings: JobSettings,
    pub metrics: Option<Arc<MetricsCollector>>,
    pub shutdown: CancellationToken,
    pub source_backend: String,
    pub source_extenddb_version: String,
}

impl JobEnv {
    /// From the runtime and its configured store, whose identity must be
    /// known (it is once a check has passed, which `require_healthy_store`
    /// has established).
    pub(crate) fn new(
        runtime: &BackupRuntime,
        store: &Arc<ConfiguredStore>,
    ) -> Result<Self, DynamoDbError> {
        let identity = store.identity().ok_or_else(|| {
            tracing::error!("the configured store is healthy but its id is unknown");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        })?;
        let (source_backend, source_extenddb_version) = runtime.source();
        Ok(Self {
            configured: Arc::clone(store),
            store: Arc::clone(&store.store),
            fence: store.fence(),
            identity,
            bucket: store.bucket.clone(),
            key_prefix: store.key_prefix.clone(),
            spool: store.spool.clone(),
            settings: runtime.job_settings(),
            metrics: runtime.metrics().cloned(),
            shutdown: runtime.shutdown(),
            source_backend: source_backend.to_owned(),
            source_extenddb_version: source_extenddb_version.to_owned(),
        })
    }
}

/// The request's members the task needs.
pub(crate) struct CreateRequest {
    pub account_id: String,
    pub region: String,
    pub table_name: String,
    pub backup_name: String,
    /// The instance's limits the validator holds every item to.
    pub limits: RestorableLimits,
}

/// The one-shot the task answers the request through.
pub(crate) type Reply = oneshot::Sender<Result<BackupDetails, DynamoDbError>>;

/// The detached task (module doc): everything from the open to the
/// response, then the job.
pub(crate) async fn start<S>(
    storage: Arc<S>,
    env: JobEnv,
    permit: JobPermit,
    request: CreateRequest,
    reply: Reply,
) where
    S: BackupCatalog + SnapshotEngine + MetadataEngine + ?Sized + Send + Sync + 'static,
{
    let job = match open_and_claim(&*storage, &env, &request).await {
        Ok(job) => {
            let details = job.details();
            // A request whose client went away still has its job.
            let _ = reply.send(Ok(details));
            *job
        }
        Err(e) => {
            drop(permit);
            let _ = reply.send(Err(e));
            return;
        }
    };
    run(job).await;
    drop(permit);
}

/// The job once its row exists.
pub(crate) struct Job {
    claim: Arc<dyn JobClaim>,
    snapshot: TableSnapshot,
    arn: BackupArn,
    backup_name: String,
    source_table: SourceTableSchema,
    key_schema: RestorableKeySchema,
    /// The table's key order when the stream is key-ordered.
    key_order: Option<KeyOrder>,
    limits: RestorableLimits,
    env: JobEnv,
    /// This instance's catalog id, which the fence before the first put and
    /// the failure path's fence compare the identity file with.
    catalog_id: String,
    /// The id's instant, the row's `created_at`.
    created_at: f64,
    started: Instant,
    /// The instant taken before the claim's insert, the heartbeat's first
    /// confirmed beat.
    claimed_at: Instant,
}

impl Job {
    /// The response the request answers: the row as inserted, CREATING.
    fn details(&self) -> BackupDetails {
        BackupDetails {
            backup_arn: self.arn.to_arn_string(),
            backup_name: self.backup_name.clone(),
            backup_status: BackupStatus::Creating.as_str().to_owned(),
            backup_type: BACKUP_TYPE_USER.to_owned(),
            backup_size_bytes: 0,
            backup_creation_date_time: self.created_at,
        }
    }
}

/// The open, the id, the TTL read, the row and the claim (module doc). The
/// open and the insert are selected against the shutdown token.
async fn open_and_claim<S>(
    storage: &S,
    env: &JobEnv,
    request: &CreateRequest,
) -> Result<Box<Job>, DynamoDbError>
where
    S: BackupCatalog + SnapshotEngine + MetadataEngine + ?Sized,
{
    let CreateRequest {
        account_id,
        region,
        table_name,
        backup_name,
        limits,
    } = request;
    let started = Instant::now();
    // The open and the claim's insert are selected against the shutdown
    // token: an open that waits on a lock the table holds (an `ACCESS
    // EXCLUSIVE` on PostgreSQL) does not hold the drain for its whole bound,
    // and no row is inserted for a server that is stopping. The answer
    // reaches a client only after a registration loss (module doc): on
    // SIGTERM the listener's drain has closed the connection by the time the
    // token is cancelled.
    let snapshot = tokio::select! {
        biased;
        () = env.shutdown.cancelled() => return Err(shutting_down(table_name)),
        opened = storage.open_table_snapshot(account_id, table_name) => {
            opened.map_err(|e| create_backup_storage_err_to_dynamo(e, table_name))?
        }
    };
    // The id carries the millisecond the open returned.
    let backup_id = extenddb_storage::util::new_backup_id();
    let created_at = backup_creation_date_time(&backup_id)
        .ok_or_else(|| internal("the backup id carries no instant"))?;
    let arn = BackupArn::new(
        extenddb_storage::util::ARN_PARTITION,
        region,
        account_id,
        table_name,
        &backup_id,
    )
    .map_err(|e| internal(&format!("no backup ARN for {table_name}: {e}")))?;
    let ttl = storage
        .describe_ttl(account_id, table_name)
        .await
        .map_err(storage_err_to_dynamo)?;
    let source_table =
        source_table_schema(&snapshot.description, Some(ttl)).map_err(storage_err_to_dynamo)?;
    let key_schema = RestorableKeySchema::resolve(
        &source_table.key_schema,
        &source_table.attribute_definitions,
    )
    .map_err(|e| {
        internal(&format!(
            "the key schema of {table_name} cannot be checked: {e}"
        ))
    })?;
    let key_order = snapshot
        .key_ordered
        .then(|| {
            KeyOrder::resolve(
                &source_table.key_schema,
                &source_table.attribute_definitions,
            )
            .ok()
        })
        .flatten();
    let row = BackupRow {
        backup_id: backup_id.clone(),
        account_id: account_id.clone(),
        table_name: table_name.clone(),
        backup_arn: arn.to_arn_string(),
        backup_name: backup_name.clone(),
        status: BackupStatus::Creating,
        store: env.identity.clone(),
        size_bytes: 0,
        item_count: 0,
        created_at,
        completed_at: None,
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&source_table)
            .map_err(|e| internal(&format!("the source table does not serialize: {e}")))?,
        owner_generation: UNOWNED_GENERATION,
        heartbeat_at: 0.0,
    };
    // The fence before the row: a root another catalog adopted since the
    // store's last check gets no row from this catalog, and the store is
    // marked unhealthy. A read that recorded a problem (the file names
    // another catalog, is missing, or cannot be served) answers as the
    // unhealthy store the next request meets; only a read that failed in
    // transport, which recorded nothing, answers as a failed store call.
    // The job runs the fence again right before its first put.
    let catalog_id = env
        .fence
        .catalog_id(storage)
        .await
        .map_err(|e| internal(&format!("the catalog id could not be read: {e}")))?;
    if let Err(problem) = env.fence.check(&catalog_id).await {
        return Err(env.fence.refusal().unwrap_or_else(|| {
            internal(&format!(
                "{table_name}: the store's identity file could not be read before the row: \
                 {problem}"
            ))
        }));
    }
    // The maintenance claim once more, immediately before the insert: the
    // request's check ran before the open, the TTL read and the fence, any
    // of which can wait (an open behind the table's lock), and a command
    // that took the claim since gets no row of this job. A claim taken
    // between this read and the insert is not seen: the CREATING row lands
    // under the claim, where the command's wait lists it, the move's re-list
    // of the old root reports it, or (an insert after that re-list) the
    // move's closing report of other stores or a later plain sync does.
    if storage
        .check_maintenance_claim(env.settings.stale_job_after)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        return Err(maintenance_held());
    }
    let claimed_at = Instant::now();
    // The insert too is selected against the token: a server that is
    // stopping inserts no row it would have to fail at once.
    let claimed = tokio::select! {
        biased;
        () = env.shutdown.cancelled() => return Err(shutting_down(table_name)),
        claimed = storage.claim_backup(&row) => claimed,
    };
    let claim: Arc<dyn JobClaim> = match claimed {
        Ok(claim) => Arc::from(claim),
        Err(StorageError::AlreadyExists(what)) => {
            return Err(internal(&format!(
                "backup id {backup_id} is already claimed: {what}"
            )));
        }
        Err(e) => return Err(storage_err_to_dynamo(e)),
    };
    Ok(Box::new(Job {
        claim,
        snapshot,
        arn,
        backup_name: backup_name.clone(),
        source_table,
        key_schema,
        key_order,
        limits: *limits,
        env: env.clone(),
        catalog_id,
        created_at,
        started,
        claimed_at,
    }))
}

fn internal(why: &str) -> DynamoDbError {
    tracing::error!("CreateBackup: {why}");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

/// The refusal of a request whose server is stopping before its row exists
/// (module doc); the client sees it only when the token was cancelled while
/// the listener still served, after a registration loss.
fn shutting_down(table_name: &str) -> DynamoDbError {
    tracing::info!("CreateBackup of {table_name}: refused, the server is shutting down");
    DynamoDbError::ServiceUnavailable("The server is shutting down. Retry the request.".to_owned())
}

/// The FAILED write of a row whose job makes no store call: nothing of the
/// job's is in the store, or the store is not to be called, so there is
/// nothing to delete here.
async fn fail_without_store_access(claim: &dyn JobClaim, arn: &BackupArn, reason: &str) {
    let fields = StatusFields {
        failure_reason: Some(reason.to_owned()),
        completed: true,
        ..StatusFields::default()
    };
    match failure::write_status(claim, JobStatus::Creating, JobStatus::Failed, &fields).await {
        Written::Matched => {}
        Written::NotMatched(why) | Written::Error(why) => tracing::error!(
            "backup {}: the row could not be marked FAILED ({reason}): {why}",
            arn.to_arn_string()
        ),
    }
}

/// The job (module doc), from the data files to the final status write.
pub(crate) async fn run(job: Job) -> BackupJobOutcome {
    let Job {
        claim,
        snapshot,
        arn,
        backup_name,
        mut source_table,
        key_schema,
        key_order,
        limits,
        env,
        catalog_id,
        created_at,
        started,
        claimed_at,
    } = job;
    let arn_text = arn.to_arn_string();
    let prefix = arn.store_prefix();
    let metrics = env.metrics.as_deref();
    let heartbeat = HeartbeatHandle::start(
        Arc::clone(&claim),
        env.settings.heartbeat_interval,
        match env.metrics.clone() {
            Some(metrics) => HeartbeatMetric::BackupSnapshotAge {
                metrics,
                created_at,
            },
            None => HeartbeatMetric::None,
        },
        claimed_at,
    );
    tracing::info!(
        "backup {arn_text}: started, {} items in the table as last counted by the stats pass, {}",
        snapshot.description.item_count,
        if key_order.is_some() {
            "key-ordered"
        } else {
            "unordered"
        }
    );
    let TableSnapshot { info, items, .. } = snapshot;
    let pipeline = Pipeline {
        store: &*env.store,
        configured: &env.configured,
        identity: &env.identity,
        prefix: &prefix,
        arn: &arn_text,
        heartbeat: &heartbeat,
        shutdown: &env.shutdown,
        metrics,
        target_bytes: env.settings.data_file_target_bytes,
        key_prefix: &env.key_prefix,
        bucket: &env.bucket,
        written: None,
    };
    let tracker = key_order.as_ref().map(KeyRangeTracker::new);
    let mut item_count = 0;
    let end = async {
        // The fence right before the first put (`StoreFence`'s doc): the
        // request's read was before the row, and a root another catalog
        // adopted since gets no object of this job's. The refusal, recorded
        // or not, fails the row with no store write.
        env.fence.check(&catalog_id).await.map_err(|problem| {
            Stop::StoreUnhealthy(format!(
                "the store's identity file was re-read before the first write and refused \
                     it ({problem}); no object was written"
            ))
        })?;
        let files = writer::write_data_files(&pipeline, items, key_schema, limits, tracker).await?;
        item_count = files.item_count;
        // Both figures are the stream's: the items written and their
        // DynamoDB sizes summed, not the statistics pass's (which can
        // be stale, or 0 on a fresh table).
        source_table.item_count = i64::try_from(files.item_count).unwrap_or(i64::MAX);
        source_table.table_size_bytes = i64::try_from(files.item_bytes).unwrap_or(i64::MAX);
        let header = ManifestHeader {
            backup_arn: arn.clone(),
            backup_name,
            source_backend: env.source_backend.clone(),
            source_extenddb_version: env.source_extenddb_version.clone(),
            snapshot: info,
            item_order: if key_order.is_some() {
                ItemOrder::KeyOrdered
            } else {
                ItemOrder::Unordered
            },
            source_table,
        };
        write_documents(&pipeline, &claim, header, files).await
    }
    .await;
    let (outcome, bytes) = match end {
        Ok(bytes) => {
            tracing::info!(
                "backup {arn_text}: AVAILABLE, {item_count} items, {bytes} bytes written in {:.1} s",
                started.elapsed().as_secs_f64()
            );
            (BackupJobOutcome::Succeeded, bytes)
        }
        Err(Stop::ClaimLost(why)) => {
            tracing::error!(
                "backup {arn_text}: stopped with no further store or catalog access: {why}; the \
                 row is left to its owner or to the reconciler"
            );
            (BackupJobOutcome::Failed, 0)
        }
        Err(Stop::StoreUnhealthy(reason)) => {
            // The store is not to be written: FAILED with the store's
            // reason, the prefix left to the FAILED-prefix duty once the
            // store is healthy again.
            fail_without_store_access(&*claim, &arn, &reason).await;
            tracing::error!(
                "backup {arn_text}: FAILED ({reason}); no store write was made and its objects, \
                 if any, are deleted by the reconciler once the store's check passes and the \
                 row's heartbeat is stale (never, while another catalog has adopted the root: \
                 the objects then stay under it)"
            );
            (BackupJobOutcome::Failed, 0)
        }
        Err(stop) => {
            let (reason, outcome) = match stop {
                Stop::Cancelled => (SHUTDOWN.to_owned(), BackupJobOutcome::Cancelled),
                Stop::Fail(reason) | Stop::Refused(reason) | Stop::Occupied(reason) => {
                    (reason, BackupJobOutcome::Failed)
                }
                Stop::ClaimLost(_) | Stop::StoreUnhealthy(_) => unreachable!("matched above"),
            };
            match failure::fail_row(
                &*claim,
                &*env.store,
                &env.fence,
                &catalog_id,
                &prefix,
                &reason,
                metrics,
            )
            .await
            {
                FailurePath::Deleted => tracing::error!(
                    "backup {arn_text}: FAILED ({reason}); its objects were deleted"
                ),
                FailurePath::Fenced(why) => tracing::error!(
                    "backup {arn_text}: FAILED ({reason}); its objects were not deleted: {why}"
                ),
                FailurePath::DeleteFailed(e) => tracing::error!(
                    "backup {arn_text}: FAILED ({reason}); its objects could not be deleted and \
                     the reconciler deletes them once the row's heartbeat is stale: {e}"
                ),
                FailurePath::NoStoreAccess(why) => tracing::error!(
                    "backup {arn_text}: failed ({reason}) and the row is not this job's to \
                     write ({why}); no store access was made"
                ),
            }
            (outcome, 0)
        }
    };
    // After the failure path too, so a FAILED job's duration covers its
    // FAILED write and the prefix's deletion.
    let duration = started.elapsed();
    drop(heartbeat);
    if let Some(metrics) = metrics {
        #[allow(clippy::cast_precision_loss)]
        let duration_us = duration.as_micros() as f64;
        metrics.record_backup_job(outcome, duration_us);
        if bytes > 0 {
            metrics.record_backup_bytes_uploaded(bytes);
        }
        match outcome {
            BackupJobOutcome::Succeeded => {
                metrics.record_worker_success(QuerySource::BackupJob, duration_us);
            }
            BackupJobOutcome::Failed => {
                metrics.record_worker_error(QuerySource::BackupJob);
            }
            // A clean shutdown is not a worker error.
            BackupJobOutcome::Cancelled => {}
        }
    }
    outcome
}

/// After the data files: the three documents, the claim re-check, the
/// manifest, the AVAILABLE write. Returns the bytes written over the whole
/// backup.
async fn write_documents(
    p: &Pipeline<'_>,
    claim: &Arc<dyn JobClaim>,
    header: ManifestHeader,
    files: DataFiles,
) -> Result<u64, Stop> {
    let DataFiles {
        entries,
        item_count,
        item_bytes: _,
        mut bytes_uploaded,
    } = files;
    let docs = BackupDocuments::build(header, entries, p.key_prefix, p.bucket)
        .map_err(|e| Stop::Fail(format!("building the manifest: {e}")))?;
    for (name, bytes) in docs.documents_before_manifest() {
        bytes_uploaded += put_document(p, name, bytes).await?;
    }
    // The re-check narrows the window in which a fenced-out job can still
    // write a manifest; the bound is the backend's (`heartbeat_within`).
    p.heartbeat
        .recheck()
        .await
        .map_err(|why| Stop::ClaimLost(format!("the claim re-check before the manifest: {why}")))?;
    kill_point(KillPoint::BeforeManifest, p.shutdown).await;
    let (name, bytes) = docs.manifest_document();
    bytes_uploaded += put_document(p, name, bytes).await?;
    kill_point(KillPoint::AfterManifest, p.shutdown).await;
    let manifest = docs.manifest();
    let fields = StatusFields {
        size_bytes: Some(manifest.backup_size_bytes),
        item_count: Some(i64::try_from(item_count).unwrap_or(i64::MAX)),
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&manifest.source_table).ok(),
        completed: true,
    };
    match failure::write_status(&**claim, JobStatus::Creating, JobStatus::Available, &fields).await
    {
        Written::Matched => Ok(bytes_uploaded),
        Written::NotMatched(why) | Written::Error(why) => Err(Stop::ClaimLost(format!(
            "the AVAILABLE write did not settle: {why}"
        ))),
    }
}

/// One document's put under the backup prefix, after the pre-put check;
/// returns the bytes the store reported.
pub(crate) async fn put_document<W: writer::ClaimWatch + ?Sized>(
    p: &Pipeline<'_, W>,
    name: &str,
    bytes: &[u8],
) -> Result<u64, Stop> {
    if p.shutdown.is_cancelled() {
        return Err(Stop::Cancelled);
    }
    p.store_check(name)?;
    if !p.heartbeat.confirms_the_claim() {
        return Err(Stop::ClaimLost(format!(
            "no heartbeat confirmed the claim recently enough for {name}"
        )));
    }
    let key = format!("{}{name}", p.prefix);
    tokio::select! {
        biased;
        () = p.shutdown.cancelled() => Err(Stop::Cancelled),
        () = p.heartbeat.lost().cancelled() => Err(Stop::ClaimLost(p.heartbeat.reason())),
        result = p.put_object(&key, name, byte_stream_from(bytes.to_vec())) => {
            result.map(|meta| meta.size)
        }
    }
}

/// The points at which a backup or restore job pauses under
/// `backup-test-hooks` when `EXTENDDB_BACKUP_PAUSE` names one, until the
/// file `EXTENDDB_BACKUP_PAUSE_RELEASE` names exists (or for good, for a
/// test that kills the server there). Each is logged as `job reached kill
/// point <name>` (the backend's own points log `restore job reached kill
/// point <name>`; the harness matches `reached kill point <name>`). This
/// enum is the engine's list: the points inside a backend's own writes are
/// paused by the backend through
/// `extenddb_storage::backup_catalog::restore_pause_point` under
/// `extenddb-storage/test-hooks`, and each backend's own point is named
/// beside `restore_pause_point`, once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillPoint {
    /// After the put of the first data file returned, before the next file
    /// or the documents.
    AfterDataFile,
    /// After the three documents, before `extenddb-manifest.json`.
    BeforeManifest,
    /// After the manifest's put returned, before the AVAILABLE write.
    AfterManifest,
    /// Restore: after every data file downloaded and verified, before the
    /// first `bulk_load`.
    RestoreAfterDownload,
    /// Restore: after the first file's `bulk_load` returned, before the
    /// next file.
    RestoreAfterFileCommit,
    /// Restore: after a `finish_restore` that left the ACTIVE flip to the
    /// backend's index backfill returned, before the job ends (MongoDB).
    RestoreMongodbBeforeWorkerFlip,
    /// Restore: after the FAILED write matched, before
    /// `remove_restore_target`.
    RestoreBetweenFailedAndRemoval,
    /// Restore: before the DONE write, with the job's heartbeat stopped
    /// first, so a reconciler takes the record over while the job pauses
    /// and the job's write then matches no record.
    RestoreFencedOut,
}

impl KillPoint {
    /// The value of `EXTENDDB_BACKUP_PAUSE` that names this point.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::AfterDataFile => "after_data_file",
            Self::BeforeManifest => "before_manifest",
            Self::AfterManifest => "after_manifest",
            Self::RestoreAfterDownload => "restore_after_download",
            Self::RestoreAfterFileCommit => "restore_after_file_commit",
            Self::RestoreMongodbBeforeWorkerFlip => "restore_mongodb_before_worker_flip",
            Self::RestoreBetweenFailedAndRemoval => "restore_between_failed_and_removal",
            Self::RestoreFencedOut => "restore_fenced_out",
        }
    }
}

/// Whether the environment names `point` as the pause point (so a caller
/// can do what the pause needs first, as the fenced-out point stops the
/// heartbeat); `false` without `backup-test-hooks`.
pub(crate) fn kill_point_named(point: KillPoint) -> bool {
    #[cfg(feature = "backup-test-hooks")]
    {
        std::env::var("EXTENDDB_BACKUP_PAUSE").ok().as_deref() == Some(point.name())
    }
    #[cfg(not(feature = "backup-test-hooks"))]
    {
        let _ = point;
        false
    }
}

/// Pause at `point` if the environment names it (type doc); a ready future
/// without `backup-test-hooks`.
#[cfg_attr(not(feature = "backup-test-hooks"), allow(clippy::unused_async))]
pub(crate) async fn kill_point(point: KillPoint, shutdown: &CancellationToken) {
    #[cfg(feature = "backup-test-hooks")]
    {
        if std::env::var("EXTENDDB_BACKUP_PAUSE").ok().as_deref() != Some(point.name()) {
            return;
        }
        let release = std::env::var("EXTENDDB_BACKUP_PAUSE_RELEASE").ok();
        tracing::warn!(
            "job reached kill point {}; paused until {} exists",
            point.name(),
            release
                .as_deref()
                .unwrap_or("no release file (the pause does not end)")
        );
        loop {
            if let Some(release) = &release
                && std::path::Path::new(release).exists()
            {
                tracing::warn!("job released from kill point {}", point.name());
                return;
            }
            if shutdown.is_cancelled() {
                tracing::warn!(
                    "job at kill point {} released by the shutdown",
                    point.name()
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    #[cfg(not(feature = "backup-test-hooks"))]
    let _ = (point, shutdown);
}
