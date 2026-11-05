// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The store-backed RestoreTableFromBackup: the request's manifest branch
//! and the restore job (the design's 5.3).
//!
//! The request ([`request`]) runs once `check_catalog_row_for_restore` has
//! accepted the backup's row: it takes the job permit, reads the sidecar and
//! the manifest from the store (a missing one is `BackupNotFoundException`
//! and an error line naming the remedies; each read
//! collects at most its bound, [`super::reconciler::SIDECAR_READ_BOUND`] and
//! [`super::reconciler::MANIFEST_READ_BOUND`], and an object over it is a
//! `ValidationException` naming a corrupt prefix, as a bad sidecar is),
//! verifies the
//! sidecar, parses the manifest (`ExtenddbManifest::from_json_bytes`, whose
//! own gates are the `format_version` and the identity rule that the ARN,
//! `source_table.table_name` and `source_table.table_arn` name one table),
//! checks that it names the row's backup, refuses a non-empty
//! `vector_indexes` into a backend without vector support with the text the
//! legacy reader uses, builds the target's `CreateTableInput` from the
//! manifest with the overrides applied, validates it as CreateTable does,
//! resolves the key schema the validator holds every item to, and spawns
//! the detached task ([`start`]) whose answer it awaits for at most
//! [`RESTORE_REGISTRATION_TIMEOUT`]; past that it answers
//! `InternalServerError` while the task goes on (the target then appears
//! through DescribeTable). The task registers the target and its RUNNING
//! record in one write (`claim_restore`, under the backup row's lock), sends
//! the job's first beat through the claim (the registration's own
//! `heartbeat_at` stamp can be old: the design's 4(e) states each backend's
//! clock, PostgreSQL's `now()` being the transaction's start, so a
//! registration stalled on the `backup_index` lock or the DDL commits a
//! stale beat that the reconciler under a maintenance claim and
//! `remove-target`, which judge the owner by the heartbeat alone, would
//! read as dead; the beat refreshes it at the registration's end on every
//! backend, and a beat that finds the claim lost or fails ends the task as
//! a lost claim before the reply, the record failed as the maintenance
//! refusal fails it and the request answered `InternalServerError`), then
//! checks the maintenance claim: held, the record is FAILED `maintenance`,
//! the target removed, and the request refused with
//! `LimitExceededException` (a job end: `RestoreJobs{failed}` and the worker
//! error are recorded); otherwise the request is answered CREATING and the
//! job runs ([`run`]). The heartbeat's first confirmed beat is that first
//! beat, and the instant it returned is what the pre-write check
//! (`heartbeat.rs`) counts the two periods from, so the registration's own
//! duration, however long, does not end the job at its first check. A
//! resumed job's instant is the takeover's return, taken by the reconciler
//! beside `claim_restore_dead` and passed in (the takeover stamped
//! `heartbeat_at` inside its write), so the reads between the takeover and
//! the job's start count against the window rather than widen it.
//!
//! The job: step 3, every data file downloaded and verified
//! ([`download`]: into the spool entry on an S3 store, hashed in place on a
//! filesystem store, the manifest and sidecar spooled first for a resume,
//! each as a data file is: `.partial`, synced, renamed; a resume that read
//! them from the spool does not write them again);
//! step 5, each file decoded, validated and written through one
//! `bulk_load` ([`load`]), skipping the files and items a resume has
//! recorded; the count check (`count_restore_target` against the manifest's
//! total, a shortfall being a key the backup holds twice); step 6,
//! `finish_restore` (DONE and ACTIVE, or DONE with the index backfill
//! pending on MongoDB). A `finish_restore` that returns an error is a
//! conditional write whose outcome is unknown, handled as `write_status`
//! handles one: the record is re-read through the claim; DONE under this
//! generation means the write landed and the restore is reported complete
//! with a WARN naming the error; RUNNING under this generation means it did
//! not, and the write is sent once more, at most, its own error re-read the
//! same way; anything else is the DONE write that matched no record. The
//! heartbeat runs on a timer for the job's life
//! and is checked before every download attempt, every `bulk_load`, the
//! count and the flip; a lost claim ends the job with no catalog or table
//! access. Step 7 ([`fail_restore`]): FAILED with the reason, then
//! `remove_restore_target` only when that write matched (the claim holder
//! owns the table); the spool entry is removed on every end but the
//! shutdown's and the ends at which the job learnt the record is another's
//! (a lost claim, a DONE or FAILED write that matched no record): a
//! same-host successor may be reading that entry, so it is left to the
//! reconciler's sweep, which lists the spool before it reads the records and
//! removes the entries it listed that no RUNNING record of this host names.
//! The shutdown token stops the job between download attempts,
//! files and chunks with no status write: the record stays RUNNING with its
//! spool and progress for a resume ([`resume`], the reconciler's entry).
//!
//! Each job records `RestoreJobs` and `RestoreJobDuration` under its
//! outcome, `RestoreBytesDownloaded` as the files arrive,
//! `BackupStoreErrors{get}` on every failed `get` of a restore job (the
//! request's and a resume's two document reads and each download attempt
//! that the store failed, whatever the store's error), and the worker
//! metrics under `QuerySource::RestoreJob` (`WorkerLastSuccess` on DONE,
//! `WorkerErrorCount` on FAILED or a job that ended without its write; a
//! stopped job records neither); one info line at start and end, one error
//! line per failure.

pub(crate) mod download;
pub(crate) mod load;
pub(crate) mod spool;

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::backup_format::{
    BackupArn, EXTENDDB_MANIFEST_FILE, ExtenddbManifest, ItemOrder, MANIFEST_CHECKSUM_FILE,
    verify_manifest_checksum,
};
use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::{BackupJobOutcome, QuerySource};
use extenddb_core::types::{BillingMode, CreateTableInput, TableDescription};
use extenddb_core::validation::{RestorableKeySchema, RestorableLimits};
use extenddb_storage::backup_catalog::{
    BackupKey, Heartbeat, JobStatus, RestoreJobRow, RestoreRegistrationRow, RestoreTargetKey,
    StatusFields, UNOWNED_GENERATION, billing_mode_name,
};
use extenddb_storage::backup_definition::{restore_input_from_manifest, vector_index_refusal};
use extenddb_storage::{
    BackupCatalog, CancellationToken, DataEngine, Finished, JobClaim, JobPermit, RestoreEngine,
    RestoreSource, RestoreTableOverrides,
};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::job::failure::{Written, write_status};
use super::job::heartbeat::{HeartbeatHandle, HeartbeatMetric};
use super::job::{JobEnv, KillPoint, MAINTENANCE, kill_point, kill_point_named};
use super::overrides::IndexOverrides;
use super::reconciler::{MANIFEST_READ_BOUND, ReadObject, SIDECAR_READ_BOUND};
use super::runtime::{BackupRuntime, ConfiguredStore};
use super::{
    backup_not_found, maintenance_held, restore_response, restore_storage_err_to_dynamo,
    storage_err_to_dynamo, validate_override_for_source,
};
use download::{Dest, Downloader, Stop};
use load::{FileLoad, Source};

/// How long the request waits for the detached task to register the target
/// before it answers `InternalServerError` and leaves the task to finish
/// (against a catalog that has stopped handing out connections). One value
/// on every backend.
pub const RESTORE_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(60);

/// The reason a resumed restore's record carries when no progress was
/// recorded and the target already holds items (a catalog restored from a
/// dump can name a restore that finished and was written to since): the
/// target is left for the operator (`extenddb backup remove-target`).
pub const TARGET_NOT_EMPTY: &str = "target_not_empty";

/// The reason the reconciler writes on a RUNNING record whose store is not
/// the configured one.
pub const OTHER_STORE: &str = "the restore reads a store other than the configured one; configure that store in [backup.store] and restore again";

/// What the request resolved before it spawned the task.
pub(crate) struct RestoreRequestParts {
    pub account_id: String,
    pub arn: BackupArn,
    pub backup: BackupKey,
    pub manifest: Arc<ExtenddbManifest>,
    pub manifest_bytes: Vec<u8>,
    pub sidecar_bytes: Vec<u8>,
    pub input: CreateTableInput,
    /// The key schema the validator holds every item to.
    pub key_schema: RestorableKeySchema,
    pub limits: RestorableLimits,
    /// This instance's in-flight entry for the registration, which the task
    /// ends with the registration's outcome; `None` for a caller outside
    /// the request path.
    pub registration: Option<super::runtime::RegistrationGuard>,
}

/// The one-shot the task answers the request through.
pub(crate) type Reply = oneshot::Sender<Result<TableDescription, DynamoDbError>>;

/// The request's manifest branch (module doc), from the permit to the
/// response body, for a row `check_catalog_row_for_restore` accepted whose
/// store is the healthy configured one.
#[allow(clippy::too_many_arguments)]
pub(super) async fn request<S>(
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    store: &Arc<ConfiguredStore>,
    account_id: &str,
    limits: &LimitsConfig,
    target_table_name: &str,
    arn: &BackupArn,
    overrides: &RestoreTableOverrides,
    index_overrides: &IndexOverrides,
) -> Result<(String, Value), DynamoDbError>
where
    S: BackupCatalog + DataEngine + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    let arn_text = arn.to_arn_string();
    let env = JobEnv::new(runtime, store)?;
    // The permit, last among the checks that need no store access.
    let permit = runtime.take_permit().await?;

    // The sidecar and the manifest; a missing one behind an AVAILABLE row is
    // BackupNotFound and the operator's line; one over its bound is a
    // prefix nothing this server wrote produces.
    let prefix = arn.store_prefix();
    let read = async |name: &str, bound: u64| -> Result<Vec<u8>, DynamoDbError> {
        match super::reconciler::read_object(&*env.store, &format!("{prefix}{name}"), bound).await {
            Ok(ReadObject::Bytes(bytes)) => Ok(bytes),
            Ok(ReadObject::Absent) if name == EXTENDDB_MANIFEST_FILE => {
                tracing::error!(
                    "backup {arn_text} (account {account_id}): its row is AVAILABLE but the store \
                     holds no {name} under its prefix; put the prefix back, or, unless it is the \
                     store's newest backup (whose missing manifest makes the store unhealthy), \
                     remove the row with backup sync --prune-missing"
                );
                Err(backup_not_found(&arn_text))
            }
            Ok(ReadObject::Absent) => {
                tracing::error!(
                    "backup {arn_text} (account {account_id}): its row is AVAILABLE but the store \
                     holds no {name} under its prefix; put the prefix back (backup sync \
                     --prune-missing removes only a row whose manifest is gone)"
                );
                Err(backup_not_found(&arn_text))
            }
            Ok(ReadObject::Oversize) => Err(DynamoDbError::ValidationException(format!(
                "Backup {arn_text} has a {name} over {bound} bytes, which no backup this server \
                 wrote produces; the backup's prefix is corrupt"
            ))),
            Err(e) => {
                if let Some(metrics) = runtime.metrics() {
                    metrics.record_backup_store_error("get");
                }
                tracing::error!("backup {arn_text}: reading {name} from the store failed: {e}");
                Err(DynamoDbError::InternalServerError(
                    "Internal server error".to_owned(),
                ))
            }
        }
    };
    let sidecar_bytes = read(MANIFEST_CHECKSUM_FILE, SIDECAR_READ_BOUND).await?;
    let manifest_bytes = read(EXTENDDB_MANIFEST_FILE, MANIFEST_READ_BOUND).await?;
    verify_manifest_checksum(&manifest_bytes, &sidecar_bytes).map_err(|e| {
        DynamoDbError::ValidationException(format!(
            "Backup {arn_text} has a manifest whose checksum sidecar does not match: {e}"
        ))
    })?;
    // The parser ran the version gate and the identity rule (the ARN's
    // table is `source_table.table_name`); what is left is that the
    // manifest's backup is the row's.
    let manifest = ExtenddbManifest::from_json_bytes(&manifest_bytes).map_err(|e| {
        DynamoDbError::ValidationException(format!(
            "Backup {arn_text} has a manifest this server cannot restore from: {e}"
        ))
    })?;
    match manifest.backup_arn() {
        Ok(named) if named == *arn => {}
        Ok(named) => {
            return Err(DynamoDbError::ValidationException(format!(
                "Backup {arn_text} has a manifest that names another backup, {}",
                named.to_arn_string()
            )));
        }
        Err(e) => {
            return Err(DynamoDbError::ValidationException(format!(
                "Backup {arn_text} has a manifest whose backup ARN is refused: {e}"
            )));
        }
    }

    // Step 2: the one feature a target can lack.
    let source_table = &manifest.source_table;
    if crate::vector_gate::ensure_create_table_supported(
        source_table.vector_indexes.as_ref(),
        storage.as_vector_search(),
    )
    .is_err()
    {
        let count = source_table.vector_indexes.as_ref().map_or(0, Vec::len);
        return Err(storage_err_to_dynamo(vector_index_refusal(
            &format!("backup {arn_text}"),
            count,
        )));
    }

    // The target's input, with the overrides, validated as CreateTable does.
    validate_override_for_source(overrides, billing_mode_name(source_table.billing_mode))?;
    let mut input = restore_input_from_manifest(source_table, target_table_name, overrides);
    index_overrides.apply(&mut input, overrides.billing_mode)?;
    extenddb_core::validation::validate_create_table(&input, limits)?;
    // The key schema the validator holds every item to: a pure function of
    // the manifest, resolved here with step 2's refusals rather than after
    // the client was told the target is CREATING.
    let key_schema = RestorableKeySchema::resolve(
        &source_table.key_schema,
        &source_table.attribute_definitions,
    )
    .map_err(|e| {
        DynamoDbError::ValidationException(format!(
            "Backup {arn_text} has a manifest whose key schema cannot be checked: {e}"
        ))
    })?;
    let on_demand = input.billing_mode == Some(BillingMode::PayPerRequest);
    let source = RestoreSource {
        backup_arn: arn_text.clone(),
        source_table_arn: source_table.table_arn.clone(),
        backup_creation_date_time: manifest.backup_creation_date_time,
    };

    // Step 4 in the detached task; the request waits for its answer. A
    // registration of this instance already in flight for the name is met
    // here: the same backup's is this request's own, retried by a client
    // whose read timeout (botocore's default is this bound, 60 s) ended its
    // first attempt while the registration waited on the `backup_index`
    // lock or the DDL, and the retry waits for that registration's answer
    // rather than registering a second target or being refused the name;
    // another backup's is `TableInUseException`, as a committed registration
    // answers it.
    let guard = match runtime.begin_restore_registration(account_id, target_table_name, &arn_text) {
        Ok(guard) => guard,
        Err(in_flight) => {
            drop(permit);
            if in_flight.backup_arn != arn_text {
                return Err(restore_storage_err_to_dynamo(
                    extenddb_storage::error::StorageError::TableBeingRestored {
                        table_name: target_table_name.to_owned(),
                        backup_arn: in_flight.backup_arn,
                    },
                ));
            }
            tracing::info!(
                "RestoreTableFromBackup of {arn_text} into {target_table_name} (account \
                 {account_id}): a registration of this request is in flight; this request waits \
                 for its answer"
            );
            let desc =
                await_registration(in_flight, &arn_text, target_table_name, account_id).await?;
            let response = restore_response(desc, &source, on_demand)?;
            return Ok((target_table_name.to_owned(), response));
        }
    };
    let parts = RestoreRequestParts {
        account_id: account_id.to_owned(),
        arn: arn.clone(),
        backup: BackupKey::from_arn(arn),
        manifest: Arc::new(manifest),
        manifest_bytes,
        sidecar_bytes,
        input,
        key_schema,
        limits: limits.into(),
        registration: Some(guard),
    };
    let (reply, answer) = oneshot::channel();
    runtime.spawn_job(start(Arc::clone(storage), env, permit, parts, reply));
    let desc = match tokio::time::timeout(RESTORE_REGISTRATION_TIMEOUT, answer).await {
        Ok(Ok(answer)) => answer?,
        Ok(Err(_)) => {
            tracing::error!(
                "RestoreTableFromBackup of {arn_text} into {target_table_name} (account \
                 {account_id}): the job task ended without answering"
            );
            return Err(DynamoDbError::InternalServerError(
                "Internal server error".to_owned(),
            ));
        }
        Err(_) => {
            tracing::error!(
                "RestoreTableFromBackup of {arn_text} into {target_table_name} (account \
                 {account_id}): the registration did not answer within {} s; the task goes on \
                 and the target, if registered, appears through DescribeTable",
                RESTORE_REGISTRATION_TIMEOUT.as_secs()
            );
            return Err(DynamoDbError::InternalServerError(
                "Internal server error".to_owned(),
            ));
        }
    };
    let response = restore_response(desc, &source, on_demand)?;
    Ok((target_table_name.to_owned(), response))
}

/// A retry's wait on its own registration in flight (module doc): the
/// registration's outcome, for at most [`RESTORE_REGISTRATION_TIMEOUT`]
/// from this request's arrival; a registration that returned no outcome
/// (its task ended) or none within the bound is `InternalServerError`, as
/// the registering request's own bound is.
async fn await_registration(
    mut in_flight: super::runtime::InFlightRegistration,
    arn_text: &str,
    target_table_name: &str,
    account_id: &str,
) -> Result<TableDescription, DynamoDbError> {
    let internal = || DynamoDbError::InternalServerError("Internal server error".to_owned());
    let outcome = async {
        loop {
            if let Some(outcome) = in_flight.outcome.borrow_and_update().clone() {
                return Some(outcome);
            }
            if in_flight.outcome.changed().await.is_err() {
                return in_flight.outcome.borrow().clone();
            }
        }
    };
    match tokio::time::timeout(RESTORE_REGISTRATION_TIMEOUT, outcome).await {
        Ok(Some(outcome)) => outcome,
        Ok(None) => {
            tracing::error!(
                "RestoreTableFromBackup of {arn_text} into {target_table_name} (account \
                 {account_id}): the registration this request waited on ended without answering"
            );
            Err(internal())
        }
        Err(_) => {
            tracing::error!(
                "RestoreTableFromBackup of {arn_text} into {target_table_name} (account \
                 {account_id}): the registration this request waited on did not answer within {} \
                 s; it goes on and the target, if registered, appears through DescribeTable",
                RESTORE_REGISTRATION_TIMEOUT.as_secs()
            );
            Err(internal())
        }
    }
}

/// The detached task: the registration, the maintenance check, the reply,
/// then the job.
pub(crate) async fn start<S>(
    storage: Arc<S>,
    env: JobEnv,
    permit: JobPermit,
    parts: RestoreRequestParts,
    reply: Reply,
) where
    S: BackupCatalog + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    let started = Instant::now();
    let RestoreRequestParts {
        account_id,
        arn,
        backup,
        manifest,
        manifest_bytes,
        sidecar_bytes,
        input,
        key_schema,
        limits,
        registration,
    } = parts;
    let arn_text = arn.to_arn_string();
    let target_table_name = input.table_name.clone();
    let spool_host = env.spool.as_ref().map(|s| s.spool_host.clone());
    let registration_row = RestoreRegistrationRow {
        backup,
        source_table_arn: manifest.source_table.table_arn.clone(),
        restore_date_time: manifest.backup_creation_date_time,
        store: env.identity.clone(),
        spool_host: spool_host.clone(),
        owner_generation: UNOWNED_GENERATION,
    };
    let registered = storage
        .claim_restore(&account_id, input, &registration_row)
        .await;
    // The in-flight entry ends with the reply, for the retries waiting on
    // it: the description, or the error the request itself is answered.
    let (desc, claim) = match registered {
        Ok(registered) => registered,
        Err(e) => {
            drop(permit);
            let error = restore_storage_err_to_dynamo(e);
            if let Some(registration) = registration {
                registration.finish(Err(error.clone()));
            }
            if let Err(Err(error)) = reply.send(Err(error)) {
                // The request has answered `InternalServerError` at its bound
                // and told the operator the target appears if registered;
                // this line is the one trace of why it will not.
                tracing::error!(
                    "restore of {arn_text} into {target_table_name} (account {account_id}): the \
                     registration failed after the request had stopped waiting for it; no target \
                     was created: {error}"
                );
            }
            return;
        }
    };
    // The job's first beat, before the reply: the record's `heartbeat_at` as
    // the registration stamped it is the catalog's clock at some point of
    // the registration (on PostgreSQL `now()` is the transaction's start, so
    // a registration stalled on the `backup_index` lock or the DDL commits a
    // stale beat), and the two readers that judge the owner by the heartbeat
    // alone (the reconciler under a maintenance claim, `remove-target`)
    // would find a live job dead until its timer's first beat. A confirmed
    // beat refreshes it at the registration's end on every backend, and its
    // return is the instant the heartbeat's two-period rule counts from. A
    // beat that finds the claim lost, or fails, ends the task as a lost
    // claim before the reply: the record is failed as the maintenance
    // refusal fails it (the write matches nothing when the claim is
    // another's) and the request is answered `InternalServerError`.
    let first_beat = match claim.heartbeat().await {
        Ok(Heartbeat::Confirmed) => Ok(Instant::now()),
        Ok(Heartbeat::Lost) => Err("the claim was lost before the job started".to_owned()),
        Err(e) => Err(format!(
            "the claim was lost before the job started: its first heartbeat failed: {e}"
        )),
    };
    let claim: Arc<dyn JobClaim> = Arc::from(claim);
    let target = RestoreTargetKey {
        account_id: account_id.clone(),
        table_name: desc.table_name.clone(),
        table_id: desc.table_id.clone(),
    };
    // The maintenance claim, once the record exists so the command's wait
    // sees it: held, the record is FAILED `maintenance`, the target removed
    // and the request refused.
    let (claimed_at, refused) = match first_beat {
        Err(reason) => (
            started,
            Some((
                reason,
                DynamoDbError::InternalServerError("Internal server error".to_owned()),
            )),
        ),
        Ok(beat) => (
            beat,
            match storage
                .check_maintenance_claim(env.settings.stale_job_after)
                .await
            {
                Ok(false) => None,
                Ok(true) => Some((MAINTENANCE.to_owned(), maintenance_held())),
                Err(e) => Some((
                    format!("the maintenance claim could not be read: {e}"),
                    storage_err_to_dynamo(e),
                )),
            },
        ),
    };
    if let Some((reason, error)) = refused {
        let path = fail_restore(&*storage, &*claim, &target, &reason, None, &env.shutdown).await;
        tracing::error!(
            "restore of {arn_text} into {target_table_name} (account {account_id}): FAILED \
             ({reason}) before it started; {path}"
        );
        // A FAILED record is a job end: the outcome and the worker error.
        if let Some(metrics) = env.metrics.as_deref() {
            #[allow(clippy::cast_precision_loss)]
            let duration_us = started.elapsed().as_micros() as f64;
            metrics.record_restore_job(BackupJobOutcome::Failed, duration_us);
            metrics.record_worker_error(QuerySource::RestoreJob);
        }
        drop(permit);
        if let Some(registration) = registration {
            registration.finish(Err(error.clone()));
        }
        let _ = reply.send(Err(error));
        return;
    }
    // A request whose client went away still has its job.
    if let Some(registration) = registration {
        registration.finish(Ok(desc.clone()));
    }
    let _ = reply.send(Ok(desc));
    let job = RestoreJob {
        storage,
        claim,
        target,
        arn,
        manifest,
        manifest_bytes,
        sidecar_bytes,
        key_schema,
        limits,
        env,
        started,
        claimed_at,
        progress: None,
        recorded_spool_host: spool_host,
        documents_from_spool: false,
    };
    run(job).await;
    drop(permit);
}

/// Per file, what an earlier run recorded as applied; `None` for a file
/// with no record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Progress {
    recorded: Vec<Option<u64>>,
}

impl Progress {
    /// From a dead owner's record and, on PostgreSQL, its progress rows:
    /// every file before the record's `file_index` is complete, `file_index`
    /// itself has `items_applied` of its items (all of them when the backend
    /// records per file), and each progress row names its file's count.
    pub(crate) fn from_records(
        row: &RestoreJobRow,
        progress_rows: &[(u32, u64)],
        manifest: &ExtenddbManifest,
    ) -> Self {
        let counts: Vec<u64> = manifest.data_files.iter().map(|f| f.item_count).collect();
        let mut recorded = vec![None; counts.len()];
        if let Some(last) = row.file_index {
            let last = last as usize;
            for (i, slot) in recorded.iter_mut().enumerate().take(last.min(counts.len())) {
                *slot = Some(counts[i]);
            }
            if let Some(slot) = recorded.get_mut(last) {
                *slot = Some(row.items_applied.unwrap_or(counts[last]));
            }
        }
        for (index, applied) in progress_rows {
            if let Some(slot) = recorded.get_mut(*index as usize) {
                *slot = Some((*applied).max(slot.unwrap_or(0)));
            }
        }
        Self { recorded }
    }

    /// The first file not complete, for the start line.
    fn first_open(&self, counts: &[u64]) -> Option<(usize, u64)> {
        self.recorded
            .iter()
            .enumerate()
            .find_map(|(i, recorded)| match recorded {
                Some(n) if *n >= counts[i] => None,
                Some(n) => Some((i, *n)),
                None => Some((i, 0)),
            })
    }

    /// Per file, the items recorded as applied; `None` for a file with no
    /// record.
    #[cfg(test)]
    pub(crate) fn recorded(&self) -> &[Option<u64>] {
        &self.recorded
    }

    /// [`Self::first_open`] against `manifest`'s counts.
    #[cfg(test)]
    pub(crate) fn first_open_in(&self, manifest: &ExtenddbManifest) -> Option<(usize, u64)> {
        let counts: Vec<u64> = manifest.data_files.iter().map(|f| f.item_count).collect();
        self.first_open(&counts)
    }
}

/// The job once its record exists.
pub(crate) struct RestoreJob<S: ?Sized> {
    storage: Arc<S>,
    claim: Arc<dyn JobClaim>,
    target: RestoreTargetKey,
    arn: BackupArn,
    manifest: Arc<ExtenddbManifest>,
    manifest_bytes: Vec<u8>,
    sidecar_bytes: Vec<u8>,
    key_schema: RestorableKeySchema,
    limits: RestorableLimits,
    env: JobEnv,
    started: Instant,
    /// The instant the heartbeat's two-period rule counts from: the return
    /// of the job's first beat, sent right after the registration, for a
    /// fresh job; the takeover's return for a resume (the takeover stamped
    /// the record's `heartbeat_at` inside its write).
    claimed_at: Instant,
    /// A resumed job's recorded progress; `None` for a fresh one.
    progress: Option<Progress>,
    /// The `spool_host` the record carried before this job's claim: the
    /// registration's own for a fresh job, the dead owner's for a resume.
    /// A spool entry of this table id is this job's to reuse only when it
    /// is this host's.
    recorded_spool_host: Option<String>,
    /// Whether the manifest and sidecar in hand were read from this host's
    /// spool entry (a resume), in which case the job does not write them
    /// there again.
    documents_from_spool: bool,
}

/// The job (module doc), from the download to the final write.
pub(crate) async fn run<S>(job: RestoreJob<S>) -> BackupJobOutcome
where
    S: BackupCatalog + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    let RestoreJob {
        storage,
        claim,
        target,
        arn,
        manifest,
        manifest_bytes,
        sidecar_bytes,
        key_schema,
        limits,
        env,
        started,
        claimed_at,
        progress,
        recorded_spool_host,
        documents_from_spool,
    } = job;
    let arn_text = arn.to_arn_string();
    let name = format!(
        "restore of {arn_text} into {} (account {})",
        target.table_name, target.account_id
    );
    let metrics = env.metrics.as_deref();
    let heartbeat = HeartbeatHandle::start(
        Arc::clone(&claim),
        env.settings.heartbeat_interval,
        HeartbeatMetric::None,
        claimed_at,
    );
    #[cfg(test)]
    test_support::register_timer(&target.table_id, &heartbeat);
    let counts: Vec<u64> = manifest.data_files.iter().map(|f| f.item_count).collect();
    let total_items: u64 = counts.iter().sum();
    let resumed_from = progress.as_ref().and_then(|p| p.first_open(&counts));
    // The files a resume's progress records as complete: neither
    // downloaded nor loaded again.
    let complete: Vec<bool> = counts
        .iter()
        .enumerate()
        .map(|(i, count)| {
            progress
                .as_ref()
                .and_then(|p| p.recorded[i])
                .is_some_and(|n| n >= *count)
        })
        .collect();
    tracing::info!(
        "{name}: started, {} data file(s), {total_items} items, {}{}",
        manifest.data_files.len(),
        match manifest.item_order {
            ItemOrder::KeyOrdered => "key-ordered",
            ItemOrder::Unordered => "unordered",
        },
        match (&progress, resumed_from) {
            (None, _) => String::new(),
            (Some(_), Some((file, items))) =>
                format!("; resumed from data file {file} after {items} of its items"),
            (Some(_), None) => "; resumed with every file recorded".to_owned(),
        }
    );
    let prefix = arn.store_prefix();
    let mut entry = None;
    let mut bytes_downloaded = 0;
    let mut applied_total = 0;
    let end: Result<Ended, Stop> = async {
        // Step 3. The entry is the job's from the open, so a document write
        // that fails (the disk full, a name taken by a directory) reaches
        // the failure path with the entry in hand and it is removed as any
        // other failed end's is.
        if let Some(spool) = &env.spool {
            let opened = spool::Entry::open(
                &spool.path,
                &target.table_id,
                recorded_spool_host.as_deref(),
                &spool.spool_host,
            )
            .map_err(|e| Stop::Fail(format!("opening the spool entry: {e}")))?;
            let opened = entry.insert(opened);
            if !documents_from_spool {
                for (doc, bytes) in [
                    (EXTENDDB_MANIFEST_FILE, &manifest_bytes),
                    (MANIFEST_CHECKSUM_FILE, &sidecar_bytes),
                ] {
                    opened
                        .write_document(doc, bytes)
                        .await
                        .map_err(|e| Stop::Fail(format!("writing {doc} to the spool: {e}")))?;
                }
            }
        }
        let dest = entry.as_ref().map_or(Dest::InPlace, Dest::Spool);
        let downloader = Downloader {
            store: &*env.store,
            prefix: &prefix,
            heartbeat: &heartbeat,
            shutdown: &env.shutdown,
            metrics,
        };
        bytes_downloaded = download::download_all(&downloader, &manifest, &dest, &complete).await?;
        kill_point(KillPoint::RestoreAfterDownload, &env.shutdown).await;

        // Step 5.
        #[cfg(not(test))]
        let chunk_bytes = load::LOAD_CHUNK_BYTES;
        #[cfg(test)]
        let chunk_bytes = test_support::chunk_bytes_for(&target.table_id);
        let mut first_loaded = true;
        let mut reapplying = progress.is_some();
        for (i, file) in manifest.data_files.iter().enumerate() {
            let recorded = progress.as_ref().and_then(|p| p.recorded[i]);
            let start_at = match recorded {
                Some(n) if n >= file.item_count => continue,
                Some(n) => n,
                None => 0,
            };
            if env.shutdown.is_cancelled() {
                return Err(Stop::Cancelled);
            }
            if !heartbeat.confirms_the_claim() {
                return Err(Stop::ClaimLost(
                    heartbeat.why_not_confirmed(&format!("to load {}", file.key)),
                ));
            }
            let file_index =
                u32::try_from(i).map_err(|_| Stop::Fail("too many data files".to_owned()))?;
            let source = match &entry {
                Some(entry) => Source::Spool(entry.file_path(file_index)),
                None => Source::InPlace {
                    store: Arc::clone(&env.store),
                    key: format!("{prefix}{}", file.key),
                },
            };
            let applied = load::load_file(
                &*storage,
                &target,
                &*claim,
                FileLoad {
                    manifest: &manifest,
                    entry: file,
                    file_index,
                    start_at,
                    reapplying,
                    key_schema: &key_schema,
                    limits,
                    shutdown: &env.shutdown,
                    heartbeat: &heartbeat,
                    chunk_bytes,
                },
                source,
            )
            .await?;
            applied_total += applied;
            // Only the first file a resume meets may have been applied in
            // part without a record.
            reapplying = false;
            if first_loaded {
                first_loaded = false;
                kill_point(KillPoint::RestoreAfterFileCommit, &env.shutdown).await;
            }
        }

        // The count: every restore ends with it, so a backend that kept one
        // of two rows with one key, and a key-range check that missed, are
        // both caught. A count that fails is retried; one that keeps
        // failing stops the job with the record RUNNING, since the load is
        // complete and nothing of it is in doubt (on Cassandra the count is a
        // full `COUNT(*)`, which a node can time out), and the reconciler
        // resumes the job once the record is stale.
        if env.shutdown.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        if !heartbeat.confirms_the_claim() {
            return Err(Stop::ClaimLost(
                heartbeat.why_not_confirmed("to count the target"),
            ));
        }
        let count = count_target(&*storage, &target, &heartbeat, &env.shutdown).await?;
        if count != total_items {
            return Err(Stop::Fail(format!(
                "restored {count} items, the manifest lists {total_items}: {}",
                if count < total_items {
                    "the backup holds a key twice"
                } else {
                    "the target holds items the backup does not account for"
                }
            )));
        }

        // Step 6. The fenced-out kill point stops the heartbeat first, so a
        // reconciler can take the record over while the job pauses, and the
        // pre-write check is skipped so the conditional write itself is what
        // the test observes.
        let fenced = kill_point_named(KillPoint::RestoreFencedOut);
        if fenced {
            heartbeat.stop_timer();
        }
        kill_point(KillPoint::RestoreFencedOut, &env.shutdown).await;
        if env.shutdown.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        if !fenced && !heartbeat.confirms_the_claim() {
            return Err(Stop::ClaimLost(
                heartbeat.why_not_confirmed("for the DONE write"),
            ));
        }
        // The restored table's ItemCount and TableSizeBytes are 0 at ACTIVE,
        // as the service reports them (measured: 0 and 0 on 15 restored
        // tables, one of them 10 minutes after it went ACTIVE); the
        // statistics pass rewrites them later where the backend has one.
        finish(&*storage, &*claim, &target, 0, 0).await
    }
    .await;
    let elapsed = started.elapsed().as_secs_f64();
    let (outcome, remove_entry) = match end {
        Ok(Ended::Written(Finished::Active)) => {
            tracing::info!(
                "{name}: ACTIVE, {total_items} items ({applied_total} applied by this run), \
                 {bytes_downloaded} bytes downloaded, in {elapsed:.1} s"
            );
            (BackupJobOutcome::Succeeded, true)
        }
        Ok(Ended::Written(Finished::Pending)) => {
            tracing::info!(
                "{name}: DONE, {total_items} items ({applied_total} applied by this run), \
                 {bytes_downloaded} bytes downloaded, in {elapsed:.1} s; index backfill pending, \
                 the backend's worker flips the table ACTIVE"
            );
            kill_point(KillPoint::RestoreMongodbBeforeWorkerFlip, &env.shutdown).await;
            (BackupJobOutcome::Succeeded, true)
        }
        // The write landed and its answer did not come back: the table is
        // ACTIVE, or awaits the backend's worker, which this job did not see.
        Ok(Ended::DoneOnReRead) => {
            tracing::info!(
                "{name}: complete (DONE), {total_items} items ({applied_total} applied by this \
                 run), {bytes_downloaded} bytes downloaded, in {elapsed:.1} s; the DONE write's \
                 answer was lost and the record was re-read DONE under this job's generation (the \
                 table is ACTIVE, or the backend's worker flips it)"
            );
            (BackupJobOutcome::Succeeded, true)
        }
        // The record is another's: its owner may be reading this host's
        // entry, so the entry is left to the sweep.
        Ok(Ended::Written(Finished::NotMatched(why))) => {
            tracing::error!(
                "{name}: the DONE write matched no record ({why}); the record is not this job's \
                 to write and no table access was made"
            );
            (BackupJobOutcome::Failed, false)
        }
        Err(Stop::ClaimLost(why)) => {
            tracing::error!(
                "{name}: stopped with no further catalog or table access: {why}; the record is \
                 left to its owner or to the reconciler"
            );
            (BackupJobOutcome::Failed, false)
        }
        // The load is complete and a read after it kept failing: no FAILED
        // write, the record stays RUNNING with its progress for a resume.
        Err(Stop::LeftRunning(why)) => {
            tracing::error!(
                "{name}: stopped with the record RUNNING and no FAILED written: {why}; the \
                 reconciler resumes it once the record is stale"
            );
            (BackupJobOutcome::Failed, false)
        }
        Err(Stop::Cancelled) => {
            tracing::info!(
                "{name}: stopped by the server's shutdown; the record stays RUNNING with its \
                 spool and its progress for a resume"
            );
            (BackupJobOutcome::Cancelled, false)
        }
        Err(Stop::Fail(reason)) => {
            let path = fail_restore(
                &*storage,
                &*claim,
                &target,
                &reason,
                entry.as_ref(),
                &env.shutdown,
            )
            .await;
            tracing::error!("{name}: FAILED ({reason}); {path}");
            // `fail_restore` removed the entry, or left it to the sweep.
            (BackupJobOutcome::Failed, false)
        }
    };
    if remove_entry && let Some(entry) = &entry {
        remove_spool_entry(entry).await;
    }
    // After the failure path too, so a FAILED job's duration covers its
    // FAILED write and the target's removal.
    let duration = started.elapsed();
    drop(heartbeat);
    if let Some(metrics) = metrics {
        #[allow(clippy::cast_precision_loss)]
        let duration_us = duration.as_micros() as f64;
        metrics.record_restore_job(outcome, duration_us);
        match outcome {
            BackupJobOutcome::Succeeded => {
                metrics.record_worker_success(QuerySource::RestoreJob, duration_us);
            }
            BackupJobOutcome::Failed => metrics.record_worker_error(QuerySource::RestoreJob),
            // A clean shutdown is not a worker error.
            BackupJobOutcome::Cancelled => {}
        }
    }
    outcome
}

/// How many times the final count is attempted before the job stops with
/// the record RUNNING.
const COUNT_ATTEMPTS: u32 = 3;

/// The final count (module doc): `count_restore_target`, retried after a
/// short wait on an error, [`COUNT_ATTEMPTS`] times in all, the shutdown
/// token and the heartbeat read before each attempt; an error on the last
/// attempt is [`Stop::LeftRunning`], never a FAILED write, since every item
/// is loaded and recorded and the reconciler's resume counts again.
async fn count_target<S: RestoreEngine + ?Sized>(
    storage: &S,
    target: &RestoreTargetKey,
    heartbeat: &HeartbeatHandle,
    shutdown: &CancellationToken,
) -> Result<u64, Stop> {
    for attempt in 1..=COUNT_ATTEMPTS {
        if attempt > 1 {
            if shutdown.is_cancelled() {
                return Err(Stop::Cancelled);
            }
            if !heartbeat.confirms_the_claim() {
                return Err(Stop::ClaimLost(
                    heartbeat.why_not_confirmed("to count the target again"),
                ));
            }
        }
        match storage.count_restore_target(target).await {
            Ok(count) => return Ok(count),
            Err(e) if attempt == COUNT_ATTEMPTS => {
                return Err(Stop::LeftRunning(format!(
                    "counting the target's items failed {COUNT_ATTEMPTS} times, the last: {e}"
                )));
            }
            Err(e) => {
                let wait = Duration::from_secs(u64::from(attempt));
                tracing::warn!(
                    "counting the target's items failed (attempt {attempt} of \
                     {COUNT_ATTEMPTS}); retried in {} s: {e}",
                    wait.as_secs()
                );
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Err(Stop::Cancelled),
                    () = heartbeat.lost().cancelled() => return Err(Stop::ClaimLost(heartbeat.reason())),
                    () = tokio::time::sleep(wait) => {}
                }
            }
        }
    }
    unreachable!("the attempt loop returns on its last attempt")
}

/// What step 6 settled to.
enum Ended {
    /// The backend's own answer to the DONE write.
    Written(Finished),
    /// The DONE write's answer was lost and the record re-read DONE under
    /// this generation: the restore is complete; whether the table is ACTIVE
    /// or awaits the backend's worker (MongoDB) is a fact the backend holds
    /// and this job did not see.
    DoneOnReRead,
}

/// Step 6's write (module doc): `finish_restore`, with an error from it
/// handled as a conditional write whose outcome is unknown. The record is
/// re-read through the claim: DONE under this generation is the write
/// landed ([`Ended::DoneOnReRead`], with a WARN naming the error), RUNNING
/// under this generation is the write not taken, sent once more at most;
/// another generation, another status, a failed re-read, or a second error
/// after a RUNNING re-read is the DONE write that matched no record, after
/// which the job makes no table access.
async fn finish<S: RestoreEngine + ?Sized>(
    storage: &S,
    claim: &dyn JobClaim,
    target: &RestoreTargetKey,
    item_count: u64,
    size_bytes: u64,
) -> Result<Ended, Stop> {
    let not_matched = |why: String| Ok(Ended::Written(Finished::NotMatched(why)));
    for attempt in 0..2 {
        let error = match storage
            .finish_restore(target, claim, item_count, size_bytes)
            .await
        {
            Ok(finished) => return Ok(Ended::Written(finished)),
            Err(e) => e,
        };
        let row = match claim.read().await {
            Ok(row) => row,
            Err(e) => {
                return not_matched(format!(
                    "the DONE write failed ({error}) and the record could not be re-read: {e}"
                ));
            }
        };
        if row.owner_generation != claim.generation() {
            return not_matched(format!(
                "the DONE write failed ({error}) and the record carries generation {} (this \
                 claim's is {})",
                row.owner_generation,
                claim.generation()
            ));
        }
        if row.status == JobStatus::Done {
            tracing::warn!(
                "the DONE write returned an error ({error}) and the record is DONE under this \
                 job's generation: the write landed and the restore is complete"
            );
            return Ok(Ended::DoneOnReRead);
        }
        if row.status != JobStatus::Running {
            return not_matched(format!(
                "the DONE write failed ({error}) and the record is {} under this claim",
                row.status
            ));
        }
        if attempt == 1 {
            return not_matched(format!(
                "the DONE write failed twice ({error}) and the record is still RUNNING under \
                 this claim; the reconciler resumes it once the record is stale"
            ));
        }
        tracing::warn!(
            "the DONE write returned an error ({error}) and the record is still RUNNING under \
             this claim; writing once more"
        );
    }
    unreachable!("the loop returns on its second attempt")
}

/// The end of the restore's failure path.
#[derive(Debug)]
pub(crate) enum RestoreFailurePath {
    /// FAILED written and the target removed.
    Removed,
    /// FAILED written; the target was no longer a CREATING restore target
    /// with this id and belongs to whoever moved it.
    LeftToOwner,
    /// FAILED written; the removal failed and the reconciler's FAILED-target
    /// duty removes the target once the record is stale.
    RemoveFailed(String),
    /// FAILED not written by this claim (the record is another's, or the
    /// claim connection failed): no table access was made.
    NoTableAccess(String),
}

impl std::fmt::Display for RestoreFailurePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Removed => f.write_str("the target was removed"),
            Self::LeftToOwner => f.write_str(
                "the target was no longer a CREATING restore target with this id and is left to \
                 its owner",
            ),
            Self::RemoveFailed(e) => write!(
                f,
                "the target could not be removed and the reconciler removes it once the record \
                 is stale: {e}"
            ),
            Self::NoTableAccess(why) => write!(
                f,
                "the record is not this job's to write ({why}); no table access was made"
            ),
        }
    }
}

/// Step 7 (module doc): FAILED with `reason` through `claim`, then
/// `remove_restore_target` only when that write matched; the spool entry,
/// when there is one, removed when the write matched and left to the sweep
/// when it did not (the record is another's, whose job may be reading the
/// entry).
pub(crate) async fn fail_restore<S: BackupCatalog + ?Sized>(
    storage: &S,
    claim: &dyn JobClaim,
    target: &RestoreTargetKey,
    reason: &str,
    entry: Option<&spool::Entry>,
    shutdown: &CancellationToken,
) -> RestoreFailurePath {
    let fields = StatusFields {
        failure_reason: Some(reason.to_owned()),
        completed: true,
        ..StatusFields::default()
    };
    let path = match write_status(claim, JobStatus::Running, JobStatus::Failed, &fields).await {
        Written::Matched => {
            kill_point(KillPoint::RestoreBetweenFailedAndRemoval, shutdown).await;
            match storage.remove_restore_target(target).await {
                Ok(true) => RestoreFailurePath::Removed,
                Ok(false) => RestoreFailurePath::LeftToOwner,
                Err(e) => RestoreFailurePath::RemoveFailed(e.to_string()),
            }
        }
        Written::NotMatched(why) | Written::Error(why) => RestoreFailurePath::NoTableAccess(why),
    };
    if let (Some(entry), false) = (entry, matches!(path, RestoreFailurePath::NoTableAccess(_))) {
        remove_spool_entry(entry).await;
    }
    path
}

async fn remove_spool_entry(entry: &spool::Entry) {
    if let Err(e) = entry.remove().await {
        tracing::error!(
            "the spool entry {} could not be removed; the reconciler's sweep retries: {e}",
            entry.dir().display()
        );
    }
}

/// A dead owner's record as the reconciler hands it to [`resume`].
pub(crate) struct DeadRecord {
    /// The record as read under the takeover's claim.
    pub row: RestoreJobRow,
    /// The `spool_host` the record carried before the takeover (which wrote
    /// this host's): whose spool entry, if any, holds the files.
    pub recorded_spool_host: Option<String>,
    /// The progress rows a backend keeps apart from the record.
    pub progress_rows: Vec<(u32, u64)>,
    /// The instant `claim_restore_dead` returned: the resumed job's first
    /// confirmed beat (module doc).
    pub claimed_at: Instant,
}

/// The reconciler's entry: a dead owner's RUNNING record taken over
/// (`claim`), resumed from its progress (the record and, on PostgreSQL, its
/// progress rows). The manifest and sidecar are read from this host's spool
/// entry when the record named this host before the takeover and both are
/// there and verify, else from the store; either way they are verified and
/// must name the record's backup. A manifest that cannot be read or is
/// refused fails the restore (step 7), and this host's entry, when the
/// record named this host, goes with it. The first confirmed beat is
/// `record.claimed_at`, the takeover's return.
pub(crate) async fn resume<S>(
    storage: Arc<S>,
    env: JobEnv,
    permit: JobPermit,
    claim: Box<dyn JobClaim>,
    record: DeadRecord,
    limits: RestorableLimits,
) where
    S: BackupCatalog + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    let started = Instant::now();
    let claim: Arc<dyn JobClaim> = Arc::from(claim);
    let DeadRecord {
        row,
        recorded_spool_host,
        progress_rows,
        claimed_at,
    } = record;
    let target = row.target.clone();
    let name = format!(
        "restore of {} into {} (account {})",
        row.source_backup_arn, target.table_name, target.account_id
    );
    // This host's entry, when the record named this host before the
    // takeover: the spooled documents are read from it, and a resume that
    // fails before it runs removes it with the failure path, as the job
    // removes its entry at every failed end. An entry that cannot be opened
    // is left to the sweep and the store's documents are read.
    let entry = env
        .spool
        .as_ref()
        .filter(|spool| recorded_spool_host.as_deref() == Some(spool.spool_host.as_str()))
        .and_then(|spool| {
            spool::Entry::open(
                &spool.path,
                &target.table_id,
                recorded_spool_host.as_deref(),
                &spool.spool_host,
            )
            .ok()
        });
    let prepared = prepare_resume(&env, &row, entry.as_ref(), &progress_rows).await;
    let (arn, manifest, manifest_bytes, sidecar_bytes, key_schema, progress, from_spool) =
        match prepared {
            Ok(prepared) => prepared,
            Err(reason) => {
                let path = fail_restore(
                    &*storage,
                    &*claim,
                    &target,
                    &reason,
                    entry.as_ref(),
                    &env.shutdown,
                )
                .await;
                tracing::error!("{name}: FAILED on resume ({reason}); {path}");
                if let Some(metrics) = env.metrics.as_deref() {
                    metrics.record_worker_error(QuerySource::RestoreJob);
                    #[allow(clippy::cast_precision_loss)]
                    let duration_us = started.elapsed().as_micros() as f64;
                    metrics.record_restore_job(BackupJobOutcome::Failed, duration_us);
                }
                drop(permit);
                return;
            }
        };
    tracing::info!(
        "{name}: resumed by this instance; the manifest was read from {}",
        if from_spool {
            "this host's spool"
        } else {
            "the store"
        }
    );
    let job = RestoreJob {
        storage,
        claim,
        target,
        arn,
        manifest: Arc::new(manifest),
        manifest_bytes,
        sidecar_bytes,
        key_schema,
        limits,
        env,
        started,
        claimed_at,
        progress: Some(progress),
        recorded_spool_host,
        documents_from_spool: from_spool,
    };
    run(job).await;
    drop(permit);
}

/// What a resume reads before it runs: the manifest and sidecar (from this
/// host's spool entry when there is one and the spooled pair verifies, else
/// the store), verified, with the key schema and the progress, and whether
/// the pair came from the spool; the `Err` is the FAILED reason.
type Prepared = (
    BackupArn,
    ExtenddbManifest,
    Vec<u8>,
    Vec<u8>,
    RestorableKeySchema,
    Progress,
    bool,
);

/// The manifest of a (manifest, sidecar) pair that verifies and names
/// `arn`; the `Err` says what failed.
fn verified_manifest(
    manifest_bytes: &[u8],
    sidecar_bytes: &[u8],
    arn: &BackupArn,
) -> Result<ExtenddbManifest, String> {
    verify_manifest_checksum(manifest_bytes, sidecar_bytes)
        .map_err(|e| format!("the manifest's checksum sidecar does not match: {e}"))?;
    let manifest = ExtenddbManifest::from_json_bytes(manifest_bytes)
        .map_err(|e| format!("the manifest is refused: {e}"))?;
    match manifest.backup_arn() {
        Ok(named) if named == *arn => Ok(manifest),
        Ok(named) => Err(format!(
            "the manifest names another backup, {}",
            named.to_arn_string()
        )),
        Err(e) => Err(format!("the manifest's backup ARN is refused: {e}")),
    }
}

async fn prepare_resume(
    env: &JobEnv,
    row: &RestoreJobRow,
    entry: Option<&spool::Entry>,
    progress_rows: &[(u32, u64)],
) -> Result<Prepared, String> {
    let target = &row.target;
    let arn = BackupArn::parse(&row.source_backup_arn)
        .map_err(|e| format!("the record's backup ARN is refused: {e}"))?;
    let prefix = arn.store_prefix();
    let spooled = entry.and_then(|entry| {
        let manifest = entry.read_document(EXTENDDB_MANIFEST_FILE).ok()??;
        let sidecar = entry.read_document(MANIFEST_CHECKSUM_FILE).ok()??;
        Some((manifest, sidecar))
    });
    // The spooled pair, when it verifies; a pair a crash left short, or
    // that names another backup, is replaced by the store's.
    let spooled = spooled.and_then(|(manifest_bytes, sidecar_bytes)| {
        match verified_manifest(&manifest_bytes, &sidecar_bytes, &arn) {
            Ok(manifest) => Some((manifest, manifest_bytes, sidecar_bytes)),
            Err(e) => {
                tracing::warn!(
                    "the spooled manifest and sidecar of the restore into {} (account {}) do not \
                     verify ({e}); the store's are read instead",
                    target.table_name,
                    target.account_id
                );
                None
            }
        }
    });
    let from_spool = spooled.is_some();
    let (manifest, manifest_bytes, sidecar_bytes) = match spooled {
        Some(documents) => documents,
        None => {
            let read = async |name: &str, bound: u64| -> Result<Vec<u8>, String> {
                match super::reconciler::read_object(&*env.store, &format!("{prefix}{name}"), bound)
                    .await
                {
                    Ok(ReadObject::Bytes(bytes)) => Ok(bytes),
                    Ok(ReadObject::Absent) => {
                        Err(format!("the store holds no {name} for the backup"))
                    }
                    Ok(ReadObject::Oversize) => Err(format!(
                        "the store's {name} for the backup is over {bound} bytes, which no \
                         backup this server wrote produces"
                    )),
                    Err(e) => {
                        if let Some(metrics) = env.metrics.as_deref() {
                            metrics.record_backup_store_error("get");
                        }
                        Err(format!("reading {name} from the store: {e}"))
                    }
                }
            };
            let manifest_bytes = read(EXTENDDB_MANIFEST_FILE, MANIFEST_READ_BOUND).await?;
            let sidecar_bytes = read(MANIFEST_CHECKSUM_FILE, SIDECAR_READ_BOUND).await?;
            let manifest = verified_manifest(&manifest_bytes, &sidecar_bytes, &arn)?;
            (manifest, manifest_bytes, sidecar_bytes)
        }
    };
    let key_schema = RestorableKeySchema::resolve(
        &manifest.source_table.key_schema,
        &manifest.source_table.attribute_definitions,
    )
    .map_err(|e| format!("the key schema cannot be checked: {e}"))?;
    let progress = Progress::from_records(row, progress_rows, &manifest);
    Ok((
        arn,
        manifest,
        manifest_bytes,
        sidecar_bytes,
        key_schema,
        progress,
        from_spool,
    ))
}

/// What a test can do to a running job by its target table id: end its
/// heartbeat timer without raising the loss (`HeartbeatHandle::stop_timer`)
/// while the job is held at a gate of the storage double, and give its
/// loads a smaller chunk byte bound than [`load::LOAD_CHUNK_BYTES`]. Tests
/// that use it give their target a name of their own; the maps are
/// process-wide.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex, PoisonError};

    use super::HeartbeatHandle;

    static TIMERS: LazyLock<Mutex<HashMap<String, tokio::task::AbortHandle>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    static CHUNK_BYTES: LazyLock<Mutex<HashMap<String, u64>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Give the loads of the job into `table_id` a chunk byte bound of
    /// `bytes`.
    pub(crate) fn set_chunk_bytes(table_id: &str, bytes: u64) {
        CHUNK_BYTES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(table_id.to_owned(), bytes);
    }

    pub(super) fn chunk_bytes_for(table_id: &str) -> u64 {
        CHUNK_BYTES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(table_id)
            .copied()
            .unwrap_or(super::load::LOAD_CHUNK_BYTES)
    }

    pub(super) fn register_timer(table_id: &str, heartbeat: &HeartbeatHandle) {
        TIMERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(table_id.to_owned(), heartbeat.timer_abort_handle());
    }

    /// End the heartbeat timer of the running job into `table_id`.
    pub(crate) fn stop_timer(table_id: &str) {
        let handle = TIMERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(table_id)
            .cloned()
            .expect("a job into the table is running");
        handle.abort();
    }
}
