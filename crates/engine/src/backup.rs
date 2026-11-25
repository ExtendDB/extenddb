// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine handlers for the `DynamoDB` backup operations that use the backup
//! store: `CreateBackup`, `DescribeBackup`, `DeleteBackup` and
//! `RestoreTableFromBackup`. `ListBackups` and the point-in-time recovery
//! operations, which read no store, are in [`crate::backup_requests`].
//!
//! The rules that decide what a backup restores to, which backups and
//! requests are refused, and what the responses carry live here rather than in
//! the backends, so every backend restores the same table from the same backup
//! and reports it the same way. Backends store and copy.
//!
//! Wire member names follow the 2012-08-10 service model in botocore 1.33.13
//! (`botocore/data/dynamodb/2012-08-10/service-2.json.gz`):
//! `RestoreTableFromBackupInput`, `RestoreSummary`, `SourceTableFeatureDetails`,
//! and the operation's `TableAlreadyExistsException`.

use std::sync::Arc;

use extenddb_core::backup_format::{BackupArn, check_store_prefix_table_name, is_region};
use extenddb_core::error::{DynamoDbError, ErrorMessageKey, error_message};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BackupDescription, BillingMode, CreateTableInput, GlobalSecondaryIndexInfo,
    LocalSecondaryIndexInfo, ProvisionedThroughput, RestoreSummary, SourceTableFeatureDetails,
    SseDescription, StreamSpecification, TableDescription, TableStatus,
};
use extenddb_storage::backup_catalog::{
    BackupKey, BackupRow, BackupStatus, DeleteStart, is_backup_not_found,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, BackupRestoreSource, DataEngine, LegacyBackupReader, MetadataEngine,
    RestoreEngine, RestoreRequest, RestoreSource, RestoreTableOverrides, SnapshotEngine,
    TableEngine,
};
use futures::TryStreamExt;
use serde_json::{Value, json};

pub use job::KillPoint;
pub use maintenance::HeldClaim;
pub use restore::{OTHER_STORE, RESTORE_REGISTRATION_TIMEOUT, TARGET_NOT_EMPTY};
pub use runtime::{
    BackupRuntime, ConfiguredStore, JobSettings, LOCAL_CHECK_TIMEOUT, S3_CHECK_TIMEOUT, Spool,
    StoreFence, StoreHealth, StoreHealthReport, check_configured_store,
    check_configured_store_read_only, read_identity_bounded,
};
pub use store_identity::{STORE_IDENTITY_KEY, StoreIdentityFile, StoreProblem};

use crate::backup_requests::{
    canonical_backup_arn, check_backup_arn, create_backup_table_name, required_str,
    storage_err_to_dynamo, validate_name_member,
};
use crate::{OperationContext, serialize_output};

/// Resolve the `BackupArn` field to the ARNs the engine tries, in order.
///
/// Order, matching the service (measured on DynamoDB):
/// a missing member is a `ValidationException`; a string that is not a backup
/// ARN (see [`check_backup_arn`]) is a `ValidationException` with the
/// service's text for its class; one in a partition other than `aws` is a
/// `ValidationException` too; a well-formed ARN naming another account is an
/// `AccessDeniedException`, not reported as absent, because the service
/// authorizes on the ARN's account before resolving the backup. A caller can
/// therefore tell "my backup does not exist" (`BackupNotFoundException`, from
/// the backend) from "that ARN belongs to another account".
///
/// The service trims a trailing newline and resolves an ARN naming another
/// region of the caller's account (measured on DynamoDB). The backends look
/// up the exact string they stored, which carries the `server.region` in
/// force when the backup was created, so the engine tries the ARN rebuilt in
/// this deployment's region first and, when that is not found and the caller
/// wrote a different ARN, the ARN as written (trimmed); see
/// [`resolve_backup`]. A backup created under an earlier `server.region`
/// therefore stays reachable by the ARN `ListBackups` reports for it.
///
/// Backends additionally filter on `account_id`, so a mismatch can never
/// resolve even if this check were bypassed.
fn backup_arn_field(
    body: &Value,
    account_id: &str,
    region: &str,
) -> Result<BackupArnSpellings, DynamoDbError> {
    let backup_arn = body
        .get("BackupArn")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'backupArn' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;
    let parts = check_backup_arn(backup_arn)?;
    if parts.account_id != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    Ok(BackupArnSpellings::new(&parts, region))
}

/// The spellings of one `BackupArn` the engine looks a backup up under:
/// the ARN this deployment issues for the backup the caller named
/// ([`canonical_backup_arn`]) and, when it differs, the ARN as the caller
/// wrote it, trimmed. The strings are what the legacy reader is addressed
/// by, built as main built them; each is also parsed once, here, into the
/// [`BackupArn`] the catalog index is addressed by. `BackupArn` applies two
/// store-prefix rules `check_backup_arn` does not (a table name over 255
/// bytes, or made only of dots), which a legacy backup can break: main's
/// CreateTable and CreateBackup accept `...`, and an operator can raise
/// `limits.max_table_name_length`. A spelling that fails them (or one built
/// under a `server.region` outside the ARN alphabet, which the legacy path
/// never checked) has no parsed form and no catalog-index read (no index row
/// can carry such an ARN: the store-backed writer, the one path that forms a
/// store prefix, refuses the name before it writes), and the legacy reader is
/// tried under its string as on main.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupArnSpellings {
    canonical: String,
    as_written: Option<String>,
    canonical_arn: Option<BackupArn>,
    as_written_arn: Option<BackupArn>,
}

impl BackupArnSpellings {
    fn new(parts: &extenddb_storage::util::BackupArnParts<'_>, region: &str) -> Self {
        let canonical = canonical_backup_arn(parts, region);
        let as_written = (parts.arn != canonical).then(|| parts.arn.to_owned());
        let canonical_arn = BackupArn::parse(&canonical).ok();
        let as_written_arn = as_written.as_deref().and_then(|a| BackupArn::parse(a).ok());
        Self {
            canonical,
            as_written,
            canonical_arn,
            as_written_arn,
        }
    }

    /// The ARN this deployment issues for the backup named.
    fn canonical(&self) -> &str {
        &self.canonical
    }

    /// The parsed spellings in lookup order: canonical, then as written;
    /// none for a name the store-prefix rules refuse.
    fn arns(&self) -> impl Iterator<Item = &BackupArn> {
        self.canonical_arn
            .iter()
            .chain(self.as_written_arn.as_ref())
    }
}

/// The catalog row one of `arn`'s spellings names, read in [`resolve_backup`]'s
/// order, with the spelling that resolved. A row read by account and backup
/// id is returned by the backend only when its account, table and ARN equal
/// the spelling's, so an ARN of account B carrying the id of account A's
/// backup finds nothing (and never A's row). `None` when neither spelling
/// has a row: the caller falls back to the legacy reader.
async fn read_catalog_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    arn: &BackupArnSpellings,
) -> Result<Option<(BackupArn, BackupRow)>, StorageError> {
    for spelling in arn.arns() {
        if let Some(row) = storage.read_backup_row(spelling).await? {
            return Ok(Some((spelling.clone(), row)));
        }
    }
    Ok(None)
}

/// The error for a backup the catalog index has no describable row for.
fn backup_not_found(arn: &str) -> DynamoDbError {
    DynamoDbError::BackupNotFoundException(format!(
        "{}: {arn}",
        extenddb_storage::backup_catalog::BACKUP_NOT_FOUND
    ))
}

/// The error for a CREATING row, or one whose backup a CREATING restore
/// target still reads: `BackupInUseException`, as the service answers. The
/// in-use text names the backup, not the target (the service's text); the
/// target's name goes to the log.
fn backup_in_use(arn: &str, restoring_into: Option<&str>) -> DynamoDbError {
    DynamoDbError::BackupInUseException(match restoring_into {
        Some(table) => {
            tracing::info!("backup {arn} is in use: a restore into {table} is still CREATING");
            extenddb_storage::backup_catalog::backup_in_use_message(arn)
        }
        None => format!("Backup is being created: {arn}"),
    })
}

/// The error for a row of a store other than the configured one. Names no
/// root: an API error never does.
fn backup_in_another_store(arn: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Backup {arn} is in a backup store other than the one this server is configured with"
    ))
}

/// Look a backup up by `lookup`, first under the canonical ARN, then, if that
/// is not found and the caller wrote a different ARN, under the ARN as
/// written. Returns the ARN that resolved with the lookup's answer; every
/// other error, and not-found under both, is the lookup's own. One rule for
/// `DescribeBackup`, `DeleteBackup`, and `RestoreTableFromBackup`, so the
/// three agree on which stored backup a request names.
async fn resolve_backup<'f, T, F>(
    arn: &BackupArnSpellings,
    lookup: F,
) -> Result<(String, T), StorageError>
where
    F: Fn(String) -> extenddb_storage::BoxedFuture<'f, Result<T, StorageError>>,
{
    match lookup(arn.canonical.clone()).await {
        Err(e) if is_backup_not_found(&e) && arn.as_written.is_some() => {
            let as_written = arn.as_written.clone().unwrap_or_default();
            lookup(as_written.clone())
                .await
                .map(|found| (as_written, found))
        }
        other => other.map(|found| (arn.canonical.clone(), found)),
    }
}

/// [`resolve_backup`]'s lookup for one [`LegacyBackupReader`] method taking
/// the ARN: `by!(storage, account_id, method)`.
macro_rules! by {
    ($storage:expr, $account_id:expr, $method:ident) => {{
        let storage = $storage;
        let account_id = $account_id;
        move |a: String| -> extenddb_storage::BoxedFuture<'_, _> {
            Box::pin(async move { storage.$method(account_id, &a).await })
        }
    }};
}

/// The service model's `BackupName` length bounds.
const BACKUP_NAME_BOUNDS: (usize, usize) = (3, 255);

/// Handle `CreateBackup`.
pub(crate) async fn handle_create_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = create_backup_table_name(&body, &ctx.account_id, &ctx.region, &ctx.limits)?;
    let backup_name = required_str(&body, "BackupName", "backupName")?;
    validate_name_member(backup_name, "backupName", BACKUP_NAME_BOUNDS)?;
    let probe = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            extenddb_core::types::DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await;
    let details = create_backup(
        &ctx.storage,
        &ctx.backup,
        &ctx.account_id,
        &ctx.region,
        &ctx.limits,
        table_name,
        backup_name,
        probe,
    )
    .await?;
    serialize_output(&json!({ "BackupDetails": details }))
}

/// One CreateBackup after its members are checked, in the service's order,
/// every check before the permit: the table exists and is ACTIVE for the
/// account (`TableNotFoundException`, "Table not found: <name>", otherwise,
/// whether or not a permit is free); the server's region fits a backup ARN
/// (`check_backup_arn_fits`: a `server.region` outside `[a-z0-9-]` is a
/// `ValidationException` naming the setting); on the store-backed path, the
/// table name fits a store prefix (`check_store_prefix_fits`: a name over 255
/// bytes or made only of dots is a `ValidationException` naming the rule; the
/// in-catalog path writes no prefix and backs such a table up, as the service
/// does); on the in-catalog path, the table has no vector index (the
/// in-catalog format cannot restore one); then the job permit
/// (`LimitExceededException` when none is freed within the runtime's wait).
/// `probe` is the `DescribeTable` of the table, which the handler ran.
///
/// With a store configured and the backend not bound to the in-catalog
/// create, the create is store-backed: the store must be healthy
/// ([`BackupRuntime::require_healthy_store`], refused before a slot is
/// taken as every operation that touches the store is), the maintenance
/// claim must not be held ([`refuse_while_maintenance_held`], before the
/// permit as on the in-catalog path, so a refusal inserts no row), then the
/// permit (`LimitExceededException` when none is free), then the detached
/// task of [`job::start`] (the snapshot's open, the row under the job's
/// claim, the response CREATING) whose answer the request awaits. A vector
/// table is backed up on this path (the manifest records its indexes).
///
/// Otherwise the in-catalog create: the table has no vector index (the
/// in-catalog format cannot restore one), the maintenance claim is not held
/// ([`refuse_while_maintenance_held`], last before the permit), the permit,
/// then `LegacyBackupReader::create`; each call logs that the format is
/// deprecated. `probe` is the `DescribeTable` of the table, which the
/// handler ran.
#[allow(clippy::too_many_arguments)]
async fn create_backup<S>(
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    account_id: &str,
    region: &str,
    limits: &LimitsConfig,
    table_name: &str,
    backup_name: &str,
    probe: Result<TableDescription, StorageError>,
) -> Result<extenddb_core::types::BackupDetails, DynamoDbError>
where
    S: BackupCatalog
        + LegacyBackupReader
        + SnapshotEngine
        + MetadataEngine
        + ?Sized
        + Send
        + Sync
        + 'static,
{
    let desc = match probe {
        Ok(desc) if desc.table_status == TableStatus::Active => desc,
        Ok(_) => {
            return Err(create_backup_storage_err_to_dynamo(
                StorageError::TableNotActive(table_name.to_owned()),
                table_name,
            ));
        }
        Err(e) => return Err(create_backup_storage_err_to_dynamo(e, table_name)),
    };
    check_backup_arn_fits(region)?;
    let store_backed = runtime.configured_store().is_some() && !runtime.legacy_create_only();
    if store_backed {
        check_store_prefix_fits(region, account_id, table_name)?;
        let store = runtime.require_healthy_store()?;
        let env = job::JobEnv::new(runtime, store)?;
        refuse_while_maintenance_held(&**storage, runtime).await?;
        let permit = runtime.take_permit().await?;
        let (reply, answer) = tokio::sync::oneshot::channel();
        let request = job::CreateRequest {
            account_id: account_id.to_owned(),
            region: region.to_owned(),
            table_name: table_name.to_owned(),
            backup_name: backup_name.to_owned(),
            limits: limits.into(),
        };
        runtime.spawn_job(job::start(Arc::clone(storage), env, permit, request, reply));
        return answer.await.unwrap_or_else(|_| {
            tracing::error!(
                "CreateBackup of {table_name} (account {account_id}): the job task ended \
                 without answering"
            );
            Err(DynamoDbError::InternalServerError(
                "Internal server error".to_owned(),
            ))
        });
    }
    refuse_backup_of_vector_table(&desc)?;
    refuse_while_maintenance_held(&**storage, runtime).await?;
    let permit = runtime.take_permit().await?;
    match runtime.configured_store() {
        None => tracing::warn!(
            "CreateBackup of {table_name} (account {account_id}) is writing the in-catalog \
             backup format, which a later release stops creating; configure [backup.store] to \
             write backups to a backup store"
        ),
        Some(store) => tracing::info!(
            "CreateBackup of {table_name} (account {account_id}) is writing the in-catalog \
             backup format: this backend is bound to it; the configured {} backup store serves \
             restores and deletes of the backups it holds",
            store.kind
        ),
    }
    storage
        .create(account_id, table_name, backup_name, permit)
        .await
        .map_err(|e| create_backup_storage_err_to_dynamo(e, table_name))
}

/// The jobs' side of the catalog-level maintenance claim: a backup or
/// restore job is refused with `LimitExceededException`
/// ([`maintenance_held`]) while a maintenance command (`extenddb backup
/// purge`) holds the claim, read with `stale_job_after_secs` so a dead
/// command's claim expires. The check runs after every other refusal and
/// before the permit, which is before the job's row exists: the in-catalog
/// create and restore commit their row inside the backend and detach, so the
/// engine has no point between the row and the copy to check at, and the
/// store-backed create checks here too rather than after its insert, since
/// a row written FAILED for the refusal carried the insert's heartbeat and
/// `backup purge`'s wait counted it as a live job for the stale window. The
/// store-backed create reads the claim again immediately before its
/// `claim_backup`, and the store-backed restore immediately before its
/// registration (and once more after it), so a claim a command took while
/// the request opened the table or read the manifest refuses the job with
/// no row. What remains is the window from that last read to the insert,
/// however long the insert takes: a claim taken in it is not seen by the
/// job, and its CREATING row or RUNNING record lands under the claim. One
/// that lands during the command's wait is listed by it, and one that lands
/// before `--move-store`'s rewrite stays under the old root, which the
/// move's re-list reports (the rewrite skips CREATING rows). A CreateBackup
/// whose insert lands after the move's re-list, during its sync of the
/// configured root, is reported by the sync's closing report of other
/// stores if it lands before that report (and the command exits 1), and
/// only by a later plain sync otherwise. The in-catalog
/// create, which commits its row inside the backend, writes no object of
/// the store a command deletes from.
async fn refuse_while_maintenance_held<S: BackupCatalog + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
) -> Result<(), DynamoDbError> {
    let held = storage
        .check_maintenance_claim(runtime.stale_job_after())
        .await
        .map_err(storage_err_to_dynamo)?;
    if held {
        return Err(maintenance_held());
    }
    Ok(())
}

/// The refusal every job answers while the maintenance claim is held.
fn maintenance_held() -> DynamoDbError {
    DynamoDbError::LimitExceededException(
        "A backup maintenance command holds this catalog's maintenance claim; backup and \
         restore jobs are refused until it finishes"
            .to_owned(),
    )
}

/// The one backup ARN rule CreateBackup applies on this path: the server's
/// own region (`server.region` outside `[a-z0-9-]` forms no backup ARN
/// whatever the name, so the message names the setting, not the table). The
/// store-prefix rules on the name (over 255 bytes, or made only of dots;
/// `extenddb_core::backup_format::check_store_prefix_table_name`) are not
/// applied here: the in-catalog create writes no store prefix, and the
/// service backs up any table it can create (measured on DynamoDB: a table
/// named `...` backs up), so the engine keeps main's behaviour for it, and
/// the store-backed writer, the one path that forms a prefix, applies them
/// before it writes.
fn check_backup_arn_fits(region: &str) -> Result<(), DynamoDbError> {
    if !is_region(region) {
        return Err(DynamoDbError::ValidationException(format!(
            "CreateBackup is refused on this server: server.region {region:?} is outside \
             [a-z0-9-], so no backup ARN can be formed; set server.region to a value in \
             that class"
        )));
    }
    Ok(())
}

/// The store-backed create's name rules, applied before the job's row exists:
/// the table name has to fit a store prefix
/// (`extenddb_core::backup_format::check_store_prefix_table_name`: over 255
/// bytes, or made only of dots, fits none) and the backup ARN has to form
/// (checked with a placeholder id, so the message names the limit whatever
/// the length). The in-catalog create applies neither: it writes no prefix,
/// and the service backs up any table it can create.
fn check_store_prefix_fits(
    region: &str,
    account_id: &str,
    table_name: &str,
) -> Result<(), DynamoDbError> {
    check_store_prefix_table_name(table_name).map_err(|e| {
        DynamoDbError::ValidationException(format!(
            "TableName cannot be backed up: a backup ARN cannot be formed for it: {e}"
        ))
    })?;
    let placeholder = extenddb_core::backup_format::format_backup_id(0, 0);
    BackupArn::new(
        extenddb_storage::util::ARN_PARTITION,
        region,
        account_id,
        table_name,
        &placeholder,
    )
    .map(drop)
    .map_err(|e| {
        DynamoDbError::ValidationException(format!(
            "TableName cannot be backed up: a backup ARN cannot be formed for it: {e}"
        ))
    })
}

/// [`storage_err_to_dynamo`] for CreateBackup, where a table that does not
/// exist or is not yet `ACTIVE` is `TableNotFoundException` with the
/// service's text (`Table not found: <name>`, measured on DynamoDB for both
/// cases), built from the request's name rather than the
/// backend's payload, which differs by backend.
fn create_backup_storage_err_to_dynamo(e: StorageError, table_name: &str) -> DynamoDbError {
    match e {
        StorageError::TableNotFound(_) | StorageError::TableNotActive(_) => {
            DynamoDbError::TableNotFoundException(format!("Table not found: {table_name}"))
        }
        other => storage_err_to_dynamo(other),
    }
}

/// The in-catalog create's refusal for a table with vector indexes (the
/// in-catalog format cannot restore one); the store-backed path has none.
fn refuse_backup_of_vector_table(desc: &TableDescription) -> Result<(), DynamoDbError> {
    let count = desc.vector_indexes.as_ref().map_or(0, Vec::len);
    if count == 0 {
        return Ok(());
    }
    Err(storage_err_to_dynamo(
        extenddb_storage::backup_definition::vector_index_refusal(
            &format!("table {}", desc.table_name),
            count,
        ),
    ))
}

/// Handle `DescribeBackup`.
pub(crate) async fn handle_describe_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let backup_arn = backup_arn_field(&body, &ctx.account_id, &ctx.region)?;
    let desc = describe_backup(&*ctx.storage, &ctx.account_id, &ctx.region, &backup_arn).await?;
    serialize_output(&json!({ "BackupDescription": desc }))
}

/// The DescribeBackup body. The catalog index is consulted first: a row of
/// one of the ARN's spellings answers on its own, derived from its recorded
/// source table ([`BackupRow::description`]), with DELETING reported as
/// DELETED and a DELETED or FAILED row `BackupNotFoundException`. With no
/// row, the legacy reader's description, with
/// `SourceTableDetails.ProvisionedThroughput` and `SourceTableFeatureDetails`
/// filled from the backup's recorded source when the backup is AVAILABLE.
/// The backup is looked up under the ARN's spellings in [`resolve_backup`]'s
/// order; the source is then read under the spelling that resolved. A
/// legacy not-found re-reads the catalog index once, so a `backup
/// migrate-legacy` replacement that committed between the two reads is
/// described from its row.
async fn describe_backup<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    account_id: &str,
    region: &str,
    backup_arn: &BackupArnSpellings,
) -> Result<BackupDescription, DynamoDbError> {
    let described = |(arn, row): (BackupArn, BackupRow)| {
        if !row.status.is_described() {
            return Err(backup_not_found(&arn.to_arn_string()));
        }
        row.description().map_err(storage_err_to_dynamo)
    };
    if let Some(held) = read_catalog_row(storage, backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        return described(held);
    }
    let (backup_arn, mut desc) =
        match resolve_backup(backup_arn, by!(storage, account_id, describe)).await {
            Ok(found) => found,
            // A `backup migrate-legacy` replacement that committed between
            // the two reads: the row is there now.
            Err(e) if is_backup_not_found(&e) => {
                return match read_catalog_row(storage, backup_arn)
                    .await
                    .map_err(storage_err_to_dynamo)?
                {
                    Some(held) => described(held),
                    None => Err(storage_err_to_dynamo(e)),
                };
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        };
    let backup_arn = backup_arn.as_str();

    // A CREATING or DELETED backup has no restore source to read; it is
    // described from the backend's row alone. So is one that vanished between
    // the two reads, and one whose definition this build cannot read
    // (Unsupported: a definition version from a later build): its
    // BackupDetails do not depend on the definition, so DescribeBackup still
    // answers, without feature details. Every other error propagates,
    // including Internal (a definition row that does not parse, a renamed
    // column, a revoked privilege): answering those with a description that
    // reports on-demand `{0, 0}` and no feature details would hand a wrong
    // answer for a database failure, while RestoreTableFromBackup on the same
    // read is a 500.
    let source = if desc.backup_details.backup_status == "AVAILABLE" {
        match storage.restore_source(account_id, backup_arn).await {
            Ok(source) => Some(source),
            Err(e) if is_backup_not_found(&e) => None,
            Err(StorageError::Unsupported(m)) => {
                tracing::warn!("DescribeBackup {backup_arn}: definition not described: {m}");
                None
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        }
    } else {
        None
    };
    apply_source_to_description(&mut desc, source.as_ref(), region, account_id);
    Ok(desc)
}

/// Fill the members DescribeBackup derives from the recorded source.
///
/// `SourceTableDetails.ProvisionedThroughput` is required in the service
/// shape and is `{0, 0}` for an on-demand table (measured on DynamoDB); it
/// is always set. Feature details are omitted for a
/// backup without a recorded definition, and each member is omitted when the
/// backup recorded nothing for it.
fn apply_source_to_description(
    desc: &mut BackupDescription,
    source: Option<&BackupRestoreSource>,
    region: &str,
    account_id: &str,
) {
    let zero = ProvisionedThroughput {
        read_capacity_units: 0,
        write_capacity_units: 0,
    };
    let details = &mut desc.source_table_details;
    if let Some(source) = source {
        if details.billing_mode.is_none() {
            details.billing_mode = Some(source.billing_mode.clone());
        }
        let definition = source.definition.as_ref();
        details.provisioned_throughput = if source.billing_mode == "PAY_PER_REQUEST" {
            Some(zero.clone())
        } else {
            source
                .provisioned_throughput
                .clone()
                .or_else(|| definition.and_then(|d| d.provisioned_throughput.clone()))
                .or_else(|| details.provisioned_throughput.take())
        };
        if let Some(d) = definition {
            if details.on_demand_throughput.is_none() {
                details
                    .on_demand_throughput
                    .clone_from(&d.on_demand_throughput);
            }
            let on_demand = d.billing_mode == "PAY_PER_REQUEST";
            let features = SourceTableFeatureDetails {
                local_secondary_indexes: (!d.local_secondary_indexes.is_empty()).then(|| {
                    d.local_secondary_indexes
                        .iter()
                        .map(|i| LocalSecondaryIndexInfo {
                            index_name: i.index_name.clone(),
                            key_schema: i.key_schema.clone(),
                            projection: i.projection.clone(),
                        })
                        .collect()
                }),
                global_secondary_indexes: (!d.global_secondary_indexes.is_empty()).then(|| {
                    d.global_secondary_indexes
                        .iter()
                        .map(|i| GlobalSecondaryIndexInfo {
                            index_name: i.index_name.clone(),
                            key_schema: i.key_schema.clone(),
                            projection: i.projection.clone(),
                            provisioned_throughput: if on_demand {
                                Some(zero.clone())
                            } else {
                                i.provisioned_throughput.clone()
                            },
                        })
                        .collect()
                }),
                // A backup of a table whose stream was disabled carries the
                // stored `{StreamEnabled: false}`, and DescribeBackup shows it
                // as `StreamDescription: {StreamEnabled: false}`, as it did
                // before the view type was kept in the stored specification;
                // the service's answer for that backup is not measured. The
                // view type the backends now keep for the closed stream is
                // dropped here so the answer stays the one before.
                stream_description: d.stream_specification.clone().map(|s| {
                    if s.stream_enabled {
                        s
                    } else {
                        StreamSpecification {
                            stream_enabled: false,
                            stream_view_type: None,
                        }
                    }
                }),
                time_to_live_description: d.time_to_live.clone(),
                sse_description: SseDescription::from_specification(
                    d.sse_specification.as_ref(),
                    region,
                    account_id,
                ),
            };
            desc.source_table_feature_details = (!features.is_empty()).then_some(features);
        }
    }
    if desc.source_table_details.provisioned_throughput.is_none() {
        // Required member, and nothing recorded it (a backup that is not
        // AVAILABLE, or a legacy row without throughput): report the
        // on-demand value rather than omit a required member.
        desc.source_table_details.provisioned_throughput = Some(zero);
    }
}

/// Handle `DeleteBackup`.
pub(crate) async fn handle_delete_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let backup_arn = backup_arn_field(&body, &ctx.account_id, &ctx.region)?;
    let desc = delete_backup(
        &*ctx.storage,
        &ctx.backup,
        &ctx.account_id,
        &ctx.region,
        &backup_arn,
    )
    .await?;
    serialize_output(&json!({ "BackupDescription": desc }))
}

/// The DeleteBackup body: the full DescribeBackup description of the backup
/// as it was, with `BackupStatus` `DELETED` (measured on DynamoDB). `SourceTableDetails.ProvisionedThroughput` is a required member
/// of the shape and a typed SDK can fail to unmarshal a response without it.
///
/// The catalog index is consulted first ([`delete_catalog_backup`]); with no
/// row of either spelling, the legacy reader's delete.
///
/// The restore source is read before the delete, since it is gone after,
/// under the ARN spelling that resolves ([`resolve_backup`]); the delete then
/// names that spelling. A backup the read does not find under either
/// spelling (a `CREATING` one, for instance) is left to `delete_backup`,
/// tried in the same order, which reports an absent backup as
/// `BackupNotFoundException`. A definition this build cannot read or that
/// does not parse (Unsupported, Internal) does not stop the delete: a backup
/// whose definition row is damaged must still be deletable, so the
/// description is built without feature details (logged at warn), which
/// means a damaged `PROVISIONED` backup is described with on-demand `{0, 0}`
/// (ExtendDB's choice: deletability over the throughput member); a failure to
/// reach the database propagates, as the delete itself would fail on it.
async fn delete_backup<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
    account_id: &str,
    region: &str,
    backup_arn: &BackupArnSpellings,
) -> Result<BackupDescription, DynamoDbError> {
    if let Some((arn, row)) = read_catalog_row(storage, backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        return delete_catalog_backup(storage, runtime, &arn, &row).await;
    }
    let (resolved, source) =
        match resolve_backup(backup_arn, by!(storage, account_id, restore_source)).await {
            Ok((resolved, source)) => (Some(resolved), Some(source)),
            Err(e) if is_backup_not_found(&e) => (None, None),
            Err(StorageError::Unsupported(m) | StorageError::Internal(m)) => {
                tracing::warn!(
                    "DeleteBackup {}: definition not described: {m}",
                    backup_arn.canonical()
                );
                (None, None)
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        };
    let deleted = match resolved {
        Some(resolved) => storage.delete(account_id, &resolved).await,
        None => resolve_backup(backup_arn, by!(storage, account_id, delete))
            .await
            .map(|(_, desc)| desc),
    };
    let mut desc = match deleted {
        Ok(desc) => desc,
        // A `backup migrate-legacy` replacement that committed between the
        // catalog read and the legacy delete: the row is there now.
        Err(e) if is_backup_not_found(&e) => {
            return match read_catalog_row(storage, backup_arn)
                .await
                .map_err(storage_err_to_dynamo)?
            {
                Some((arn, row)) => delete_catalog_backup(storage, runtime, &arn, &row).await,
                None => Err(storage_err_to_dynamo(e)),
            };
        }
        Err(e) => return Err(storage_err_to_dynamo(e)),
    };
    desc.backup_details.backup_status = "DELETED".to_owned();
    apply_source_to_description(&mut desc, source.as_ref(), region, account_id);
    Ok(desc)
}

/// DeleteBackup of a backup the catalog index has a row for. In the order
/// the request's checks run, the same order as a restore's
/// ([`check_catalog_row_for_restore`]): a DELETED or FAILED row is
/// `BackupNotFoundException`; a CREATING row is `BackupInUseException`; a
/// backup a CREATING restore target still reads is `BackupInUseException`
/// naming that table ([`BackupCatalog::backup_in_use`]); a row of a store
/// other than the configured one is a `ValidationException` naming no root,
/// and a row of the configured store while that store is unhealthy or its
/// first check pending is a `ValidationException` naming the store's kind;
/// then the in-use check runs again with the AVAILABLE-to-DELETING write in
/// one transaction under the row's lock
/// ([`BackupCatalog::start_backup_delete`]), so a restore's registration and
/// this order wholly one before the other: a registration that won leaves
/// this `BackupInUseException` naming its target. A DELETING row (a retried
/// DeleteBackup) resumes the deletion. The deletion fence
/// ([`StoreFence`]) runs before the DELETING write and again right before
/// the first deletion. At the first, a root it finds owned by another
/// catalog, a root with no identity file, a file the store refuses to
/// serve and a file that does not parse are recorded on the store and
/// refused with the row as it was (and its objects kept), answered as the
/// unhealthy store's `ValidationException`; a read that fails in transport
/// (the store unreachable, or the read past its bound) is a store failure
/// and the request goes on to the DELETING write, the second fence then
/// refusing the deletion. A refusal at the second (another catalog adopted
/// the root in between, or the read still fails) leaves the row
/// DELETING with nothing deleted, which the reconciler finishes once its
/// own fence passes (for an adopted root, only if this catalog owns the
/// root again). Then the deletion itself
/// ([`finish_backup_delete`], through the store's bounded calls,
/// [`ConfiguredStore::calls`]): the manifest first, so the prefix is no
/// longer a complete backup to any reader, then the rest of the prefix,
/// then the row to DELETED once a listing of the prefix returns nothing. The
/// request answers the description with `BackupStatus` DELETED once the
/// DELETING write has committed, whatever the deletion does, the second
/// fence's refusal included: every reader already reports the backup
/// deleted from that write on, and the service has no counterpart for a
/// failed object deletion. If a step fails (the store not answering, a
/// listing that still shows an object) the row stays DELETING, the failure
/// is logged, and the reconciler finishes the deletion at its next cycle.
///
/// The description is built before the DELETING write: a row whose
/// `source_table_details_json` this build cannot read is answered as an
/// internal error with the row still AVAILABLE, rather than moved to DELETING
/// and then refused on every retry.
async fn delete_catalog_backup<S: BackupCatalog + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
    arn: &BackupArn,
    row: &BackupRow,
) -> Result<BackupDescription, DynamoDbError> {
    let arn_text = arn.to_arn_string();
    match row.status {
        BackupStatus::Deleted | BackupStatus::Failed => return Err(backup_not_found(&arn_text)),
        BackupStatus::Creating => return Err(backup_in_use(&arn_text, None)),
        BackupStatus::Available | BackupStatus::Deleting => {}
    }
    let key = BackupKey::from_arn(arn);
    if let Some(table) = storage
        .backup_in_use(&key)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        return Err(backup_in_use(&arn_text, Some(&table)));
    }
    if !runtime.is_configured_store(&row.store) {
        return Err(backup_in_another_store(&arn_text));
    }
    let store = runtime.require_healthy_store()?;
    let mut desc = row.description().map_err(storage_err_to_dynamo)?;
    // The fence before the DELETING write as well as before the deletion: a
    // root it finds owned by another catalog (or by this one under another
    // store id) is refused here with the row as it was, so the API never
    // answers that refusal with a DELETED backup. A root with no file, a
    // file the store refuses to serve and one that does not parse are
    // recorded on the store and refused here the same way. A read that
    // fails in transport (the store unreachable, or the read past its
    // bound) records nothing and is a store failure: the request goes on as
    // it does when the store does not answer the deletion, and the fence
    // before the deletion refuses it, leaving the row DELETING for the
    // reconciler, whose deletion runs the fence again.
    let fence = store.fence();
    if let Err(problem) = fence.check_catalog(storage).await {
        if let Some(refusal) = fence.refusal() {
            tracing::error!(
                "DeleteBackup {arn_text}: refused before the row was changed; nothing was \
                 deleted: {problem}"
            );
            return Err(refusal);
        }
        if let StoreProblem::Catalog(e) = &problem {
            tracing::error!("DeleteBackup {arn_text}: the catalog id could not be read: {e}");
            return Err(DynamoDbError::InternalServerError(
                "Internal server error".to_owned(),
            ));
        }
        tracing::warn!(
            "DeleteBackup {arn_text}: the deletion fence could not read the store's identity \
             file before the DELETING write ({problem}); the deletion is fenced again before \
             any object is deleted"
        );
    }
    match storage
        .start_backup_delete(&key)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        DeleteStart::Deleting | DeleteStart::Status(BackupStatus::Deleting) => {}
        DeleteStart::InUse(table) => return Err(backup_in_use(&arn_text, Some(&table))),
        DeleteStart::Status(BackupStatus::Creating) => {
            return Err(backup_in_use(&arn_text, None));
        }
        DeleteStart::Status(BackupStatus::Deleted | BackupStatus::Failed) => {
            return Err(backup_not_found(&arn_text));
        }
        // The contract says an AVAILABLE row becomes `Deleting`; a backend
        // that answers its status instead has broken it, which is reported
        // as the fault it is rather than as an absent backup.
        DeleteStart::Status(BackupStatus::Available) => {
            return Err(storage_err_to_dynamo(StorageError::Internal(format!(
                "start_backup_delete of {arn_text} answered Status(AVAILABLE), which the \
                 contract excludes"
            ))));
        }
    }
    if let Err(e) = finish_backup_delete(storage, &store.calls("deletion"), &fence, arn).await {
        match e {
            FinishError::Fenced(problem) => tracing::error!(
                "DeleteBackup {arn_text}: the row is DELETING and no object was deleted; {}",
                fenced_deletion_note(&problem)
            ),
            FinishError::Failed(e) => tracing::error!(
                "DeleteBackup {arn_text}: the row is DELETING but its objects could not be \
                 deleted; the reconciler retries: {e}"
            ),
        }
    }
    desc.backup_details.backup_status = BackupStatus::Deleted.as_str().to_owned();
    Ok(desc)
}

/// The rest of the log line for a deletion the fence refused: what the
/// fence found (`problem`) and when the deletion finishes. Only a file that
/// names another catalog is an adoption; a file that could not be read, or
/// is missing, says so, and an unhealthy store names its problem.
fn fenced_deletion_note(problem: &StoreProblem) -> String {
    match problem {
        StoreProblem::ForeignCatalog { .. } => format!(
            "another catalog adopted the root after the first fence ({problem}); the deletion \
             finishes if this catalog owns the root again"
        ),
        StoreProblem::Transport(_)
        | StoreProblem::Denied(_)
        | StoreProblem::Validate(_)
        | StoreProblem::FileUnreadable(_)
        | StoreProblem::FileMissingWithRows(_) => format!(
            "the store's identity file could not be read ({problem}); the reconciler finishes \
             the deletion once it can"
        ),
        StoreProblem::IdMismatch { .. }
        | StoreProblem::MultipleRowIds(_)
        | StoreProblem::CreateRaceLost { .. }
        | StoreProblem::ManifestMissing { .. }
        | StoreProblem::Catalog(_) => format!(
            "the store is unhealthy ({problem}); the reconciler finishes the deletion once the \
             store's check passes"
        ),
    }
}

/// Why [`finish_backup_delete`] left the row DELETING.
#[derive(Debug)]
pub(crate) enum FinishError {
    /// The deletion fence refused before any object was deleted
    /// ([`StoreFence`]): the store is not healthy, or its identity file
    /// names another catalog or could not be read.
    Fenced(StoreProblem),
    /// A store or catalog call failed, or the prefix still lists an object.
    Failed(String),
}

impl std::fmt::Display for FinishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fenced(problem) => write!(f, "the deletion was refused: {problem}"),
            Self::Failed(e) => f.write_str(e),
        }
    }
}

/// The deletion of a DELETING row's objects and the row's move to DELETED:
/// the deletion fence (`fence`, which re-reads `extenddb-store.json` and
/// refuses when the root is no longer this catalog's), then
/// `extenddb-manifest.json` (a `delete_prefix` on its exact key, so the
/// prefix is no longer a complete backup to any reader), then the rest of the
/// prefix, then, once a listing of the prefix returns nothing,
/// `set_backup_deleted`. Shared by DeleteBackup and the reconciler's duty for
/// a DELETING row; either may run it on a row the other started, and a run
/// that finds the row no longer DELETING (`false` from the catalog) changes
/// nothing more. Returns whether this run moved the row.
///
/// # Errors
///
/// The fence's refusal, with nothing deleted; a store failure, a listing
/// that still shows objects (a put that overlapped the deletion, which the
/// next run removes), or a catalog write failure. The row stays DELETING
/// for the next run.
pub(crate) async fn finish_backup_delete<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn extenddb_backup_store::BackupStore,
    fence: &StoreFence,
    arn: &BackupArn,
) -> Result<bool, FinishError> {
    let prefix = arn.store_prefix();
    let manifest_key = format!(
        "{prefix}{}",
        extenddb_core::backup_format::EXTENDDB_MANIFEST_FILE
    );
    fence
        .check_catalog(storage)
        .await
        .map_err(FinishError::Fenced)?;
    let failed =
        |what: &str, e: &dyn std::fmt::Display| FinishError::Failed(format!("{what}: {e}"));
    store
        .delete_prefix(&manifest_key)
        .await
        .map_err(|e| failed("deleting the manifest", &e))?;
    store
        .delete_prefix(&prefix)
        .await
        .map_err(|e| failed("deleting the prefix", &e))?;
    let remaining = store
        .list(&prefix)
        .try_next()
        .await
        .map_err(|e| failed("listing the prefix after the deletion", &e))?;
    if let Some(object) = remaining {
        return Err(FinishError::Failed(format!(
            "the prefix still lists {} after the deletion",
            object.key
        )));
    }
    storage
        .set_backup_deleted(&BackupKey::from_arn(arn))
        .await
        .map_err(|e| failed("marking the row DELETED", &e))
}

/// Overrides refused outright. `VectorIndexOverride` is not in the botocore
/// 1.33.13 model; newer models and SDKs send it. The index overrides and
/// `OnDemandThroughputOverride` are applied as the service applies them; see
/// [`overrides`].
const UNSUPPORTED_RESTORE_TABLE_OVERRIDE_FIELDS: [&str; 1] = ["VectorIndexOverride"];

/// The rules that depend only on the request itself: a PROVISIONED override
/// needs a throughput, and a throughput must be well formed. Checked before
/// anything is read from storage.
fn validate_override_shape(overrides: &RestoreTableOverrides) -> Result<(), DynamoDbError> {
    if matches!(overrides.billing_mode, Some(BillingMode::Provisioned))
        && overrides.provisioned_throughput.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: ProvisionedThroughputOverride must \
             be specified when BillingModeOverride is PROVISIONED"
                .to_owned(),
        ));
    }

    if let Some(throughput) = &overrides.provisioned_throughput {
        let input = CreateTableInput {
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(throughput.clone()),
            ..Default::default()
        };
        extenddb_core::validation::validate_provisioned_throughput(&input)?;
    }
    Ok(())
}

/// The rule that needs the backup's own billing mode: a throughput override
/// is only meaningful if the restored table is PROVISIONED.
fn validate_override_for_source(
    overrides: &RestoreTableOverrides,
    source_billing_mode: &str,
) -> Result<(), DynamoDbError> {
    let effective = overrides
        .billing_mode
        .unwrap_or(if source_billing_mode == "PAY_PER_REQUEST" {
            BillingMode::PayPerRequest
        } else {
            BillingMode::Provisioned
        });
    if overrides.provisioned_throughput.is_some() && effective == BillingMode::PayPerRequest {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: ProvisionedThroughputOverride can \
             only be specified with BillingModeOverride PROVISIONED"
                .to_owned(),
        ));
    }
    Ok(())
}

fn parse_restore_table_overrides(body: &Value) -> Result<RestoreTableOverrides, DynamoDbError> {
    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "BillingModeOverride",
            valid: &["PROVISIONED", "PAY_PER_REQUEST"],
            clause: crate::EnumClause::Named("billingModeOverride"),
        }],
    )?;

    for field in UNSUPPORTED_RESTORE_TABLE_OVERRIDE_FIELDS {
        if body.get(field).is_some_and(|v| !v.is_null()) {
            return Err(DynamoDbError::ValidationException(format!(
                "RestoreTableFromBackup does not support {field} on ExtendDB"
            )));
        }
    }
    let field = |name: &str| body.get(name).filter(|v| !v.is_null()).cloned();
    let billing_mode = field("BillingModeOverride")
        .map(serde_json::from_value)
        .transpose()
        .map_err(crate::deserialize_error)?;
    let provisioned_throughput = field("ProvisionedThroughputOverride")
        .map(serde_json::from_value)
        .transpose()
        .map_err(crate::deserialize_error)?;
    let sse_specification = match field("SSESpecificationOverride") {
        None => None,
        Some(v @ Value::Object(_)) => Some(v),
        Some(_) => {
            return Err(DynamoDbError::SerializationException(
                "Start of structure or map found where not expected: SSESpecificationOverride"
                    .to_owned(),
            ));
        }
    };

    Ok(RestoreTableOverrides {
        billing_mode,
        provisioned_throughput,
        sse_specification,
    })
}

/// Handle `RestoreTableFromBackup`.
pub(crate) async fn handle_restore_table_from_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let auth_cache = ctx.auth_cache.clone();
    restore_table_from_backup_detached(
        Arc::clone(&ctx.storage),
        Arc::clone(&ctx.backup),
        Arc::clone(&ctx.account_id),
        Arc::clone(&ctx.region),
        Arc::clone(&ctx.limits),
        body,
        move |account_id, target_table_name| async move {
            // Drop a negative TableKeyInfo entry cached by a probe of the
            // target name before this restore registered it. This runs once,
            // at registration, before the copy: a probe during the copy
            // (the target is CREATING, so `table_key_info` reports it not
            // found) caches a negative entry again, and nothing here fires
            // at the ACTIVE transition. The key-info cache therefore does not
            // cache not-found for a name the catalog has in a non-ACTIVE
            // state (`crates/server/src/key_info_cache.rs`), which is what
            // makes the first read after ACTIVE succeed. Tags are not
            // propagated through restore today; if that ever changes, also
            // invalidate resource_tags for the new ARN, see
            // handle_create_table.
            auth_cache
                .invalidate_table_key_info(&account_id, &target_table_name)
                .await;
        },
    )
    .await
}

/// Run one restore, and then `after` with the target name, in a task of its
/// own, and wait for it. A client that disconnects after sending the request
/// drops only the wait, so the restore is not cancelled part-way. Without
/// this, a disconnect while the backup was being read (step 4 below) dropped
/// the request before the backend registered the restore, and no restore ran.
async fn restore_table_from_backup_detached<S, F, Fut>(
    storage: Arc<S>,
    runtime: Arc<BackupRuntime>,
    account_id: Arc<str>,
    region: Arc<str>,
    limits: Arc<LimitsConfig>,
    body: Value,
    after: F,
) -> Result<Value, DynamoDbError>
where
    S: BackupCatalog
        + LegacyBackupReader
        + TableEngine
        + DataEngine
        + RestoreEngine
        + ?Sized
        + Send
        + Sync
        + 'static,
    F: FnOnce(Arc<str>, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let (target_table_name, response) =
            restore_table_from_backup(&storage, &runtime, &account_id, &region, &limits, &body)
                .await?;
        after(account_id, target_table_name).await;
        Ok(response)
    })
    .await
    .map_err(|e| {
        tracing::error!("RestoreTableFromBackup task failed: {e}");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })?
}

/// One restore, returning the target name and the response body. Steps, in
/// order: (1) parse the overrides and check their shape, with no storage
/// read; (2) check `BackupArn`: missing or malformed is a
/// `ValidationException`, another account's is an `AccessDeniedException`;
/// (3) check `TargetTableName`'s syntax; (4) read the backup: the catalog
/// index's row first ([`check_catalog_row_status`]: a CREATING row is
/// `BackupInUseException`, a DELETING, DELETED or FAILED row
/// `BackupNotFoundException`), else the legacy reader's restore source, a
/// missing backup being `BackupNotFoundException` whatever the target
/// name's state; (5) check the target name under the account through
/// `describe_table` ([`check_restore_target_name`]: a table of that name is
/// `TableAlreadyExistsException`, a CREATING restore target
/// `TableInUseException` naming its source backup), after the backup is
/// known to exist and before its in-use rule and the overrides are checked
/// against it, the order the service uses (measured on DynamoDB: a restore
/// of a missing backup into an existing table, into a table CreateTable is
/// still creating and into a CREATING restore target all answer
/// `BackupNotFoundException`; an in-use backup into an existing table
/// answers `TableAlreadyExistsException`, and a restore of a backup into
/// its own CREATING target `TableInUseException`); then, for a row, the
/// rest of its checks ([`check_catalog_row_for_restore`]: a backup in use is
/// `BackupInUseException`, a row of another store a `ValidationException`,
/// a row of the configured store while it is unhealthy one naming the
/// store's kind), and an AVAILABLE row of the healthy configured store is
/// restored from the store by the restore job ([`restore::request`]: the
/// permit (when none is freed within the runtime's wait,
/// `LimitExceededException`), the manifest and its checks, the vector
/// refusal of a target without vector support, the overrides, the detached
/// registration whose answer the request awaits; the maintenance claim is
/// checked by the job once its record exists, the design's 5.3 step 4); the
/// steps below are the legacy path's: (6) refuse what no backend restores
/// faithfully from an in-catalog backup (multi-part key, vector indexes);
/// (7) check the throughput override against the backup's billing mode,
/// build the target's `CreateTableInput`, and apply the index and on-demand
/// throughput overrides to it, refusing what the service refuses
/// ([`overrides`]); (8) validate that input as CreateTable does; (9) refuse
/// while a maintenance command holds the catalog's claim
/// ([`refuse_while_maintenance_held`]), then take the job permit, last among
/// the checks (`LimitExceededException` for either; the permit when none is
/// freed within the runtime's wait), and ask the backend to create and
/// register the target; (10) map its errors, a taken name being
/// `TableAlreadyExistsException`, a name being restored into
/// `TableInUseException` (the backend checks the name again, fenced, under
/// the registration), a backup being restored from `BackupInUseException`.
async fn restore_table_from_backup<S>(
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    account_id: &str,
    region: &str,
    limits: &LimitsConfig,
    body: &Value,
) -> Result<(String, Value), DynamoDbError>
where
    S: BackupCatalog
        + LegacyBackupReader
        + TableEngine
        + DataEngine
        + RestoreEngine
        + ?Sized
        + Send
        + Sync
        + 'static,
{
    // 1. The overrides, parsed and shape-checked: no storage I/O, so a
    // malformed request is refused before a missing backup can take
    // precedence over the request's own error.
    let overrides = parse_restore_table_overrides(body)?;
    validate_override_shape(&overrides)?;
    let index_overrides = overrides::IndexOverrides::parse(body)?;

    // 2. The ARN before the target name: the service checks the ARN's account
    // first (measured on DynamoDB with two invalid inputs).
    let backup_arn = backup_arn_field(body, account_id, region)?;

    // 3. The name's syntax, before the backup is read.
    let target_table_name = required_str(body, "TargetTableName", "targetTableName")?;
    extenddb_core::validation::validate_table_name(target_table_name, limits)?;

    // 4. The catalog index first: a row of the healthy configured store is
    // restored from the store. With no row, the legacy reader under the ARN
    // spelling that resolves; the restore is registered against that
    // spelling, the one the backend stored. A legacy not-found re-reads the
    // catalog index once.
    let mut held = read_catalog_row(&**storage, &backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;
    let mut legacy = None;
    if held.is_none() {
        match resolve_backup(&backup_arn, by!(&**storage, account_id, restore_source)).await {
            Ok(found) => legacy = Some(found),
            // A `backup migrate-legacy` replacement that committed between
            // the two reads: the row is there now.
            Err(e) if is_backup_not_found(&e) => {
                held = read_catalog_row(&**storage, &backup_arn)
                    .await
                    .map_err(storage_err_to_dynamo)?;
                if held.is_none() {
                    return Err(storage_err_to_dynamo(e));
                }
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        }
    }
    if let Some((arn, row)) = held {
        return restore_from_row(
            storage,
            runtime,
            account_id,
            limits,
            target_table_name,
            &arn,
            &row,
            &overrides,
            &index_overrides,
        )
        .await;
    }
    let storage_arc = storage;
    let requested = backup_arn;
    let storage = &**storage;
    let Some((backup_arn, source)) = legacy else {
        return Err(storage_err_to_dynamo(StorageError::Internal(
            "the restore found neither a catalog row nor an in-catalog backup".to_owned(),
        )));
    };

    // 5. The target name, after the backup resolved and before the overrides
    // are checked against it.
    check_restore_target_name(storage, account_id, target_table_name).await?;

    // 6. Refusals every backend gets, whatever it stores.
    source.ensure_restorable().map_err(storage_err_to_dynamo)?;

    // 7.
    validate_override_for_source(&overrides, &source.billing_mode)?;
    let mut input = source.restore_input(target_table_name, &overrides);
    index_overrides.apply(&mut input, overrides.billing_mode)?;

    // 8. A stored definition the service would refuse at CreateTable is
    // refused here too (the service re-validates on restore): a backup row
    // is data, and data can be wrong. The rules are CreateTable's but two:
    // a name defined twice in AttributeDefinitions and a key schema with
    // more keys than definitions, which earlier releases accepted at
    // CreateTable (`validate_recorded_table`). So a table CreateTable or
    // UpdateTable accepted restores.
    extenddb_core::validation::validate_recorded_table(&input, limits)?;
    let on_demand = input.billing_mode == Some(BillingMode::PayPerRequest);

    // 9. The maintenance claim, then the permit last: every refusal above
    // needs no slot, and the request holds nothing while it waits for one.
    let request = RestoreRequest {
        backup_arn: backup_arn.clone(),
        source_table_arn: source.source_table_arn.clone(),
        restore_date_time: source.backup_creation_date_time,
        input,
    };
    refuse_while_maintenance_held(storage, runtime).await?;
    let permit = runtime.take_permit().await?;
    let desc = match storage.restore(account_id, request, permit).await {
        Ok(desc) => desc,
        // A `backup migrate-legacy` replacement that committed between the
        // source read and the backend's registration (which re-checks the
        // backup under its lock, and removes the target it created): the
        // row is there now, and the restore takes the row's path.
        Err(e) if is_backup_not_found(&e) => {
            let Some((arn, row)) = read_catalog_row(storage, &requested)
                .await
                .map_err(storage_err_to_dynamo)?
            else {
                return Err(restore_storage_err_to_dynamo(e));
            };
            return restore_from_row(
                storage_arc,
                runtime,
                account_id,
                limits,
                target_table_name,
                &arn,
                &row,
                &overrides,
                &index_overrides,
            )
            .await;
        }
        // 10.
        Err(e) => return Err(restore_storage_err_to_dynamo(e)),
    };

    let response = restore_response(desc, &RestoreSource::from(&source), on_demand)?;
    Ok((target_table_name.to_owned(), response))
}

/// Steps 4 and 5 of a restore for a backup the catalog index has a row
/// for, then the store-backed restore job ([`restore::request`]).
#[allow(clippy::too_many_arguments)]
async fn restore_from_row<S>(
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    account_id: &str,
    limits: &LimitsConfig,
    target_table_name: &str,
    arn: &BackupArn,
    row: &BackupRow,
    overrides: &RestoreTableOverrides,
    index_overrides: &overrides::IndexOverrides,
) -> Result<(String, Value), DynamoDbError>
where
    S: BackupCatalog
        + LegacyBackupReader
        + TableEngine
        + DataEngine
        + RestoreEngine
        + ?Sized
        + Send
        + Sync
        + 'static,
{
    check_catalog_row_status(arn, row)?;
    // 5, for a row: the target name once the backup is known to exist,
    // before the backup's in-use rule (measured on DynamoDB: an in-use
    // backup into an existing table is TableAlreadyExistsException, a
    // backup into its own CREATING target TableInUseException, not
    // BackupInUseException) and before the manifest is read.
    check_restore_target_name(&**storage, account_id, target_table_name).await?;
    let store = check_catalog_row_for_restore(&**storage, runtime, arn, row).await?;
    restore::request(
        storage,
        runtime,
        store,
        account_id,
        limits,
        target_table_name,
        arn,
        overrides,
        index_overrides,
    )
    .await
}

/// Step 4 of a restore for a backup the catalog index has a row for, the
/// row's own state, before the target name is read: a CREATING row is
/// `BackupInUseException` ("Backup is being created"); a DELETING, DELETED
/// or FAILED row is `BackupNotFoundException`; an AVAILABLE row passes.
fn check_catalog_row_status(arn: &BackupArn, row: &BackupRow) -> Result<(), DynamoDbError> {
    let arn_text = arn.to_arn_string();
    match row.status {
        BackupStatus::Creating => Err(backup_in_use(&arn_text, None)),
        BackupStatus::Deleting | BackupStatus::Deleted | BackupStatus::Failed => {
            Err(backup_not_found(&arn_text))
        }
        BackupStatus::Available => Ok(()),
    }
}

/// Step 4 of a restore for an AVAILABLE row ([`check_catalog_row_status`]),
/// after the target name and before the permit, reading no store: a backup
/// a CREATING restore target still reads is `BackupInUseException` naming
/// that table in the log ([`BackupCatalog::backup_in_use`]); a row of a
/// store other than the configured one is a `ValidationException` naming no
/// root, and a row of the configured store while that store is unhealthy or
/// its first check pending is a `ValidationException` naming the store's
/// kind ([`BackupRuntime::require_healthy_store`]). An AVAILABLE row of the
/// healthy configured store passes, and that store is returned: the restore
/// reads the backup from it ([`restore::request`]).
async fn check_catalog_row_for_restore<'r, S: BackupCatalog + ?Sized>(
    storage: &S,
    runtime: &'r BackupRuntime,
    arn: &BackupArn,
    row: &BackupRow,
) -> Result<&'r Arc<ConfiguredStore>, DynamoDbError> {
    let arn_text = arn.to_arn_string();
    if let Some(table) = storage
        .backup_in_use(&BackupKey::from_arn(arn))
        .await
        .map_err(storage_err_to_dynamo)?
    {
        return Err(backup_in_use(&arn_text, Some(&table)));
    }
    if !runtime.is_configured_store(&row.store) {
        return Err(backup_in_another_store(&arn_text));
    }
    runtime.require_healthy_store()
}

/// Step 5 of a restore, the target name under the account, read through
/// `describe_table` after the backup resolved and before the permit: a
/// CREATING table with a
/// `RestoreSummary` is a restore target, `TableInUseException` naming its
/// source backup (the text the backend's own registration answers with,
/// [`StorageError::TableBeingRestored`]); any other table of that name is
/// `TableAlreadyExistsException`; no table passes. Unfenced: the backend
/// checks the name again under its registration, so two restores into one
/// free name still have one winner.
async fn check_restore_target_name<S: TableEngine + ?Sized>(
    storage: &S,
    account_id: &str,
    target_table_name: &str,
) -> Result<(), DynamoDbError> {
    let probe = storage
        .describe_table(
            account_id,
            extenddb_core::types::DescribeTableInput {
                table_name: target_table_name.to_owned(),
            },
        )
        .await;
    match probe {
        Ok(desc) => {
            let source = desc
                .restore_summary
                .as_ref()
                .filter(|_| desc.table_status == TableStatus::Creating)
                .and_then(|s| s.source_backup_arn.clone());
            Err(restore_storage_err_to_dynamo(match source {
                Some(backup_arn) => StorageError::TableBeingRestored {
                    table_name: target_table_name.to_owned(),
                    backup_arn,
                },
                None => StorageError::TableAlreadyExists(target_table_name.to_owned()),
            }))
        }
        Err(StorageError::TableNotFound(_)) => Ok(()),
        Err(e) => Err(storage_err_to_dynamo(e)),
    }
}

/// The RestoreTableFromBackup response body for the backend's description,
/// on both paths (`source` is the legacy reader's source or the manifest's
/// three members).
///
/// The service returns the target CREATING with `RestoreSummary`
/// { `SourceBackupArn`, `SourceTableArn`, `RestoreDateTime` = the backup's
/// creation time, `RestoreInProgress: true` }, omits `IndexStatus` on each
/// GSI, and omits `LastUpdateToPayPerRequestDateTime`, `DeletionProtectionEnabled`,
/// and the table's `ProvisionedThroughput.NumberOfDecreasesToday` (all measured on
/// DynamoDB; `SourceBackupArn` and `RestoreInProgress` also by the
/// ground-truth runs of 2026-08-24).
fn restore_response(
    mut desc: TableDescription,
    source: &RestoreSource,
    on_demand: bool,
) -> Result<Value, DynamoDbError> {
    desc.table_status = TableStatus::Creating;
    if on_demand {
        // Backends may retain catalog capacity fields while creating an
        // on-demand restore target; the response reports on-demand zeros.
        desc.provisioned_throughput = Default::default();
        for index in desc.global_secondary_indexes.iter_mut().flatten() {
            index.provisioned_throughput = Some(Default::default());
        }
    }
    desc.restore_summary = Some(RestoreSummary {
        source_backup_arn: Some(source.backup_arn.clone()),
        source_table_arn: Some(source.source_table_arn.clone()),
        restore_date_time: source.backup_creation_date_time,
        restore_in_progress: true,
    });
    if let Some(summary) = desc.billing_mode_summary.as_mut() {
        summary.last_update_to_pay_per_request_date_time = None;
    }

    // The readiness invariant is applied on every path that hands a
    // description to a client: a legacy restore's target has no vector
    // indexes (the reader refuses them); a store-backed restore's may, and
    // the backend reports them as its CreateTable does.
    desc.validate_vector_index_readiness()?;
    // Same rules as every other path that emits a table description; the
    // target is CREATING, so the summary set above is kept.
    desc.prepare_for_response();

    let mut out = serialize_output(&json!({ "TableDescription": desc }))?;
    // Members the service omits from this response (measured on DynamoDB):
    // the table's DeletionProtectionEnabled and its ProvisionedThroughput's
    // NumberOfDecreasesToday. DescribeTable reports both. A GSI's
    // NumberOfDecreasesToday is kept; what the service sends there was not
    // measured.
    if let Some(td) = out
        .get_mut("TableDescription")
        .and_then(Value::as_object_mut)
    {
        td.remove("DeletionProtectionEnabled");
        if let Some(pt) = td
            .get_mut("ProvisionedThroughput")
            .and_then(Value::as_object_mut)
        {
            pt.remove("NumberOfDecreasesToday");
        }
    }
    if let Some(indexes) = out
        .pointer_mut("/TableDescription/GlobalSecondaryIndexes")
        .and_then(Value::as_array_mut)
    {
        for index in indexes.iter_mut().filter_map(Value::as_object_mut) {
            index.remove("IndexStatus");
        }
    }
    Ok(out)
}

/// [`storage_err_to_dynamo`] for RestoreTableFromBackup, where a taken target
/// name is `TableAlreadyExistsException` (the operation's own error in the
/// service model, and what the service returns), and a name taken by a
/// `CREATING` restore target is `TableInUseException` with the service's text
/// (`Table: <name> is already being restored from backup: <arn>`, measured on
/// DynamoDB), which the storage error's `Display` carries.
/// A backup with a restore from it still `CREATING` is `BackupInUseException`
/// (the generic mapping; measured on DynamoDB). Only this operation maps the
/// first two so; CreateTable keeps `ResourceInUseException`.
fn restore_storage_err_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::TableAlreadyExists(name) => DynamoDbError::TableAlreadyExistsException(
            error_message(ErrorMessageKey::TableAlreadyExists, &[&name]),
        ),
        e @ StorageError::TableBeingRestored { .. } => {
            DynamoDbError::TableInUseException(e.to_string())
        }
        other => storage_err_to_dynamo(other),
    }
}

mod job;
pub mod maintenance;
pub mod migrate_legacy;
mod overrides;
pub mod reconciler;
mod restore;
mod runtime;
pub mod store_identity;
pub mod store_maintenance;
pub mod sync;
#[cfg(test)]
mod tests;
