// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine handlers for `DynamoDB` backup and point-in-time recovery operations.
//!
//! The rules that decide what a backup restores to, which backups and
//! requests are refused, and what the responses carry live here rather than in
//! the backends, so every backend restores the same table from the same backup
//! and reports it the same way. Backends store and copy.
//!
//! Wire member names follow the 2012-08-10 service model in botocore 1.33.13
//! (`botocore/data/dynamodb/2012-08-10/service-2.json.gz`): `ListBackupsInput`
//! (`Limit` 1..=100, `ExclusiveStartBackupArn`), `ListBackupsOutput`
//! (`LastEvaluatedBackupArn`), `RestoreTableFromBackupInput`, `RestoreSummary`,
//! `SourceTableFeatureDetails`, and the operation's `TableAlreadyExistsException`.

use std::sync::Arc;

use extenddb_core::backup_format::{BackupArn, check_store_prefix_table_name, is_region};
use extenddb_core::error::{DynamoDbError, ErrorMessageKey, error_message};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BackupDescription, BackupSummary, BillingMode, CreateTableInput, GlobalSecondaryIndexInfo,
    LocalSecondaryIndexInfo, ProvisionedThroughput, RestoreSummary, SourceTableFeatureDetails,
    SseDescription, TableDescription, TableStatus,
};
use extenddb_storage::backup_catalog::{
    BackupKey, BackupRow, BackupStatus, DeleteStart, TimeBounds, is_backup_not_found,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, BackupRestoreSource, LegacyBackupReader, RestoreRequest, RestoreTableOverrides,
    TableEngine,
};
use futures::TryStreamExt;
use serde_json::{Value, json};

pub use runtime::BackupRuntime;

use crate::{OperationContext, serialize_output};

/// `ListBackups` page size when the request gives no `Limit`, and the largest
/// it accepts (`BackupsInputLimit`: min 1, max 100).
const LIST_BACKUPS_MAX_LIMIT: i64 = 100;

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
/// can carry such an ARN, since `check_backup_arn_fits` refuses the name at
/// CreateBackup), and the legacy reader is tried under its string as on
/// main.
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
/// target still reads: `BackupInUseException`, as the service answers.
fn backup_in_use(arn: &str, restoring_into: Option<&str>) -> DynamoDbError {
    DynamoDbError::BackupInUseException(match restoring_into {
        Some(table) => extenddb_storage::backup_catalog::backup_in_use_message(table, arn),
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

/// The ARN this deployment issued for the backup `parts` names: its own
/// region, whatever region the caller wrote.
fn canonical_backup_arn(
    parts: &extenddb_storage::util::BackupArnParts<'_>,
    region: &str,
) -> String {
    extenddb_storage::util::backup_arn(region, parts.account_id, parts.table_name, parts.backup_id)
}

/// The shape check of [`backup_arn_field`], for any member that carries a
/// backup ARN, with the service's two texts (measured on DynamoDB; see
/// [`extenddb_storage::util::BackupArnError`] for which inputs get which).
/// A partition other than [`ARN_PARTITION`] is refused here too (the service
/// answers `ValidationException` for one naming the caller's account,
/// measured on DynamoDB; its text was not captured, so the "not valid" one
/// is used), before the account is looked at.
///
/// [`ARN_PARTITION`]: extenddb_storage::util::ARN_PARTITION
fn check_backup_arn(
    backup_arn: &str,
) -> Result<extenddb_storage::util::BackupArnParts<'_>, DynamoDbError> {
    use extenddb_storage::util::BackupArnError;
    let parts = extenddb_storage::util::check_backup_arn(backup_arn).map_err(|e| {
        DynamoDbError::ValidationException(match e {
            BackupArnError::Invalid => BACKUP_ARN_NOT_VALID.to_owned(),
            BackupArnError::Id => "Invalid Backup ARN".to_owned(),
        })
    })?;
    if parts.partition != extenddb_storage::util::ARN_PARTITION {
        return Err(DynamoDbError::ValidationException(
            BACKUP_ARN_NOT_VALID.to_owned(),
        ));
    }
    Ok(parts)
}

/// The service's text for a `BackupArn` that is not an ARN of the right
/// shape (measured on DynamoDB).
const BACKUP_ARN_NOT_VALID: &str = "Invalid Request: BackupArn is not valid";

fn required_str<'a>(body: &'a Value, member: &str, path: &str) -> Result<&'a str, DynamoDbError> {
    body.get(member).and_then(|v| v.as_str()).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value null at '{path}' \
             failed to satisfy constraint: Member must not be null"
        ))
    })
}

/// The service model's constraints on `BackupName` and `TableName` (botocore
/// 1.33.13: length 3 to 255, pattern `[a-zA-Z0-9_.-]+`), with the service's
/// texts: one constraint failed is `1 validation error detected: Value '<v>'
/// at '<path>' failed to satisfy constraint: <c>`; both failed join under
/// `2 validation errors detected:` with `; ` between (measured on DynamoDB;
/// the order of the two clauses was not captured, so length
/// comes first, as `CreateTable`'s table-name check reports it). The value is
/// echoed as sent, as the service does, so a long name makes a long message.
///
/// Length is counted in characters, as the model's `min`/`max` are, so a
/// name of two-byte characters is measured as the service measures it.
/// `BackupName` takes the model's 3 to 255; a `TableName` member takes the
/// deployment's `LimitsConfig` bounds ([`table_name_bounds`]), as
/// `CreateTable` does, so a table the deployment could create can be backed
/// up and listed.
fn validate_name_member(
    value: &str,
    path: &str,
    (min, max): (usize, usize),
) -> Result<(), DynamoDbError> {
    let mut failed = Vec::new();
    let length = value.chars().count();
    let too_short = format!("Member must have length greater than or equal to {min}");
    let too_long = format!("Member must have length less than or equal to {max}");
    if length < min {
        failed.push(too_short.as_str());
    } else if length > max {
        failed.push(too_long.as_str());
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
    {
        failed.push("Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
    }
    match failed.len() {
        0 => Ok(()),
        n => Err(DynamoDbError::ValidationException(format!(
            "{n} validation error{} detected: {}",
            if n == 1 { "" } else { "s" },
            failed
                .iter()
                .map(|c| format!("Value '{value}' at '{path}' failed to satisfy constraint: {c}"))
                .collect::<Vec<_>>()
                .join("; ")
        ))),
    }
}

/// The service model's `BackupName` length bounds.
const BACKUP_NAME_BOUNDS: (usize, usize) = (3, 255);

/// The length bounds for a `TableName` member: the deployment's, which
/// default to the model's 3 to 255.
fn table_name_bounds(limits: &LimitsConfig) -> (usize, usize) {
    (limits.min_table_name_length, limits.max_table_name_length)
}

/// The `TableName` of `CreateBackup` and `ListBackups`, which the service
/// accepts as the table's name or its ARN (measured on DynamoDB; the model
/// gives the member the `TableName` shape, max 255, so an SDK built from it
/// refuses the ARN form client-side and only a hand-built request sends it).
/// An ARN is `arn:aws:dynamodb:<region>:<account>:table/<name>`: another
/// account's is `AccessDeniedException`, as a backup ARN of another account
/// is, and another region's resolves, as a backup ARN's does (what the
/// service answers for those two was not measured). The name part is then
/// checked as a name.
fn create_backup_table_name<'a>(
    body: &'a Value,
    account_id: &str,
    limits: &LimitsConfig,
) -> Result<&'a str, DynamoDbError> {
    let value = required_str(body, "TableName", "tableName")?;
    let Some(rest) = value.strip_prefix("arn:") else {
        validate_name_member(value, "tableName", table_name_bounds(limits))?;
        return Ok(value);
    };
    let not_valid = || {
        DynamoDbError::ValidationException(
            "Invalid Request: TableName is not a valid table name or table ARN".to_owned(),
        )
    };
    let fields: Vec<&str> = rest.split(':').collect();
    let [partition, "dynamodb", region, arn_account, resource] = fields.as_slice() else {
        return Err(not_valid());
    };
    if *partition != extenddb_storage::util::ARN_PARTITION
        || region.is_empty()
        || arn_account.is_empty()
        || !arn_account.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(not_valid());
    }
    let Some(name) = resource.strip_prefix("table/") else {
        return Err(not_valid());
    };
    if name.contains('/') {
        return Err(not_valid());
    }
    validate_name_member(name, "tableName", table_name_bounds(limits))?;
    if *arn_account != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    Ok(name)
}

/// Handle `CreateBackup`.
pub(crate) async fn handle_create_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = create_backup_table_name(&body, &ctx.account_id, &ctx.limits)?;
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
        &*ctx.storage,
        &ctx.backup,
        &ctx.account_id,
        &ctx.region,
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
/// whether or not a permit is free); the server's region and the table name
/// fit a backup ARN and its store prefix (`check_backup_arn_fits`: a
/// `server.region` outside `[a-z0-9-]` is a `ValidationException` naming the
/// setting, a name over 255 bytes or made only of dots one naming the rule);
/// the table has no vector index
/// (the in-catalog format cannot restore one); then the job permit
/// (`LimitExceededException` when none is free). `probe` is the
/// `DescribeTable` of the table, which the handler ran.
///
/// This is the in-catalog create, the path while no `[backup.store]` is
/// configured; each call logs that the format is deprecated.
async fn create_backup<S: LegacyBackupReader + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
    account_id: &str,
    region: &str,
    table_name: &str,
    backup_name: &str,
    probe: Result<TableDescription, StorageError>,
) -> Result<extenddb_core::types::BackupDetails, DynamoDbError> {
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
    check_backup_arn_fits(region, account_id, table_name)?;
    refuse_backup_of_vector_table(&desc)?;
    let permit = runtime.take_permit()?;
    tracing::warn!(
        "CreateBackup of {table_name} (account {account_id}) is writing the in-catalog backup \
         format, which a later release stops creating; configure [backup.store] to write \
         backups to a backup store"
    );
    storage
        .create(account_id, table_name, backup_name, permit)
        .await
        .map_err(|e| create_backup_storage_err_to_dynamo(e, table_name))
}

/// The backup ARN rules a table name has to fit before a backup of it can be
/// created, each refusal naming its cause: first the server's own region
/// (`server.region` outside `[a-z0-9-]` forms no backup ARN whatever the
/// name, so the message names the setting, not the table), then the two
/// store-prefix rules on the name (over 255 bytes, or made only of dots;
/// [`check_store_prefix_table_name`] names the limit whatever the name's
/// length, where forming the ARN first would answer "not a backup ARN" for a
/// name that takes the ARN over 1024 bytes), then the ARN itself. The id is
/// a placeholder of the right shape; only the region and the name are being
/// checked.
fn check_backup_arn_fits(
    region: &str,
    account_id: &str,
    table_name: &str,
) -> Result<(), DynamoDbError> {
    if !is_region(region) {
        return Err(DynamoDbError::ValidationException(format!(
            "CreateBackup is refused on this server: server.region {region:?} is outside \
             [a-z0-9-], so no backup ARN can be formed; set server.region to a value in \
             that class"
        )));
    }
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

/// The CreateBackup refusal for a table with vector indexes.
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
/// order; the source is then read under the spelling that resolved.
async fn describe_backup<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    account_id: &str,
    region: &str,
    backup_arn: &BackupArnSpellings,
) -> Result<BackupDescription, DynamoDbError> {
    if let Some((arn, row)) = read_catalog_row(storage, backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        if !row.status.is_described() {
            return Err(backup_not_found(&arn.to_arn_string()));
        }
        return row.description().map_err(storage_err_to_dynamo);
    }
    let (backup_arn, mut desc) = resolve_backup(backup_arn, by!(storage, account_id, describe))
        .await
        .map_err(storage_err_to_dynamo)?;
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
                stream_description: d.stream_specification.clone(),
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

/// Handle `ListBackups`.
pub(crate) async fn handle_list_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let page = list_backups(
        &*ctx.storage,
        &ctx.account_id,
        &ctx.region,
        &ctx.limits,
        &body,
    )
    .await?;
    serialize_output(&page)
}

/// `TableName` is a table name or a table ARN, as `CreateBackup` accepts it
/// (the service accepts both, measured on DynamoDB). The listing merges the
/// catalog index's rows (CREATING and AVAILABLE ones, summarized from their
/// recorded source table) with the legacy reader's summaries into one list,
/// which is then filtered and paged in (`BackupCreationDateTime`, ARN) order:
/// `created_at` is the id's instant on both sides, so a cursor places itself
/// in the merged order the same way whichever side issued it.
async fn list_backups<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    account_id: &str,
    region: &str,
    limits: &LimitsConfig,
    body: &Value,
) -> Result<Value, DynamoDbError> {
    let limit = match body.get("Limit") {
        None | Some(Value::Null) => LIST_BACKUPS_MAX_LIMIT,
        Some(v) => {
            let limit = v.as_i64().ok_or_else(|| {
                DynamoDbError::SerializationException(
                    "Start of structure or map found where not expected: Limit".to_owned(),
                )
            })?;
            if limit < 1 {
                return Err(DynamoDbError::ValidationException(format!(
                    "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy \
                     constraint: Member must have value greater than or equal to 1"
                )));
            }
            if limit > LIST_BACKUPS_MAX_LIMIT {
                return Err(DynamoDbError::ValidationException(format!(
                    "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy \
                     constraint: Member must have value less than or equal to \
                     {LIST_BACKUPS_MAX_LIMIT}"
                )));
            }
            limit
        }
    };
    let table_name = match body.get("TableName") {
        None | Some(Value::Null) => None,
        Some(Value::String(_)) => Some(create_backup_table_name(body, account_id, limits)?),
        Some(_) => {
            return Err(DynamoDbError::SerializationException(
                "Start of structure or map found where not expected: TableName".to_owned(),
            ));
        }
    };
    let start = match body.get("ExclusiveStartBackupArn") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let arn = v.as_str().ok_or_else(|| {
                DynamoDbError::SerializationException(
                    "Start of structure or map found where not expected: ExclusiveStartBackupArn"
                        .to_owned(),
                )
            })?;
            Some(list_backups_cursor(arn, account_id, region, table_name)?)
        }
    };
    let filter = ListBackupsFilter::parse(body)?;

    let bounds = TimeBounds {
        lower: filter.lower,
        upper: filter.upper,
    };
    // A row whose summary does not derive (a `source_table_details_json`
    // this build cannot read) is logged and left out, so one such row does
    // not turn every ListBackups of the account into an internal error; a
    // failure to read the rows propagates.
    let mut summaries: Vec<BackupSummary> = storage
        .list_account_backup_rows(account_id, table_name, &bounds)
        .try_filter_map(|row| async move {
            if !row.status.is_listed() {
                return Ok(None);
            }
            match row.summary() {
                Ok(summary) => Ok(Some(summary)),
                Err(e) => {
                    tracing::warn!(
                        "ListBackups (account {account_id}): backup {} left out of the \
                         listing: {e}",
                        row.backup_arn
                    );
                    Ok(None)
                }
            }
        })
        .try_collect()
        .await
        .map_err(storage_err_to_dynamo)?;
    summaries.extend(
        storage
            .list(account_id, table_name)
            .await
            .map_err(storage_err_to_dynamo)?,
    );
    let start = start.map(|cursor| cursor.resolve(&summaries)).transpose()?;
    let summaries = summaries.into_iter().filter(|s| filter.keeps(s)).collect();
    let (page, last) = page_backups(
        summaries,
        usize::try_from(limit).unwrap_or(100),
        start.as_deref(),
    )?;

    let mut out = json!({ "BackupSummaries": page });
    if let Some(last) = last {
        out["LastEvaluatedBackupArn"] = Value::String(last);
    }
    Ok(out)
}

/// The `ExclusiveStartBackupArn` of `ListBackups`, checked as the service
/// checks it (measured on DynamoDB): the shape as any backup
/// ARN, then the account, region, and (when `TableName` is given) the table
/// name must be the listing's, each a `ValidationException`. Unlike
/// `BackupArn`, another account's cursor is a `ValidationException`, not
/// `AccessDeniedException`, and another region's is refused rather than
/// resolved. The service's text for the account case was captured in full;
/// the region and table-name texts were captured as fragments ("does not
/// match the region", "does not match the table name predicate") and are
/// completed on the account text's pattern. Returns the cursor as this
/// deployment issued it (whitespace trimmed).
fn list_backups_cursor(
    arn: &str,
    account_id: &str,
    region: &str,
    table_name: Option<&str>,
) -> Result<ListBackupsCursor, DynamoDbError> {
    let parts = check_backup_arn(arn)?;
    if parts.account_id != account_id {
        return Err(cursor_mismatch("account"));
    }
    let other_table = table_name.is_some_and(|t| t != parts.table_name);
    if parts.region != region {
        // Region before table name; a listed backup's ARN names the filter's
        // table, so another table's cursor cannot be one.
        if other_table {
            return Err(cursor_mismatch("region"));
        }
        return Ok(ListBackupsCursor::OtherRegion(parts.arn.to_owned()));
    }
    if other_table {
        return Err(cursor_mismatch("table name predicate"));
    }
    Ok(ListBackupsCursor::Canonical(canonical_backup_arn(
        &parts, region,
    )))
}

fn cursor_mismatch(what: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Invalid Request: The supplied ExclusiveStartBackupArn does not match the {what}"
    ))
}

/// A checked `ExclusiveStartBackupArn`. One in another region is refused
/// unless it is the stored ARN of a listed backup: a backup created under an
/// earlier `server.region` keeps that region in its ARN, and `ListBackups`
/// hands that ARN out as `LastEvaluatedBackupArn`, so the next page must
/// accept it.
enum ListBackupsCursor {
    Canonical(String),
    OtherRegion(String),
}

impl ListBackupsCursor {
    fn resolve(self, listed: &[BackupSummary]) -> Result<String, DynamoDbError> {
        match self {
            Self::Canonical(arn) => Ok(arn),
            Self::OtherRegion(arn) if listed.iter().any(|s| s.backup_arn == arn) => Ok(arn),
            Self::OtherRegion(_) => Err(cursor_mismatch("region")),
        }
    }
}

/// `BackupTypeFilter` members (`ListBackupsInput.BackupType`).
const BACKUP_TYPE_FILTERS: [&str; 4] = ["USER", "SYSTEM", "AWS_BACKUP", "ALL"];

/// The `ListBackups` members that narrow the listing, applied before paging
/// so the cursor and `Limit` count only backups that pass.
///
/// `BackupType` keeps summaries of that type: `USER` when the member is
/// absent (the service model's default), every type for `ALL`. Every ExtendDB
/// backup is `USER`, so `SYSTEM` and `AWS_BACKUP` list nothing.
/// `TimeRangeLowerBound` keeps
/// backups created at or after it, `TimeRangeUpperBound` those created
/// before it (the service model documents the lower bound as inclusive and
/// the upper as exclusive). Bounds are epoch seconds, as the JSON protocol
/// sends a timestamp. A bound before the epoch is refused, and so is a lower
/// bound above the upper bound, with the service's text (both measured on
/// DynamoDB; the text for a bound before the epoch was not
/// captured). Equal bounds list nothing.
#[derive(Debug, Default)]
struct ListBackupsFilter {
    backup_type: Option<String>,
    lower: Option<f64>,
    upper: Option<f64>,
}

impl ListBackupsFilter {
    fn parse(body: &Value) -> Result<Self, DynamoDbError> {
        let backup_type = match body.get("BackupType") {
            // The service model's default.
            None | Some(Value::Null) => Some("USER".to_owned()),
            Some(Value::String(s)) => {
                crate::validate_enum_fields(
                    body,
                    &[crate::EnumField {
                        json_name: "BackupType",
                        valid: &BACKUP_TYPE_FILTERS,
                        clause: crate::EnumClause::Named("backupType"),
                    }],
                )?;
                (s != "ALL").then(|| s.clone())
            }
            Some(_) => {
                return Err(DynamoDbError::SerializationException(
                    "Start of structure or map found where not expected: BackupType".to_owned(),
                ));
            }
        };
        let bound = |member: &str| match body.get(member) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => {
                let at = v.as_f64().ok_or_else(|| {
                    DynamoDbError::SerializationException(format!(
                        "Start of structure or map found where not expected: {member}"
                    ))
                })?;
                if at < 0.0 || !at.is_finite() {
                    return Err(DynamoDbError::ValidationException(format!(
                        "Invalid Request: {member} must be a timestamp at or after \
                         1970-01-01T00:00:00Z"
                    )));
                }
                Ok(Some(at))
            }
        };
        let lower = bound("TimeRangeLowerBound")?;
        let upper = bound("TimeRangeUpperBound")?;
        if let (Some(lower), Some(upper)) = (lower, upper)
            && lower > upper
        {
            return Err(DynamoDbError::ValidationException(
                "Time range lower bound must be less than or equal to upper bound".to_owned(),
            ));
        }
        Ok(Self {
            backup_type,
            lower,
            upper,
        })
    }

    fn keeps(&self, summary: &BackupSummary) -> bool {
        let created = summary.backup_creation_date_time;
        self.backup_type
            .as_deref()
            .is_none_or(|t| summary.backup_type == t)
            && self.lower.is_none_or(|lower| created >= lower)
            && self.upper.is_none_or(|upper| created < upper)
    }
}

/// One `ListBackups` page: sorted by (`BackupCreationDateTime`, ARN)
/// ascending so the cursor is stable, starting after `start`, at most
/// `limit` long, with the last ARN returned as the cursor when more remain.
///
/// A cursor whose backup has since been deleted is placed by the creation
/// time embedded in its id (`util::backup_id_epoch_millis`); the backends
/// take `BackupCreationDateTime` from the id, so the position is exact. A
/// cursor that is not in the list and whose id carries no time (an ARN from a
/// build before the shared id) cannot be placed, and is refused rather than
/// answered with an empty page a client would read as the end of the list.
fn page_backups(
    mut all: Vec<BackupSummary>,
    limit: usize,
    start: Option<&str>,
) -> Result<(Vec<BackupSummary>, Option<String>), DynamoDbError> {
    fn key(s: &BackupSummary) -> (f64, &str) {
        (s.backup_creation_date_time, s.backup_arn.as_str())
    }
    fn cmp(a: (f64, &str), b: (f64, &str)) -> std::cmp::Ordering {
        a.0.total_cmp(&b.0).then_with(|| a.1.cmp(b.1))
    }
    all.sort_by(|a, b| cmp(key(a), key(b)));

    let first = match start {
        None => 0,
        Some(cursor) => match all.iter().position(|s| s.backup_arn == cursor) {
            Some(i) => i + 1,
            None => match extenddb_storage::util::backup_creation_date_time(cursor) {
                Some(at) => all
                    .iter()
                    .position(|s| cmp(key(s), (at, cursor)).is_gt())
                    .unwrap_or(all.len()),
                None => {
                    return Err(DynamoDbError::ValidationException(format!(
                        "ExclusiveStartBackupArn {cursor} does not name a backup in this \
                         listing, and its id carries no creation time to resume from"
                    )));
                }
            },
        },
    };
    let rest = all.split_off(first.min(all.len()));
    let more = rest.len() > limit;
    let page: Vec<BackupSummary> = rest.into_iter().take(limit).collect();
    let last = more
        .then(|| page.last().map(|s| s.backup_arn.clone()))
        .flatten();
    Ok((page, last))
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
    let mut desc = match resolved {
        Some(resolved) => storage.delete(account_id, &resolved).await,
        None => resolve_backup(backup_arn, by!(storage, account_id, delete))
            .await
            .map(|(_, desc)| desc),
    }
    .map_err(storage_err_to_dynamo)?;
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
/// other than the configured one is a `ValidationException` naming no root;
/// then the in-use check runs again with the AVAILABLE-to-DELETING write in
/// one transaction under the row's lock
/// ([`BackupCatalog::start_backup_delete`]), so a restore's registration and
/// this order wholly one before the other: a registration that won leaves
/// this `BackupInUseException` naming its target. A DELETING row (a retried
/// DeleteBackup) resumes the deletion. The request returns the description
/// with `BackupStatus` DELETED; the store's objects are deleted by the
/// duty a configured store brings, and the row stays DELETING until a
/// listing of its prefix returns nothing.
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
    let mut desc = row.description().map_err(storage_err_to_dynamo)?;
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
    desc.backup_details.backup_status = BackupStatus::Deleted.as_str().to_owned();
    Ok(desc)
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
    S: BackupCatalog + LegacyBackupReader + TableEngine + ?Sized + Send + Sync + 'static,
    F: FnOnce(Arc<str>, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let (target_table_name, response) =
            restore_table_from_backup(&*storage, &runtime, &account_id, &region, &limits, &body)
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
/// (3) check `TargetTableName`: its syntax, then the name under the account
/// through `describe_table` ([`check_restore_target_name`]: a table of that
/// name is `TableAlreadyExistsException`, a CREATING restore target
/// `TableInUseException` naming its source backup), so a client retrying its
/// own restore is answered at once whatever the backup's state and whether
/// or not a permit is free; (4) read the backup: the catalog index's row
/// first ([`check_catalog_row_for_restore`]: a CREATING row or one in use is
/// `BackupInUseException`, a DELETING, DELETED or FAILED row
/// `BackupNotFoundException`, a row of another store a
/// `ValidationException`), else the legacy reader's restore source;
/// (5) refuse what no backend restores faithfully (multi-part key, vector
/// indexes); (6) check the throughput override against the backup's billing
/// mode, build the target's `CreateTableInput`, and apply the index and
/// on-demand throughput overrides to it, refusing what the service refuses
/// ([`overrides`]); (7) validate that input as CreateTable does; (8) take the
/// job permit, last among the checks (`LimitExceededException` when none is
/// free), and ask the backend to create and register the target; (9) map
/// its errors, a taken name being `TableAlreadyExistsException`, a name
/// being restored into `TableInUseException` (the backend checks the name
/// again, fenced, under the registration), a backup being restored from
/// `BackupInUseException`.
async fn restore_table_from_backup<S: BackupCatalog + LegacyBackupReader + TableEngine + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
    account_id: &str,
    region: &str,
    limits: &LimitsConfig,
    body: &Value,
) -> Result<(String, Value), DynamoDbError> {
    // 1. The overrides, parsed and shape-checked: no storage I/O, so a
    // malformed request is refused before a missing backup can take
    // precedence over the request's own error.
    let overrides = parse_restore_table_overrides(body)?;
    validate_override_shape(&overrides)?;
    let index_overrides = overrides::IndexOverrides::parse(body)?;

    // 2. The ARN before the target name: the service checks the ARN's account
    // first (measured on DynamoDB with two invalid inputs).
    let backup_arn = backup_arn_field(body, account_id, region)?;

    // 3. The name's syntax, then the name itself, before the backup is read.
    let target_table_name = required_str(body, "TargetTableName", "targetTableName")?;
    extenddb_core::validation::validate_table_name(target_table_name, limits)?;
    check_restore_target_name(storage, account_id, target_table_name).await?;

    // 4. The catalog index first, then, with no row, the legacy reader under
    // the ARN spelling that resolves; the restore is registered against that
    // spelling, the one the backend stored.
    if let Some((arn, row)) = read_catalog_row(storage, &backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?
    {
        check_catalog_row_for_restore(storage, runtime, &arn, &row).await?;
    }
    let (backup_arn, source) =
        resolve_backup(&backup_arn, by!(storage, account_id, restore_source))
            .await
            .map_err(storage_err_to_dynamo)?;

    // 5. Refusals every backend gets, whatever it stores.
    source.ensure_restorable().map_err(storage_err_to_dynamo)?;

    // 6.
    validate_override_for_source(&overrides, &source.billing_mode)?;
    let mut input = source.restore_input(target_table_name, &overrides);
    index_overrides.apply(&mut input, overrides.billing_mode)?;

    // 7. A stored definition the service would refuse at CreateTable is
    // refused here too (the service re-validates on restore): a backup row
    // is data, and data can be wrong. Only CreateTable's rules apply, so a
    // table CreateTable or UpdateTable accepted restores.
    extenddb_core::validation::validate_create_table(&input, limits)?;
    let on_demand = input.billing_mode == Some(BillingMode::PayPerRequest);

    // 8. The permit last: every refusal above needs no slot, and a job
    // that waited for one would hold what it opened while it waited.
    let request = RestoreRequest {
        backup_arn: backup_arn.clone(),
        source_table_arn: source.source_table_arn.clone(),
        restore_date_time: source.backup_creation_date_time,
        input,
    };
    let permit = runtime.take_permit()?;
    let desc = storage
        .restore(account_id, request, permit)
        .await
        // 9.
        .map_err(restore_storage_err_to_dynamo)?;

    let response = restore_response(desc, &source, on_demand)?;
    Ok((target_table_name.to_owned(), response))
}

/// Step 4 of a restore for a backup the catalog index has a row for, before
/// the permit and reading no store: a CREATING row is `BackupInUseException`;
/// a DELETING, DELETED or FAILED row is `BackupNotFoundException`; a backup
/// a CREATING restore target still reads is `BackupInUseException` naming
/// that table ([`BackupCatalog::backup_in_use`]); a row of a store other
/// than the configured one is a `ValidationException` naming no root. An
/// AVAILABLE row of the configured store is a backup in a store, whose
/// restore this release does not run.
async fn check_catalog_row_for_restore<S: BackupCatalog + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
    arn: &BackupArn,
    row: &BackupRow,
) -> Result<(), DynamoDbError> {
    let arn_text = arn.to_arn_string();
    match row.status {
        BackupStatus::Creating => return Err(backup_in_use(&arn_text, None)),
        BackupStatus::Deleting | BackupStatus::Deleted | BackupStatus::Failed => {
            return Err(backup_not_found(&arn_text));
        }
        BackupStatus::Available => {}
    }
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
    Err(DynamoDbError::ValidationException(format!(
        "Backup {arn_text} is in a backup store; this release restores in-catalog backups only"
    )))
}

/// Step 3 of a restore, the target name under the account, read through
/// `describe_table` before the backup and the permit: a CREATING table with a
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

/// The RestoreTableFromBackup response body for the backend's description.
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
    source: &BackupRestoreSource,
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
    // description to a client. A restored table has no vector indexes (the
    // restore refuses them), so this holds trivially, but the invariant claims
    // to cover every such path.
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

/// Handle `DescribeContinuousBackups`.
pub(crate) async fn handle_describe_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = required_str(&body, "TableName", "tableName")?;

    let desc = ctx
        .storage
        .describe_continuous_backups(&ctx.account_id, table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "ContinuousBackupsDescription": desc }))
}

/// Handle `UpdateContinuousBackups`.
pub(crate) async fn handle_update_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = required_str(&body, "TableName", "tableName")?;

    let pitr_enabled = body
        .get("PointInTimeRecoverySpecification")
        .and_then(|v| v.get("PointInTimeRecoveryEnabled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let desc = ctx
        .storage
        .update_continuous_backups(&ctx.account_id, table_name, pitr_enabled)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "ContinuousBackupsDescription": desc }))
}

/// Handle `RestoreTableToPointInTime`.
///
/// Point-in-time recovery is not yet implemented. The previous implementation
/// faked a restore by snapshotting the current table state (ignoring
/// `RestoreDateTime`), which violates tenet 1 (fidelity over features).
/// Until real PITR is implemented, return an error. There is no storage
/// method behind this; it was removed from the storage trait as unreachable.
pub(crate) async fn handle_restore_table_to_point_in_time(
    _body: Value,
    _ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // TODO(fidelity): Implement real PITR using PostgreSQL temporal/history
    // table approach — item_history table capturing every mutation, DISTINCT ON
    // query to reconstruct state at time T, 35-day retention via background
    // pruning.
    Err(DynamoDbError::ValidationException(
        "Point-in-time recovery restore is not yet supported".to_owned(),
    ))
}

/// Convert storage errors to `DynamoDB` errors.
fn storage_err_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::TableNotFound(msg) => DynamoDbError::ResourceNotFoundException(msg),
        StorageError::TableAlreadyExists(msg)
        | StorageError::TableNotActive(msg)
        | StorageError::IndexesInUse(msg) => DynamoDbError::ResourceInUseException(msg),
        e @ StorageError::TableBeingRestored { .. } => {
            DynamoDbError::ResourceInUseException(e.to_string())
        }
        StorageError::NoOpUpdate(msg) => DynamoDbError::ValidationException(msg),
        StorageError::Validation(msg) => {
            // A missing (or deleted) backup surfaces from the backend as a
            // Validation error carrying "Backup not found"; DynamoDB reports
            // this as BackupNotFoundException, not ResourceNotFoundException.
            if msg.contains(extenddb_storage::backup_catalog::BACKUP_NOT_FOUND) {
                DynamoDbError::BackupNotFoundException(msg)
            } else {
                DynamoDbError::ValidationException(msg)
            }
        }
        // Not a fault, so deliberately not logged at error level: the backend
        // never claimed the feature, and the request is invalid against this
        // deployment rather than a server failure. Amazon DynamoDB has no
        // "unsupported" error class, so this reports as a validation error, the
        // same mapping CreateTable and UpdateTable use for a refused capability.
        StorageError::Unsupported(msg) => DynamoDbError::ValidationException(msg),
        StorageError::LimitExceeded(msg) => DynamoDbError::LimitExceededException(msg),
        StorageError::BackupInUse(msg) => DynamoDbError::BackupInUseException(msg),
        other => {
            tracing::error!(internal_error = %other, "backup storage error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

mod overrides;
pub mod reconciler;
mod runtime;
#[cfg(test)]
mod tests;
