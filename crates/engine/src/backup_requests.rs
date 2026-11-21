// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The backup and point-in-time recovery requests that read no backup store:
//! `ListBackups`, `DescribeContinuousBackups`, `UpdateContinuousBackups` and
//! `RestoreTableToPointInTime`, with the request checks the store-backed
//! handlers in the `backup` module share.
//!
//! These handlers read the request, the limits, the caller's account and
//! region, and the storage engine, never the backup runtime, so this module
//! compiles on every target, the WebAssembly build included, and both builds
//! answer these operations through the same code. The store-backed operations (`CreateBackup`,
//! `DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup`) stay in
//! `backup`, which needs the backup store.
//!
//! Wire member names follow the 2012-08-10 service model in botocore 1.33.13
//! (`botocore/data/dynamodb/2012-08-10/service-2.json.gz`): `ListBackupsInput`
//! (`Limit` 1..=100, `ExclusiveStartBackupArn`) and `ListBackupsOutput`
//! (`LastEvaluatedBackupArn`).

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::BackupSummary;
use extenddb_storage::backup_catalog::TimeBounds;
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupCatalog, LegacyBackupReader};
use futures::TryStreamExt;
use serde_json::{Value, json};

use crate::{OperationContext, serialize_output};

/// `ListBackups` page size when the request gives no `Limit`, and the largest
/// it accepts (`BackupsInputLimit`: min 1, max 100).
const LIST_BACKUPS_MAX_LIMIT: i64 = 100;

/// The ARN this deployment issued for the backup `parts` names: its own
/// region, whatever region the caller wrote.
pub(crate) fn canonical_backup_arn(
    parts: &extenddb_storage::util::BackupArnParts<'_>,
    region: &str,
) -> String {
    extenddb_storage::util::backup_arn(region, parts.account_id, parts.table_name, parts.backup_id)
}

/// The shape check of `backup::backup_arn_field`, for any member that carries a
/// backup ARN, with the service's two texts (measured on DynamoDB; see
/// [`extenddb_storage::util::BackupArnError`] for which inputs get which).
/// A partition other than [`ARN_PARTITION`] is refused here too (the service
/// answers `ValidationException` for one naming the caller's account,
/// measured on DynamoDB; its text was not captured, so the "not valid" one
/// is used), before the account is looked at.
///
/// [`ARN_PARTITION`]: extenddb_storage::util::ARN_PARTITION
pub(crate) fn check_backup_arn(
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
pub(crate) const BACKUP_ARN_NOT_VALID: &str = "Invalid Request: BackupArn is not valid";

pub(crate) fn required_str<'a>(
    body: &'a Value,
    member: &str,
    path: &str,
) -> Result<&'a str, DynamoDbError> {
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
pub(crate) fn validate_name_member(
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

/// The length bounds for a `TableName` member: the deployment's, which
/// default to the model's 3 to 255.
pub(crate) fn table_name_bounds(limits: &LimitsConfig) -> (usize, usize) {
    (limits.min_table_name_length, limits.max_table_name_length)
}

/// The `TableName` of `CreateBackup` and `ListBackups`, which the service
/// accepts as the table's name or its ARN (measured on DynamoDB; the model
/// gives the member the `TableName` shape, max 255, so an SDK built from it
/// refuses the ARN form client-side and only a hand-built request sends it).
/// An ARN is `arn:aws:dynamodb:<region>:<account>:table/<name>`: another
/// account's is `AccessDeniedException`, as a backup ARN of another account
/// is, checked first (an ARN of another account in another region is
/// `AccessDeniedException` on the service, measured on DynamoDB for
/// CreateBackup and ListBackups); one whose region is not this server's
/// (`region`, compared as written, so `US-EAST-1` differs from `us-east-1`)
/// is `ValidationException` with the service's text (`1 validation error
/// detected: Invalid AWS region in '<arn>'`, measured the same way). The
/// name part is then checked as a name.
pub(crate) fn create_backup_table_name<'a>(
    body: &'a Value,
    account_id: &str,
    region: &str,
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
    let [partition, "dynamodb", arn_region, arn_account, resource] = fields.as_slice() else {
        return Err(not_valid());
    };
    if *partition != extenddb_storage::util::ARN_PARTITION
        || arn_region.is_empty()
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
    // The account before the region: another account's ARN in another
    // region is AccessDeniedException on the service (measured on DynamoDB
    // for CreateBackup and ListBackups, twice each).
    if *arn_account != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    if *arn_region != region {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Invalid AWS region in '{value}'"
        )));
    }
    validate_name_member(name, "tableName", table_name_bounds(limits))?;
    Ok(name)
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
pub(crate) async fn list_backups<S: BackupCatalog + LegacyBackupReader + ?Sized>(
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
        Some(Value::String(_)) => Some(create_backup_table_name(body, account_id, region, limits)?),
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
    // The legacy reader first, then the catalog index: a `backup
    // migrate-legacy` replacement that commits between the two reads is
    // then in the second (an index row), never in neither.
    let legacy = storage
        .list(account_id, table_name)
        .await
        .map_err(storage_err_to_dynamo)?;
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
    // A backup both readers list is listed once, with the catalog row's
    // summary: a replacement that committed between the two reads above,
    // or an in-catalog backup whose copy a `backup sync` without the
    // in-catalog skip (a #825 or #880 build during a rolling upgrade)
    // adopted before `backup migrate-legacy` replaced it.
    let indexed: std::collections::HashSet<String> =
        summaries.iter().map(|s| s.backup_arn.clone()).collect();
    summaries.extend(
        legacy
            .into_iter()
            .filter(|s| !indexed.contains(&s.backup_arn)),
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

/// Convert storage errors to `DynamoDB` errors.
pub(crate) fn storage_err_to_dynamo(e: StorageError) -> DynamoDbError {
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

mod pitr;

pub use pitr::refuse_before_authorization as refuse_point_in_time_recovery;
pub(crate) use pitr::{
    handle_describe_continuous_backups, handle_restore_table_to_point_in_time,
    handle_update_continuous_backups,
};
