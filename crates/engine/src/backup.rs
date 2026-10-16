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

use extenddb_core::error::{DynamoDbError, ErrorMessageKey, error_message};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BackupDescription, BackupSummary, BillingMode, CreateTableInput, GlobalSecondaryIndexInfo,
    LocalSecondaryIndexInfo, ProvisionedThroughput, RestoreSummary, SourceTableFeatureDetails,
    SseDescription, TableDescription, TableStatus,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupRestoreSource, RestoreRequest, RestoreTableOverrides};
use serde_json::{Value, json};

use crate::{OperationContext, serialize_output};

/// `ListBackups` page size when the request gives no `Limit`, and the largest
/// it accepts (`BackupsInputLimit`: min 1, max 100).
const LIST_BACKUPS_MAX_LIMIT: i64 = 100;

/// Resolve the `BackupArn` field.
///
/// Order, matching the service (measured on DynamoDB):
/// a missing member is a `ValidationException`; an ARN that is not a backup
/// ARN at all (not six colon fields, not `:table/<name>/backup/<id>`) is a
/// `ValidationException`; a well-formed ARN naming another account is an
/// `AccessDeniedException`, not reported as absent, because the service
/// authorizes on the ARN's account before resolving the backup. A caller can
/// therefore tell "my backup does not exist" (`BackupNotFoundException`, from
/// the backend) from "that ARN belongs to another account".
///
/// Backends additionally filter on `account_id`, so a mismatch can never
/// resolve even if this check were bypassed.
fn backup_arn_field(body: &Value, account_id: &str) -> Result<String, DynamoDbError> {
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
    check_backup_arn(backup_arn, account_id)?;
    Ok(backup_arn.to_owned())
}

/// The shape and account checks of [`backup_arn_field`], for any member that
/// carries a backup ARN.
fn check_backup_arn(backup_arn: &str, account_id: &str) -> Result<(), DynamoDbError> {
    let Some(parts) = extenddb_storage::util::parse_backup_arn(backup_arn) else {
        // The service's text for this case was not captured; the class is.
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid BackupArn: {backup_arn}"
        )));
    };
    if parts.account_id != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    Ok(())
}

fn required_str<'a>(body: &'a Value, member: &str, path: &str) -> Result<&'a str, DynamoDbError> {
    body.get(member).and_then(|v| v.as_str()).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value null at '{path}' \
             failed to satisfy constraint: Member must not be null"
        ))
    })
}

/// Handle `CreateBackup`.
pub(crate) async fn handle_create_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = required_str(&body, "TableName", "tableName")?;
    let backup_name = required_str(&body, "BackupName", "backupName")?;

    // A backup of a table with vector indexes could never be restored (the
    // restore refuses it), so refuse it here with the restore's own reason
    // rather than hand back an AVAILABLE backup the customer finds out about
    // at restore time. A lookup failure is left to create_backup, which
    // reports a missing table in its own terms.
    if let Ok(desc) = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            extenddb_core::types::DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
    {
        refuse_backup_of_vector_table(&desc)?;
    }

    let details = ctx
        .storage
        .create_backup(&ctx.account_id, table_name, backup_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupDetails": details }))
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
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;
    let desc = describe_backup(&*ctx.storage, &ctx.account_id, &ctx.region, &backup_arn).await?;
    serialize_output(&json!({ "BackupDescription": desc }))
}

/// The DescribeBackup body: the backend's description, with
/// `SourceTableDetails.ProvisionedThroughput` and `SourceTableFeatureDetails`
/// filled from the backup's recorded source when the backup is AVAILABLE.
async fn describe_backup<S: BackupEngine + ?Sized>(
    storage: &S,
    account_id: &str,
    region: &str,
    backup_arn: &str,
) -> Result<BackupDescription, DynamoDbError> {
    let mut desc = storage
        .describe_backup(account_id, backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    // A CREATING or DELETED backup has no restore source to read; it is
    // described from the backend's row alone. So is one that vanished between
    // the two reads, one whose definition this build cannot read
    // (Unsupported), and one whose definition does not parse (Internal): its
    // BackupDetails do not depend on the definition, so DescribeBackup still
    // answers, without feature details. A failure to reach the database
    // (Transient, Connection) and every other error propagate.
    let source = if desc.backup_details.backup_status == "AVAILABLE" {
        match storage.backup_restore_source(account_id, backup_arn).await {
            Ok(source) => Some(source),
            Err(StorageError::Validation(m)) if m.contains("Backup not found") => None,
            Err(StorageError::Unsupported(m) | StorageError::Internal(m)) => {
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
    let page = list_backups(&*ctx.storage, &ctx.account_id, &body).await?;
    serialize_output(&page)
}

async fn list_backups<S: BackupEngine + ?Sized>(
    storage: &S,
    account_id: &str,
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
    let start = match body.get("ExclusiveStartBackupArn") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let arn = v.as_str().ok_or_else(|| {
                DynamoDbError::SerializationException(
                    "Start of structure or map found where not expected: ExclusiveStartBackupArn"
                        .to_owned(),
                )
            })?;
            check_backup_arn(arn, account_id)?;
            Some(arn)
        }
    };
    let table_name = body.get("TableName").and_then(|v| v.as_str());

    let summaries = storage
        .list_backups(account_id, table_name)
        .await
        .map_err(storage_err_to_dynamo)?;
    let (page, last) = page_backups(summaries, usize::try_from(limit).unwrap_or(100), start)?;

    let mut out = json!({ "BackupSummaries": page });
    if let Some(last) = last {
        out["LastEvaluatedBackupArn"] = Value::String(last);
    }
    Ok(out)
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
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;

    let desc = ctx
        .storage
        .delete_backup(&ctx.account_id, &backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupDescription": desc }))
}

/// Overrides refused outright. `OnDemandThroughputOverride` and
/// `VectorIndexOverride` are not in the botocore 1.33.13 model; newer models
/// and SDKs send them.
const UNSUPPORTED_RESTORE_TABLE_OVERRIDE_FIELDS: [&str; 2] =
    ["OnDemandThroughputOverride", "VectorIndexOverride"];

/// Index overrides: an empty list is accepted as a no-op (the service does
/// the same, measured on DynamoDB; consoles and SDKs send the member
/// empty), a non-empty one is refused.
const INDEX_OVERRIDE_FIELDS: [&str; 2] = [
    "GlobalSecondaryIndexOverride",
    "LocalSecondaryIndexOverride",
];

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
    for field in INDEX_OVERRIDE_FIELDS {
        match body.get(field) {
            None | Some(Value::Null) => {}
            Some(Value::Array(list)) if list.is_empty() => {}
            Some(Value::Array(_)) => {
                return Err(DynamoDbError::ValidationException(format!(
                    "RestoreTableFromBackup does not support a non-empty {field} on ExtendDB"
                )));
            }
            Some(_) => {
                return Err(DynamoDbError::SerializationException(format!(
                    "Start of structure or map found where not expected: {field}"
                )));
            }
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
        Arc::clone(&ctx.account_id),
        Arc::clone(&ctx.limits),
        body,
        move |account_id, target_table_name| async move {
            // Drop any cached negative TableKeyInfo from a prior probe so
            // subsequent requests against the restored table see it without
            // TTL lag. Tags are not propagated through restore today; if that
            // ever changes, also invalidate resource_tags for the new ARN —
            // see handle_create_table.
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
    account_id: Arc<str>,
    limits: Arc<LimitsConfig>,
    body: Value,
    after: F,
) -> Result<Value, DynamoDbError>
where
    S: BackupEngine + ?Sized + Send + Sync + 'static,
    F: FnOnce(Arc<str>, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let (target_table_name, response) =
            restore_table_from_backup(&*storage, &account_id, &limits, &body).await?;
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
/// (3) check `TargetTableName`; (4) read the backup's restore source;
/// (5) refuse what no backend restores faithfully (multi-part key, vector
/// indexes); (6) check the throughput override against the backup's billing
/// mode and build the target's `CreateTableInput`; (7) validate that input as
/// CreateTable does; (8) ask the backend to create and register the target;
/// (9) map its errors, a taken name being `TableAlreadyExistsException`.
async fn restore_table_from_backup<S: BackupEngine + ?Sized>(
    storage: &S,
    account_id: &str,
    limits: &LimitsConfig,
    body: &Value,
) -> Result<(String, Value), DynamoDbError> {
    // 1. The overrides, parsed and shape-checked: no storage I/O, so a
    // malformed request is refused before a missing backup can take
    // precedence over the request's own error.
    let overrides = parse_restore_table_overrides(body)?;
    validate_override_shape(&overrides)?;

    // 2. The ARN before the target name: the service checks the ARN's account
    // first (measured on DynamoDB with two invalid inputs).
    let backup_arn = backup_arn_field(body, account_id)?;

    // 3.
    let target_table_name = required_str(body, "TargetTableName", "targetTableName")?;
    extenddb_core::validation::validate_table_name(target_table_name, limits)?;

    // 4.
    let source = storage
        .backup_restore_source(account_id, &backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    // 5. Refusals every backend gets, whatever it stores.
    source.ensure_restorable().map_err(storage_err_to_dynamo)?;

    // 6.
    validate_override_for_source(&overrides, &source.billing_mode)?;
    let input = source
        .restore_input(target_table_name, &overrides)
        .map_err(storage_err_to_dynamo)?;

    // 7. A stored definition the service would refuse at CreateTable is
    // refused here too (the service re-validates on restore): a backup row
    // is data, and data can be wrong. Only CreateTable's rules apply, so a
    // table CreateTable or UpdateTable accepted restores.
    extenddb_core::validation::validate_create_table(&input, limits)?;
    let on_demand = input.billing_mode == Some(BillingMode::PayPerRequest);

    // 8.
    let request = RestoreRequest {
        backup_arn: backup_arn.clone(),
        source_table_arn: source.source_table_arn.clone(),
        restore_date_time: source.backup_creation_date_time,
        input,
    };
    let desc = storage
        .restore_table_from_backup(account_id, request)
        .await
        // 9.
        .map_err(restore_storage_err_to_dynamo)?;

    let response = restore_response(desc, &source, on_demand)?;
    Ok((target_table_name.to_owned(), response))
}

/// The RestoreTableFromBackup response body for the backend's description.
///
/// The service returns the target CREATING with `RestoreSummary`
/// { `SourceBackupArn`, `SourceTableArn`, `RestoreDateTime` = the backup's
/// creation time, `RestoreInProgress: true` }, omits `IndexStatus` on each
/// GSI, and omits `LastUpdateToPayPerRequestDateTime` (all measured on
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
/// service model, and what the service returns). Only this operation maps it
/// so; CreateTable keeps `ResourceInUseException`.
fn restore_storage_err_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::TableAlreadyExists(name) => DynamoDbError::TableAlreadyExistsException(
            error_message(ErrorMessageKey::TableAlreadyExists, &[&name]),
        ),
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
/// method behind this; it was removed from `BackupEngine` as unreachable.
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
        StorageError::NoOpUpdate(msg) => DynamoDbError::ValidationException(msg),
        StorageError::Validation(msg) => {
            // A missing (or deleted) backup surfaces from the backend as a
            // Validation error carrying "Backup not found"; DynamoDB reports
            // this as BackupNotFoundException, not ResourceNotFoundException.
            if msg.contains("Backup not found") {
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

#[cfg(test)]
mod tests;
