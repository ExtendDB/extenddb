// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine handlers for DynamoDB backup and point-in-time recovery operations.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use crate::{OperationContext, serialize_output};

/// Handle `CreateBackup`.
pub(crate) async fn handle_create_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = body
        .get("TableName")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'tableName' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;
    let backup_name = body
        .get("BackupName")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'backupName' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;

    let details = ctx
        .storage
        .create_backup(&ctx.account_id, table_name, backup_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupDetails": details }))
}

/// Handle `DescribeBackup`.
pub(crate) async fn handle_describe_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
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

    ensure_backup_arn_account(backup_arn, &ctx.account_id)?;

    let desc = ctx
        .storage
        .describe_backup(backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupDescription": desc }))
}

/// Handle `ListBackups`.
pub(crate) async fn handle_list_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = body.get("TableName").and_then(|v| v.as_str());

    let summaries = ctx
        .storage
        .list_backups(&ctx.account_id, table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupSummaries": summaries }))
}

/// Handle `DeleteBackup`.
pub(crate) async fn handle_delete_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
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

    ensure_backup_arn_account(backup_arn, &ctx.account_id)?;

    let desc = ctx
        .storage
        .delete_backup(backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "BackupDescription": desc }))
}

/// Handle `RestoreTableFromBackup`.
pub(crate) async fn handle_restore_table_from_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let target_table_name = body
        .get("TargetTableName")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'targetTableName' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;
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

    ensure_backup_arn_account(backup_arn, &ctx.account_id)?;

    let desc = ctx
        .storage
        .restore_table_from_backup(&ctx.account_id, target_table_name, backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Drop any cached negative TableKeyInfo from a prior probe so subsequent
    // requests against the restored table see it without TTL lag. Tags are
    // not propagated through restore today; if that ever changes, also
    // invalidate resource_tags for the new ARN — see handle_create_table.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, target_table_name)
        .await;

    serialize_output(&json!({ "TableDescription": desc }))
}

/// Handle `DescribeContinuousBackups`.
pub(crate) async fn handle_describe_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = body
        .get("TableName")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'tableName' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;

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
    let table_name = body
        .get("TableName")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'tableName' \
                 failed to satisfy constraint: Member must not be null"
                    .to_owned(),
            )
        })?;

    let pitr_enabled = body
        .get("PointInTimeRecoverySpecification")
        .and_then(|v| v.get("PointInTimeRecoveryEnabled"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let desc = ctx
        .storage
        .update_continuous_backups(&ctx.account_id, table_name, pitr_enabled)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "ContinuousBackupsDescription": desc }))
}

pub(crate) async fn handle_restore_table_to_point_in_time(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let target_table_name = required_string(
        &body,
        "TargetTableName",
        "targetTableName",
        "Member must not be null",
    )?;
    let source_table_name = resolve_source_table_name(&body, &ctx.account_id)?;
    let use_latest = body
        .get("UseLatestRestorableTime")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let restore_time = optional_timestamp(&body, "RestoreDateTime", "restoreDateTime")?;

    if use_latest && restore_time.is_some() {
        return Err(DynamoDbError::ValidationException(
            "RestoreDateTime and UseLatestRestorableTime cannot both be specified".to_owned(),
        ));
    }
    if !use_latest && restore_time.is_none() {
        return Err(DynamoDbError::ValidationException(
            "Either RestoreDateTime or UseLatestRestorableTime must be specified".to_owned(),
        ));
    }

    let desc = ctx
        .storage
        .restore_table_to_point_in_time(
            &ctx.account_id,
            &source_table_name,
            target_table_name,
            restore_time,
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&json!({ "TableDescription": desc }))
}

fn required_string<'a>(
    body: &'a Value,
    field: &str,
    wire_name: &str,
    constraint: &str,
) -> Result<&'a str, DynamoDbError> {
    body.get(field).and_then(|v| v.as_str()).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value null at '{wire_name}' \
             failed to satisfy constraint: {constraint}"
        ))
    })
}

fn optional_timestamp(
    body: &Value,
    field: &str,
    wire_name: &str,
) -> Result<Option<f64>, DynamoDbError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    let Some(timestamp) = value.as_f64() else {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at '{wire_name}' failed to satisfy constraint: \
             Member must be a timestamp"
        )));
    };
    if !timestamp.is_finite() || timestamp < 0.0 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value at '{wire_name}' failed to satisfy constraint: \
             Member must be a valid timestamp"
        )));
    }
    Ok(Some(timestamp))
}

fn resolve_source_table_name(body: &Value, account_id: &str) -> Result<String, DynamoDbError> {
    let source_name = body.get("SourceTableName").and_then(|v| v.as_str());
    let source_arn = body.get("SourceTableArn").and_then(|v| v.as_str());

    match (source_name, source_arn) {
        (Some(name), None) => Ok(name.to_owned()),
        (None, Some(arn)) => table_name_from_arn(arn, account_id),
        (Some(name), Some(arn)) => {
            let arn_name = table_name_from_arn(arn, account_id)?;
            if arn_name != name {
                return Err(DynamoDbError::ValidationException(
                    "SourceTableName and SourceTableArn refer to different tables".to_owned(),
                ));
            }
            Ok(name.to_owned())
        }
        (None, None) => Err(DynamoDbError::ValidationException(
            "Either SourceTableName or SourceTableArn must be specified".to_owned(),
        )),
    }
}

fn table_name_from_arn(arn: &str, account_id: &str) -> Result<String, DynamoDbError> {
    let parsed = parse_dynamodb_arn(arn, "source table")?;
    if parsed.account_id != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access denied for source table ARN".to_owned(),
        ));
    }
    parsed
        .resource
        .strip_prefix("table/")
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .map(str::to_owned)
        .ok_or_else(|| {
            DynamoDbError::ValidationException(format!("Invalid source table ARN: {arn}"))
        })
}

struct DynamoDbArn<'a> {
    account_id: &'a str,
    resource: &'a str,
}

fn parse_dynamodb_arn<'a>(arn: &'a str, kind: &str) -> Result<DynamoDbArn<'a>, DynamoDbError> {
    let mut parts = arn.splitn(6, ':');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (
            Some("arn"),
            Some(partition),
            Some("dynamodb"),
            Some(region),
            Some(account),
            Some(resource),
        ) if !partition.is_empty()
            && !region.is_empty()
            && !account.is_empty()
            && !resource.is_empty() =>
        {
            Ok(DynamoDbArn {
                account_id: account,
                resource,
            })
        }
        _ => Err(DynamoDbError::ValidationException(format!(
            "Invalid {kind} ARN: {arn}"
        ))),
    }
}

fn ensure_backup_arn_account(backup_arn: &str, account_id: &str) -> Result<(), DynamoDbError> {
    let parsed = parse_dynamodb_arn(backup_arn, "backup")?;
    if parsed.account_id != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access denied for backup ARN".to_owned(),
        ));
    }
    Ok(())
}

/// Convert storage errors to DynamoDB errors.
fn storage_err_to_dynamo(e: extenddb_storage::error::StorageError) -> DynamoDbError {
    match e {
        extenddb_storage::error::StorageError::TableNotFound(msg) => {
            DynamoDbError::ResourceNotFoundException(msg)
        }
        extenddb_storage::error::StorageError::TableAlreadyExists(msg) => {
            DynamoDbError::ResourceInUseException(msg)
        }
        extenddb_storage::error::StorageError::Validation(msg) => {
            // Backup-not-found errors come through as Validation.
            if msg.contains("Backup not found") {
                DynamoDbError::ResourceNotFoundException(msg)
            } else {
                DynamoDbError::ValidationException(msg)
            }
        }
        other => {
            tracing::error!(internal_error = %other, "backup storage error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{optional_timestamp, resolve_source_table_name};
    use extenddb_core::error::DynamoDbError;
    use serde_json::json;

    #[test]
    fn source_table_name_accepts_matching_name_and_arn() {
        let body = json!({
            "SourceTableName": "orders",
            "SourceTableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/orders"
        });

        assert_eq!(
            resolve_source_table_name(&body, "123456789012").expect("source table"),
            "orders"
        );
    }

    #[test]
    fn source_table_name_rejects_cross_account_arn() {
        let body = json!({
            "SourceTableArn": "arn:aws:dynamodb:us-east-1:999999999999:table/orders"
        });

        let err = resolve_source_table_name(&body, "123456789012").unwrap_err();
        assert!(matches!(err, DynamoDbError::AccessDeniedException(_)));
    }

    #[test]
    fn source_table_name_rejects_non_dynamodb_arn() {
        let body = json!({
            "SourceTableArn": "arn:aws:s3:us-east-1:123456789012:table/orders"
        });

        let err = resolve_source_table_name(&body, "123456789012").unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(_)));
    }

    #[test]
    fn source_table_name_rejects_subresource_arn() {
        let body = json!({
            "SourceTableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/orders/index/by_status"
        });

        let err = resolve_source_table_name(&body, "123456789012").unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(_)));
    }

    #[test]
    fn optional_timestamp_rejects_invalid_values() {
        let body = json!({ "RestoreDateTime": "not-a-timestamp" });

        let err = optional_timestamp(&body, "RestoreDateTime", "restoreDateTime").unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(_)));
    }
}
