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

/// Handle `RestoreTableToPointInTime`.
///
/// Point-in-time recovery is not yet implemented. The previous implementation
/// faked a restore by snapshotting the current table state (ignoring
/// `RestoreDateTime`), which violates tenet 1 (fidelity over features).
/// Until real PITR is implemented, return an error.
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
