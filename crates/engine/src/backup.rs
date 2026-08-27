// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine handlers for `DynamoDB` backup and point-in-time recovery operations.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use crate::{OperationContext, serialize_output};

/// Resolve the `BackupArn` field, denying ARNs that name a different account.
///
/// A backup ARN embeds the owning account:
/// `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`. DynamoDB
/// authorizes on the ARN's account before resolving the backup, so an ARN whose
/// account differs from the caller's is rejected with `AccessDeniedException`
/// (verified against the service) — not reported as absent. A caller can
/// therefore distinguish "my backup does not exist" (`BackupNotFoundException`,
/// from the backend) from "that ARN belongs to another account"
/// (`AccessDeniedException`, here), matching DynamoDB.
///
/// Keeping this check in the engine means it holds for every backend; backends
/// additionally filter on `account_id` so a mismatch can never resolve.
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

    // Field 4 of a colon-delimited ARN is the account id. An ARN naming another
    // account — or one malformed enough to have no account in that position — is
    // denied before the backend resolves it, matching DynamoDB's authorize-first
    // behavior.
    let arn_account = backup_arn.split(':').nth(4);
    if arn_account != Some(account_id) {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }

    Ok(backup_arn.to_owned())
}

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
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;

    let desc = ctx
        .storage
        .describe_backup(&ctx.account_id, &backup_arn)
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
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;

    let desc = ctx
        .storage
        .delete_backup(&ctx.account_id, &backup_arn)
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
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;

    let mut desc = ctx
        .storage
        .restore_table_from_backup(&ctx.account_id, target_table_name, &backup_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    // The restore response's TableDescription reports where the data came
    // from and that the restore is under way: SourceBackupArn and
    // RestoreInProgress: true, pinned by the ground-truth runs of 2026-08-24
    // (us-east-1 and eu-west-2). Set here rather than in each backend because
    // the summary is response metadata about this call, not table state the
    // backends persist.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default();
    desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
        source_backup_arn: Some(backup_arn.clone()),
        restore_date_time: now,
        restore_in_progress: true,
    });

    // Drop any cached negative TableKeyInfo from a prior probe so subsequent
    // requests against the restored table see it without TTL lag. Tags are
    // not propagated through restore today; if that ever changes, also
    // invalidate resource_tags for the new ARN — see handle_create_table.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, target_table_name)
        .await;

    // The readiness invariant is applied on every path that hands a description to
    // a client, and restore was the one that omitted it. Currently harmless (a
    // restored index is CREATING, and a non-vector backend could never hold a
    // vector-index backup because create is gated), but the invariant claims to
    // cover exactly this class of path, so the omission was a latent inconsistency
    // rather than a deliberate exception.
    desc.validate_vector_index_readiness()?;
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

/// Convert storage errors to `DynamoDB` errors.
fn storage_err_to_dynamo(e: extenddb_storage::error::StorageError) -> DynamoDbError {
    match e {
        extenddb_storage::error::StorageError::TableNotFound(msg) => {
            DynamoDbError::ResourceNotFoundException(msg)
        }
        extenddb_storage::error::StorageError::TableAlreadyExists(msg) => {
            DynamoDbError::ResourceInUseException(msg)
        }
        extenddb_storage::error::StorageError::Validation(msg) => {
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
        extenddb_storage::error::StorageError::Unsupported(msg) => {
            DynamoDbError::ValidationException(msg)
        }
        other => {
            tracing::error!(internal_error = %other, "backup storage error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{backup_arn_field, storage_err_to_dynamo};
    use extenddb_core::error::DynamoDbError;
    use extenddb_storage::error::StorageError;
    use serde_json::json;

    const ACCOUNT: &str = "123456789012";

    fn arn(account: &str) -> String {
        format!("arn:aws:dynamodb:us-east-1:{account}:table/Music/backup/01489602797149-73d8d5bc")
    }

    #[test]
    fn own_account_arn_is_accepted() {
        let body = json!({ "BackupArn": arn(ACCOUNT) });
        assert_eq!(backup_arn_field(&body, ACCOUNT).unwrap(), arn(ACCOUNT));
    }

    /// A refusal the backend states plainly must not reach the client as a fault.
    ///
    /// The restore path refuses a backup whose source carried vector indexes,
    /// because restore does not recreate them and dropping a declared index
    /// silently is worse than refusing. Without this arm that refusal fell to the
    /// catch-all: the client got a 500 with no reason, indistinguishable from a
    /// broken server, and the operator got an error-level log for a request that
    /// was answered correctly.
    #[test]
    fn an_unsupported_feature_is_a_validation_exception() {
        let err = storage_err_to_dynamo(StorageError::Unsupported(
            "restoring a table with vector indexes is not supported by this storage backend"
                .to_owned(),
        ));
        match err {
            DynamoDbError::ValidationException(msg) => {
                assert_eq!(
                    msg,
                    "restoring a table with vector indexes is not supported by this storage backend"
                );
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn other_account_arn_is_denied() {
        let body = json!({ "BackupArn": arn("999999999999") });
        let err = backup_arn_field(&body, ACCOUNT).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::AccessDeniedException(_)),
            "expected AccessDeniedException, got {err:?}"
        );
    }

    #[test]
    fn malformed_arn_is_denied() {
        for candidate in [
            "not-an-arn",
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1",
            "",
            // Account field present but empty.
            "arn:aws:dynamodb:us-east-1::table/Music/backup/1",
            // Account appears later in the string but not in field 4.
            "arn:aws:dynamodb:us-east-1:table/Music/backup/123456789012",
        ] {
            let body = json!({ "BackupArn": candidate });
            let err = backup_arn_field(&body, ACCOUNT).unwrap_err();
            assert!(
                matches!(err, DynamoDbError::AccessDeniedException(_)),
                "expected AccessDeniedException for {candidate:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn missing_arn_field_is_a_validation_error() {
        let body = json!({});
        let err = backup_arn_field(&body, ACCOUNT).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(_)),
            "expected ValidationException, got {err:?}"
        );
    }

    #[test]
    fn account_prefix_does_not_match() {
        // A shorter account id that is a prefix of the caller's must not pass.
        let body = json!({ "BackupArn": arn("12345678901") });
        assert!(backup_arn_field(&body, ACCOUNT).is_err());
    }
}
