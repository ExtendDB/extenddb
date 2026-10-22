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

const UNSUPPORTED_RESTORE_TABLE_OVERRIDE_FIELDS: [&str; 5] = [
    "GlobalSecondaryIndexOverride",
    "LocalSecondaryIndexOverride",
    "SSESpecificationOverride",
    "OnDemandThroughputOverride",
    "VectorIndexOverride",
];

#[derive(Debug, Clone, PartialEq)]
struct RestoreTableOverrides {
    billing_mode: Option<extenddb_core::types::BillingMode>,
    provisioned_throughput: Option<extenddb_core::types::ProvisionedThroughput>,
}

impl RestoreTableOverrides {
    fn is_empty(&self) -> bool {
        self.billing_mode.is_none() && self.provisioned_throughput.is_none()
    }

    fn effective_billing_mode(
        &self,
        source_billing_mode: Option<&str>,
    ) -> extenddb_core::types::BillingMode {
        self.billing_mode.unwrap_or_else(|| {
            if source_billing_mode == Some("PAY_PER_REQUEST") {
                extenddb_core::types::BillingMode::PayPerRequest
            } else {
                extenddb_core::types::BillingMode::Provisioned
            }
        })
    }

    /// The rules that depend only on the request itself: a PROVISIONED
    /// override needs a throughput, and a throughput must be well formed.
    /// Checked before anything is read from storage.
    fn validate_shape(&self) -> Result<(), DynamoDbError> {
        use extenddb_core::types::BillingMode;

        if matches!(self.billing_mode, Some(BillingMode::Provisioned))
            && self.provisioned_throughput.is_none()
        {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: ProvisionedThroughputOverride must \
                 be specified when BillingModeOverride is PROVISIONED"
                    .to_owned(),
            ));
        }

        if let Some(throughput) = &self.provisioned_throughput {
            let input = extenddb_core::types::CreateTableInput {
                billing_mode: Some(BillingMode::Provisioned),
                provisioned_throughput: Some(throughput.clone()),
                ..Default::default()
            };
            extenddb_core::validation::validate_provisioned_throughput(&input)?;
        }
        Ok(())
    }

    /// The rule that needs the backup's own billing mode: a throughput
    /// override is only meaningful if the restored table is PROVISIONED.
    fn validate_for_source(&self, source_billing_mode: Option<&str>) -> Result<(), DynamoDbError> {
        use extenddb_core::types::BillingMode;

        if self.provisioned_throughput.is_some()
            && self.effective_billing_mode(source_billing_mode) == BillingMode::PayPerRequest
        {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: ProvisionedThroughputOverride can \
                 only be specified with BillingModeOverride PROVISIONED"
                    .to_owned(),
            ));
        }
        Ok(())
    }
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
        if body.get(field).is_some() {
            return Err(DynamoDbError::ValidationException(format!(
                "RestoreTableFromBackup does not support {field} on ExtendDB"
            )));
        }
    }

    let billing_mode = body
        .get("BillingModeOverride")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(crate::deserialize_error)?;
    let provisioned_throughput = body
        .get("ProvisionedThroughputOverride")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(crate::deserialize_error)?;

    Ok(RestoreTableOverrides {
        billing_mode,
        provisioned_throughput,
    })
}

/// Handle `RestoreTableFromBackup`.
pub(crate) async fn handle_restore_table_from_backup(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let overrides = parse_restore_table_overrides(&body)?;

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
    extenddb_core::validation::validate_table_name(target_table_name, &ctx.limits)?;
    let backup_arn = backup_arn_field(&body, &ctx.account_id)?;

    // Rules that need nothing from the backup come first, so a malformed
    // request is refused before any storage read and before a missing backup
    // can take precedence over the request's own error.
    overrides.validate_shape()?;
    let source_billing_mode = if overrides.is_empty() {
        None
    } else {
        ctx.storage
            .describe_backup(&ctx.account_id, &backup_arn)
            .await
            .map_err(storage_err_to_dynamo)?
            .source_table_details
            .billing_mode
    };
    overrides.validate_for_source(source_billing_mode.as_deref())?;
    let effective_billing_mode = overrides.effective_billing_mode(source_billing_mode.as_deref());

    let storage_overrides = extenddb_storage::RestoreTableOverrides {
        billing_mode: overrides.billing_mode,
        provisioned_throughput: overrides.provisioned_throughput.clone(),
    };
    let mut desc = ctx
        .storage
        .restore_table_from_backup(
            &ctx.account_id,
            target_table_name,
            &backup_arn,
            storage_overrides,
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    if effective_billing_mode == extenddb_core::types::BillingMode::PayPerRequest
        && !overrides.is_empty()
    {
        // Backends may retain catalog capacity fields while creating an
        // on-demand restore target. The response must expose normalized zero
        // values, and subsequent backups normalize retained values before storage.
        desc.provisioned_throughput = Default::default();
        for index in desc.global_secondary_indexes.iter_mut().flatten() {
            index.provisioned_throughput = Some(Default::default());
        }
    }

    // The restore response's TableDescription reports where the data came
    // from and that the restore is under way: SourceBackupArn and
    // RestoreInProgress: true, pinned by the ground-truth runs of 2026-08-24
    // (us-east-1 and eu-west-2). The service returns the table CREATING with
    // the restore in progress; the backends report CREATING here too, whatever
    // the copy has reached. The time is the backend's own record where it
    // keeps one, so the response and later DescribeTable calls agree.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default();
    let restore_date_time = desc
        .restore_summary
        .as_ref()
        .map_or(now, |r| r.restore_date_time);
    desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
        source_backup_arn: Some(backup_arn.clone()),
        restore_date_time,
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
    // Same rule as every other path that emits a table description: the
    // throughput-mode summary mirrors the billing-mode summary.
    desc.populate_table_throughput_mode_summary();
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
        extenddb_storage::error::StorageError::TableNotActive(msg)
        | extenddb_storage::error::StorageError::IndexesInUse(msg) => {
            DynamoDbError::ResourceInUseException(msg)
        }
        extenddb_storage::error::StorageError::NoOpUpdate(msg) => {
            DynamoDbError::ValidationException(msg)
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
        extenddb_storage::error::StorageError::LimitExceeded(msg) => {
            DynamoDbError::LimitExceededException(msg)
        }
        extenddb_storage::error::StorageError::BackupInUse(msg) => {
            DynamoDbError::BackupInUseException(msg)
        }
        other => {
            tracing::error!(internal_error = %other, "backup storage error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{backup_arn_field, parse_restore_table_overrides, storage_err_to_dynamo};
    use extenddb_core::error::DynamoDbError;
    use extenddb_core::types::{BillingMode, ProvisionedThroughput};
    use extenddb_storage::error::StorageError;
    use serde_json::{Value, json};

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
    fn restore_request_without_overrides_is_accepted() {
        let body = json!({
            "TargetTableName": "MusicRestored",
            "BackupArn": arn(ACCOUNT),
        });
        assert!(parse_restore_table_overrides(&body).unwrap().is_empty());
    }

    fn throughput(read: i64, write: i64) -> ProvisionedThroughput {
        ProvisionedThroughput {
            read_capacity_units: read,
            write_capacity_units: write,
        }
    }

    #[test]
    fn provisioned_override_with_throughput_is_accepted() {
        let overrides = parse_restore_table_overrides(&json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        }))
        .unwrap();
        overrides
            .validate_for_source(Some("PAY_PER_REQUEST"))
            .unwrap();

        assert_eq!(overrides.billing_mode, Some(BillingMode::Provisioned));
        assert_eq!(overrides.provisioned_throughput, Some(throughput(5, 5)));
    }

    #[test]
    fn provisioned_override_without_throughput_is_refused() {
        let overrides = parse_restore_table_overrides(&json!({
            "BillingModeOverride": "PROVISIONED",
        }))
        .unwrap();
        let err = overrides.validate_shape().unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(_)));
    }

    #[test]
    fn throughput_override_under_pay_per_request_is_refused() {
        let overrides = parse_restore_table_overrides(&json!({
            "BillingModeOverride": "PAY_PER_REQUEST",
            "ProvisionedThroughputOverride": {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        }))
        .unwrap();
        let err = overrides
            .validate_for_source(Some("PROVISIONED"))
            .unwrap_err();
        match err {
            DynamoDbError::ValidationException(message) => assert!(message.contains(
                "ProvisionedThroughputOverride can only be specified with \
                     BillingModeOverride PROVISIONED"
            )),
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn invalid_provisioned_throughput_override_matches_create_table() {
        let expected = "One or more parameter values were invalid: ReadCapacityUnits and \
                        WriteCapacityUnits must both be greater than or equal to 1 for table";
        for (read, write) in [(0, 5), (5, 0), (-1, 5), (5, -1)] {
            let overrides = parse_restore_table_overrides(&json!({
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": {
                    "ReadCapacityUnits": read,
                    "WriteCapacityUnits": write,
                },
            }))
            .unwrap();
            let err = overrides.validate_shape().unwrap_err();
            match err {
                DynamoDbError::ValidationException(message) => assert_eq!(message, expected),
                other => panic!("expected ValidationException, got {other:?}"),
            }
        }
    }

    #[test]
    fn missing_provisioned_throughput_override_member_matches_create_table() {
        for (member, body) in [
            (
                "ReadCapacityUnits",
                json!({
                    "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "WriteCapacityUnits": 5 },
                }),
            ),
            (
                "WriteCapacityUnits",
                json!({
                    "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5 },
                }),
            ),
        ] {
            let err = parse_restore_table_overrides(&body).unwrap_err();
            match err {
                DynamoDbError::SerializationException(message) => {
                    assert!(
                        message.contains(&format!("missing field `{member}`")),
                        "{message}"
                    );
                }
                other => panic!("expected SerializationException, got {other:?}"),
            }
        }
    }

    fn assert_restore_override_rejected(field: &str) {
        let mut body = json!({
            "TargetTableName": "MusicRestored",
            "BackupArn": arn(ACCOUNT),
        });
        body[field] = Value::Null;
        let err = parse_restore_table_overrides(&body).unwrap_err();
        match err {
            DynamoDbError::ValidationException(message) => assert_eq!(
                message,
                format!("RestoreTableFromBackup does not support {field} on ExtendDB")
            ),
            other => panic!("expected ValidationException for {field}, got {other:?}"),
        }
    }

    #[test]
    fn index_and_sse_overrides_remain_refused() {
        for field in [
            "GlobalSecondaryIndexOverride",
            "LocalSecondaryIndexOverride",
            "SSESpecificationOverride",
        ] {
            assert_restore_override_rejected(field);
        }
    }

    #[test]
    fn on_demand_throughput_override_is_refused() {
        assert_restore_override_rejected("OnDemandThroughputOverride");
    }

    #[test]
    fn vector_index_override_is_refused() {
        assert_restore_override_rejected("VectorIndexOverride");
    }

    #[test]
    fn account_prefix_does_not_match() {
        // A shorter account id that is a prefix of the caller's must not pass.
        let body = json!({ "BackupArn": arn("12345678901") });
        assert!(backup_arn_field(&body, ACCOUNT).is_err());
    }
}
