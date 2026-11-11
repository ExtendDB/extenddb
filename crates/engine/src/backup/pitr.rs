// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeContinuousBackups`, `UpdateContinuousBackups` and
//! `RestoreTableToPointInTime`.
//!
//! No storage backend implements point-in-time recovery, so the engine answers
//! the three operations itself and consults storage only to look the table up.
//! The checks run in the service's order (measured on DynamoDB, two samples of
//! each request):
//!
//! - DescribeContinuousBackups: the `TableName` member and its constraints,
//!   then the table.
//! - UpdateContinuousBackups: the `TableName` member and its constraints,
//!   then `PointInTimeRecoverySpecification` and its
//!   `PointInTimeRecoveryEnabled`, then the table, then the enable request.
//! - RestoreTableToPointInTime: the source member (`SourceTableName` or
//!   `SourceTableArn`) and `TargetTableName`, then the source ARN's kind or
//!   the `SourceTableName` constraints, then the source table, then the time
//!   members, then the recovery check. The target table is never looked up:
//!   the recovery check, which the service makes before any check of the
//!   target, refuses every restore.
//!
//! A table that does not exist is `TableNotFoundException` with the service's
//! text, `Table not found: <name>`, on all three. A table in any status
//! resolves, CREATING and DELETING included.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ContinuousBackupsDescription, DescribeTableInput, PointInTimeRecoveryDescription,
};
use extenddb_storage::error::StorageError;
use serde_json::{Value, json};

use super::storage_err_to_dynamo;
use crate::{OperationContext, serialize_output};

/// The service's text for a request without the table name member it routes
/// on: `TableName` on the two continuous-backups operations, and
/// `SourceTableName` (or `SourceTableArn`) or `TargetTableName` on the
/// restore.
const TABLE_NAME_REQUIRED: &str =
    "The parameter 'TableName' is required but was not present in the request";

/// The one message every point-in-time recovery refusal carries.
const PITR_UNSUPPORTED_MESSAGE: &str =
    "Point-in-time recovery is not supported by this storage backend";

/// The service's text for a restore that names its source both ways.
const SOURCE_NAMED_TWICE: &str =
    "Invalid Request: Must provide exactly one of: sourceTableArn, sourceTableName";

/// The service's text for a restore that gives neither time member, or
/// `UseLatestRestorableTime` false without a `RestoreDateTime`.
const NO_RESTORE_TIME: &str = "Invalid Request: Either one of RestoreDateTime or \
     UseLatestRestorableTime must be specified, but not both";

/// The service's text for a restore that gives `RestoreDateTime` and
/// `UseLatestRestorableTime` true.
const TWO_RESTORE_TIMES: &str = "Invalid Request: Both RestoreDateTime and \
     UseLatestRestorableTime cannot be set for point-in-time restore request. Please \
     specify either RestoreDateTime time or UseLatestRestorableTime";

/// The model's pattern constraint on a table name, in the service's words.
const NAME_PATTERN_CONSTRAINT: &str =
    "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";

/// The member's string value, or the service's missing-table-name refusal.
fn table_name_member<'a>(body: &'a Value, member: &str) -> Result<&'a str, DynamoDbError> {
    body.get(member)
        .and_then(Value::as_str)
        .ok_or_else(|| DynamoDbError::ValidationException(TABLE_NAME_REQUIRED.to_owned()))
}

/// `TableName` on the two continuous-backups operations, checked against the
/// model's constraints with the deployment's length bounds, as `CreateBackup`
/// checks its member ([`super::validate_name_member`]). An empty name fails
/// the pattern as well as the length, since the pattern needs one character:
/// the service lists both constraints for it (measured: "2 validation errors
/// detected: Value '' at 'tableName' ..."), where `validate_name_member`
/// lists only the length. A name that starts with `arn:` is left to the
/// lookup unchecked, so a table ARN keeps the answer it had before this
/// check (`TableNotFoundException` naming the ARN).
fn check_table_name(name: &str, (min, max): (usize, usize)) -> Result<(), DynamoDbError> {
    if name.starts_with("arn:") {
        return Ok(());
    }
    if name.is_empty() {
        let at = "Value '' at 'tableName' failed to satisfy constraint:";
        return Err(DynamoDbError::ValidationException(if min > 0 {
            format!(
                "2 validation errors detected: {at} Member must have length greater than \
                 or equal to {min}; {at} {NAME_PATTERN_CONSTRAINT}"
            )
        } else {
            format!("1 validation error detected: {at} {NAME_PATTERN_CONSTRAINT}")
        }));
    }
    super::validate_name_member(name, "tableName", (min, max))
}

/// `SourceTableName` on the restore, checked before the source table is
/// looked up. A length outside the bounds is the service's "TableName must
/// be at least <min> characters long and at most <max> characters long"
/// (measured with an empty name, where the bounds are the model's 3 and
/// 255; here they are the deployment's), and a name that fails the pattern
/// is the model's constraint text at `sourceTableName` (measured with a
/// table ARN as the name).
fn check_source_table_name(name: &str, (min, max): (usize, usize)) -> Result<(), DynamoDbError> {
    let length = name.chars().count();
    if length < min || length > max {
        return Err(DynamoDbError::ValidationException(format!(
            "TableName must be at least {min} characters long and at most {max} characters long"
        )));
    }
    super::validate_name_member(name, "sourceTableName", (min, max))
}

/// The service's refusal for a missing member, by its model path.
fn member_null(path: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "1 validation error detected: Value null at '{path}' \
         failed to satisfy constraint: Member must not be null"
    ))
}

/// The typed refusal for enabling point-in-time recovery.
///
/// `ContinuousBackupsUnavailableException` is the exception the service models
/// on `UpdateContinuousBackups`, so every SDK surfaces it as a typed error a
/// caller can match on, unlike a generic `ValidationException` whose meaning
/// lives only in the message text.
fn pitr_unsupported_error() -> DynamoDbError {
    DynamoDbError::ContinuousBackupsUnavailableException(PITR_UNSUPPORTED_MESSAGE.to_owned())
}

/// The typed refusal for `RestoreTableToPointInTime`.
///
/// The service models `PointInTimeRecoveryUnavailableException` on this
/// operation and returns it whenever recovery is not enabled on the source
/// table, which for ExtendDB is always. The message is the service's
/// ("Point in time recovery is not enabled for table '<name>'").
fn pitr_restore_unavailable_error(table_name: &str) -> DynamoDbError {
    DynamoDbError::PointInTimeRecoveryUnavailableException(format!(
        "Point in time recovery is not enabled for table '{table_name}'"
    ))
}

/// The continuous-backups description every table reports.
///
/// `ContinuousBackupsStatus` is always `ENABLED`, matching the service, which
/// reports `ENABLED` unconditionally. `PointInTimeRecoveryStatus` is always
/// `DISABLED` because no storage backend implements point-in-time recovery,
/// and the restorable-time fields are omitted, as the service omits them for
/// a table whose recovery is disabled. Owned by the engine rather than the
/// backends: the answer does not depend on stored state, so no backend is
/// consulted and all of them report identically.
fn default_continuous_backups_description() -> ContinuousBackupsDescription {
    ContinuousBackupsDescription {
        continuous_backups_status: "ENABLED".to_owned(),
        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
            point_in_time_recovery_status: "DISABLED".to_owned(),
            earliest_restorable_date_time: None,
            latest_restorable_date_time: None,
        }),
    }
}

/// Verify a table exists in the caller's account, in any lifecycle status.
///
/// Through `describe_table` rather than `table_key_info`, which only
/// resolves ACTIVE tables. A missing table is `TableNotFoundException` with
/// the service's text, built from the request's name rather than the
/// backend's payload, which differs by backend.
async fn require_table_exists(
    ctx: &OperationContext,
    table_name: &str,
) -> Result<(), DynamoDbError> {
    match ctx
        .storage
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(StorageError::TableNotFound(_)) => Err(DynamoDbError::TableNotFoundException(format!(
            "Table not found: {table_name}"
        ))),
        Err(other) => Err(storage_err_to_dynamo(other)),
    }
}

/// `PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled`, which the
/// model requires: a request without the specification, or with one that
/// does not hold the flag, is refused with the service's text for the
/// missing member. The JSON strings `"true"` and `"false"` are taken as the
/// booleans (the service answers 200 for `"false"`, measured); any other
/// value that is not a boolean is refused as a missing flag.
fn point_in_time_recovery_enabled(body: &Value) -> Result<bool, DynamoDbError> {
    let spec = body
        .get("PointInTimeRecoverySpecification")
        .filter(|v| !v.is_null())
        .ok_or_else(|| member_null("pointInTimeRecoverySpecification"))?;
    match spec.get("PointInTimeRecoveryEnabled") {
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(Value::String(s)) if s == "true" => Ok(true),
        Some(Value::String(s)) if s == "false" => Ok(false),
        _ => Err(member_null(
            "pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled",
        )),
    }
}

/// The restore's source table name, from `SourceTableName`, checked against
/// `bounds` ([`check_source_table_name`]), or from `SourceTableArn`, which
/// must name a table: an index, stream or backup ARN is refused with the
/// service's text. Exactly one of the two members may be given.
fn restore_source_table_name(
    body: &Value,
    bounds: (usize, usize),
) -> Result<String, DynamoDbError> {
    let name = body.get("SourceTableName").and_then(Value::as_str);
    let arn = body.get("SourceTableArn").and_then(Value::as_str);
    match (name, arn) {
        (Some(name), None) => {
            check_source_table_name(name, bounds)?;
            Ok(name.to_owned())
        }
        (None, Some(arn)) => crate::import_export::extract_table_name_from_arn(arn),
        (Some(_), Some(_)) => Err(DynamoDbError::ValidationException(
            SOURCE_NAMED_TWICE.to_owned(),
        )),
        (None, None) => Err(DynamoDbError::ValidationException(
            TABLE_NAME_REQUIRED.to_owned(),
        )),
    }
}

/// Exactly one of `RestoreDateTime` and `UseLatestRestorableTime` true.
fn check_restore_time_members(body: &Value) -> Result<(), DynamoDbError> {
    let has_date = body.get("RestoreDateTime").is_some_and(|v| !v.is_null());
    let use_latest = body
        .get("UseLatestRestorableTime")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match (has_date, use_latest) {
        (true, true) => Err(DynamoDbError::ValidationException(
            TWO_RESTORE_TIMES.to_owned(),
        )),
        (false, false) => Err(DynamoDbError::ValidationException(
            NO_RESTORE_TIME.to_owned(),
        )),
        _ => Ok(()),
    }
}

/// Handle `DescribeContinuousBackups`.
///
/// Continuous backups report `ENABLED` (the service reports this
/// unconditionally) with `PointInTimeRecoveryStatus: DISABLED` and no
/// restorable-time window.
pub(crate) async fn handle_describe_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = table_name_member(&body, "TableName")?;
    check_table_name(table_name, super::table_name_bounds(&ctx.limits))?;
    require_table_exists(ctx, table_name).await?;
    serialize_output(
        &json!({ "ContinuousBackupsDescription": default_continuous_backups_description() }),
    )
}

/// Handle `UpdateContinuousBackups`.
///
/// Enabling point-in-time recovery is refused with
/// `ContinuousBackupsUnavailableException`, because no storage backend
/// implements it and reporting it as enabled would promise a restore that can
/// never happen. Disabling it returns the description
/// `DescribeContinuousBackups` reports. The specification is checked before
/// the table, as the service checks it.
pub(crate) async fn handle_update_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = table_name_member(&body, "TableName")?;
    check_table_name(table_name, super::table_name_bounds(&ctx.limits))?;
    let pitr_enabled = point_in_time_recovery_enabled(&body)?;
    require_table_exists(ctx, table_name).await?;
    if pitr_enabled {
        return Err(pitr_unsupported_error());
    }
    serialize_output(
        &json!({ "ContinuousBackupsDescription": default_continuous_backups_description() }),
    )
}

/// Handle `RestoreTableToPointInTime`.
///
/// An existing source table is refused with
/// `PointInTimeRecoveryUnavailableException`, the exception the service models
/// on this operation (`ContinuousBackupsUnavailableException` belongs to
/// `UpdateContinuousBackups`). A previous implementation faked a restore by
/// snapshotting the current table state and ignoring `RestoreDateTime`, which
/// violates tenet 1 (fidelity over features): a restore that silently returns
/// the wrong data is worse than a refusal the caller can act on.
pub(crate) async fn handle_restore_table_to_point_in_time(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // A missing target is reported before the source lookup (measured with a
    // missing source table and no TargetTableName).
    table_name_member(&body, "TargetTableName")?;
    let source_table_name =
        restore_source_table_name(&body, super::table_name_bounds(&ctx.limits))?;
    require_table_exists(ctx, &source_table_name).await?;
    check_restore_time_members(&body)?;
    Err(pitr_restore_unavailable_error(&source_table_name))
}

#[cfg(test)]
mod tests;
