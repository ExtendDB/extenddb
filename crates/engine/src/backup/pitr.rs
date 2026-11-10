// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeContinuousBackups`, `UpdateContinuousBackups` and
//! `RestoreTableToPointInTime`.
//!
//! Without the `pitr` feature, or on a backend without an item-history store,
//! the engine answers the three operations itself and consults storage only
//! to look the table up. With both, DescribeContinuousBackups and
//! UpdateContinuousBackups read and write the store (`capture`), and
//! RestoreTableToPointInTime still answers as below: it refuses every
//! restore, including one from a table whose recovery is enabled.
//! The checks run in the service's order (measured on DynamoDB, two samples of
//! each request):
//!
//! - DescribeContinuousBackups: the `TableName` member and its constraints,
//!   then the table.
//! - UpdateContinuousBackups: the `TableName` member and its constraints,
//!   then `PointInTimeRecoverySpecification` and its
//!   `PointInTimeRecoveryEnabled`, then the table, then the enable request.
//!   With an item-history store, the JSON type of `RecoveryPeriodInDays`
//!   comes first of all, and its bounds are reported with a missing
//!   `PointInTimeRecoveryEnabled` in one message (`capture`).
//! - RestoreTableToPointInTime: the source member (`SourceTableName` or
//!   `SourceTableArn`) and `TargetTableName`, then the source ARN's kind or
//!   the `SourceTableName` constraints, then the source table, then the time
//!   members, then the recovery check. The target table is never looked up:
//!   the recovery check, which the service makes before any check of the
//!   target, refuses every restore.
//!
//! A table that does not exist is `TableNotFoundException` with the service's
//! text, `Table not found: <name>`, on all three. A table in any status
//! resolves, CREATING and DELETING included, except that an enable with an
//! item-history store refuses a CREATING table.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ContinuousBackupsDescription, DescribeTableInput, PointInTimeRecoveryDescription,
    TableDescription,
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
/// operation and returns it when recovery is not enabled on the source table,
/// with this message ("Point in time recovery is not enabled for table
/// '<name>'"). ExtendDB returns it for every restore, so with an item-history
/// store the message is also returned for a table whose recovery is enabled.
fn pitr_restore_unavailable_error(table_name: &str) -> DynamoDbError {
    DynamoDbError::PointInTimeRecoveryUnavailableException(format!(
        "Point in time recovery is not enabled for table '{table_name}'"
    ))
}

/// The continuous-backups description every table reports.
///
/// `ContinuousBackupsStatus` is always `ENABLED`, matching the service, which
/// reports `ENABLED` unconditionally. `PointInTimeRecoveryStatus` is
/// `DISABLED` and the restorable-time fields are omitted, as the service omits
/// them for a table whose recovery is disabled. The answer of every table
/// where no item-history store is available.
fn default_continuous_backups_description() -> ContinuousBackupsDescription {
    ContinuousBackupsDescription {
        continuous_backups_status: "ENABLED".to_owned(),
        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
            point_in_time_recovery_status: "DISABLED".to_owned(),
            earliest_restorable_date_time: None,
            latest_restorable_date_time: None,
            recovery_period_in_days: None,
            point_in_time_recovery_window_days: None,
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
) -> Result<TableDescription, DynamoDbError> {
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
        Ok(table) => Ok(table),
        Err(StorageError::TableNotFound(_)) => Err(DynamoDbError::TableNotFoundException(format!(
            "Table not found: {table_name}"
        ))),
        Err(other) => Err(storage_err_to_dynamo(other)),
    }
}

/// The model path of `PointInTimeRecoveryEnabled`.
const FLAG_PATH: &str = "pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled";

/// The `Value null ... Member must not be null` constraint text of a missing
/// `PointInTimeRecoveryEnabled`, without the count prefix.
#[cfg(feature = "pitr")]
fn flag_null_constraint() -> String {
    format!("Value null at '{FLAG_PATH}' failed to satisfy constraint: Member must not be null")
}

/// `PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled`: `None`
/// when the specification does not hold the flag, and the service's
/// missing-member refusal when there is no specification. The JSON strings
/// `"true"` and `"false"` are taken as the booleans (the service answers 200
/// for `"false"`, measured); any other value that is not a boolean is taken
/// as a missing flag.
fn recovery_flag(body: &Value) -> Result<Option<bool>, DynamoDbError> {
    let spec = body
        .get("PointInTimeRecoverySpecification")
        .filter(|v| !v.is_null())
        .ok_or_else(|| member_null("pointInTimeRecoverySpecification"))?;
    Ok(match spec.get("PointInTimeRecoveryEnabled") {
        Some(Value::Bool(flag)) => Some(*flag),
        Some(Value::String(s)) if s == "true" => Some(true),
        Some(Value::String(s)) if s == "false" => Some(false),
        _ => None,
    })
}

/// [`recovery_flag`], with a missing flag refused with the service's text.
fn point_in_time_recovery_enabled(body: &Value) -> Result<bool, DynamoDbError> {
    recovery_flag(body)?.ok_or_else(|| member_null(FLAG_PATH))
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
/// unconditionally). Without an item-history store,
/// `PointInTimeRecoveryStatus` is `DISABLED` with no restorable-time window;
/// with one, the table's recovery state and window (`capture::describe`).
pub(crate) async fn handle_describe_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let table_name = table_name_member(&body, "TableName")?;
    check_table_name(table_name, super::table_name_bounds(&ctx.limits))?;
    let table = require_table_exists(ctx, table_name).await?;
    #[cfg(feature = "pitr")]
    if let Some(store) = ctx.storage.as_pitr() {
        let description = capture::describe(store, &table).await?;
        return serialize_output(&json!({ "ContinuousBackupsDescription": description }));
    }
    let _ = table;
    serialize_output(
        &json!({ "ContinuousBackupsDescription": default_continuous_backups_description() }),
    )
}

/// Handle `UpdateContinuousBackups`.
///
/// Where no item-history store is available, enabling point-in-time recovery
/// is refused with `ContinuousBackupsUnavailableException`: reporting it as
/// enabled would promise a restore that cannot happen. Disabling it returns
/// the description `DescribeContinuousBackups` reports. With a store, the
/// request enables or disables recovery (`capture::update`). The
/// specification is checked before the table, as the service checks it.
pub(crate) async fn handle_update_continuous_backups(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    #[cfg(feature = "pitr")]
    if let Some(store) = ctx.storage.as_pitr() {
        return update_with_history(store, &body, ctx).await;
    }
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

/// The JSON-type checks the service makes before every member constraint of
/// `operation`, `TableName`'s included: on `UpdateContinuousBackups` with an
/// item-history store, the type of `RecoveryPeriodInDays` (measured: a
/// TableName the pattern refuses with a string period is the period's
/// `SerializationException`). The server runs this before its table name
/// check (`authorize_request`); the handler makes the same check.
///
/// # Errors
///
/// The service's `SerializationException` for the member.
pub fn check_member_types(
    operation: &str,
    body: &Value,
    storage: &dyn extenddb_storage::StorageEngine,
) -> Result<(), DynamoDbError> {
    #[cfg(feature = "pitr")]
    if operation == "UpdateContinuousBackups" && storage.as_pitr().is_some() {
        capture::period_member(body)?;
    }
    let _ = (operation, body, storage);
    Ok(())
}

/// The service's answer to an enable of a CREATING table (measured: two
/// tables right after CreateTable, two requests each, and two
/// RestoreTableFromBackup targets polled every 4.5 s). The first seconds
/// of any new table are `TableNotFoundException` (to 9 s and 22 s on the
/// two targets); a restore target then answers
/// `ContinuousBackupsUnavailableException` "Backups are being enabled for
/// the table: <name>. Please retry later" for the rest of its load (to
/// 796 s on one target). ExtendDB answers each kind of table with the
/// answer of most of its CREATING time, and does not time the first
/// seconds.
#[cfg(feature = "pitr")]
fn enable_while_creating(table: &TableDescription) -> DynamoDbError {
    if table.restore_summary.is_some() {
        DynamoDbError::ContinuousBackupsUnavailableException(format!(
            "Backups are being enabled for the table: {}. Please retry later",
            table.table_name
        ))
    } else {
        DynamoDbError::TableNotFoundException(format!("Table not found: {}", table.table_name))
    }
}

/// `UpdateContinuousBackups` with an item-history store. Measured on the
/// live service, two samples each: the JSON type of `RecoveryPeriodInDays`
/// is checked before every other member, `TableName` included; the
/// specification's constraints are reported together; an enable of a
/// CREATING table is refused ([`enable_while_creating`]).
#[cfg(feature = "pitr")]
async fn update_with_history(
    store: &dyn extenddb_storage::pitr::PitrCaptureStore,
    body: &Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let days = capture::period_member(body)?;
    let table_name = table_name_member(body, "TableName")?;
    check_table_name(table_name, super::table_name_bounds(&ctx.limits))?;
    let flag = recovery_flag(body)?;
    capture::check_specification(days, flag.is_none().then(flag_null_constraint))?;
    let pitr_enabled = flag.ok_or_else(|| member_null(FLAG_PATH))?;
    let period = capture::recovery_period(days, pitr_enabled)?;
    let table = require_table_exists(ctx, table_name).await?;
    // An enable never runs on a table still being created: a restore
    // target's load writes no history.
    if pitr_enabled && table.table_status == extenddb_core::types::TableStatus::Creating {
        return Err(enable_while_creating(&table));
    }
    let description = capture::update(store, &table, pitr_enabled, period).await?;
    serialize_output(&json!({ "ContinuousBackupsDescription": description }))
}

/// Handle `RestoreTableToPointInTime`.
///
/// An existing source table is refused with
/// `PointInTimeRecoveryUnavailableException`, the exception the service models
/// on this operation (`ContinuousBackupsUnavailableException` belongs to
/// `UpdateContinuousBackups`), whether or not its recovery is enabled. A
/// previous implementation faked a restore by snapshotting the current table
/// state and ignoring `RestoreDateTime`, which violates tenet 1 (fidelity
/// over features): a restore that silently returns the wrong data is worse
/// than a refusal the caller can act on.
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

#[cfg(feature = "pitr")]
mod capture;

#[cfg(test)]
mod tests;
