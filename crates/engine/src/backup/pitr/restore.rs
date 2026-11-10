// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `RestoreTableToPointInTime` on a backend with an item-history store: the
//! request's checks in the service's order, the target's definition, and the
//! stream of the source's items at the restore time. Nothing calls them yet:
//! the handler answers `PointInTimeRecoveryUnavailableException`.
//!
//! Measured on the live service, two samples each:
//! - Order: the JSON types of the time members first, before the source
//!   member; then the model's member constraints (the overrides' shapes and
//!   enums, the target name's length); then the source table and the
//!   recovery check; then the time window; then the overrides against the
//!   source; then the target name.
//! - `RestoreDateTime` is epoch seconds rounded to the millisecond (.0005 is
//!   .001, .0004 is .000), saturated to the 64-bit millisecond range.
//!   `UseLatestRestorableTime` takes the strings "true", "TRUE", "yes" and
//!   others as true and "false", "no" and others as false.
//! - The window's floor is the reported EarliestRestorableDateTime and its
//!   ceiling the request's own time: restores to now - 2 s were accepted
//!   while the reported Latest was minutes behind, now + 1.5 s refused, and
//!   so was the floor less 1 ms. The refusal prints the requested time to
//!   the millisecond, the floor as reported, and as Latest the request's
//!   time less five minutes (never before the floor) to the nanosecond.
//! - `RestoreSummary.RestoreDateTime` is the requested time to the
//!   millisecond; with `UseLatestRestorableTime` it is the reported Latest.
//!   A restore at the floor holds the items written before the enable.
//! - The target name taken: `TableAlreadyExistsException` "Table already
//!   exists: <name>"; a restore target still being created:
//!   `TableInUseException` "Table is in the process of being created: <name>".

#![allow(dead_code)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    CreateTableInput, DescribeTableInput, Item, TableDescription, TableStatus,
};
use extenddb_storage::backup_definition::restore_input_from_manifest;
use extenddb_storage::error::StorageError;
use extenddb_storage::pitr::{PitrCaptureStore, PitrState};
use extenddb_storage::snapshot::source_table_schema;
use extenddb_storage::{RestoreTableOverrides, TableEngine};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;

use super::super::overrides::IndexOverrides;
use extenddb_core::types::BillingMode;

use super::super::{
    parse_restore_table_overrides, storage_err_to_dynamo, validate_override_for_source,
};
use super::capture::{LATEST_RESTORABLE_LAG, restorable_window, window_floor};

/// Items read from the history per page while a restore loads its target.
pub(crate) const RECONSTRUCT_PAGE: u32 = 1_000;

/// The members checked before any table is read: the time members, the
/// overrides, and the target name's length and pattern.
pub(crate) struct RestoreMembers {
    pub time: TimeMembers,
    pub overrides: RestoreTableOverrides,
    pub index_overrides: IndexOverrides,
    pub target_table_name: String,
}

/// `RestoreDateTime` in epoch milliseconds and `UseLatestRestorableTime`, as
/// the service's deserializer reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimeMembers {
    pub restore_date_time_ms: Option<i64>,
    pub use_latest: bool,
}

/// The time a restore asks for, its time members' exclusivity checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Requested {
    Latest,
    AtMillis(i64),
}

/// The time a restore reconstructs at, and the one its `RestoreSummary`
/// reports, epoch seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RestoreTime {
    pub at: SystemTime,
    pub restore_date_time: f64,
}

fn serialization(message: &str) -> DynamoDbError {
    DynamoDbError::SerializationException(message.to_owned())
}

/// `RestoreDateTime`, as the service reads it (measured).
fn restore_date_time_ms(member: Option<&Value>) -> Result<Option<i64>, DynamoDbError> {
    match member {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => Ok(Some(match n.as_i64() {
            Some(secs) => secs.saturating_mul(1_000),
            // `as` saturates at the bounds of i64.
            #[allow(clippy::cast_possible_truncation)]
            None => n
                .as_f64()
                .map_or(i64::MAX, |f| (f * 1_000.0).round() as i64),
        })),
        Some(Value::String(_)) => Err(serialization("STRING_VALUE cannot be converted to Date")),
        Some(Value::Bool(true)) => Err(serialization("TRUE_VALUE cannot be converted to Date")),
        Some(Value::Bool(false)) => Err(serialization("FALSE_VALUE cannot be converted to Date")),
        Some(Value::Array(_)) => Err(serialization("")),
        Some(Value::Object(_)) => Err(serialization(
            "Start of structure or map found where not expected",
        )),
    }
}

/// `UseLatestRestorableTime`, as the service reads it (measured).
fn use_latest(member: Option<&Value>) -> Result<bool, DynamoDbError> {
    match member {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) => match s.as_str() {
            "true" | "True" | "TRUE" | "yes" | "y" | "t" | "1" => Ok(true),
            "false" | "False" | "FALSE" | "no" | "n" | "f" | "0" => Ok(false),
            _ => Err(serialization("Unexpected token received from parser")),
        },
        Some(Value::Number(n)) if n.is_f64() => Err(serialization(
            "DECIMAL_VALUE cannot be converted to Boolean",
        )),
        Some(Value::Number(_)) => Err(serialization("NUMBER_VALUE cannot be converted to Boolean")),
        Some(Value::Array(_)) => Err(serialization("")),
        Some(Value::Object(_)) => Err(serialization(
            "Start of structure or map found where not expected",
        )),
    }
}

/// The time members' JSON types, before every other member.
pub(crate) fn time_members(body: &Value) -> Result<TimeMembers, DynamoDbError> {
    Ok(TimeMembers {
        restore_date_time_ms: restore_date_time_ms(body.get("RestoreDateTime"))?,
        use_latest: use_latest(body.get("UseLatestRestorableTime"))?,
    })
}

impl TimeMembers {
    /// Exactly one of the two, with the service's texts (measured).
    pub(crate) fn requested(self) -> Result<Requested, DynamoDbError> {
        match (self.restore_date_time_ms, self.use_latest) {
            (Some(_), true) => Err(DynamoDbError::ValidationException(
                super::TWO_RESTORE_TIMES.to_owned(),
            )),
            (None, false) => Err(DynamoDbError::ValidationException(
                super::NO_RESTORE_TIME.to_owned(),
            )),
            (Some(ms), false) => Ok(Requested::AtMillis(ms)),
            (None, true) => Ok(Requested::Latest),
        }
    }
}

/// Step (a): the time members' types, then the model's member constraints,
/// before the source is read.
pub(crate) fn restore_members(
    body: &Value,
    bounds: (usize, usize),
) -> Result<RestoreMembers, DynamoDbError> {
    let time = time_members(body)?;
    let overrides = parse_restore_table_overrides(body)?;
    if let Some(throughput) = &overrides.provisioned_throughput {
        extenddb_core::validation::validate_provisioned_throughput(&CreateTableInput {
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(throughput.clone()),
            ..Default::default()
        })?;
    }
    let index_overrides = IndexOverrides::parse(body)?;
    let target = body
        .get("TargetTableName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (min, max) = bounds;
    let length = target.chars().count();
    if length < min || length > max {
        return Err(DynamoDbError::ValidationException(format!(
            "TableName must be at least {min} characters long and at most {max} characters long"
        )));
    }
    super::super::validate_name_member(target, "targetTableName", bounds)?;
    Ok(RestoreMembers {
        time,
        overrides,
        index_overrides,
        target_table_name: target.to_owned(),
    })
}

/// A UTC instant as the service prints it in the window refusal:
/// `YYYY-MM-DDTHH:MM:SS` then the fraction in groups of three digits as
/// needed, then `Z`. A year past 9999 takes a `+` and a year before 0 a `-`
/// (measured: "+292278994-08-17T07:12:55.807Z", "+10000-01-01T00:00:00Z",
/// "0000-12-31T23:59:59Z", "-292275055-05-16T16:47:04.192Z").
fn utc_instant(epoch_secs: i64, nanos: u32) -> String {
    let days = epoch_secs.div_euclid(86_400);
    let secs = epoch_secs.rem_euclid(86_400);
    // Days since 1970-01-01 to a civil date (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let year = if year > 9_999 {
        format!("+{year}")
    } else if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("{year:04}")
    };
    let fraction = if nanos == 0 {
        String::new()
    } else if nanos.is_multiple_of(1_000_000) {
        format!(".{:03}", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!(".{:06}", nanos / 1_000)
    } else {
        format!(".{nanos:09}")
    };
    format!(
        "{year}-{month:02}-{day:02}T{:02}:{:02}:{:02}{fraction}Z",
        secs / 3_600,
        secs % 3_600 / 60,
        secs % 60
    )
}

fn utc_millis(ms: i64) -> String {
    let nanos = u32::try_from(ms.rem_euclid(1_000)).expect("below 1000") * 1_000_000;
    utc_instant(ms.div_euclid(1_000), nanos)
}

fn utc_duration(since_epoch: Duration) -> String {
    let secs = i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX);
    utc_instant(secs, since_epoch.subsec_nanos())
}

/// Step (c): the restore time against the window of `state` at `now`. The
/// reconstruct runs at the requested time, or at the enable instant for a
/// time inside the enable's second, which the window reports as its floor.
pub(crate) fn restore_time(
    requested: Requested,
    state: &PitrState,
    now: SystemTime,
    source_table_name: &str,
) -> Result<RestoreTime, DynamoDbError> {
    let floor = window_floor(state, now);
    let (_, latest) = restorable_window(state, now);
    let ms = match requested {
        // The reported Latest is whole milliseconds.
        #[allow(clippy::cast_possible_truncation)]
        Requested::Latest => (latest * 1_000.0).round() as i64,
        Requested::AtMillis(ms) => ms,
    };
    let now_since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let requested_ns = i128::from(ms) * 1_000_000;
    let as_ns = |d: Duration| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX);
    if requested_ns < as_ns(floor) || requested_ns > as_ns(now_since) {
        let latest_text = now_since.saturating_sub(LATEST_RESTORABLE_LAG).max(floor);
        return Err(DynamoDbError::InvalidRestoreTimeException(format!(
            "RestoreDateTime '{}' must be within EarliestRestorableDateTime '{}' and \
             LatestRestorableDateTime '{}' for table '{source_table_name}'",
            utc_millis(ms),
            utc_duration(floor),
            utc_duration(latest_text),
        )));
    }
    let since = Duration::from_millis(u64::try_from(ms).expect("at or after the floor"));
    Ok(RestoreTime {
        at: (UNIX_EPOCH + since).max(state.enabled_at),
        #[allow(clippy::cast_precision_loss)]
        restore_date_time: ms as f64 / 1_000.0,
    })
}

/// The service's text for `BillingModeOverride` PROVISIONED without a
/// `ProvisionedThroughputOverride`, checked after the time window (measured;
/// RestoreTableFromBackup answers this case earlier and with other text).
const PROVISIONED_NEEDS_THROUGHPUT: &str = "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to \
     PROVISIONED";

/// Step (d): the target's `CreateTableInput` from the source's description,
/// as a backup of it would restore (the source's keys, billing mode and
/// indexes; not TTL, streams, tags or recovery), with the overrides applied
/// and checked as `CreateTable` checks its input.
pub(crate) fn target_input(
    source: &TableDescription,
    members: &RestoreMembers,
    limits: &LimitsConfig,
) -> Result<CreateTableInput, DynamoDbError> {
    if members.overrides.billing_mode == Some(BillingMode::Provisioned)
        && members.overrides.provisioned_throughput.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            PROVISIONED_NEEDS_THROUGHPUT.to_owned(),
        ));
    }
    let schema = source_table_schema(source, None).map_err(storage_err_to_dynamo)?;
    validate_override_for_source(
        &members.overrides,
        extenddb_storage::backup_catalog::billing_mode_name(schema.billing_mode),
    )?;
    let mut input =
        restore_input_from_manifest(&schema, &members.target_table_name, &members.overrides);
    members
        .index_overrides
        .apply(&mut input, members.overrides.billing_mode)?;
    extenddb_core::validation::validate_create_table(&input, limits)?;
    Ok(input)
}

/// Step (e): the target name under the account.
pub(crate) async fn check_target_name<S: TableEngine + ?Sized>(
    storage: &S,
    account_id: &str,
    target: &str,
) -> Result<(), DynamoDbError> {
    let probe = storage
        .describe_table(
            account_id,
            DescribeTableInput {
                table_name: target.to_owned(),
            },
        )
        .await;
    match probe {
        Ok(desc)
            if desc.table_status == TableStatus::Creating && desc.restore_summary.is_some() =>
        {
            Err(DynamoDbError::TableInUseException(format!(
                "Table is in the process of being created: {target}"
            )))
        }
        Ok(_) => Err(DynamoDbError::TableAlreadyExistsException(format!(
            "Table already exists: {target}"
        ))),
        Err(StorageError::TableNotFound(_)) => Ok(()),
        Err(e) => Err(storage_err_to_dynamo(e)),
    }
}

/// The source's items at `at`, read from the history a page at a time.
pub(crate) fn reconstruct_stream<'a>(
    store: &'a dyn PitrCaptureStore,
    table_id: &'a str,
    at: SystemTime,
    page: u32,
) -> BoxStream<'a, Result<Item, StorageError>> {
    // State: the cursor of the next page, `None` once the last was read.
    stream::unfold(Some(None), move |cursor: Option<Option<i64>>| async move {
        let after = cursor?;
        match store.reconstruct_page(table_id, at, after, page).await {
            Ok(p) => {
                let next = p.next.map(Some);
                Some((
                    stream::iter(p.items.into_iter().map(Ok)).left_stream(),
                    next,
                ))
            }
            Err(e) => Some((stream::once(async { Err(e) }).right_stream(), None)),
        }
    })
    .flatten()
    .boxed()
}

#[cfg(test)]
#[path = "restore_target_tests.rs"]
mod target_tests;
#[cfg(test)]
#[path = "restore_tests.rs"]
mod tests;
