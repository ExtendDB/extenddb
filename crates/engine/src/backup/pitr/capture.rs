// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Point-in-time recovery on a backend with an item-history store
//! (`extenddb_storage::pitr::PitrCaptureStore`): the enable, the disable and
//! the description. Measured on the live service, two samples each:
//!
//! - `RecoveryPeriodInDays` is read as the service's Integer member: a JSON
//!   number with a fraction is truncated toward zero (7.9 is 7), a number
//!   outside the 32-bit range is clamped to it, and any other JSON type is a
//!   `SerializationException` checked before every other member. The value
//!   is then 1 to 35, checked before the table, on an enable and on a
//!   disable, together with a missing `PointInTimeRecoveryEnabled`; a disable
//!   that gives one is refused. A fresh enable without one takes 35; an
//!   enable on an enabled table changes the period only when one is given,
//!   and keeps the window.
//! - A disable drops the history; a re-enable starts a new window.
//! - The description carries `RecoveryPeriodInDays` and an undocumented
//!   `PointInTimeRecoveryWindowDays` equal to it. Right after the enable,
//!   `EarliestRestorableDateTime` and `LatestRestorableDateTime` are the enable
//!   instant in whole seconds. `LatestRestorableDateTime` stays there for the
//!   first minutes and then runs about five minutes behind the request.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ContinuousBackupsDescription, PointInTimeRecoveryDescription, TableDescription, TableKeyInfo,
};
use extenddb_storage::pitr::{PitrCaptureStore, PitrState, retention_floor};
use serde_json::Value;

use super::super::storage_err_to_dynamo;

/// How far `LatestRestorableDateTime` runs behind the request, never before
/// the enable instant. Measured: still the enable instant 228 s after the
/// enable, and 299 s behind the request about 7 minutes after it.
pub(super) const LATEST_RESTORABLE_LAG: Duration = Duration::from_secs(300);

/// The model's bounds on `RecoveryPeriodInDays`.
const PERIOD_MIN: i32 = 1;
const PERIOD_MAX: i32 = 35;

const PERIOD_ON_DISABLE: &str =
    "Invalid Request: Cannot specify RecoveryPeriodInDays when disabling point-in-time recovery.";

fn period_constraint(value: i32, constraint: &str) -> String {
    format!(
        "Value '{value}' at 'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to \
         satisfy constraint: Member must have value {constraint}"
    )
}

/// `PointInTimeRecoverySpecification.RecoveryPeriodInDays` as the service's
/// Integer member reads it, before any other member is checked (measured on
/// the live service, two samples each): a fraction is
/// truncated toward zero, a value outside the 32-bit range is clamped to it,
/// and a string, a boolean, a list or an object is a `SerializationException`
/// with the deserializer's text (none for a list). `None` when the member is
/// absent or null.
pub(super) fn period_member(body: &Value) -> Result<Option<i32>, DynamoDbError> {
    let member = body
        .get("PointInTimeRecoverySpecification")
        .and_then(|spec| spec.get("RecoveryPeriodInDays"));
    let serialization = |m: &str| Err(DynamoDbError::SerializationException(m.to_owned()));
    match member {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => Ok(Some(match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i32::try_from(i).unwrap_or(if i < 0 { i32::MIN } else { i32::MAX }),
            (None, Some(_)) => i32::MAX,
            // A float: `as` truncates toward zero and saturates at the bounds.
            #[allow(clippy::cast_possible_truncation)]
            (None, None) => n.as_f64().map_or(0, |f| f as i32),
        })),
        Some(Value::String(_)) => serialization("STRING_VALUE cannot be converted to Integer"),
        Some(Value::Bool(true)) => serialization("TRUE_VALUE cannot be converted to Integer"),
        Some(Value::Bool(false)) => serialization("FALSE_VALUE cannot be converted to Integer"),
        Some(Value::Array(_)) => serialization(""),
        Some(Value::Object(_)) => {
            serialization("Start of structure or map found where not expected")
        }
    }
}

/// The specification's model constraints, reported together in the
/// service's order: the period's bounds, then `flag_null`, the text of a
/// missing `PointInTimeRecoveryEnabled` (measured: "2
/// validation errors detected: Value '0' at '...recoveryPeriodInDays' ...;
/// Value null at '...pointInTimeRecoveryEnabled' ...").
pub(super) fn check_specification(
    days: Option<i32>,
    flag_null: Option<String>,
) -> Result<(), DynamoDbError> {
    let bound = days.and_then(|d| {
        if d < PERIOD_MIN {
            Some(period_constraint(d, "greater than or equal to 1"))
        } else if d > PERIOD_MAX {
            Some(period_constraint(d, "less than or equal to 35"))
        } else {
            None
        }
    });
    let errors: Vec<String> = bound.into_iter().chain(flag_null).collect();
    match errors.len() {
        0 => Ok(()),
        1 => Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: {}",
            errors[0]
        ))),
        n => Err(DynamoDbError::ValidationException(format!(
            "{n} validation errors detected: {}",
            errors.join("; ")
        ))),
    }
}

/// The period an update applies, its bounds already checked: refused on a
/// disable, `None` when the request gives none.
pub(super) fn recovery_period(
    days: Option<i32>,
    enabling: bool,
) -> Result<Option<u8>, DynamoDbError> {
    let Some(days) = days else {
        return Ok(None);
    };
    if !enabling {
        return Err(DynamoDbError::ValidationException(
            PERIOD_ON_DISABLE.to_owned(),
        ));
    }
    u8::try_from(days).map(Some).map_err(|_| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: {}",
            period_constraint(days, "less than or equal to 35")
        ))
    })
}

fn key_info(table: &TableDescription) -> TableKeyInfo {
    TableKeyInfo {
        table_name: table.table_name.clone(),
        table_id: table.table_id.clone(),
        key_schema: table.key_schema.clone(),
        base_key_schema: table.key_schema.clone(),
        attribute_definitions: table.attribute_definitions.clone(),
        ..Default::default()
    }
}

fn epoch_seconds(t: SystemTime) -> Duration {
    t.duration_since(UNIX_EPOCH).unwrap_or_default()
}

/// The floor of the restorable window of `state` at `now`, from
/// [`retention_floor`]: the enable instant in whole seconds, as the service
/// reports it, or the later floor the retention sets.
pub(crate) fn window_floor(state: &PitrState, now: SystemTime) -> Duration {
    let floor = retention_floor(state, now);
    let since = epoch_seconds(floor);
    if floor == state.enabled_at {
        Duration::from_secs(since.as_secs())
    } else {
        since
    }
}

/// The restorable window of `state` at `now`: the floor ([`window_floor`]),
/// and a ceiling [`LATEST_RESTORABLE_LAG`] behind `now`, in milliseconds,
/// never before the floor.
pub(crate) fn restorable_window(state: &PitrState, now: SystemTime) -> (f64, f64) {
    let earliest = window_floor(state, now);
    let lagging = epoch_seconds(now).saturating_sub(LATEST_RESTORABLE_LAG);
    let latest = Duration::from_millis(u64::try_from(lagging.as_millis()).unwrap_or(u64::MAX));
    let latest = latest.max(earliest);
    (earliest.as_secs_f64(), latest.as_secs_f64())
}

fn description(state: Option<&PitrState>, now: SystemTime) -> ContinuousBackupsDescription {
    let pitr = match state {
        Some(state) => {
            let (earliest, latest) = restorable_window(state, now);
            PointInTimeRecoveryDescription {
                point_in_time_recovery_status: "ENABLED".to_owned(),
                earliest_restorable_date_time: Some(earliest),
                latest_restorable_date_time: Some(latest),
                recovery_period_in_days: Some(state.recovery_period_days),
                point_in_time_recovery_window_days: Some(state.recovery_period_days),
            }
        }
        None => PointInTimeRecoveryDescription {
            point_in_time_recovery_status: "DISABLED".to_owned(),
            earliest_restorable_date_time: None,
            latest_restorable_date_time: None,
            recovery_period_in_days: None,
            point_in_time_recovery_window_days: None,
        },
    };
    ContinuousBackupsDescription {
        continuous_backups_status: "ENABLED".to_owned(),
        point_in_time_recovery_description: Some(pitr),
    }
}

/// `DescribeContinuousBackups` for `table`.
pub(super) async fn describe(
    store: &dyn PitrCaptureStore,
    table: &TableDescription,
) -> Result<ContinuousBackupsDescription, DynamoDbError> {
    let state = store
        .pitr_state(&table.table_id)
        .await
        .map_err(storage_err_to_dynamo)?;
    Ok(description(state.as_ref(), SystemTime::now()))
}

/// A store error as the operation answers it: a table a DeleteTable removed
/// while the request ran is the service's missing-table text.
fn store_err(e: extenddb_storage::error::StorageError, table_name: &str) -> DynamoDbError {
    match e {
        extenddb_storage::error::StorageError::TableNotFound(_) => {
            DynamoDbError::TableNotFoundException(format!("Table not found: {table_name}"))
        }
        other => storage_err_to_dynamo(other),
    }
}

/// `UpdateContinuousBackups` for `table`, its members already checked.
pub(super) async fn update(
    store: &dyn PitrCaptureStore,
    table: &TableDescription,
    enabling: bool,
    period: Option<u8>,
) -> Result<ContinuousBackupsDescription, DynamoDbError> {
    let info = key_info(table);
    let now = SystemTime::now();
    if enabling {
        let state = store
            .enable_pitr(&info, period, now)
            .await
            .map_err(|e| store_err(e, &table.table_name))?;
        Ok(description(Some(&state), now))
    } else {
        store
            .disable_pitr(&info)
            .await
            .map_err(|e| store_err(e, &table.table_name))?;
        Ok(description(None, now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state(enabled: u64, days: u8) -> PitrState {
        PitrState::new(days, UNIX_EPOCH + Duration::from_micros(enabled))
    }

    #[test]
    fn the_window_is_the_enable_second_until_the_lag_passes() {
        let s = state(1_000_000_400_250_000, 35);
        let now = |secs: u64| UNIX_EPOCH + Duration::from_secs(1_000_000_400 + secs);
        assert_eq!(
            restorable_window(&s, now(0)),
            (1_000_000_400.0, 1_000_000_400.0)
        );
        assert_eq!(
            restorable_window(&s, now(228)),
            (1_000_000_400.0, 1_000_000_400.0)
        );
        assert_eq!(
            restorable_window(&s, now(420)),
            (1_000_000_400.0, 1_000_000_520.0)
        );
    }

    #[test]
    fn the_floor_moves_with_the_retention() {
        let s = state(1_000_000_000_000_000, 7);
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000_000 + 8 * 86_400);
        assert_eq!(
            restorable_window(&s, now).0,
            (1_000_000_000 + 86_400) as f64
        );
    }

    /// After a prune, the window starts where the history does, even when
    /// a raised period would reach further back.
    #[test]
    fn the_floor_is_never_before_the_pruned_history() {
        let mut s = state(1_000_000_000_000_000, 35);
        s.pruned_to = UNIX_EPOCH + Duration::from_secs(1_000_000_000 + 9 * 86_400);
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000_000 + 10 * 86_400);
        assert_eq!(
            restorable_window(&s, now).0,
            (1_000_000_000 + 9 * 86_400) as f64
        );
    }

    /// A table a DeleteTable removed while the request ran answers as a
    /// missing table does.
    #[test]
    fn a_table_gone_from_the_store_is_the_missing_table_text() {
        let e = store_err(
            extenddb_storage::error::StorageError::TableNotFound("x".to_owned()),
            "t1",
        );
        match e {
            DynamoDbError::TableNotFoundException(m) => assert_eq!(m, "Table not found: t1"),
            other => panic!("{other:?}"),
        }
    }

    /// The answer to a specification, through the three checks in the
    /// handler's order: the member's type, the constraints with the flag,
    /// the disable.
    fn period(spec: Value) -> Result<Option<u8>, DynamoDbError> {
        let body = json!({"PointInTimeRecoverySpecification": spec});
        let days = period_member(&body)?;
        let flag = spec
            .get("PointInTimeRecoveryEnabled")
            .and_then(Value::as_bool);
        let flag_null = flag.is_none().then(|| {
            "Value null at 'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' \
             failed to satisfy constraint: Member must not be null"
                .to_owned()
        });
        check_specification(days, flag_null)?;
        recovery_period(days, flag.unwrap_or(false))
    }

    fn validation(r: Result<Option<u8>, DynamoDbError>) -> String {
        match r {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("{other:?}"),
        }
    }

    fn serialization(r: Result<Option<u8>, DynamoDbError>) -> String {
        match r {
            Err(DynamoDbError::SerializationException(m)) => m,
            other => panic!("{other:?}"),
        }
    }

    const AT: &str = "at 'pointInTimeRecoverySpecification.recoveryPeriodInDays' failed to \
                      satisfy constraint: Member must have value";
    const FLAG_NULL: &str = "Value null at \
        'pointInTimeRecoverySpecification.pointInTimeRecoveryEnabled' failed to satisfy \
        constraint: Member must not be null";

    fn too_low(v: &str) -> String {
        format!("1 validation error detected: Value '{v}' {AT} greater than or equal to 1")
    }

    fn too_high(v: &str) -> String {
        format!("1 validation error detected: Value '{v}' {AT} less than or equal to 35")
    }

    fn on(v: Value) -> Value {
        json!({"PointInTimeRecoveryEnabled": true, "RecoveryPeriodInDays": v})
    }

    fn off(v: Value) -> Value {
        json!({"PointInTimeRecoveryEnabled": false, "RecoveryPeriodInDays": v})
    }

    /// Measured on the live service, two samples each.
    #[test]
    fn the_period_bounds_are_the_services() {
        assert_eq!(validation(period(on(json!(0)))), too_low("0"));
        assert_eq!(validation(period(on(json!(-1)))), too_low("-1"));
        assert_eq!(validation(period(on(json!(36)))), too_high("36"));
        assert_eq!(validation(period(off(json!(0)))), too_low("0"));
        assert_eq!(validation(period(off(json!(36)))), too_high("36"));
        assert_eq!(validation(period(off(json!(7)))), PERIOD_ON_DISABLE);
        assert_eq!(period(on(json!(7))).ok(), Some(Some(7)));
        assert_eq!(period(on(json!(1))).ok(), Some(Some(1)));
        assert_eq!(period(on(json!(35))).ok(), Some(Some(35)));
        assert_eq!(period(on(Value::Null)).ok(), Some(None));
        assert_eq!(
            period(json!({"PointInTimeRecoveryEnabled": true})).ok(),
            Some(None)
        );
    }

    /// A fraction is truncated toward zero, then checked (measured): 7.9 is
    /// stored as 7; 7.5, 7.0 and 10.0 pass the bounds; 7.5 on a disable is
    /// the disable refusal; 0.5, 0.9, 1e-5 and -0.5 are '0'; 1.9 is 1; 35.9
    /// passes; 36.5 is '36'; -1.5 is '-1'.
    #[test]
    fn a_fraction_is_truncated_toward_zero() {
        assert_eq!(period(on(json!(7.9))).ok(), Some(Some(7)));
        assert_eq!(period(on(json!(7.5))).ok(), Some(Some(7)));
        assert_eq!(period(on(json!(7.0))).ok(), Some(Some(7)));
        assert_eq!(period(on(json!(10.0))).ok(), Some(Some(10)));
        assert_eq!(period(on(json!(1.9))).ok(), Some(Some(1)));
        assert_eq!(period(on(json!(35.9))).ok(), Some(Some(35)));
        assert_eq!(period(on(json!(3.5e1))).ok(), Some(Some(35)));
        assert_eq!(validation(period(off(json!(7.5)))), PERIOD_ON_DISABLE);
        for v in [json!(0.5), json!(0.9), json!(1e-5), json!(-0.5)] {
            assert_eq!(validation(period(on(v))), too_low("0"));
        }
        assert_eq!(validation(period(on(json!(-1.5)))), too_low("-1"));
        assert_eq!(validation(period(on(json!(36.5)))), too_high("36"));
        assert_eq!(validation(period(off(json!(36.5)))), too_high("36"));
    }

    /// Outside the 32-bit range the value is clamped to it (measured): 1e20,
    /// 2147483648 and 2^63 are '2147483647'; -2147483649 is '-2147483648';
    /// and 2147483647, 2147483647.5, -2147483648 and -1e20.
    #[test]
    fn a_value_outside_the_integer_range_is_clamped() {
        for v in [
            json!(2_147_483_647_i64),
            json!(2_147_483_647.5),
            json!(2_147_483_648_i64),
            json!(9_223_372_036_854_775_808_u64),
            json!(1e20),
        ] {
            assert_eq!(validation(period(on(v))), too_high("2147483647"));
        }
        for v in [
            json!(-2_147_483_648_i64),
            json!(-2_147_483_649_i64),
            json!(-1e20),
        ] {
            assert_eq!(validation(period(on(v))), too_low("-2147483648"));
        }
    }

    /// Every other JSON type is a SerializationException with the
    /// deserializer's text (measured): "7", "abc", "", "7.5", true, [] and {},
    /// "7" on a disable, false and "35".
    #[test]
    fn another_json_type_is_a_serialization_error() {
        for v in ["7", "abc", "", "7.5", "35"] {
            assert_eq!(
                serialization(period(on(json!(v)))),
                "STRING_VALUE cannot be converted to Integer"
            );
        }
        assert_eq!(
            serialization(period(off(json!("7")))),
            "STRING_VALUE cannot be converted to Integer"
        );
        assert_eq!(
            serialization(period(on(json!(true)))),
            "TRUE_VALUE cannot be converted to Integer"
        );
        assert_eq!(
            serialization(period(on(json!(false)))),
            "FALSE_VALUE cannot be converted to Integer"
        );
        assert_eq!(serialization(period(on(json!([])))), "");
        assert_eq!(
            serialization(period(on(json!({})))),
            "Start of structure or map found where not expected"
        );
    }

    /// A bad period and a missing flag are one message, the period first:
    /// 0, 36, 0.5, 1e20, -2147483649, and 0 with the flag null (measured).
    /// A valid period with a missing flag names only the flag.
    #[test]
    fn a_bad_period_and_a_missing_flag_are_reported_together() {
        let two = |v: &str, c: &str| {
            format!("2 validation errors detected: Value '{v}' {AT} {c}; {FLAG_NULL}")
        };
        let spec = |v: Value| json!({"RecoveryPeriodInDays": v});
        assert_eq!(
            validation(period(spec(json!(0)))),
            two("0", "greater than or equal to 1")
        );
        assert_eq!(
            validation(period(
                json!({"PointInTimeRecoveryEnabled": null, "RecoveryPeriodInDays": 0})
            )),
            two("0", "greater than or equal to 1")
        );
        assert_eq!(
            validation(period(spec(json!(36)))),
            two("36", "less than or equal to 35")
        );
        assert_eq!(
            validation(period(spec(json!(0.5)))),
            two("0", "greater than or equal to 1")
        );
        assert_eq!(
            validation(period(spec(json!(1e20)))),
            two("2147483647", "less than or equal to 35")
        );
        assert_eq!(
            validation(period(spec(json!(-2_147_483_649_i64)))),
            two("-2147483648", "greater than or equal to 1")
        );
        assert_eq!(
            validation(period(spec(json!(7)))),
            format!("1 validation error detected: {FLAG_NULL}")
        );
    }

    #[test]
    fn the_description_carries_the_window_days_member() {
        let d = serde_json::to_value(description(
            Some(&state(1_000_000_000_000_000, 20)),
            UNIX_EPOCH + Duration::from_secs(1_000_000_001),
        ))
        .expect("json");
        assert_eq!(
            d,
            json!({"ContinuousBackupsStatus": "ENABLED", "PointInTimeRecoveryDescription": {
                "PointInTimeRecoveryStatus": "ENABLED",
                "EarliestRestorableDateTime": 1_000_000_000.0,
                "LatestRestorableDateTime": 1_000_000_000.0,
                "RecoveryPeriodInDays": 20,
                "PointInTimeRecoveryWindowDays": 20}})
        );
        assert_eq!(
            serde_json::to_value(description(None, UNIX_EPOCH)).expect("json"),
            json!({"ContinuousBackupsStatus": "ENABLED",
                "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "DISABLED"}})
        );
    }
}
